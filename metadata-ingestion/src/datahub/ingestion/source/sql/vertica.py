import logging
import traceback
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Set, Tuple, Union

import pydantic
from pydantic.class_validators import validator
from vertica_sqlalchemy_dialect.base import VerticaInspector

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import (
    dataset_urn_to_key,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import add_owner_to_entity_wu
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source_helpers import auto_workunit_reporter
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.sql.sql_common import (
    SQLAlchemySource,
    SQLSourceReport,
    SqlWorkUnit,
    get_schema_metadata,
)
from datahub.ingestion.source.sql.sql_config import (
    BasicSQLAlchemyConfig,
    SQLAlchemyConfig,
)
from datahub.ingestion.source.sql.sql_utils import get_domain_wu
from datahub.metadata.com.linkedin.pegasus2avro.common import StatusClass
from datahub.metadata.com.linkedin.pegasus2avro.dataset import UpstreamLineage
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    ForeignKeyConstraintClass,
    SubTypesClass,
    UpstreamClass,
    _Aspect,
)
from datahub.utilities import config_clean

if TYPE_CHECKING:
    from datahub.ingestion.source.ge_data_profiler import GEProfilerRequest
MISSING_COLUMN_INFO = "missing column information"
logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class VerticaSourceReport(SQLSourceReport):
    projection_scanned: int = 0
    models_scanned: int = 0

    def report_entity_scanned(self, name: str, ent_type: str = "table") -> None:
        """
        Entity could be a projection or a model.
        """

        if ent_type == "projection":
            self.projection_scanned += 1
        elif ent_type == "models":
            self.models_scanned += 1
        else:
            super().report_entity_scanned(name, ent_type)


# Extended BasicSQLAlchemyConfig to config for projections,models  metadata.
class VerticaConfig(BasicSQLAlchemyConfig):
    models_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for ml models to filter in ingestion. ",
    )
    include_projections: Optional[bool] = pydantic.Field(
        default=True, description="Whether projections should be ingested."
    )
    include_models: Optional[bool] = pydantic.Field(
        default=True, description="Whether Models should be ingested."
    )

    include_view_lineage: Optional[bool] = pydantic.Field(
        default=True,
        description="If the source supports it, include view lineage to the underlying storage location.",
    )

    include_projection_lineage: Optional[bool] = pydantic.Field(
        default=True,
        description="If the source supports it, include view lineage to the underlying storage location.",
    )

    # defaults
    scheme: str = pydantic.Field(default="vertica+vertica_python")

    @validator("host_port")
    def clean_host_port(cls, v):
        return config_clean.remove_protocol(v)


@platform_name("Vertica")
@config_class(VerticaConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default, can be disabled via configuration `include_view_lineage` and `include_projection_lineage`",
)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Optionally enabled via `stateful_ingestion.remove_stale_metadata`",
    supported=True,
)
class VerticaSource(SQLAlchemySource):
    def __init__(self, config: VerticaConfig, ctx: PipelineContext):
        # self.platform = platform
        super(VerticaSource, self).__init__(config, ctx, "vertica")
        self.report: SQLSourceReport = VerticaSourceReport()
        self.config: VerticaConfig = config

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "VerticaSource":
        config = VerticaConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        return auto_workunit_reporter(self.report, self.get_workunits_internal())

    def get_workunits_internal(self) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        sql_config = self.config
        if logger.isEnabledFor(logging.DEBUG):
            # If debug logging is enabled, we also want to echo each SQL query issued.
            sql_config.options.setdefault("echo", False)

        # Extra default SQLAlchemy option for better connection pooling and threading.
        # https://docs.sqlalchemy.org/en/14/core/pooling.html#sqlalchemy.pool.QueuePool.params.max_overflow
        if sql_config.profiling.enabled:
            sql_config.options.setdefault(
                "max_overflow", sql_config.profiling.max_workers
            )

        for inspector in self.get_inspectors():
            profiler = None
            profile_requests: List["GEProfilerRequest"] = []
            if sql_config.profiling.enabled:
                profiler = self.get_profiler_instance(inspector)

            db_name = self.get_db_name(inspector)
            yield from self.gen_database_containers(
                database=db_name,
                extra_properties=self.get_database_properties(
                    inspector=inspector, database=db_name
                ),
            )

            for schema in self.get_allowed_schemas(inspector, db_name):
                self.add_information_for_schema(inspector, schema)

                yield from self.gen_schema_containers(
                    schema=schema,
                    database=db_name,
                    extra_properties=self.get_schema_properties(
                        inspector=inspector, schema=schema, database=db_name
                    ),
                )

                if sql_config.include_tables:
                    yield from self.loop_tables(inspector, schema, sql_config)

                if sql_config.include_views:
                    yield from self.loop_views(inspector, schema, sql_config)
                if sql_config.include_projections:
                    yield from self.loop_projections(inspector, schema, sql_config)
                if sql_config.include_models:
                    yield from self.loop_models(inspector, schema, sql_config)

                if profiler:
                    profile_requests += list(
                        self.loop_profiler_requests(inspector, schema, sql_config)
                    )

            if profiler and profile_requests:
                yield from self.loop_profiler(
                    profile_requests, profiler, platform=self.platform
                )

    def get_database_properties(
        self, inspector: VerticaInspector, database: str
    ) -> Optional[Dict[str, str]]:
        try:
            custom_properties = inspector._get_database_properties(database)
            return custom_properties

        except Exception as ex:
            self.report.report_failure(
                f"{database}", f"unable to get extra_properties : {ex}"
            )
        return None

    def get_schema_properties(
        self, inspector: VerticaInspector, database: str, schema: str
    ) -> Optional[Dict[str, str]]:
        try:
            custom_properties = inspector._get_schema_properties(schema)
            return custom_properties
        except Exception as ex:
            self.report.report_failure(
                f"{database}.{schema}", f"unable to get extra_properties : {ex}"
            )
        return None

    def loop_tables(  # noqa: C901
        self,
        inspector: VerticaInspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        tables_seen: Set[str] = set()
        try:
            tables = inspector.get_table_names(schema)
            # created new function get_all_columns in vertica Dialect as the existing get_columns of SQLAlchemy VerticaInspector class is being used for profiling
            # And query in get_all_columns is modified to run at schema level.
            columns = inspector.get_all_columns(schema)

            primary_key = inspector.get_pk_constraint(schema)

            description, properties, location_urn = self.get_table_properties(
                inspector, schema, tables
            )

            # called get_table_owner function from vertica dialect , it returns a list of all owners of all table in the current schema
            table_owner = inspector.get_table_owner(schema)

            # loops on each table in the schema
            for table_name in tables:
                finalcolumns = []
                # loops through columns in the schema and creates all columns on current table
                for column in columns:
                    if column["tablename"] == table_name.lower():
                        finalcolumns.append(column)

                final_primary_key: dict = {}
                # loops through primary_key in the schema and saves the pk of current table
                for primary_key_column in primary_key:

                    if (
                        isinstance(primary_key_column, dict)
                        and primary_key_column.get("tablename", "").lower()
                        == table_name.lower()
                    ):
                        final_primary_key = primary_key_column

                table_properties: Dict[str, str] = {}
                # loops through properties  in the schema and saves the properties of current table
                for data in properties:
                    if (
                        isinstance(data, dict)
                        and "table_name" in data
                        and data["table_name"] == table_name.lower()
                    ):
                        if "create_time" in data:
                            table_properties["create_time"] = data["create_time"]
                        if "table_size" in data:
                            table_properties["table_size"] = data["table_size"]

                owner_name = None
                # loops through all owners in the schema and saved the value of current table owner
                for owner in table_owner:
                    if owner[0].lower() == table_name.lower():
                        owner_name = owner[1]

                dataset_name = self.get_identifier(
                    schema=schema, entity=table_name, inspector=inspector
                )

                if dataset_name not in tables_seen:
                    tables_seen.add(dataset_name)
                else:
                    logger.debug(f"{dataset_name} has already been seen, skipping...")
                    continue
                self.report.report_entity_scanned(dataset_name, ent_type="table")
                if not sql_config.table_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue
                dataset_urn = make_dataset_urn_with_platform_instance(
                    self.platform,
                    dataset_name,
                    self.config.platform_instance,
                    self.config.env,
                )

                yield from add_owner_to_entity_wu(
                    entity_type="dataset",
                    entity_urn=dataset_urn,
                    owner_urn=f"urn:li:corpuser:{owner_name}",
                )
                dataset_snapshot = DatasetSnapshot(
                    urn=dataset_urn,
                    aspects=[StatusClass(removed=False)],
                )

                dataset_properties = DatasetPropertiesClass(
                    name=table_name,
                    description=description,
                    customProperties=table_properties,
                )

                dataset_snapshot.aspects.append(dataset_properties)

                pk_constraints: dict = final_primary_key
                extra_tags: Optional[Dict[str, List[str]]] = None
                foreign_keys: list = []

                schema_fields = self.get_schema_fields(
                    dataset_name, finalcolumns, pk_constraints, tags=extra_tags
                )

                schema_metadata = get_schema_metadata(
                    self.report,
                    dataset_name,
                    self.platform,
                    finalcolumns,
                    pk_constraints,
                    foreign_keys,
                    schema_fields,
                )
                dataset_snapshot.aspects.append(schema_metadata)

                db_name = self.get_db_name(inspector)
                yield from self.add_table_to_schema_container(
                    dataset_urn=dataset_urn, db_name=db_name, schema=schema
                )
                mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)

                wu = SqlWorkUnit(id=dataset_name, mce=mce)
                self.report.report_workunit(wu)
                yield wu

                dpi_aspect = self.get_dataplatform_instance_aspect(
                    dataset_urn=dataset_urn
                )
                if dpi_aspect:
                    yield dpi_aspect
                subtypes_aspect = MetadataWorkUnit(
                    id=f"{dataset_name}-subtypes",
                    mcp=MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn,
                        aspect=SubTypesClass(typeNames=[DatasetSubTypes.TABLE]),
                    ),
                )
                self.report.report_workunit(subtypes_aspect)
                yield subtypes_aspect
                if self.config.domain:
                    assert self.domain_registry

                    yield from get_domain_wu(
                        dataset_name=dataset_name,
                        entity_urn=dataset_urn,
                        domain_config=sql_config.domain,
                        domain_registry=self.domain_registry,
                    )

        except Exception as e:
            print(traceback.format_exc())
            self.report.report_failure(f"{schema}", f"Tables error: {e}")

    def loop_views(  # noqa: C901
        self, inspector: VerticaInspector, schema: str, sql_config: SQLAlchemyConfig
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        views_seen: Set[str] = set()

        try:
            views = inspector.get_view_names(schema)

            # created new function get_all_view_columns in vertica Dialect as the existing get_columns of SQLAlchemy VerticaInspector class is being used for profiling
            # And query in get_all_view_columns is modified to run at schema level.
            columns = inspector.get_all_view_columns(schema)

            # called get_view_properties function from dialect , it returns a list description and properties of all view in the schema
            description, properties, location_urn = self.get_view_properties(
                inspector, schema, views
            )

            # called get_view_owner function from dialect , it returns a list of all owner of all view in the schema
            view_owner = inspector.get_view_owner(schema)

            # started a loop on each view in the schema
            for view_name in views:
                finalcolumns = []
                # loops through columns in the schema and creates all columns on current view
                for column in columns:
                    if column["tablename"].lower() == view_name.lower():
                        finalcolumns.append(column)

                view_properties = {}
                # loops through properties  in the schema and saves the properties of current views
                for data in properties:
                    if (
                        isinstance(data, dict)
                        and data.get("table_name", "").lower() == view_name.lower()
                    ):
                        view_properties["create_time"] = data.get("create_time", "")

                owner_name = None
                # loops through all views in the schema and returns the owner name of current view
                for owner in view_owner:
                    if owner[0].lower() == view_name.lower():
                        owner_name = owner[1]

                try:
                    view_definition = inspector.get_view_definition(view_name, schema)
                    if view_definition is None:
                        view_definition = ""
                    else:
                        # Some dialects return a TextClause instead of a raw string,
                        # so we need to convert them to a string.
                        view_definition = str(view_definition)

                except NotImplementedError:
                    view_definition = ""
                view_properties["view_definition"] = view_definition
                view_properties["is_view"] = "True"

                dataset_name = self.get_identifier(
                    schema=schema, entity=view_name, inspector=inspector
                )

                if dataset_name not in views_seen:
                    views_seen.add(dataset_name)
                else:
                    logger.debug(f"{dataset_name} has already been seen, skipping...")
                    continue

                self.report.report_entity_scanned(dataset_name, ent_type="view")

                if not sql_config.table_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue

                dataset_urn = make_dataset_urn_with_platform_instance(
                    self.platform,
                    dataset_name,
                    self.config.platform_instance,
                    self.config.env,
                )

                yield from add_owner_to_entity_wu(
                    entity_type="dataset",
                    entity_urn=dataset_urn,
                    owner_urn=f"urn:li:corpuser:{owner_name}",
                )

                dataset_snapshot = DatasetSnapshot(
                    urn=dataset_urn,
                    aspects=[StatusClass(removed=False)],
                )

                dataset_properties = DatasetPropertiesClass(
                    name=view_name,
                    description=description,
                    customProperties=view_properties,
                )

                dataset_snapshot.aspects.append(dataset_properties)
                pk_constraints: dict = {}
                extra_tags: Optional[Dict[str, List[str]]] = None
                foreign_keys: Optional[List[ForeignKeyConstraintClass]] = None

                schema_fields = self.get_schema_fields(
                    dataset_name, finalcolumns, pk_constraints, tags=extra_tags
                )

                schema_metadata = get_schema_metadata(
                    self.report,
                    dataset_name,
                    self.platform,
                    finalcolumns,
                    pk_constraints,
                    foreign_keys,
                    schema_fields,
                )
                dataset_snapshot.aspects.append(schema_metadata)

                db_name = self.get_db_name(inspector)
                yield from self.add_table_to_schema_container(
                    dataset_urn=dataset_urn, db_name=db_name, schema=schema
                )
                mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)

                wu = SqlWorkUnit(id=dataset_name, mce=mce)
                self.report.report_workunit(wu)
                yield wu

                dpi_aspect = self.get_dataplatform_instance_aspect(
                    dataset_urn=dataset_urn
                )
                if dpi_aspect:
                    yield dpi_aspect
                subtypes_aspect = MetadataWorkUnit(
                    id=f"{dataset_name}-subtypes",
                    mcp=MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn,
                        aspect=SubTypesClass(typeNames=[DatasetSubTypes.VIEW]),
                    ),
                )
                self.report.report_workunit(subtypes_aspect)
                yield subtypes_aspect
                if self.config.domain:
                    assert self.domain_registry
                    yield from get_domain_wu(
                        dataset_name=dataset_name,
                        entity_urn=dataset_urn,
                        domain_config=sql_config.domain,
                        domain_registry=self.domain_registry,
                    )

                if self.config.include_view_lineage:
                    try:
                        dataset_urn = make_dataset_urn_with_platform_instance(
                            self.platform,
                            dataset_name,
                            self.config.platform_instance,
                            self.config.env,
                        )

                        dataset_snapshot = DatasetSnapshot(
                            urn=dataset_urn, aspects=[StatusClass(removed=False)]
                        )

                        lineage_info = self._get_upstream_lineage_info(
                            dataset_urn, schema, inspector
                        )

                        if lineage_info is not None:
                            upstream_lineage = lineage_info

                            lineage_mcpw = MetadataChangeProposalWrapper(
                                entityType="dataset",
                                changeType=ChangeTypeClass.UPSERT,
                                entityUrn=dataset_snapshot.urn,
                                aspectName="upstreamLineage",
                                aspect=upstream_lineage,
                            )

                            lineage_wu = MetadataWorkUnit(
                                id=f"{self.platform}-{lineage_mcpw.entityUrn}-{lineage_mcpw.aspectName}",
                                mcp=lineage_mcpw,
                            )

                            self.report.report_workunit(lineage_wu)
                            yield lineage_wu

                    except Exception as e:
                        logger.warning(
                            f"Unable to get lineage of view {view_name} due to an exception.\n {traceback.format_exc()}"
                        )
                        self.report.report_warning(
                            f"{view_name}", f"Ingestion error: {e}"
                        )

        except Exception as e:
            print(traceback.format_exc())
            self.report.report_failure(f"{schema}", f"Views error: {e}")

    def get_view_properties(
        self, inspector: VerticaInspector, schema: str, view: Optional[str]
    ) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
        description: Optional[str] = None
        properties: Dict[str, str] = {}

        # The location cannot be fetched generically, but subclasses may override
        # this method and provide a location.
        location: Optional[str] = None

        try:
            # SQLAlchemy stubs are incomplete and missing this method.
            # PR: https://github.com/dropbox/sqlalchemy-stubs/pull/223.
            table_info: dict = inspector.get_view_comment(schema)
        except NotImplementedError:
            return description, properties, location

        description = table_info.get("text")

        # The "properties" field is a non-standard addition to SQLAlchemy's interface.
        properties = table_info.get("properties", {})

        return description, properties, location

    def _get_upstream_lineage_info(
        self, dataset_urn: str, schema: str, inspector: VerticaInspector
    ) -> Optional[_Aspect]:
        dataset_key = dataset_urn_to_key(dataset_urn)
        if dataset_key is None:
            logger.warning(f"Invalid dataset urn {dataset_urn}. Could not get key!")
            return None

        view_lineage_map = inspector._populate_view_lineage(schema)
        if dataset_key.name is not None:
            dataset_name = dataset_key.name

        else:
            # Handle the case when dataset_key.name is None
            # You can raise an exception, log a warning, or take any other appropriate action
            logger.warning("Invalid dataset name")

        lineage = view_lineage_map[dataset_name]

        if lineage is None:
            logger.debug(f"No lineage found for {dataset_name}")
            return None
        upstream_tables: List[UpstreamClass] = []

        for lineage_entry in lineage:
            # Update the view-lineage
            upstream_table_name = lineage_entry[0]

            upstream_table = UpstreamClass(
                dataset=make_dataset_urn_with_platform_instance(
                    self.platform,
                    upstream_table_name,
                    self.config.platform_instance,
                    self.config.env,
                ),
                type=DatasetLineageTypeClass.TRANSFORMED,
            )
            upstream_tables.append(upstream_table)

        if upstream_tables:
            logger.debug(
                f" lineage of '{dataset_name}': {[u.dataset for u in upstream_tables]}"
            )

            return UpstreamLineage(upstreams=upstream_tables)

        return None

    def loop_projections(  # noqa: C901
        self,
        inspector: VerticaInspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        projection_seen: Set[str] = set()
        try:
            projections = inspector.get_projection_names(schema)

            # created new function get_all_projection_columns in vertica Dialect as the existing get_columns of SQLAlchemy VerticaInspector class is being used for profiling
            # And query in get_all_projection_columns is modified to run at schema level.
            columns = inspector.get_all_projection_columns(schema)

            # called get_projection_properties function from dialect , it returns a list description and properties of all view in the schema
            description, properties, location_urn = self.get_projection_properties(
                inspector, schema, projections
            )

            # called get_view_owner function from dialect , it returns a list of all owner of all view in the schema
            projection_owner = inspector.get_projection_owner(schema)

            # started a loop on each view in the schema
            for projection_name in projections:
                finalcolumns = []
                # loops through all the columns in the schema and find all the columns of current projection
                for column in columns:
                    if column["tablename"] == projection_name.lower():
                        finalcolumns.append(column)

                projection_properties = {}
                # loops through all the properties in current schema and find all the properties of current projection
                for projection_comment in properties:

                    if (
                        isinstance(projection_comment, dict)
                        and projection_comment.get("projection_name")
                        == projection_name.lower()
                    ):

                        projection_properties["Ros count"] = str(
                            projection_comment.get("ROS_Count", "Not Available")
                        )
                        projection_properties["Projection Type"] = str(
                            projection_comment.get("Projection_Type", "Not Available")
                        )
                        projection_properties["is_segmented"] = str(
                            projection_comment.get("is_segmented", "Not Available")
                        )
                        projection_properties["Segmentation_key"] = str(
                            projection_comment.get("Segmentation_key", "Not Available")
                        )
                        projection_properties["Partition_Key"] = str(
                            projection_comment.get("Partition_Key", "Not Available")
                        )
                        projection_properties["Partition Size"] = str(
                            projection_comment.get("Partition_Size", "0")
                        )
                        projection_properties["Projection Size"] = str(
                            projection_comment.get("projection_size", "0 KB")
                        )
                        projection_properties["Projection Cached"] = str(
                            projection_comment.get("Projection_Cached", "False")
                        )

                owner_name = None
                # loops through all owners in the schema and saved the value of current projection owner
                for owner in projection_owner:
                    if owner[0].lower() == projection_name.lower():
                        owner_name = owner[1]

                dataset_name = self.get_identifier(
                    schema=schema, entity=projection_name, inspector=inspector
                )

                if dataset_name not in projection_seen:
                    projection_seen.add(dataset_name)
                else:
                    logger.debug(f"{dataset_name} has already been seen, skipping...")
                    continue

                self.report.report_entity_scanned(dataset_name, ent_type="projection")
                if not sql_config.table_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue
                dataset_urn = make_dataset_urn_with_platform_instance(
                    self.platform,
                    dataset_name,
                    self.config.platform_instance,
                    self.config.env,
                )

                yield from add_owner_to_entity_wu(
                    entity_type="dataset",
                    entity_urn=dataset_urn,
                    owner_urn=f"urn:li:corpuser:{owner_name}",
                )
                dataset_snapshot = DatasetSnapshot(
                    urn=dataset_urn,
                    aspects=[StatusClass(removed=False)],
                )

                dataset_properties = DatasetPropertiesClass(
                    name=projection_name,
                    description=description,
                    customProperties=projection_properties,
                )

                dataset_snapshot.aspects.append(dataset_properties)

                pk_constraints: dict = {}
                extra_tags: Optional[Dict[str, List[str]]] = None
                foreign_keys: Optional[List[ForeignKeyConstraintClass]] = None

                schema_fields = self.get_schema_fields(
                    dataset_name, finalcolumns, pk_constraints, tags=extra_tags
                )

                schema_metadata = get_schema_metadata(
                    self.report,
                    dataset_name,
                    self.platform,
                    finalcolumns,
                    pk_constraints,
                    foreign_keys,
                    schema_fields,
                )
                dataset_snapshot.aspects.append(schema_metadata)
                db_name = self.get_db_name(inspector)
                yield from self.add_table_to_schema_container(
                    dataset_urn, db_name, schema
                )
                mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
                yield SqlWorkUnit(id=dataset_name, mce=mce)
                dpi_aspect = self.get_dataplatform_instance_aspect(
                    dataset_urn=dataset_urn
                )
                if dpi_aspect:
                    yield dpi_aspect
                yield MetadataChangeProposalWrapper(
                    entityType="dataset",
                    changeType=ChangeTypeClass.UPSERT,
                    entityUrn=dataset_urn,
                    aspectName="subTypes",
                    aspect=SubTypesClass(typeNames=["Projections"]),
                ).as_workunit()

                if self.config.domain:
                    assert self.domain_registry
                    yield from get_domain_wu(
                        dataset_name=dataset_name,
                        entity_urn=dataset_urn,
                        domain_config=self.config.domain,
                        domain_registry=self.domain_registry,
                    )

                if self.config.include_projection_lineage:
                    try:
                        dataset_urn = make_dataset_urn_with_platform_instance(
                            self.platform,
                            dataset_name,
                            self.config.platform_instance,
                            self.config.env,
                        )

                        dataset_snapshot = DatasetSnapshot(
                            urn=dataset_urn, aspects=[StatusClass(removed=False)]
                        )

                        lineage_info = self._get_upstream_lineage_info_projection(
                            dataset_urn, schema, inspector
                        )

                        if lineage_info is not None:
                            upstream_lineage = lineage_info

                            lineage_mcpw = MetadataChangeProposalWrapper(
                                entityType="dataset",
                                changeType=ChangeTypeClass.UPSERT,
                                entityUrn=dataset_snapshot.urn,
                                aspectName="upstreamLineage",
                                aspect=upstream_lineage,
                            )

                            lineage_wu = MetadataWorkUnit(
                                id=f"{self.platform}-{lineage_mcpw.entityUrn}-{lineage_mcpw.aspectName}",
                                mcp=lineage_mcpw,
                            )

                            self.report.report_workunit(lineage_wu)
                            yield lineage_wu

                    except Exception as e:
                        logger.warning(
                            f"Unable to get lineage of projection {projection_name} due to an exception.\n {traceback.format_exc()}"
                        )
                        self.report.report_warning(f"{schema}", f"Ingestion error: {e}")

        except Exception as e:
            print(traceback.format_exc())
            self.report.report_failure(f"{schema}", f"Projections error: {e}")

    def get_projection_properties(
        self, inspector: VerticaInspector, schema: str, projection: Optional[str]
    ) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
        description: Optional[str] = None
        properties: Dict[str, str] = {}

        # The location cannot be fetched generically, but subclasses may override
        # this method and provide a location.
        location: Optional[str] = None

        try:
            # SQLAlchemy stubs are incomplete and missing this method.
            # PR: https://github.com/dropbox/sqlalchemy-stubs/pull/223.
            table_info: dict = inspector.get_projection_comment(schema)
        except NotImplementedError:
            return description, properties, location

        description = table_info.get("text")
        if type(description) is tuple:
            # Handling for value type tuple which is coming for dialect 'db2+ibm_db'
            description = table_info["text"][0]

        # The "properties" field is a non-standard addition to SQLAlchemy's interface.
        properties = table_info.get("properties", {})

        return description, properties, location

    def _get_upstream_lineage_info_projection(
        self, dataset_urn: str, schema: str, inspector: VerticaInspector
    ) -> Optional[_Aspect]:
        dataset_key = dataset_urn_to_key(dataset_urn)
        if dataset_key is None:
            logger.warning(f"Invalid dataset urn {dataset_urn}. Could not get key!")
            return None

        projection_lineage = inspector._populate_projection_lineage(schema)
        dataset_name = dataset_key.name
        lineage = projection_lineage[dataset_name]

        if not (lineage):
            logger.debug(f"No lineage found for {dataset_name}")
            return None
        upstream_tables: List[UpstreamClass] = []

        for lineage_entry in lineage:
            # Update the view-lineage
            upstream_table_name = lineage_entry[0]

            upstream_table = UpstreamClass(
                dataset=make_dataset_urn_with_platform_instance(
                    self.platform,
                    upstream_table_name,
                    self.config.platform_instance,
                    self.config.env,
                ),
                type=DatasetLineageTypeClass.TRANSFORMED,
            )
            upstream_tables.append(upstream_table)

        if upstream_tables:
            logger.debug(
                f" lineage of '{dataset_name}': {[u.dataset for u in upstream_tables]}"
            )

            return UpstreamLineage(upstreams=upstream_tables)

        return None

    def loop_profiler_requests(
        self,
        inspector: VerticaInspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable["GEProfilerRequest"]:
        """Function is used for collecting profiling related information for every projections
            inside an schema.

        Args: schema: schema name

        """
        from datahub.ingestion.source.ge_data_profiler import GEProfilerRequest

        tables_seen: Set[str] = set()
        profile_candidates = None  # Default value if profile candidates not available.
        yield from super().loop_profiler_requests(inspector, schema, sql_config)
        for projection in inspector.get_projection_names(schema):
            dataset_name = self.get_identifier(
                schema=schema, entity=projection, inspector=inspector
            )

            if not self.is_dataset_eligible_for_profiling(
                dataset_name, sql_config, inspector, profile_candidates
            ):
                if self.config.profiling.report_dropped_profiles:
                    self.report.report_dropped(f"profile of {dataset_name}")
                continue
            if dataset_name not in tables_seen:
                tables_seen.add(dataset_name)
            else:
                logger.debug(f"{dataset_name} has already been seen, skipping...")
                continue
            missing_column_info_warn = self.report.warnings.get(MISSING_COLUMN_INFO)
            if (
                missing_column_info_warn is not None
                and dataset_name in missing_column_info_warn
            ):
                continue
            (partition, custom_sql) = self.generate_partition_profiler_query(
                schema, projection, self.config.profiling.partition_datetime
            )
            if partition is None and self.is_table_partitioned(
                database=None, schema=schema, table=projection
            ):
                self.report.report_warning(
                    "profile skipped as partitioned table is empty or partition id was invalid",
                    dataset_name,
                )
                continue
            if (
                partition is not None
                and not self.config.profiling.partition_profiling_enabled
            ):
                logger.debug(
                    f"{dataset_name} and partition {partition} is skipped because profiling.partition_profiling_enabled property is disabled"
                )
                continue
            self.report.report_entity_profiled(dataset_name)
            logger.debug(
                f"Preparing profiling request for {schema}, {projection}, {partition}"
            )

            yield GEProfilerRequest(
                pretty_name=dataset_name,
                batch_kwargs=self.prepare_profiler_args(
                    inspector=inspector,
                    schema=schema,
                    table=projection,
                    partition=partition,
                    custom_sql=custom_sql,
                ),
            )

    def loop_models(
        self,
        inspector: VerticaInspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        """
        This function is for iterating over the ml models in vertica db

        Args:
            inspector (Inspector) : inspector obj from reflection engine
            schema (str): schema name
            sql_config (SQLAlchemyConfig): config

        Returns:
            Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]

        Yields:
            Iterator[Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]]:
        """
        models_seen: Set[str] = set()
        try:
            for models in inspector.get_models_names(schema):
                dataset_name = self.get_identifier(
                    schema="Entities", entity=models, inspector=inspector
                )

                if dataset_name not in models_seen:
                    models_seen.add(dataset_name)
                else:
                    logger.debug("has already been seen, skipping... %s", dataset_name)
                    continue
                self.report.report_entity_scanned(dataset_name, ent_type="models")
                if not sql_config.table_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue
                try:
                    columns: List[Dict[Any, Any]] = []
                    dataset_urn = make_dataset_urn_with_platform_instance(
                        self.platform,
                        dataset_name,
                        self.config.platform_instance,
                        self.config.env,
                    )
                    dataset_snapshot = DatasetSnapshot(
                        urn=dataset_urn,
                        aspects=[StatusClass(removed=False)],
                    )
                    description, properties, location = self.get_model_properties(
                        inspector, schema, models
                    )

                    dataset_properties = DatasetPropertiesClass(
                        name=models,
                        description=description,
                        customProperties=properties,
                    )

                    dataset_snapshot.aspects.append(dataset_properties)
                    pk_constraints: dict = {}
                    foreign_keys: Optional[List[ForeignKeyConstraintClass]] = None
                    schema_fields = self.get_schema_fields(dataset_name, columns)

                    schema_metadata = get_schema_metadata(
                        self.report,
                        dataset_name,
                        self.platform,
                        columns,
                        pk_constraints,
                        foreign_keys,
                        schema_fields,
                    )

                    dataset_snapshot.aspects.append(schema_metadata)
                    db_name = self.get_db_name(inspector)

                    yield from self.add_table_to_schema_container(
                        dataset_urn, db_name, schema
                    )
                    mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
                    yield SqlWorkUnit(id=dataset_name, mce=mce)
                    dpi_aspect = self.get_dataplatform_instance_aspect(
                        dataset_urn=dataset_urn
                    )
                    if dpi_aspect:
                        yield dpi_aspect
                    yield MetadataChangeProposalWrapper(
                        entityType="dataset",
                        changeType=ChangeTypeClass.UPSERT,
                        entityUrn=dataset_urn,
                        aspectName="subTypes",
                        aspect=SubTypesClass(typeNames=["ML Models"]),
                    ).as_workunit()
                    if self.config.domain:
                        assert self.domain_registry
                        yield from get_domain_wu(
                            dataset_name=dataset_name,
                            entity_urn=dataset_urn,
                            domain_config=self.config.domain,
                            domain_registry=self.domain_registry,
                        )
                except Exception as error:
                    logger.warning(
                        f"Unable to ingest {schema}.{models} due to an exception. %s {traceback.format_exc()}"
                    )
                    self.report.report_warning(
                        f"{schema}.{models}", f"Ingestion error: {error}"
                    )
        except Exception as error:
            self.report.report_failure(f"{schema}", f"Model error: {error}")

    def get_model_properties(
        self, inspector: VerticaInspector, schema: str, model: str
    ) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
        """
        Returns ml models related metadata information to show in properties tab
            eg. ml model attribute and ml model specification information.

        Args:
            inspector (VerticaInspector): inspector obj from reflection engine
            schema (str): schema name
            model (str): ml model name
        Returns:
            Tuple[Optional[str], Dict[str, str], Optional[str]]
        """
        description: Optional[str] = None
        properties: Dict[str, str] = {}
        # The location cannot be fetched generically, but subclasses may override
        # this method and provide a location.
        location: Optional[str] = None
        try:
            table_info: dict = inspector.get_model_comment(model, schema)
        except NotImplementedError:
            return description, properties, location
        description = table_info.get("text")

        # The "properties" field is a non-standard addition to SQLAlchemy's interface.
        properties = table_info.get("properties", {})
        return description, properties, location
