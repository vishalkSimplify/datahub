
import logging
import traceback
from dataclasses import dataclass
from datahub.ingestion.source.sql.sql_common import (
    SQLAlchemySource,
    SQLSourceReport,
    SqlWorkUnit,
    get_schema_metadata,
)
from sqlalchemy import create_engine
from sqlalchemy import sql
from textwrap import dedent
from datahub.emitter.mcp_builder import add_owner_to_entity_wu
from sqlalchemy.exc import ProgrammingError
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from collections import defaultdict
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.ingestion.source.sql.sql_config import (
    BasicSQLAlchemyConfig,
    SQLAlchemyConfig,
)
from datahub.ingestion.source.sql.sql_utils import get_domain_wu
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.configuration.common import AllowDenyPattern
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Set, Tuple, Union
import pydantic
from pydantic.class_validators import validator
from datahub.utilities import config_clean
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import StatusClass
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.emitter.mce_builder import (
    dataset_urn_to_key,
    make_dataset_urn_with_platform_instance,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from sqlalchemy.engine.reflection import Inspector
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    SubTypesClass,
    UpstreamClass,
    _Aspect,
)

if TYPE_CHECKING:
    from datahub.ingestion.source.ge_data_profiler import GEProfilerRequest
MISSING_COLUMN_INFO = "missing column information"
logger: logging.Logger = logging.getLogger(__name__)

@dataclass
class VerticaSourceReport(SQLSourceReport):
    projection_scanned: int = 0
    models_scanned: int = 0
    oauth_scanned: int = 0

    def report_entity_scanned(self, name: str, ent_type: str = "table") -> None:
        """
        Entity could be a projection or a models or Oauth .
        """

        if ent_type == "projection":
            self.projection_scanned += 1
        elif ent_type == "models":
            self.models_scanned += 1
        elif ent_type == "oauth":
            self.oauth_scanned += 1
        else:
            super().report_entity_scanned(name, ent_type)

# Extended BasicSQLAlchemyConfig to config for projections,models and oauth metadata.
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
    include_oauth: Optional[bool] = pydantic.Field(
        default=True, description="Whether Oauth should be ingested."
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
        super(VerticaSource, self).__init__(config, ctx, "vertica8")
        self.report: SQLSourceReport = VerticaSourceReport()
        self.view_lineage_map: Optional[Dict[str, List[Tuple[str, str, str]]]] = None
        self.projection_lineage_map: Optional[
            Dict[str, List[Tuple[str, str, str]]]
        ] = None
        self.tables:Optional[Dict[str]] = None
        self.columns:Optional[Dict[str]] = None
        self.primary_key:Optional[Dict[str]] = None
        self.properties:Optional[Dict[str]] = None
        self.config: VerticaConfig = config

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "VerticaSource":
        config = VerticaConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
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



                if profiler:
                    profile_requests += list(
                        self.loop_profiler_requests(inspector, schema, sql_config)
                    )
                

            if profiler and profile_requests:
                yield from self.loop_profiler(
                    profile_requests, profiler, platform=self.platform
                )

    def get_database_properties(
        self, inspector: Inspector, database: str
    ) -> Optional[Dict[str, str]]:
        try:
            custom_properties = inspector._get_database_properties(database)  # type: ignore
            return custom_properties
        except Exception as ex:
            self.report.report_failure(
                f"{database}", f"unable to get extra_properties : {ex}"
            )
        return None

    def get_schema_properties(
        self, inspector: Inspector, database: str, schema: str
    ) -> Optional[Dict[str, str]]:
        try:
            custom_properties = inspector._get_schema_properties(schema)  # type: ignore
            return custom_properties
        except Exception as ex:
            self.report.report_failure(
                f"{database}.{schema}", f"unable to get extra_properties : {ex}"
            )
        return None

    def loop_tables(  # noqa: C901
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        tables_seen: Set[str] = set()
        try:
            self.tables = defaultdict(list)
            self.columns = defaultdict(list)
            self.primary_key = defaultdict(list)
            tables = inspector.get_table_names(schema)
            
            self.tables[schema] = tables
            columns = inspector.get_columns(schema)
            primary_key = inspector.get_pk_constraint(schema)
           
            description, properties, table_size = self.get_table_properties(
                    inspector, schema
            )
            
            table_owner = inspector.get_table_owner(schema)
   

            for table in self.tables[schema]:
          
                
                
                finalcolumns = []
                for column in columns:
                    if column['tablename'] == table:
                        finalcolumns.append(column)
                final_primary_key = []
                for primary_key_column in primary_key:
                    if primary_key_column['tablename'] == table:
                       
                        final_primary_key = primary_key_column 
                # print(properties)
                
                table_properties = {}
                for data in properties: 
                    if data['table_name'] == table:
                        table_properties['create_time'] = data['create_time']
                        table_properties['table_size'] = data['table_size']
                    # print(data['table_name'])

              
                owner_name = ""
                for owner in table_owner:
                    
                    if owner[0].lower() == table:
                        owner_name = owner[1].lower()
                        
                    

           
                schema, table = self.standardize_schema_table_names(
                    schema=schema, entity=table
                )
                
                dataset_name = self.get_identifier(
                    schema=schema, entity=table, inspector=inspector
                )
               
                dataset_name = self.normalise_dataset_name(dataset_name)
                
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
                
                normalised_table = table
                splits = dataset_name.split(".")
                if splits:
                    normalised_table = splits[-1]
                    if table_properties and normalised_table != table:
                        table_properties["original_table_name"] = table
                dataset_properties = DatasetPropertiesClass(
                    name=normalised_table,
                    description=description,
                    customProperties=table_properties,
                )

                dataset_snapshot.aspects.append(dataset_properties)
                pk_constraints: dict = final_primary_key
                extra_tags=[]
                foreign_keys=[]
               
                schema_fields = self.get_schema_fields(
                    dataset_name, finalcolumns, pk_constraints, tags=extra_tags
                )
                
                
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
                    dataset_urn=dataset_urn, db_name=db_name, schema=schema
                )
                mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
                
                wu = SqlWorkUnit(id=dataset_name, mce=mce)
                self.report.report_workunit(wu)
                yield wu

               
                
                dpi_aspect = self.get_dataplatform_instance_aspect(dataset_urn=dataset_urn)
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
                        report=self.report,
                    ) 
                
                # try:
                #     yield from self._process_table(
                #         dataset_name, inspector, schema, table, sql_config
                #     )
                # except Exception as e:
                #     logger.warning(
                #         f"Unable to ingest {schema}.{table} due to an exception.\n {e}"
                #     )
                #     self.report.report_warning(
                #         f"{schema}.{table}", f"Ingestion error: {e}"
                #     )
        except Exception as e:
            self.report.report_failure(f"{schema}", f"Tables error: {traceback.format_exc()}")

    def get_table_properties(
        self, inspector: Inspector, schema: str
    ) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
        description: Optional[str] = None
        properties: Dict[str, str] = {}

        # The location cannot be fetched generically, but subclasses may override
        # this method and provide a location.
        location: Optional[str] = None

        try:
            # SQLAlchemy stubs are incomplete and missing this method.
            # PR: https://github.com/dropbox/sqlalchemy-stubs/pull/223.
            table_info: dict = inspector.get_table_comment(schema)  # type: ignore
        except NotImplementedError:
            return description, properties, location
        except ProgrammingError as pe:
            # Snowflake needs schema names quoted when fetching table comments.
            logger.debug(
                f"Encountered ProgrammingError. Retrying with quoted schema name for schema {schema} ",
                pe,
            )
            table_info: dict = inspector.get_table_comment(f'"{schema}"')  # type: ignore

        description = table_info.get("text")
        if type(description) is tuple:
            # Handling for value type tuple which is coming for dialect 'db2+ibm_db'
            description = table_info["text"][0]

        # The "properties" field is a non-standard addition to SQLAlchemy's interface.
        properties = table_info.get("properties", {})
        return description, properties, location

