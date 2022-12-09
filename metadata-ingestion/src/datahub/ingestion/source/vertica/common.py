import datetime
import logging
from datahub.emitter.rest_emitter import DatahubRestEmitter
import json
import traceback
from abc import abstractmethod
from collections import defaultdict
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import Enum
from sqlalchemy import sql, util
from textwrap import dedent
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)

from urllib.parse import quote_plus

import pydantic
import sqlalchemy.dialects.postgresql.base
from pydantic.fields import Field
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.sql import sqltypes as types

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_domain_urn,
    make_tag_urn,
    dataset_urn_to_key
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    DatabaseKey,
    PlatformKey,
    SchemaKey,
    
    add_dataset_to_container,
    add_domain_to_entity_wu,
    gen_containers,
    wrap_aspect_as_workunit
)
import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.state.sql_common_state import (
    BaseSQLAlchemyCheckpointState,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import StatusClass
from datahub.metadata.com.linkedin.pegasus2avro.dataset import UpstreamLineage
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    EnumTypeClass,
    ForeignKeyConstraint,
    MySqlDDL,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringTypeClass,
    TimeTypeClass,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DataPlatformInstanceClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    SubTypesClass,
    TagAssociationClass,
    UpstreamClass,
    ViewPropertiesClass,
    MLModelDeploymentPropertiesClass,
    MLModelPropertiesClass
)
from datahub.telemetry import telemetry
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.registries.domain_registry import DomainRegistry
from datahub.utilities.sqlalchemy_query_combiner import SQLAlchemyQueryCombinerReport

if TYPE_CHECKING:
    from datahub.ingestion.source.ge_data_profiler import (
        DatahubGEProfiler,
        GEProfilerRequest,
    )
    

logger: logging.Logger = logging.getLogger(__name__)

MISSING_COLUMN_INFO = "missing column information"


def _platform_alchemy_uri_tester_gen(
    platform: str, opt_starts_with: Optional[str] = None
) -> Tuple[str, Callable[[str], bool]]:
    return platform, lambda x: x.startswith(
        platform if not opt_starts_with else opt_starts_with
    )


PLATFORM_TO_SQLALCHEMY_URI_TESTER_MAP: Dict[str, Callable[[str], bool]] = OrderedDict(
    [
        _platform_alchemy_uri_tester_gen("athena", "awsathena"),
        _platform_alchemy_uri_tester_gen("bigquery"),
        _platform_alchemy_uri_tester_gen("clickhouse"),
        _platform_alchemy_uri_tester_gen("druid"),
        _platform_alchemy_uri_tester_gen("hana"),
        _platform_alchemy_uri_tester_gen("hive"),
        _platform_alchemy_uri_tester_gen("mongodb"),
        _platform_alchemy_uri_tester_gen("mssql"),
        _platform_alchemy_uri_tester_gen("mysql"),
        _platform_alchemy_uri_tester_gen("oracle"),
        _platform_alchemy_uri_tester_gen("pinot"),
        _platform_alchemy_uri_tester_gen("presto"),
        (
            "redshift",
            lambda x: (
                x.startswith(("jdbc:postgres:", "postgresql"))
                and x.find("redshift.amazonaws") > 0
            )
            or x.startswith("redshift"),
        ),
        # Don't move this before redshift.
        _platform_alchemy_uri_tester_gen("postgres", "postgresql"),
        _platform_alchemy_uri_tester_gen("snowflake"),
        _platform_alchemy_uri_tester_gen("trino"),
        _platform_alchemy_uri_tester_gen("vertica"),
    ]
)


def get_platform_from_sqlalchemy_uri(sqlalchemy_uri: str) -> str:
    for platform, tester in PLATFORM_TO_SQLALCHEMY_URI_TESTER_MAP.items():
        if tester(sqlalchemy_uri):
            return platform
    return "external"


def make_sqlalchemy_uri(
    scheme: str,
    username: Optional[str],
    password: Optional[str],
    at: Optional[str],
    db: Optional[str],
    uri_opts: Optional[Dict[str, Any]] = None,
) -> str:
    url = f"{scheme}://"
    if username is not None:
        url += f"{quote_plus(username)}"
        if password is not None:
            url += f":{quote_plus(password)}"
        url += "@"
    if at is not None:
        url += f"{at}"
    if db is not None:
        url += f"/{db}"
    if uri_opts is not None:
        if db is None:
            url += "/"
        params = "&".join(
            f"{key}={quote_plus(value)}" for (key, value) in uri_opts.items() if value
        )
        url = f"{url}?{params}"
    return url


class SqlContainerSubTypes(str, Enum):
    DATABASE = "Database"
    SCHEMA = "Schema"


@dataclass
class SQLSourceReport(StaleEntityRemovalSourceReport):
    tables_scanned: int = 0
    views_scanned: int = 0
    Projection_scanned: int = 0
    models_scanned: int = 0
    Outh_scanned: int = 0
    entities_collected: int = 0
    entities_profiled: int = 0
    filtered: LossyList[str] = field(default_factory=LossyList)

    query_combiner: Optional[SQLAlchemyQueryCombinerReport] = None

    def report_entity_scanned(self, name: str, ent_type: str = "table") -> None:
        """
        Entity could be a view or a table
        """
        if ent_type == "table":
            self.tables_scanned += 1
        elif ent_type == "view":
            self.views_scanned += 1
        elif ent_type == "projection":
            self.Projection_scanned += 1
        elif ent_type == "models":
            self.models_scanned += 1
        elif ent_type == "OAuth":
            self.Outh_scanned += 1
            
        # elif ent_type == "entities":
        #     self.entities_collected += 1
        else:
            raise KeyError(f"Unknown entity {ent_type}.")

    def report_entity_profiled(self, name: str) -> None:
        self.entities_profiled += 1

    def report_dropped(self, ent_name: str) -> None:
        self.filtered.append(ent_name)

    def report_from_query_combiner(
        self, query_combiner_report: SQLAlchemyQueryCombinerReport
    ) -> None:
        self.query_combiner = query_combiner_report


class SQLAlchemyStatefulIngestionConfig(StatefulStaleMetadataRemovalConfig):
    """
    Specialization of StatefulStaleMetadataRemovalConfig to adding custom config.
    This will be used to override the stateful_ingestion config param of StatefulIngestionConfigBase
    in the SQLAlchemyConfig.
    """

    _entity_types: List[str] = pydantic.Field(
        default=["assertion", "container", "table", "view","Projection","models","Outh"]
    )


class SQLAlchemyConfig(StatefulIngestionConfigBase):
    options: dict = {}
    # Although the 'table_pattern' enables you to skip everything from certain schemas,
    # having another option to allow/deny on schema level is an optimization for the case when there is a large number
    # of schemas that one wants to skip and you want to avoid the time to needlessly fetch those tables only to filter
    # them out afterwards via the table_pattern.
    schema_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for schemas to filter in ingestion. Specify regex to only match the schema name. e.g. to match all tables in schema analytics, use the regex 'analytics'",
    )
    table_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for tables to filter in ingestion. Specify regex to match the entire table name in database.schema.table format. e.g. to match all tables starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'",
    )
    projection_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for projection to filter in ingestion. Specify regex to match the entire table name in database.schema.table format. e.g. to match all tables starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'",
    )
    view_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for views to filter in ingestion. Note: Defaults to table_pattern if not specified. Specify regex to match the entire view name in database.schema.view format. e.g. to match all views starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'",
    )
    models_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for views to filter in ingestion. Note: Defaults to table_pattern if not specified. Specify regex to match the entire view name in database.schema.view format. e.g. to match all views starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'",
    )
    oauth_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for views to filter in ingestion. Note: Defaults to table_pattern if not specified. Specify regex to match the entire view name in database.schema.view format. e.g. to match all views starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'",
    )
    profile_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter tables for profiling during ingestion. Allowed by the `table_pattern`.",
    )
    domain: Dict[str, AllowDenyPattern] = Field(
        default=dict(),
        description='Attach domains to databases, schemas or tables during ingestion using regex patterns. Domain key can be a guid like *urn:li:domain:ec428203-ce86-4db3-985d-5a8ee6df32ba* or a string like "Marketing".) If you provide strings, then datahub will attempt to resolve this name to a guid, and will error out if this fails. There can be multiple domain keys specified.',
    )

    include_views: Optional[bool] = Field(
        default=True, description="Whether views should be ingested."
    )
    include_tables: Optional[bool] = Field(
        default=True, description="Whether tables should be ingested."
    )
    include_projections: Optional[bool] = Field(
        default=True, description="Whether projections should be ingested."
    )
    
    include_models: Optional[bool] = Field(
        default=True, description="Whether Models should be ingested."
    )
    include_Outh: Optional[bool] = Field(
        default=True, description="Whether OAuth should be ingested."
    )
  
   
    profiling: GEProfilingConfig = GEProfilingConfig()
    # Custom Stateful Ingestion settings
    stateful_ingestion: Optional[SQLAlchemyStatefulIngestionConfig] = None

    @pydantic.root_validator(pre=True)
    def view_pattern_is_table_pattern_unless_specified(
        cls, values: Dict[str, Any]
    ) -> Dict[str, Any]:
        view_pattern = values.get("view_pattern")
        table_pattern = values.get("table_pattern")
        if table_pattern and not view_pattern:
            logger.info(f"Applying table_pattern {table_pattern} to view_pattern.")
            values["view_pattern"] = table_pattern
        return values

    @pydantic.root_validator()
    def ensure_profiling_pattern_is_passed_to_profiling(
        cls, values: Dict[str, Any]
    ) -> Dict[str, Any]:
        profiling: Optional[GEProfilingConfig] = values.get("profiling")
        if profiling is not None and profiling.enabled:
            profiling._allow_deny_patterns = values["profile_pattern"]
        return values

    @abstractmethod
    def get_sql_alchemy_url(self):
        pass


class Vertica_BasicSQLAlchemyConfig(SQLAlchemyConfig):
    username: Optional[str] = Field(default=None, description="username")
    password: Optional[pydantic.SecretStr] = Field(
        default=None, exclude=True, description="password"
    )
    host_port: str = Field(description="host URL")
    database: Optional[str] = Field(default=None, description="database (catalog)")
    database_alias: Optional[str] = Field(
        default=None, description="Alias to apply to database when ingesting."
    )
    scheme: str = Field(description="scheme")
    sqlalchemy_uri: Optional[str] = Field(
        default=None,
        description="URI of database to connect to. See https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls. Takes precedence over other connection parameters.",
    )

    def get_sql_alchemy_url(self, uri_opts: Optional[Dict[str, Any]] = None) -> str:
        if not ((self.host_port and self.scheme) or self.sqlalchemy_uri):
            raise ValueError("host_port and schema or connect_uri required.")

        return self.sqlalchemy_uri or make_sqlalchemy_uri(
            self.scheme,  # type: ignore
            self.username,
            self.password.get_secret_value() if self.password is not None else None,
            self.host_port,  # type: ignore
            self.database,
            uri_opts=uri_opts,
        )


class SqlWorkUnit(MetadataWorkUnit):
    pass


_field_type_mapping: Dict[Type[types.TypeEngine], Type] = {
    types.Integer: NumberTypeClass,
    types.Numeric: NumberTypeClass,
    types.Boolean: BooleanTypeClass,
    types.Enum: EnumTypeClass,
    types._Binary: BytesTypeClass,
    types.LargeBinary: BytesTypeClass,
    types.PickleType: BytesTypeClass,
    types.ARRAY: ArrayTypeClass,
    types.String: StringTypeClass,
    types.Date: DateTypeClass,
    types.DATE: DateTypeClass,
    types.Time: TimeTypeClass,
    types.DateTime: TimeTypeClass,
    types.DATETIME: TimeTypeClass,
    types.TIMESTAMP: TimeTypeClass,
    types.JSON: RecordTypeClass,
    # Because the postgresql dialect is used internally by many other dialects,
    # we add some postgres types here. This is ok to do because the postgresql
    # dialect is built-in to sqlalchemy.
    sqlalchemy.dialects.postgresql.base.BYTEA: BytesTypeClass,
    sqlalchemy.dialects.postgresql.base.DOUBLE_PRECISION: NumberTypeClass,
    sqlalchemy.dialects.postgresql.base.INET: StringTypeClass,
    sqlalchemy.dialects.postgresql.base.MACADDR: StringTypeClass,
    sqlalchemy.dialects.postgresql.base.MONEY: NumberTypeClass,
    sqlalchemy.dialects.postgresql.base.OID: StringTypeClass,
    sqlalchemy.dialects.postgresql.base.REGCLASS: BytesTypeClass,
    sqlalchemy.dialects.postgresql.base.TIMESTAMP: TimeTypeClass,
    sqlalchemy.dialects.postgresql.base.TIME: TimeTypeClass,
    sqlalchemy.dialects.postgresql.base.INTERVAL: TimeTypeClass,
    sqlalchemy.dialects.postgresql.base.BIT: BytesTypeClass,
    sqlalchemy.dialects.postgresql.base.UUID: StringTypeClass,
    sqlalchemy.dialects.postgresql.base.TSVECTOR: BytesTypeClass,
    sqlalchemy.dialects.postgresql.base.ENUM: EnumTypeClass,
    # When SQLAlchemy is unable to map a type into its internal hierarchy, it
    # assigns the NullType by default. We want to carry this warning through.
    types.NullType: NullTypeClass,
}
_known_unknown_field_types: Set[Type[types.TypeEngine]] = {
    types.Interval,
    types.CLOB,
}


def register_custom_type(
    tp: Type[types.TypeEngine], output: Optional[Type] = None
) -> None:
    if output:
        _field_type_mapping[tp] = output
    else:
        _known_unknown_field_types.add(tp)


class _CustomSQLAlchemyDummyType(types.TypeDecorator):
    impl = types.LargeBinary


def make_sqlalchemy_type(name: str) -> Type[types.TypeEngine]:
    # This usage of type() dynamically constructs a class.
    # See https://stackoverflow.com/a/15247202/5004662 and
    # https://docs.python.org/3/library/functions.html#type.
    sqlalchemy_type: Type[types.TypeEngine] = type(
        name,
        (_CustomSQLAlchemyDummyType,),
        {
            "__repr__": lambda self: f"{name}()",
        },
    )
    return sqlalchemy_type


def get_column_type(
    sql_report: SQLSourceReport, dataset_name: str, column_type: Any
) -> SchemaFieldDataType:
    """
    Maps SQLAlchemy types (https://docs.sqlalchemy.org/en/13/core/type_basics.html) to corresponding schema types
    """

    TypeClass: Optional[Type] = None
    for sql_type in _field_type_mapping.keys():
        if isinstance(column_type, sql_type):
            TypeClass = _field_type_mapping[sql_type]
            break
    if TypeClass is None:
        for sql_type in _known_unknown_field_types:
            if isinstance(column_type, sql_type):
                TypeClass = NullTypeClass
                break

    if TypeClass is None:
        sql_report.report_warning(
            dataset_name, f"unable to map type {column_type!r} to metadata schema"
        )
        TypeClass = NullTypeClass

    return SchemaFieldDataType(type=TypeClass())


def get_schema_metadata(
    sql_report: SQLSourceReport,
    dataset_name: str,
    platform: str,
    columns: List[dict],
    pk_constraints: dict = None,
    foreign_keys: List[ForeignKeyConstraint] = None,
    canonical_schema: List[SchemaField] = [],
) -> SchemaMetadata:
    schema_metadata = SchemaMetadata(
        schemaName=dataset_name,
        platform=make_data_platform_urn(platform),
        version=0,
        hash="",
        platformSchema=MySqlDDL(tableSchema=""),
        fields=canonical_schema,
    )
    if foreign_keys is not None and foreign_keys != []:
        schema_metadata.foreignKeys = foreign_keys

    return schema_metadata


# config flags to emit telemetry for
config_options_to_report = [
    "include_views",
    "include_tables",
    "include_projections",
    "include_models",
    "include_Outh"

]

# flags to emit telemetry for
profiling_flags_to_report = [
    "turn_off_expensive_profiling_metrics",
    "profile_table_level_only",
    "include_field_null_count",
    "include_field_min_value",
    "include_field_max_value",
    "include_field_mean_value",
    "include_field_median_value",
    "include_field_stddev_value",
    "include_field_quantiles",
    "include_field_distinct_value_frequencies",
    "include_field_histogram",
    "include_field_sample_values",
    "query_combiner_enabled",
]


class SchemaKeyHelper(SchemaKey):
    numberOfProjection: Optional[str]    
    udxsFunctions : Optional[str] = None
    UDXsLanguage : Optional[str] = None

class DatabaseKeyHelper(DatabaseKey):
    clusterType : Optional[str] =  None
    clusterSize : Optional[str] = None
    subClusters : Optional[str] = None
    communalStoragePath : Optional[str] = None


class Vertica_SQLAlchemySource(StatefulIngestionSourceBase):
    """A Base class for all SQL Sources that use SQLAlchemy to extend"""

    def __init__(self, config: SQLAlchemyConfig, ctx: PipelineContext, platform: str):
        super(Vertica_SQLAlchemySource, self).__init__(config, ctx)
        self.config = config
        self.platform = platform
        self.report: SQLSourceReport = SQLSourceReport()

        # Create and register the stateful ingestion use-case handlers.
        self.stale_entity_removal_handler = StaleEntityRemovalHandler(
            source=self,
            config=self.config,
            state_type_class=BaseSQLAlchemyCheckpointState,
            pipeline_name=self.ctx.pipeline_name,
            run_id=self.ctx.run_id,
        )

        config_report = {
            config_option: config.dict().get(config_option)
            for config_option in config_options_to_report
        }

        config_report = {
            **config_report,
            "profiling_enabled": config.profiling.enabled,
            "platform": platform,
        }

        telemetry.telemetry_instance.ping(
            "sql_config",
            config_report,
        )

        if config.profiling.enabled:

            telemetry.telemetry_instance.ping(
                "sql_profiling_config",
                {
                    config_flag: config.profiling.dict().get(config_flag)
                    for config_flag in profiling_flags_to_report
                },
            )
        if self.config.domain:
            self.domain_registry = DomainRegistry(
                cached_domains=[k for k in self.config.domain], graph=self.ctx.graph
            )

    def warn(self, log: logging.Logger, key: str, reason: str) -> None:
        self.report.report_warning(key, reason)
        log.warning(f"{key} => {reason}")

    def error(self, log: logging.Logger, key: str, reason: str) -> None:
        self.report.report_failure(key, reason)
        log.error(f"{key} => {reason}")

    def get_inspectors(self) -> Iterable[Inspector]:
        # This method can be overridden in the case that you want to dynamically
        # run on multiple databases.

        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **self.config.options)
        with engine.connect() as conn:
            inspector = inspect(conn)
            yield inspector

    def get_db_name(self, inspector: Inspector) -> str:
        engine = inspector.engine

        if engine and hasattr(engine, "url") and hasattr(engine.url, "database"):
            return str(engine.url.database).strip('"').lower()
        else:
            raise Exception("Unable to get database name from Sqlalchemy inspector")

    def get_schema_names(self, inspector):
        return inspector.get_schema_names()

    def get_platform_instance_id(self) -> str:
        """
        The source identifier such as the specific source host address required for stateful ingestion.
        Individual subclasses need to override this method appropriately.
        """
        config_dict = self.config.dict()
        host_port = config_dict.get("host_port", "no_host_port")
        database = config_dict.get("database", "no_database")
        return f"{self.platform}_{host_port}_{database}"

    def gen_schema_key(self, db_name: str, schema: str) -> PlatformKey:
        try:
            all_properties_keys = dict()
            for inspector in self.get_inspectors():
     
                all_properties_keys = inspector._get_properties_keys(db_name , schema, level='schema')

            
            return SchemaKeyHelper(
                database=db_name,
                schema=schema,
                platform=self.platform,
                instance=self.config.platform_instance,
                backcompat_instance_for_guid=self.config.env,
                
                numberOfProjection = all_properties_keys.get("projection_count", ""),
                udxsFunctions = all_properties_keys.get("udx_list", ""),
                UDXsLanguage = all_properties_keys.get("Udx_langauge", ""),
            )
        except Exception as e:
            traceback.print_exc()
            print("Hey something went wrong, while gettting schema in gen schema key")
            
            
    def gen_database_key(self, database: str) -> PlatformKey:
        try:
            all_properties_keys = dict()
            for inspector in self.get_inspectors():
                all_properties_keys = inspector._get_properties_keys(database , "schema", level='database')
                
            return DatabaseKeyHelper(
                database=database,
                platform=self.platform,
                instance=self.config.platform_instance,
                backcompat_instance_for_guid=self.config.env,
                
                
                clusterType = all_properties_keys.get("cluster_type", "Vertica's CLuster"),
                clusterSize = all_properties_keys.get("cluster_size", "09 GB"),
                subClusters = all_properties_keys.get("Subcluster", "MANY"),
                communalStoragePath = all_properties_keys.get("communinal_storage_path", "/dev/sda1"),
            )
        except Exception as e:
            traceback.print_exc()
            print("Hey something went wrong, while gettting Generation of database key")
            
            
    def gen_database_containers(self, database: str) -> Iterable[MetadataWorkUnit]:
        domain_urn = self._gen_domain_urn(database)

        database_container_key = self.gen_database_key(database)
        container_workunits = gen_containers(
            container_key=database_container_key,
            name=database,
            sub_types=[SqlContainerSubTypes.DATABASE],
            domain_urn=domain_urn,
        )

        for wu in container_workunits:
            self.report.report_workunit(wu)
            yield wu

    def gen_schema_containers(
        self, schema: str, db_name: str
    ) -> Iterable[MetadataWorkUnit]:

        schema_container_key = self.gen_schema_key(db_name, schema)

        database_container_key: Optional[PlatformKey] = None
        if db_name is not None:
            database_container_key = self.gen_database_key(database=db_name)

        container_workunits = gen_containers(
            # TODO: this one is bad
            schema_container_key,
            schema,
            [SqlContainerSubTypes.SCHEMA],
            database_container_key,
        )

        for wu in container_workunits:
            self.report.report_workunit(wu)
            yield wu

    def get_allowed_schemas(self, inspector: Inspector, db_name: str) -> Iterable[str]:
        # this function returns the schema names which are filtered by schema_pattern.
        for schema in self.get_schema_names(inspector):
            if not self.config.schema_pattern.allowed(schema):
                self.report.report_dropped(f"{schema}.*")
                continue
            else:
                self.add_information_for_schema(inspector, schema)
                yield schema

    def get_workunits(self) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        sql_config = self.config
        if logger.isEnabledFor(logging.DEBUG):
            # If debug logging is enabled, we also want to echo each SQL query issued.
            sql_config.options.setdefault("echo", True)

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
            yield from self.gen_database_containers(db_name)

            for schema in self.get_allowed_schemas(inspector, db_name):
                self.add_information_for_schema(inspector, schema)

                yield from self.gen_schema_containers(schema, db_name)

                if sql_config.include_tables:
                    yield from self.loop_tables(inspector, schema, sql_config)

                if sql_config.include_views:
                    yield from self.loop_views(inspector, schema, sql_config)
                    
                if sql_config.include_projections:
                    yield from self.loop_projections(inspector, schema,sql_config)
                if sql_config.include_models:
                    yield from self.loop_models(inspector, schema,sql_config)
                
                if profiler:
                    profile_requests += list(
                        self.loop_profiler_requests(inspector, schema, sql_config)
                    )
                    profile_requests += list(
                        self.loop_profiler_requests_for_projections(inspector, schema, sql_config)
                    )

            if profiler and profile_requests:
                yield from self.loop_profiler(
                    profile_requests, profiler, platform=self.platform
                )
            
          
                
            Outh_schema = "Entities"   
            if sql_config.include_Outh:
                yield from self.loop_Oauth(inspector,Outh_schema,sql_config)    
            
                
            
           
                             
            

        # Clean up stale entities.
        yield from self.stale_entity_removal_handler.gen_removed_entity_workunits()

    
    def standardize_schema_table_names(
        self, schema: str, entity: str
    ) -> Tuple[str, str]:
        # Some SQLAlchemy dialects need a standardization step to clean the schema
        # and table names. See BigQuery for an example of when this is useful.
       
        return schema, entity

    def get_identifier(
        self, *, schema: str, entity: str, inspector: Inspector, **kwargs: Any
    ) -> str:  
       
        # Many SQLAlchemy dialects have three-level hierarchies. This method, which
        # subclasses can override, enables them to modify the identifiers as needed.
        if hasattr(self.config, "get_identifier"):
            # This path is deprecated and will eventually be removed.
          
            return self.config.get_identifier(schema=schema, table=entity)  # type: ignore
        else:
            
            return f"{schema}.{entity}"

    def get_foreign_key_metadata(
        self,
        dataset_urn: str,
        schema: str,
        fk_dict: Dict[str, str],
        inspector: Inspector,
    ) -> ForeignKeyConstraint:
        referred_schema: Optional[str] = fk_dict.get("referred_schema")

        if not referred_schema:
            referred_schema = schema

        referred_dataset_name = self.get_identifier(
            schema=referred_schema,
            entity=fk_dict["referred_table"],
            inspector=inspector,
        )

        source_fields = [
            f"urn:li:schemaField:({dataset_urn},{f})"
            for f in fk_dict["constrained_columns"]
        ]
        foreign_dataset = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=referred_dataset_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )
        foreign_fields = [
            f"urn:li:schemaField:({foreign_dataset},{f})"
            for f in fk_dict["referred_columns"]
        ]

        return ForeignKeyConstraint(
            fk_dict["name"], foreign_fields, source_fields, foreign_dataset
        )

    def normalise_dataset_name(self, dataset_name: str) -> str:
        return dataset_name

    def _gen_domain_urn(self, dataset_name: str) -> Optional[str]:
        domain_urn: Optional[str] = None

        for domain, pattern in self.config.domain.items():
            if pattern.allowed(dataset_name):
                domain_urn = make_domain_urn(
                    self.domain_registry.get_domain_urn(domain)
                )

        return domain_urn

    def _get_domain_wu(
        self,
        dataset_name: str,
        entity_urn: str,
        entity_type: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[MetadataWorkUnit]:

        domain_urn = self._gen_domain_urn(dataset_name)
        if domain_urn:
            wus = add_domain_to_entity_wu(
                entity_type=entity_type,
                entity_urn=entity_urn,
                domain_urn=domain_urn,
            )
            for wu in wus:
                self.report.report_workunit(wu)
                yield wu

    def loop_tables(  # noqa: C901
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        tables_seen: Set[str] = set()
        try:
            table_tags = self.get_extra_tags(inspector, schema, "table")
            
            for table in inspector.get_table_names(schema):
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

                try:
                    yield from self._process_table(
                        dataset_name, inspector, schema, table, sql_config, table_tags
                    )
                except Exception as e:
                    logger.warning(
                        f"Unable to ingest {schema}.{table} due to an exception.\n {traceback.format_exc()}"
                    )
                    self.report.report_warning(
                        f"{schema}.{table}", f"Ingestion error: {e}"
                    )
        except Exception as e:
            self.report.report_failure(f"{schema}", f"Tables error: {e}")

    def loop_projections(  # noqa: C901
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        Projections_seen: Set[str] = set()
        try:
            projection_tags = self.get_extra_tags(inspector, schema, "projection")
            
            for projection in inspector.get_projection_names(schema):
               
                schema, projection = self.standardize_schema_table_names(
                    schema=schema, entity=projection
                )
                dataset_name = self.get_identifier(
                    schema=schema, entity=projection, inspector=inspector
                )

                dataset_name = self.normalise_dataset_name(dataset_name)

                if dataset_name not in Projections_seen:
                    Projections_seen.add(dataset_name)
                else:
                    logger.debug(f"{dataset_name} has already been seen, skipping...")
                    continue

                self.report.report_entity_scanned(dataset_name, ent_type="projection")
                if not sql_config.table_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue

                try:
                    yield from self._process_projections(
                        dataset_name, inspector, schema, projection, sql_config, projection_tags
                    )
                except Exception as e:
                    logger.warning(
                        f"Unable to ingest {schema}.{projection} due to an exception.\n {traceback.format_exc()}"
                    )
                    self.report.report_warning(
                        f"{schema}.{projection}", f"Ingestion error: {e}"
                    )
        except Exception as e:
            self.report.report_failure(f"{schema}", f"Tables error: {e}")
        
        
    def loop_models(  # noqa: C901
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        models_seen: Set[str] = set()
        try:
            # models_tags = self.get_extra_tags(inspector, schema, "table")
            
            for models in inspector.get_models_names(schema):
                
                schema, models = self.standardize_schema_table_names(
                    schema=schema, entity=models
                )
                dataset_name = self.get_identifier(
                    schema="Entities", entity=models, inspector=inspector
                )
               
                dataset_name = self.normalise_dataset_name(dataset_name)

                if dataset_name not in models_seen:
                    models_seen.add(dataset_name)
                else:
                    logger.debug(f"{dataset_name} has already been seen, skipping...")
                    continue

                self.report.report_entity_scanned(dataset_name, ent_type="models")
                if not sql_config.table_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue

                try:
                    yield from self._process_models(
                        dataset_name, inspector, schema, models, sql_config,                    )
                except Exception as e:
                    logger.warning(
                        f"Unable to ingest {schema}.{models} due to an exception.\n {traceback.format_exc()}"
                    )
                    self.report.report_warning(
                        f"{schema}.{models}", f"Ingestion error: {e}"
                    )
        except Exception as e:
            self.report.report_failure(f"{schema}", f"Tables error: {e}")    
    
    def loop_Oauth(  # noqa: C901
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        oauth_seen: Set[str] = set()
        try:
            # models_tags = self.get_extra_tags(inspector, schema, "table")
            
            for OAuth in inspector.get_Oauth_names(schema):
              
                schema, OAuth = self.standardize_schema_table_names(
                    schema=schema, entity=OAuth
                )
                dataset_name = self.get_identifier(
                    schema=schema, entity=OAuth, inspector=inspector
                )
               
                dataset_name = self.normalise_dataset_name(dataset_name)

                if dataset_name not in oauth_seen:
                    oauth_seen.add(dataset_name)
                else:
                    logger.debug(f"{dataset_name} has already been seen, skipping...")
                    continue

                self.report.report_entity_scanned(dataset_name, ent_type="OAuth")
                if not sql_config.oauth_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue

                try:
                    yield from self._process_Oauth(
                        dataset_name, inspector, schema, OAuth, sql_config,                    )
                except Exception as e:
                    logger.warning(
                        f"Unable to ingest {schema}.{OAuth} due to an exception.\n {traceback.format_exc()}"
                    )
                    self.report.report_warning(
                        f"{schema}.{OAuth}", f"Ingestion error: {e}"
                    )
        except Exception as e:
            self.report.report_failure(f"{schema}", f"Tables error: {e}") 
            
    def _process_Oauth(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        OAuth: str,
        sql_config: SQLAlchemyConfig,

    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        # columns = self._get_columns(dataset_name, inspector, schema, table)
        columns = []
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
        # Add table to the checkpoint state
        self.stale_entity_removal_handler.add_entity_to_state("OAuth", dataset_urn)
        description, properties, location_urn = self.get_oauth_properties(
            inspector, schema, OAuth
        )

        # Tablename might be different from the real table if we ran some normalisation ont it.
        # Getting normalized table name from the dataset_name
        # Table is the last item in the dataset name
        normalised_table = OAuth
        splits = dataset_name.split(".")
        if splits:
            normalised_table = splits[-1]
            if properties and normalised_table != OAuth:
                properties["original_table_name"] = OAuth

        dataset_properties = DatasetPropertiesClass(
            name=normalised_table,
            description=description,
            customProperties=properties,
        )
        dataset_snapshot.aspects.append(dataset_properties)

       

        # extra_tags = self.get_extra_tags(inspector, schema, table)
        extra_tags = list()
        pk_constraints: dict = inspector.get_pk_constraint(OAuth, schema)
        
        foreign_keys = self._get_foreign_keys(dataset_urn, inspector, schema, OAuth)
    
        schema_fields = self.get_schema_fields(
            dataset_name, columns, pk_constraints, tags=extra_tags
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
        
        # table_tags = self.get_extra_tags(inspector, schema, table)
        
        tags_to_add = []
        # if table_tags:
        #     tags_to_add.extend(
        #         [make_tag_urn(f"{table_tags.get(table)}")]
        #     )
        #     yield self.gen_tags_aspect_workunit(dataset_urn, tags_to_add)
            
        yield from self.add_table_to_schema_container(dataset_urn, db_name, schema)
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
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=dataset_urn,
                aspectName="subTypes",
                aspect=SubTypesClass(typeNames=["OAuth"]),
            ),
        )
        self.report.report_workunit(subtypes_aspect)
        yield subtypes_aspect
       
        yield from self._get_domain_wu(
            dataset_name=dataset_name,
            entity_urn=dataset_urn,
            entity_type="dataset",
            sql_config=sql_config,
        )   
    
    def _process_projections(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        projection: str,
        sql_config: SQLAlchemyConfig,
        table_tags: Dict[str,str] = dict()
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        columns = self._get_columns(dataset_name, inspector, schema, projection)
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
        # Add table to the checkpoint state
        self.stale_entity_removal_handler.add_entity_to_state("projection", dataset_urn)
        description, properties, location_urn = self.get_projection_properties(
            inspector, schema, projection
        )

        # Tablename might be different from the real table if we ran some normalisation ont it.
        # Getting normalized table name from the dataset_name
        # Table is the last item in the dataset name
        normalised_table = projection
        splits = dataset_name.split(".")
        if splits:
            normalised_table = splits[-1]
            if properties and normalised_table != projection:
                properties["original_table_name"] = projection

        dataset_properties = DatasetPropertiesClass(
            name=normalised_table,
            description=description,
            customProperties=properties,
        )
        dataset_snapshot.aspects.append(dataset_properties)


        # extra_tags = self.get_extra_tags(inspector, schema, table)
        extra_tags = list()
        pk_constraints: dict = inspector.get_pk_constraint(projection, schema)
        
        foreign_keys = self._get_foreign_keys(dataset_urn, inspector, schema, projection)
    
        schema_fields = self.get_schema_fields(
            dataset_name, columns, pk_constraints, tags=extra_tags
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
        
        # table_tags = self.get_extra_tags(inspector, schema, table)
        
        tags_to_add = []
        if table_tags:
            tags_to_add.extend(
                [make_tag_urn(f"{table_tags.get(projection)}")]
            )
            yield self.gen_tags_aspect_workunit(dataset_urn, tags_to_add)
            
        yield from self.add_table_to_schema_container(dataset_urn, db_name, schema)
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
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=dataset_urn,
                aspectName="subTypes",
                aspect=SubTypesClass(typeNames=["Projections"]),
            ),
        )
        self.report.report_workunit(subtypes_aspect)
        yield subtypes_aspect

        yield from self._get_domain_wu(
            dataset_name=dataset_name,
            entity_urn=dataset_urn,
            entity_type="dataset",
            sql_config=sql_config,
        )

        
          
        
    def add_information_for_schema(self, inspector: Inspector, schema: str) -> None:
        pass

    def get_extra_tags(
        self, inspector: Inspector, schema: str, table: str
    ) -> Optional[Dict[str, str]]:
        try:
           
            tags = inspector._get_extra_tags(table, schema)
          
            return tags
        except Exception as e:
            print("Exception : ", e)


    def _process_table(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        table: str,
        sql_config: SQLAlchemyConfig,
        table_tags: Dict[str,str] = dict()
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        columns = self._get_columns(dataset_name, inspector, schema, table)
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
        # Add table to the checkpoint state
        self.stale_entity_removal_handler.add_entity_to_state("table", dataset_urn)
        description, properties, location_urn = self.get_table_properties(
            inspector, schema, table
        )

        # Tablename might be different from the real table if we ran some normalisation ont it.
        # Getting normalized table name from the dataset_name
        # Table is the last item in the dataset name
        normalised_table = table
        splits = dataset_name.split(".")
        if splits:
            normalised_table = splits[-1]
            if properties and normalised_table != table:
                properties["original_table_name"] = table

        dataset_properties = DatasetPropertiesClass(
            name=normalised_table,
            description=description,
            customProperties=properties,
        )
        dataset_snapshot.aspects.append(dataset_properties)
        

        # extra_tags = self.get_extra_tags(inspector, schema, table)
        extra_tags = list()
        pk_constraints: dict = inspector.get_pk_constraint(table, schema)
        
        foreign_keys = self._get_foreign_keys(dataset_urn, inspector, schema, table)
    
        schema_fields = self.get_schema_fields(
            dataset_name, columns, pk_constraints, tags=extra_tags
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
        
        # table_tags = self.get_extra_tags(inspector, schema, table)
        
        tags_to_add = []
        if table_tags:
            tags_to_add.extend(
                [make_tag_urn(f"{table_tags.get(table)}")]
            )
            yield self.gen_tags_aspect_workunit(dataset_urn, tags_to_add)
            
        yield from self.add_table_to_schema_container(dataset_urn, db_name, schema)
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
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=dataset_urn,
                aspectName="subTypes",
                aspect=SubTypesClass(typeNames=["table"]),
            ),
        )
        self.report.report_workunit(subtypes_aspect)
        yield subtypes_aspect

        yield from self._get_domain_wu(
            dataset_name=dataset_name,
            entity_urn=dataset_urn,
            entity_type="dataset",
            sql_config=sql_config,
        )

    
    
    def _process_models(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        table: str,
        sql_config: SQLAlchemyConfig,

    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        # columns = self._get_columns(dataset_name, inspector, schema, table)
        columns = []
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
        # Add table to the checkpoint state
        self.stale_entity_removal_handler.add_entity_to_state("model", dataset_urn)
        description, properties, location_urn = self.get_model_properties(
            inspector, schema, table
        )

        # Tablename might be different from the real table if we ran some normalisation ont it.
        # Getting normalized table name from the dataset_name
        # Table is the last item in the dataset name
        normalised_table = table
        splits = dataset_name.split(".")
        if splits:
            normalised_table = splits[-1]
            if properties and normalised_table != table:
                properties["original_table_name"] = table

        dataset_properties = DatasetPropertiesClass(
            name=normalised_table,
            description=description,
            customProperties=properties,
        )
        dataset_snapshot.aspects.append(dataset_properties)

       

        # extra_tags = self.get_extra_tags(inspector, schema, table)
        extra_tags = list()
        pk_constraints: dict = inspector.get_pk_constraint(table, schema)
        
        foreign_keys = self._get_foreign_keys(dataset_urn, inspector, schema, table)
        
        # dataset_names = dataset_name.split(".")
        # dataset_names[0] = "Entities"
        # name = ".".join(dataset_names)
        # print(name)

        schema_fields = self.get_schema_fields(
            dataset_name, columns, pk_constraints, tags=extra_tags
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
        
        # table_tags = self.get_extra_tags(inspector, schema, table)
        
        tags_to_add = []
        # if table_tags:
        #     tags_to_add.extend(
        #         [make_tag_urn(f"{table_tags.get(table)}")]
        #     )
        #     yield self.gen_tags_aspect_workunit(dataset_urn, tags_to_add)
            
        yield from self.add_table_to_schema_container(dataset_urn, db_name, schema)
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
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=dataset_urn,
                aspectName="subTypes",
                aspect=SubTypesClass(typeNames=["ML Models"]),
            ),
        )
        self.report.report_workunit(subtypes_aspect)
        yield subtypes_aspect

        yield from self._get_domain_wu(
            dataset_name=dataset_name,
            entity_urn=dataset_urn,
            entity_type="dataset",
            sql_config=sql_config,
        )
        

        
    def gen_tags_aspect_workunit(
        self, dataset_urn: str, tags_to_add: List[str]
    ) -> MetadataWorkUnit:
        tags = GlobalTagsClass(
            tags=[TagAssociationClass(tag_to_add) for tag_to_add in tags_to_add]
        )
        wu = wrap_aspect_as_workunit("dataset", dataset_urn, "globalTags", tags)
        self.report.report_workunit(wu)
        return wu
    
    def get_table_properties(
        self, inspector: Inspector, schema: str, table: str
    ) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
        description: Optional[str] = None
        properties: Dict[str, str] = {}

        # The location cannot be fetched generically, but subclasses may override
        # this method and provide a location.
        location: Optional[str] = None

        try:
            # SQLAlchemy stubs are incomplete and missing this method.
            # PR: https://github.com/dropbox/sqlalchemy-stubs/pull/223.
            table_info: dict = inspector.get_table_comment(table, schema)  # type: ignore
        except NotImplementedError:
            return description, properties, location
        except ProgrammingError as pe:
            # Snowflake needs schema names quoted when fetching table comments.
            logger.debug(
                f"Encountered ProgrammingError. Retrying with quoted schema name for schema {schema} and table {table}",
                pe,
            )
            table_info: dict = inspector.get_table_comment(table, f'"{schema}"')  # type: ignore

        description = table_info.get("text")
        if type(description) is tuple:
            # Handling for value type tuple which is coming for dialect 'db2+ibm_db'
            description = table_info["text"][0]

        # The "properties" field is a non-standard addition to SQLAlchemy's interface.
        properties = table_info.get("properties", {})
        return description, properties, location
    
    
    def get_projection_properties(
        self, inspector: Inspector, schema: str, projection: str
    ) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
        description: Optional[str] = None
        properties: Dict[str, str] = {}

        # The location cannot be fetched generically, but subclasses may override
        # this method and provide a location.
        location: Optional[str] = None

        try:
            # SQLAlchemy stubs are incomplete and missing this method.
            # PR: https://github.com/dropbox/sqlalchemy-stubs/pull/223.
            projection_info: dict = inspector.get_projection_comment(projection, schema)  # type: ignore
        except NotImplementedError:
            return description, properties, location
        except ProgrammingError as pe:
            logger.debug(
                f"Encountered ProgrammingError. Retrying with quoted schema name for schema {schema} and table {properties}",
                pe,
            )
            projection_info: dict = inspector.get_projection_comment(properties, f'"{schema}"')  # type: ignore

        description = projection_info.get("text")
        if type(description) is tuple:
            # Handling for value type tuple which is coming for dialect 'db2+ibm_db'
            description = projection_info["text"][0]

        # The "properties" field is a non-standard addition to SQLAlchemy's interface.
        properties = projection_info.get("properties", {})
        return description, properties, location


    def get_model_properties(
        self, inspector: Inspector, schema: str, model: str
    ) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
        description: Optional[str] = None
        properties: Dict[str, str] = {}

        # The location cannot be fetched generically, but subclasses may override
        # this method and provide a location.
        location: Optional[str] = None

        try:
            # SQLAlchemy stubs are incomplete and missing this method.
            # PR: https://github.com/dropbox/sqlalchemy-stubs/pull/223.
            table_info: dict = inspector.get_model_comment(model, schema)  # type: ignore
        except NotImplementedError:
            return description, properties, location
        except ProgrammingError as pe:
            # Snowflake needs schema names quoted when fetching table comments.
            logger.debug(
                f"Encountered ProgrammingError. Retrying with quoted schema name for schema {schema} and table {model}",
                pe,
            )
            table_info: dict = inspector.get_model_comment(model, f'"{schema}"')  # type: ignore

        description = table_info.get("text")
        if type(description) is tuple:
            # Handling for value type tuple which is coming for dialect 'db2+ibm_db'
            description = table_info["text"][0]

        # The "properties" field is a non-standard addition to SQLAlchemy's interface.
        properties = table_info.get("properties", {})
        return description, properties, location
    
    def get_dataplatform_instance_aspect(
        self, dataset_urn: str
    ) -> Optional[SqlWorkUnit]:
        # If we are a platform instance based source, emit the instance aspect
        if self.config.platform_instance:
            mcp = MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=dataset_urn,
                aspectName="dataPlatformInstance",
                aspect=DataPlatformInstanceClass(
                    platform=make_data_platform_urn(self.platform),
                    instance=make_dataplatform_instance_urn(
                        self.platform, self.config.platform_instance
                    ),
                ),
            )
            wu = SqlWorkUnit(id=f"{dataset_urn}-dataPlatformInstance", mcp=mcp)
            self.report.report_workunit(wu)
            return wu
        else:
            return None

    def get_oauth_properties(
        self, inspector: Inspector, schema: str, model: str
    ) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
        description: Optional[str] = None
        properties: Dict[str, str] = {}

        # The location cannot be fetched generically, but subclasses may override
        # this method and provide a location.
        location: Optional[str] = None

        try:
            # SQLAlchemy stubs are incomplete and missing this method.
            # PR: https://github.com/dropbox/sqlalchemy-stubs/pull/223.
            table_info: dict = inspector.get_oauth_comment(model, schema)  # type: ignore
        except NotImplementedError:
            return description, properties, location
        except ProgrammingError as pe:
            # Snowflake needs schema names quoted when fetching table comments.
            logger.debug(
                f"Encountered ProgrammingError. Retrying with quoted schema name for schema {schema} and table {model}",
                pe,
            )
            table_info: dict = inspector.get_oauth_comment(model, f'"{schema}"')  # type: ignore

        description = table_info.get("text")
        if type(description) is tuple:
            # Handling for value type tuple which is coming for dialect 'db2+ibm_db'
            description = table_info["text"][0]

        # The "properties" field is a non-standard addition to SQLAlchemy's interface.
        properties = table_info.get("properties", {})
        return description, properties, location
    
    def get_dataplatform_instance_aspect(
        self, dataset_urn: str
    ) -> Optional[SqlWorkUnit]:
        # If we are a platform instance based source, emit the instance aspect
        if self.config.platform_instance:
            mcp = MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=dataset_urn,
                aspectName="dataPlatformInstance",
                aspect=DataPlatformInstanceClass(
                    platform=make_data_platform_urn(self.platform),
                    instance=make_dataplatform_instance_urn(
                        self.platform, self.config.platform_instance
                    ),
                ),
            )
            wu = SqlWorkUnit(id=f"{dataset_urn}-dataPlatformInstance", mcp=mcp)
            self.report.report_workunit(wu)
            return wu
        else:
            return None
    
    def _get_columns(
        self, dataset_name: str, inspector: Inspector, schema: str, table: str
    ) -> List[dict]:
        columns = []
        try:
            columns = inspector.get_columns(table, schema)
            if len(columns) == 0:
                self.report.report_warning(MISSING_COLUMN_INFO, dataset_name)
        except Exception as e:
            self.report.report_warning(
                dataset_name,
                f"unable to get column information due to an error -> {e}",
            )
        return columns
    
    

    def _get_foreign_keys(
        self, dataset_urn: str, inspector: Inspector, schema: str, table: str
    ) -> List[ForeignKeyConstraint]:
        try:
            foreign_keys = [
                self.get_foreign_key_metadata(dataset_urn, schema, fk_rec, inspector)
                for fk_rec in inspector.get_foreign_keys(table, schema)
            ]
        except KeyError:
            # certain databases like MySQL cause issues due to lower-case/upper-case irregularities
            logger.debug(
                f"{dataset_urn}: failure in foreign key extraction... skipping"
            )
            foreign_keys = []
        return foreign_keys

    def get_schema_fields(
        self,
        dataset_name: str,
        columns: List[dict],
        pk_constraints: dict = None,
        tags: Optional[Dict[str, List[str]]] = None,
    ) -> List[SchemaField]:
        canonical_schema = []
        for column in columns:
            column_tags: Optional[List[str]] = None
        
            if tags:
                column_tags = tags.get(column["name"], [])
            fields = self.get_schema_fields_for_column(
                dataset_name, column, pk_constraints, tags=column_tags
            )
            canonical_schema.extend(fields)
        return canonical_schema

    def get_schema_fields_for_column(
        self,
        dataset_name: str,
        column: dict,
        pk_constraints: dict = None,
        tags: Optional[List[str]] = None,
    ) -> List[SchemaField]:
        gtc: Optional[GlobalTagsClass] = None
        if tags:
            tags_str = [make_tag_urn(t) for t in tags]
            tags_tac = [TagAssociationClass(t) for t in tags_str]
            gtc = GlobalTagsClass(tags_tac)
        field = SchemaField(
            fieldPath=column["name"],
            type=get_column_type(self.report, dataset_name, column["type"]),
            nativeDataType=column.get("full_type", repr(column["type"])),
            description=column.get("comment", None),
            nullable=column["nullable"],
            recursive=False,
            globalTags=gtc,
        )
        if (
            pk_constraints is not None
            and isinstance(pk_constraints, dict)  # some dialects (hive) return list
            and column["name"] in pk_constraints.get("constrained_columns", [])
        ):
            field.isPartOfKey = True
        return [field]

    def loop_views(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        try:
            table_tags = self.get_extra_tags(inspector, schema, "view")
            for view in inspector.get_view_names(schema):
                schema, view = self.standardize_schema_table_names(
                    schema=schema, entity=view
                )
                dataset_name = self.get_identifier(
                    schema=schema, entity=view, inspector=inspector
                )
                dataset_name = self.normalise_dataset_name(dataset_name)

                self.report.report_entity_scanned(dataset_name, ent_type="view")

                if not sql_config.view_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue
                
                try:
                    
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
                    lineage_info = self._get_upstream_lineage_info(dataset_urn,view)
                    print("_______________________________________________________________________________FF",lineage_info)
                    if lineage_info is not None:
                                # Emit the lineage work unit
                                upstream_column_props = []
                                upstream_lineage = lineage_info
                                lineage_mcpw = MetadataChangeProposalWrapper(
                                    entityType="dataset",
                                    changeType=ChangeTypeClass.UPSERT,
                                    entityUrn=dataset_snapshot.urn,
                                    aspectName="upstreamLineage",
                                    aspect=upstream_lineage,
                                )
                                print(lineage_mcpw)
                                lineage_wu = MetadataWorkUnit(
                                    id=f"{self.platform}-{lineage_mcpw.entityUrn}-{lineage_mcpw.aspectName}",
                                    mcp=lineage_mcpw,
                                )
                                self.report.report_workunit(lineage_wu)
                                yield lineage_wu
                                
                except Exception as e:
                    logger.warning(
                        f"Unable to get lieange of view {schema}.{view} due to an exception.\n {traceback.format_exc()}"
                    )
                    self.report.report_warning(
                        f"{schema}.{view}", f"Ingestion error: {e}"
                    )

                try:
                    yield from self._process_view(
                        dataset_name=dataset_name,
                        inspector=inspector,
                        schema=schema,
                        view=view,
                        sql_config=sql_config,
                        table_tags=table_tags,
                    )
                except Exception as e:
                    logger.warning(
                        f"Unable to ingest view {schema}.{view} due to an exception.\n {traceback.format_exc()}"
                    )
                    self.report.report_warning(
                        f"{schema}.{view}", f"Ingestion error: {e}"
                    )
        except Exception as e:
            self.report.report_failure(f"{schema}", f"Views error: {e}")

    def _process_view(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        view: str,
        sql_config: SQLAlchemyConfig,
        table_tags: Dict[str,str] = dict()
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        try:
            columns = inspector.get_columns(view, schema)
        except KeyError:
            # For certain types of views, we are unable to fetch the list of columns.
            self.report.report_warning(
                dataset_name, "unable to get schema for this view"
            )
            schema_metadata = None
        else:
            # extra_tags = self.get_extra_tags(inspector, schema, view)
            extra_tags = dict()
            schema_fields = self.get_schema_fields(dataset_name, columns, tags=extra_tags)
            schema_metadata = get_schema_metadata(
                self.report,
                dataset_name,
                self.platform,
                columns,
                canonical_schema=schema_fields,
            )
        description, properties, location_urn = self.get_table_properties(
            inspector, schema, view
        )
        try:
            view_definition = inspector.get_view_definition(view, schema)
            if view_definition is None:
                view_definition = ""
            else:
                # Some dialects return a TextClause instead of a raw string,
                # so we need to convert them to a string.
                view_definition = str(view_definition)
   
        except NotImplementedError:
            view_definition = ""
        properties["view_definition"] = view_definition
        properties["is_view"] = "True"
       
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
        db_name = self.get_db_name(inspector)
        yield from self.add_table_to_schema_container(dataset_urn, db_name, schema)
        
        # view lineage 
        

        # table_tags = self.get_extra_tags(inspector, schema, table)
        tags_to_add = []
        if table_tags:
            tags_to_add.extend(
                [make_tag_urn(f"{table_tags.get(view)}")]
            )
        
            yield self.gen_tags_aspect_workunit(dataset_urn, tags_to_add)
            
        # Add view to the checkpoint state
        self.stale_entity_removal_handler.add_entity_to_state("view", dataset_urn)
        

        dataset_properties = DatasetPropertiesClass(
            name=view,
            description=description,
            customProperties=properties,
        )
        dataset_snapshot.aspects.append(dataset_properties)
        if schema_metadata:
            dataset_snapshot.aspects.append(schema_metadata)
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        wu = SqlWorkUnit(id=dataset_name, mce=mce)
        self.report.report_workunit(wu)
        yield wu
        dpi_aspect = self.get_dataplatform_instance_aspect(dataset_urn=dataset_urn)
        if dpi_aspect:
            yield dpi_aspect
        subtypes_aspect = MetadataWorkUnit(
            id=f"{view}-subtypes",
            mcp=MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=dataset_urn,
                aspectName="subTypes",
                aspect=SubTypesClass(typeNames=["view"]),
            ),
        )
        self.report.report_workunit(subtypes_aspect)
        yield subtypes_aspect
        if "view_definition" in properties:
            view_definition_string = properties["view_definition"]
            view_properties_aspect = ViewPropertiesClass(
                materialized=False, viewLanguage="SQL", viewLogic=view_definition_string
            )
            view_properties_wu = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=view_properties_aspect,
            ).as_workunit()
            self.report.report_workunit(view_properties_wu)
            yield view_properties_wu

        yield from self._get_domain_wu(
            dataset_name=dataset_name,
            entity_urn=dataset_urn,
            entity_type="dataset",
            sql_config=sql_config,
        )

    def get_parent_container_key(self, db_name: str, schema: str) -> PlatformKey:
        return self.gen_schema_key(db_name, schema)

    def add_table_to_schema_container(
        self, dataset_urn: str, db_name: str, schema: str
    ) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        parent_container_key = self.get_parent_container_key(db_name, schema)
        container_workunits = add_dataset_to_container(
            container_key=parent_container_key,
            dataset_urn=dataset_urn,
        )
        for wu in container_workunits:
            self.report.report_workunit(wu)
            yield wu

    def get_profiler_instance(self, inspector: Inspector) -> "DatahubGEProfiler":
        from datahub.ingestion.source.ge_data_profiler import DatahubGEProfiler

        return DatahubGEProfiler(
            conn=inspector.bind,
            report=self.report,
            config=self.config.profiling,
            platform=self.platform,
        )

    def get_profile_args(self) -> Dict:
        """Passed down to GE profiler"""
        return {}

    # Override if needed
    def generate_partition_profiler_query(
        self, schema: str, table: str, partition_datetime: Optional[datetime.datetime]
    ) -> Tuple[Optional[str], Optional[str]]:
        return None, None

    def is_table_partitioned(
        self, database: Optional[str], schema: str, table: str
    ) -> Optional[bool]:
        return None

    # Override if needed
    def generate_profile_candidates(
        self,
        inspector: Inspector,
        threshold_time: Optional[datetime.datetime],
        schema: str,
    ) -> Optional[List[str]]:
        raise NotImplementedError()

    # Override if you want to do additional checks
    def is_dataset_eligible_for_profiling(
        self,
        dataset_name: str,
        sql_config: SQLAlchemyConfig,
        inspector: Inspector,
        profile_candidates: Optional[List[str]],
    ) -> bool:
        return (
            sql_config.table_pattern.allowed(dataset_name)
            and sql_config.profile_pattern.allowed(dataset_name)
        ) and (
            sql_config.projection_pattern.allowed(dataset_name)
            and sql_config.profile_pattern.allowed(dataset_name)
        )and (
            profile_candidates is None
            or (profile_candidates is not None and dataset_name in profile_candidates)
        )

    def loop_profiler_requests_for_projections(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable["GEProfilerRequest"]:
        from datahub.ingestion.source.ge_data_profiler import GEProfilerRequest

        tables_seen: Set[str] = set()
        profile_candidates = None  # Default value if profile candidates not available.
        if (
            sql_config.profiling.profile_if_updated_since_days is not None
            or sql_config.profiling.profile_table_size_limit is not None
            or sql_config.profiling.profile_table_row_limit is None
        ):
            try:
                threshold_time: Optional[datetime.datetime] = None
                if sql_config.profiling.profile_if_updated_since_days is not None:
                    threshold_time = datetime.datetime.now(
                        datetime.timezone.utc
                    ) - datetime.timedelta(
                        sql_config.profiling.profile_if_updated_since_days
                    )
                profile_candidates = self.generate_profile_candidates(
                    inspector, threshold_time, schema
                )
            except NotImplementedError:
                logger.debug("Source does not support generating profile candidates.")

        for projection in inspector.get_projections_columns(schema):
               
            schema, projection = self.standardize_schema_table_names(
                schema=schema, entity=projection
            )
            dataset_name = self.get_identifier(
                schema=schema, entity=projection, inspector=inspector
            )
            
            
            if not self.is_dataset_eligible_for_profiling(
                dataset_name, sql_config, inspector, profile_candidates
            ):
                if self.config.profiling.report_dropped_profiles:
                    self.report.report_dropped(f"profile of {dataset_name}")
                continue

            dataset_name = self.normalise_dataset_name(dataset_name)

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


    def loop_profiler_requests(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable["GEProfilerRequest"]:
        from datahub.ingestion.source.ge_data_profiler import GEProfilerRequest

        tables_seen: Set[str] = set()
        profile_candidates = None  # Default value if profile candidates not available.
        if (
            sql_config.profiling.profile_if_updated_since_days is not None
            or sql_config.profiling.profile_table_size_limit is not None
            or sql_config.profiling.profile_table_row_limit is None
        ):
            try:
                threshold_time: Optional[datetime.datetime] = None
                if sql_config.profiling.profile_if_updated_since_days is not None:
                    threshold_time = datetime.datetime.now(
                        datetime.timezone.utc
                    ) - datetime.timedelta(
                        sql_config.profiling.profile_if_updated_since_days
                    )
                profile_candidates = self.generate_profile_candidates(
                    inspector, threshold_time, schema
                )
            except NotImplementedError:
                logger.debug("Source does not support generating profile candidates.")

        for table in inspector.get_table_names(schema):
            schema, table = self.standardize_schema_table_names(
                schema=schema, entity=table
            )
            dataset_name = self.get_identifier(
                schema=schema, entity=table, inspector=inspector
            )
            if not self.is_dataset_eligible_for_profiling(
                dataset_name, sql_config, inspector, profile_candidates
            ):
                if self.config.profiling.report_dropped_profiles:
                    self.report.report_dropped(f"profile of {dataset_name}")
                continue

            dataset_name = self.normalise_dataset_name(dataset_name)

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
                schema, table, self.config.profiling.partition_datetime
            )

            if partition is None and self.is_table_partitioned(
                database=None, schema=schema, table=table
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
                f"Preparing profiling request for {schema}, {table}, {partition}"
            )
            
           
            yield GEProfilerRequest(
                pretty_name=dataset_name,
                batch_kwargs=self.prepare_profiler_args(
                    inspector=inspector,
                    schema=schema,
                    table=table,
                    partition=partition,
                    custom_sql=custom_sql,
                ),
            )

    def loop_profiler(
        self,
        profile_requests: List["GEProfilerRequest"],
        profiler: "DatahubGEProfiler",
        platform: Optional[str] = None,
    ) -> Iterable[MetadataWorkUnit]:
        for request, profile in profiler.generate_profiles(
            profile_requests,
            self.config.profiling.max_workers,
            platform=platform,
            profiler_args=self.get_profile_args(),
        ):
            if profile is None:
                continue
            dataset_name = request.pretty_name
            dataset_urn = make_dataset_urn_with_platform_instance(
                self.platform,
                dataset_name,
                self.config.platform_instance,
                self.config.env,
            )
            mcp = MetadataChangeProposalWrapper(
                entityType="dataset",
                entityUrn=dataset_urn,
                changeType=ChangeTypeClass.UPSERT,
                aspectName="datasetProfile",
                aspect=profile,
            )
            wu = MetadataWorkUnit(id=f"profile-{dataset_name}", mcp=mcp)
            self.report.report_workunit(wu)

            yield wu

    def prepare_profiler_args(
        self,
        inspector: Inspector,
        schema: str,
        table: str,
        partition: Optional[str],
        custom_sql: Optional[str] = None,
    ) -> dict:
        return dict(
            schema=schema, table=table, partition=partition, custom_sql=custom_sql
        )
        
        
    def _get_upstream_lineage_info(
        self, dataset_urn: str,view
    ) -> Optional[Tuple[UpstreamLineage, Dict[str, str]]]:
        
        
       
        dataset_key = builder.dataset_urn_to_key(dataset_urn)
        if dataset_key is None:
            logger.warning(f"Invalid dataset urn {dataset_urn}. Could not get key!")
            return None

        if self._lineage_map is None:
            # self._populate_lineage()
            self._populate_view_lineage(view)
        # if self._external_lineage_map is None:
        #     self._populate_external_lineage()
        
        # assert self._lineage_map is not None
        # assert self._external_lineage_map is not None
        
        dataset_name = dataset_key.name
        lineage = self._lineage_map[dataset_name]
       
      
      
        # self._lineage_map = {"SQL_VIEWS.Customer_v" : [("A", "B", "C")], "cognos_schema.VINT" : [("Aman",), ("Bishal",), ("C",), ("D",)],
        #                      'cognos_schema.VNUM' : [("A", "B", "C")], 'cognos_schema.VRL': [("Aman",), ("Bishal",), ("C",), ("D",)],
        #                      "cognos_schema.VFLT" : [("A", "B", "C")], 'cognos_schema.VBINT' : [("Aman",), ("Bishal",), ("C",), ("D",)],
        #                      'cognos_schema.VBOOL' : [("A", "B", "C")], 'cognos_schema.VRL': [("Aman",), ("Bishal",), ("C",), ("D",)]}
        # lineage = self._lineage_map[dataset_name]
        # external_lineage = self._external_lineage_map[dataset_name]
        if not (lineage):
            logger.debug(f"No lineage found for {dataset_name}")
            return None
        upstream_tables: List[UpstreamClass] = []
        # column_lineage: Dict[str, str] = {}
        print("i reched here",lineage)
        for lineage_entry in lineage:
            # Update the table-lineage
            print("______________________________________________KKKKKKKKKKKKK",lineage_entry)
            upstream_table_name = lineage_entry[0]
            # if not self._is_dataset_allowed(upstream_table_name):
            #     continue
            upstream_table = UpstreamClass(
                dataset=builder.make_dataset_urn_with_platform_instance(
                    self.platform,
                    upstream_table_name,
                    self.config.platform_instance,
                    self.config.env,
                ),
                type=DatasetLineageTypeClass.TRANSFORMED,
            )
            upstream_tables.append(upstream_table)
            # Update column-lineage for each down-stream column.
            # upstream_columns = [
            #     d["columnName"].lower() for d in json.loads(lineage_entry[1])
            # ]
            # downstream_columns = [
            #     d["columnName"].lower() for d in json.loads(lineage_entry[2])
            # ]
            # upstream_column_str = (
            #     f"{upstream_table_name}({', '.join(sorted(upstream_columns))})"
            # )
            # downstream_column_str = (
            #     f"{dataset_name}({', '.join(sorted(downstream_columns))})"
            # )
            # column_lineage_key = f"column_lineage[{upstream_table_name}]"
            # column_lineage_value = (
            #     f"{{{upstream_column_str} -> {downstream_column_str}}}"
            # )
            # column_lineage[column_lineage_key] = column_lineage_value
            # logger.debug(f"{column_lineage_key}:{column_lineage_value}")

       

        if upstream_tables:
            print("_______________________i reached here __________________________________")
            logger.debug(
                f"Upstream lineage of '{dataset_name}': {[u.dataset for u in upstream_tables]}"
            )
            # if self.config.upstream_lineage_in_report:
            #     self.report.upstream_lineage[dataset_name] = [
            #         u.dataset for u in upstream_tables
            #     ]
            return UpstreamLineage(upstreams=upstream_tables)
        return None

    def _populate_view_lineage(self,view) -> None:
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **self.config.options)
        print("================================i got called")
        get_refrence_table = sql.text(dedent("""
            select reference_table_name 
            from v_catalog.view_tables                                
            where table_name = '%(view)s'
        """ % {'view': view }))
        
        refrence_table = ""
        for data in engine.execute(get_refrence_table):
            # refrence_table.append(data)
            refrence_table = data['reference_table_name']
            
        view_upstream_lineage_query = sql.text(dedent("""
            select reference_table_name ,reference_table_schema
            from v_catalog.view_tables 
            where table_name = '%(view)s'
        """ % {'view': view }))
        
        view_downstream_query= sql.text(dedent("""
            select table_name ,table_schema
            from v_catalog.view_tables 
            where reference_table_name = '%(view)s'
        """ % {'view': refrence_table }))

        num_edges: int = 0
      
        try:
            self._lineage_map = defaultdict(list)
            for db_row_key in engine.execute(view_downstream_query):
         
                downstream=f"{db_row_key['table_schema']}.{db_row_key['table_name']}"
    
               
                for db_row_value in engine.execute(view_upstream_lineage_query):
                  
            
                    upstream = f"{db_row_value['reference_table_schema']}.{db_row_value['reference_table_name']}"
                    
                
                    view_upstream: str = upstream.lower()
                    view_name: str = downstream.lower()
                    
                  
                    self._lineage_map[view_name].append(
                    # (<upstream_table_name>, <empty_json_list_of_upstream_table_columns>, <empty_json_list_of_downstream_view_columns>)
                    (view_upstream, "[]", "[]")
                    )
                    
                   
                    num_edges += 1
        
        except Exception as e:
            traceback.print_exc()
            self.warn(
                logger,
                "view_upstream_lineage",
                "Extracting the upstream view lineage from Snowflake failed."
                + f"Please check your permissions. Continuing...\nError was {e}.",
            )
            
        logger.info(f"A total of {num_edges} View upstream edges found.")
        self.report.num_table_to_view_edges_scanned = num_edges
        # self._populate_view_upstream_lineage(engine,view)
        # self._populate_view_downstream_lineage(engine,view)
        
        
    def _populate_view_upstream_lineage(self, engine: sqlalchemy.engine.Engine,view) -> None:
        # NOTE: This query captures only the upstream lineage of a view (with no column lineage).
        
        get_refrence_table = sql.text(dedent("""
            select reference_table_name 
            from v_catalog.view_tables                                
            where table_name = '%(view)s'
        """ % {'view': view }))

        refrence_table = engine.execute(get_refrence_table)
        view_upstream_lineage_query = sql.text(dedent("""
            select reference_table_name 
            from v_catalog.view_tables 
            where table_name = '%(view)s'
        """ % {'view': refrence_table }))


        # assert self._lineage_map is not None
        num_edges: int = 0

        try:
            for db_row in engine.execute(view_upstream_lineage_query):
                # Process UpstreamTable/View/ExternalTable/Materialized View->View edge.
                view_upstream: str = db_row["reference_table_name"].lower()
                # view_name: str = db_row["downstream_view"].lower()
                if not self._is_dataset_allowed(dataset_name=view, is_view=True):
                    continue
                # key is the downstream view name
                self._lineage_map[view].append(
                    # (<upstream_table_name>, <empty_json_list_of_upstream_table_columns>, <empty_json_list_of_downstream_view_columns>)
                    (view_upstream, "[]", "[]")
                )
                num_edges += 1
                logger.debug(
                    f"Upstream->View: Lineage[View(Down)={view}]:Upstream={view_upstream}"
                )
        except Exception as e:
            self.warn(
                logger,
                "view_upstream_lineage",
                "Extracting the upstream view lineage from Snowflake failed."
                + f"Please check your permissions. Continuing...\nError was {e}.",
            )
        logger.info(f"A total of {num_edges} View upstream edges found.")
        self.report.num_table_to_view_edges_scanned = num_edges
    
    
    def _populate_view_downstream_lineage(
        self, engine: sqlalchemy.engine.Engine,view
    ) -> None:
        # This query captures the downstream table lineage for views.
        # See https://docs.snowflake.com/en/sql-reference/account-usage/access_history.html#usage-notes for current limitations on capturing the lineage for views.
        # Eg: For viewA->viewB->ViewC->TableD, snowflake does not yet log intermediate view logs, resulting in only the viewA->TableD edge.
        view_lineage_query= sql.text(dedent("""
            select table_name 
            from v_catalog.view_tables 
            where reference_table_name = '%(view)s'
        """ % {'view': view }))

        # assert self._lineage_map is not None
        self.report.num_view_to_table_edges_scanned = 0

        try:
            db_rows = engine.execute(view_lineage_query)
        except Exception as e:
            self.warn(
                logger,
                "view_downstream_lineage",
                f"Extracting the view lineage from Snowflake failed."
                f"Please check your permissions. Continuing...\nError was {e}.",
            )
        else:
            for db_row in db_rows:
                view_name: str = view
                if not self._is_dataset_allowed(dataset_name=view_name, is_view=True):
                    continue
                downstream_table: str = (
                    db_row["table_name"].lower().replace('"', "")
                )
                # Capture view->downstream table lineage.
                self._lineage_map[downstream_table].append(
                    # (<upstream_view_name>, <json_list_of_upstream_view_columns>, <json_list_of_downstream_columns>)
                    (
                        view_name,
                        db_row["table_name"],
                      
                    )
                )
                self.report.num_view_to_table_edges_scanned += 1

                logger.debug(
                    f"View->Table: Lineage[Table(Down)={downstream_table}]:View(Up)={self._lineage_map[downstream_table]}"
                )

        logger.info(
            f"Found {self.report.num_view_to_table_edges_scanned} View->Table edges."
        )
    
    
    def get_report(self):
        return self.report

    def close(self):
        self.prepare_for_commit()
        
        
