from functools import cache
import re
from textwrap import dedent
from typing import Any, Dict

import pydantic
from pydantic.class_validators import validator
from sqlalchemy import sql, util
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.sqltypes import TIME, TIMESTAMP, String
from sqlalchemy_vertica.base import VerticaDialect
from sqlalchemy.engine import reflection
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.source.sql.sql_common import (
    BasicSQLAlchemyConfig,
    SQLAlchemySource,
)
from datahub.utilities import config_clean

class UUID(String):
    """The SQL UUID type."""

    __visit_name__ = "UUID"


def TIMESTAMP_WITH_TIMEZONE(*args, **kwargs):
    kwargs["timezone"] = True
    return TIMESTAMP(*args, **kwargs)


def TIME_WITH_TIMEZONE(*args, **kwargs):
    kwargs["timezone"] = True
    return TIME(*args, **kwargs)


def get_view_definition(self, connection, view_name, schema=None, **kw):
  
    if schema is not None:
        schema_condition = "lower(table_schema) = '%(schema)s'" % {
            "schema": schema.lower()
        }
    else:
        schema_condition = "1"

    view_def = connection.scalar(
        sql.text(
            dedent(
                """
                SELECT VIEW_DEFINITION
                FROM V_CATALOG.VIEWS
                WHERE table_name='%(view_name)s' AND %(schema_condition)s
                """
                % {"view_name": view_name, "schema_condition": schema_condition}
            )
        )
    )

    return view_def





def get_columns(self, connection, table_name, schema=None, **kw):
    if schema is not None:
        schema_condition = "lower(table_schema) = '%(schema)s'" % {'schema': schema.lower()}
    else:
        schema_condition = "1"

    s = sql.text(dedent("""
        SELECT column_name, data_type, column_default,is_nullable
        FROM v_catalog.columns
        WHERE lower(table_name) = '%(table)s'
        AND %(schema_condition)s
        UNION ALL
        SELECT column_name, data_type, '' as column_default, true as is_nullable
        FROM v_catalog.view_columns
        WHERE lower(table_name) = '%(table)s'
        AND %(schema_condition)s
        """ % {'table': table_name.lower(), 'schema_condition': schema_condition}))

    spk = sql.text(dedent("""
            SELECT column_name
            FROM v_catalog.primary_keys
            WHERE lower(table_name) = '%(table)s'
            AND constraint_type = 'p'
            AND %(schema_condition)s
        """ % {'table': table_name.lower(), 'schema_condition': schema_condition}))


    pk_columns = [x[0] for x in connection.execute(spk)]
    
    columns = []
    for row in connection.execute(s):
            
            name = row.column_name 
            dtype = row.data_type.lower()
            primary_key = name in pk_columns
            default = row.column_default
            nullable = row.is_nullable
          
            column_info = self._get_column_info(
                name,
                dtype,
                default,
                nullable,
                schema
                
                
                

            )
            
            
            column_info.update({'primary_key': primary_key})
            columns.append(column_info)

           
    print(columns)
    return columns


def _get_column_info(  # noqa: C901
    self, name, data_type, default,is_nullable ,schema=None,
):

    attype: str = re.sub(r"\(.*\)", "", data_type)

    charlen = re.search(r"\(([\d,]+)\)", data_type)
    if charlen:
        charlen = charlen.group(1)  # type: ignore
    args = re.search(r"\((.*)\)", data_type)
    if args and args.group(1):
        args = tuple(re.split(r"\s*,\s*", args.group(1)))  # type: ignore
    else:
        args = ()  # type: ignore
    kwargs: Dict[str, Any] = {}

    if attype == "numeric":
        if charlen:
            prec, scale = charlen.split(",")  # type: ignore
            args = (int(prec), int(scale))  # type: ignore
        else:
            args = ()  # type: ignore
    elif attype == "integer":
        args = ()  # type: ignore
    elif attype in ("timestamptz", "timetz"):
        kwargs["timezone"] = True
        if charlen:
            kwargs["precision"] = int(charlen)  # type: ignore
        args = ()  # type: ignore
    elif attype in ("timestamp", "time"):
        kwargs["timezone"] = False
        if charlen:
            kwargs["precision"] = int(charlen)  # type: ignore
        args = ()  # type: ignore
    elif attype.startswith("interval"):
        field_match = re.match(r"interval (.+)", attype, re.I)
        if charlen:
            kwargs["precision"] = int(charlen)  # type: ignore
        if field_match:
            kwargs["fields"] = field_match.group(1)  # type: ignore
        attype = "interval"
        args = ()  # type: ignore
    elif attype == "date":
        args = ()  # type: ignore
    elif charlen:
        args = (int(charlen),)  # type: ignore

    while True:
        if attype.upper() in self.ischema_names:
            coltype = self.ischema_names[attype.upper()]
            break
        else:
            coltype = None
            break

    self.ischema_names["UUID"] = UUID
    self.ischema_names["TIMESTAMPTZ"] = TIMESTAMP_WITH_TIMEZONE
    self.ischema_names["TIMETZ"] = TIME_WITH_TIMEZONE

    if coltype:
        coltype = coltype(*args, **kwargs)
    else:
        util.warn("Did not recognize type '%s' of column '%s'" % (attype, name))
        coltype = sqltypes.NULLTYPE
    # adjust the default value
    autoincrement = False
    if default is not None:
        match = re.search(r"""(nextval\(')([^']+)('.*$)""", default)
        if match is not None:
            if issubclass(coltype._type_affinity, sqltypes.Integer):
                autoincrement = True
            # the default is related to a Sequence
            sch = schema
            if "." not in match.group(2) and sch is not None:
                # unconditionally quote the schema name.  this could
                # later be enhanced to obey quoting rules /
                # "quote schema"
                default = (
                    match.group(1)
                    + ('"%s"' % sch)
                    + "."
                    + match.group(2)
                    + match.group(3)
                )
    


    column_info = dict(
        name=name,
        type=coltype,
        nullable=is_nullable,
        default=default,
        comment = "owner" + " " + "create_time",
        autoincrement=autoincrement,
    )

    return column_info





VerticaDialect.get_view_definition = get_view_definition
VerticaDialect.get_columns = get_columns
VerticaDialect._get_column_info = _get_column_info

class VerticaConfig(BasicSQLAlchemyConfig):
    # defaults
    scheme: str = pydantic.Field(default="vertica+vertica_python")

    @validator("host_port")
    def clean_host_port(cls, v):
        return config_clean.remove_protocol(v)


@platform_name("Vertica")
@config_class(VerticaConfig)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
class VerticaSource(SQLAlchemySource):
    def __init__(self, config: VerticaConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx, "vertica")

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "VerticaSource":
        config = VerticaConfig.parse_obj(config_dict)
        return cls(config, ctx)
