from __future__ import absolute_import, unicode_literals, print_function, division

import re
from sqlalchemy import exc
from sqlalchemy import sql
from sqlalchemy import util
from textwrap import dedent

from sqlalchemy.dialects.postgresql import BYTEA, DOUBLE_PRECISION, INTERVAL
from sqlalchemy.dialects.postgresql.base import PGDialect, PGDDLCompiler
from sqlalchemy.engine import reflection
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.types import INTEGER, BIGINT, SMALLINT, VARCHAR, CHAR, \
    NUMERIC, FLOAT, REAL, DATE, DATETIME, BOOLEAN, BLOB, TIMESTAMP, TIME
from sqlalchemy.sql import sqltypes
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
    cast,
)
ischema_names = {
    'INT': INTEGER,
    'INTEGER': INTEGER,
    'INT8': INTEGER,
    'BIGINT': BIGINT,
    'SMALLINT': SMALLINT,
    'TINYINT': SMALLINT,
    'CHAR': CHAR,
    'VARCHAR': VARCHAR,
    'VARCHAR2': VARCHAR,
    'TEXT': VARCHAR,
    'NUMERIC': NUMERIC,
    'DECIMAL': NUMERIC,
    'NUMBER': NUMERIC,
    'MONEY': NUMERIC,
    'FLOAT': FLOAT,
    'FLOAT8': FLOAT,
    'REAL': REAL,
    'DOUBLE': DOUBLE_PRECISION,
    'TIMESTAMP': TIMESTAMP,
    'TIMESTAMP WITH TIMEZONE': TIMESTAMP,
    'TIMESTAMPTZ': TIMESTAMP(timezone=True),
    'TIME': TIME,
    'TIME WITH TIMEZONE': TIME,
    'TIMETZ': TIME(timezone=True),
    'INTERVAL': INTERVAL,
    'DATE': DATE,
    'DATETIME': DATETIME,
    'SMALLDATETIME': DATETIME,
    'BINARY': BLOB,
    'VARBINARY': BLOB,
    'RAW': BLOB,
    'BYTEA': BYTEA,
    'BOOLEAN': BOOLEAN,
    'LONG VARBINARY': BLOB,
    'LONG VARCHAR': VARCHAR,
    'GEOMETRY': BLOB,
}


class VerticaDDLCompiler(PGDDLCompiler):
    def get_column_specification(self, column, **kwargs):
        colspec = self.preparer.format_column(column)
        # noinspection PyUnusedLocal
        impl_type = column.type.dialect_impl(self.dialect)
        # noinspection PyProtectedMember
        if column.primary_key and column is column.table._autoincrement_column:
            colspec += " AUTO_INCREMENT"
        else:
            colspec += " " + self.dialect.type_compiler.process(column.type)
            default = self.get_column_default_string(column)
            if default is not None:
                colspec += " DEFAULT " + default

        if not column.nullable:
            colspec += " NOT NULL"
        return colspec


# noinspection PyArgumentList,PyAbstractClass
class VerticaDialect(PGDialect):
    name = 'vertica'
    ischema_names = ischema_names
    ddl_compiler = VerticaDDLCompiler

    # def _get_default_schema_name(self, connection):
    #     return connection.scalar("SELECT current_schema()")

    def _get_server_version_info(self, connection):
        v = connection.scalar("SELECT version()")
        m = re.match(r".*Vertica Analytic Database v(\d+)\.(\d+)\.(\d)+.*", v)
        if not m:
            raise AssertionError("Could not determine version from string '%(ver)s'" % {'ver': v})
        return tuple([int(x) for x in m.group(1, 2, 3) if x is not None])

    # # noinspection PyRedeclaration
    def _get_default_schema_name(self, connection):
        return connection.scalar("SELECT current_schema()")

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user')
        opts.update(url.query)
        return [], opts

    def has_schema(self, connection, schema):
        has_schema_sql = sql.text(dedent("""
            SELECT EXISTS (
            SELECT schema_name
            FROM v_catalog.schemata
            WHERE lower(schema_name) = '%(schema)s')
        """ % {'schema': schema.lower()}))

        c = connection.execute(has_schema_sql)
        return bool(c.scalar())

    def has_table(self, connection, table_name, schema=None):
        if schema is None:
            schema = self._get_default_schema_name(connection)

        has_table_sql = sql.text(dedent("""
            SELECT EXISTS (
            SELECT table_name
            FROM v_catalog.all_tables
            WHERE lower(table_name) = '%(table)s'
            AND lower(schema_name) = '%(schema)s')
        """ % {'schema': schema.lower(), 'table': table_name.lower()}))

        c = connection.execute(has_table_sql)
        return bool(c.scalar())

    def has_sequence(self, connection, sequence_name, schema=None):
        if schema is None:
            schema = self._get_default_schema_name(connection)

        has_seq_sql = sql.text(dedent("""
            SELECT EXISTS (
            SELECT sequence_name
            FROM v_catalog.sequences
            WHERE lower(sequence_name) = '%(sequence)s'
            AND lower(sequence_schema) = '%(schema)s')
        """ % {'schema': schema.lower(), 'sequence': sequence_name.lower()}))

        c = connection.execute(has_seq_sql)
        return bool(c.scalar())

    def has_type(self, connection, type_name, schema=None):
        has_type_sql = sql.text(dedent("""
            SELECT EXISTS (
            SELECT type_name
            FROM v_catalog.types
            WHERE lower(type_name) = '%(type)s')
        """ % {'type': type_name.lower()}))

        c = connection.execute(has_type_sql)
        return bool(c.scalar())

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        get_schemas_sql = sql.text(dedent("""
            SELECT schema_name
            FROM v_catalog.schemata
        """))

        c = connection.execute(get_schemas_sql)
        return [row[0] for row in c if not row[0].startswith('v_')]

    @reflection.cache
    def get_table_comment(self, connection, table_name, schema=None, **kw):
     
        if schema is None:
            schema_conditional = ""
        else:
            schema_conditional = "AND object_schema = '{schema}'".format(schema=schema)
        query = """
            SELECT
                comment
            FROM
                v_catalog.comments
            WHERE
                object_type = 'TABLE'
                AND
                object_name = :table_name
                {schema_conditional}
        """.format(schema_conditional=schema_conditional)
     
        c = connection.execute(sql.text(query), table_name=table_name)
        
        return {"text": c.scalar()}
    
    @reflection.cache
    def get_projection_comment(self, connection, projection_name, schema=None, **kw):
        return {"text": "This is a test for projections"}

    @reflection.cache
    def get_table_oid(self, connection, table_name, schema=None, **kw):
        if schema is None:
            schema = self._get_default_schema_name(connection)

        get_oid_sql = sql.text(dedent("""
            SELECT A.table_id
            FROM
                (SELECT table_id, table_name, table_schema FROM v_catalog.tables
                    UNION
                 SELECT table_id, table_name, table_schema FROM v_catalog.views) AS A
            WHERE lower(A.table_name) = '%(table)s'
            AND lower(A.table_schema) = '%(schema)s'
        """ % {'schema': schema.lower(), 'table': table_name.lower()}))

        c = connection.execute(get_oid_sql)
        table_oid = c.scalar()

        if table_oid is None:
            raise exc.NoSuchTableError(table_name)
        return table_oid

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        if schema is not None:
            schema_condition = "lower(table_schema) = '%(schema)s'" % {'schema': schema.lower()}
        else:
            schema_condition = "1"

        get_tables_sql = sql.text(dedent("""
            SELECT table_name
            FROM v_catalog.tables
            WHERE %(schema_condition)s
            ORDER BY table_schema, table_name
        """ % {'schema_condition': schema_condition}))

        c = connection.execute(get_tables_sql)
        return [row[0] for row in c]

    @reflection.cache
    def get_temp_table_names(self, connection, schema=None, **kw):
        if schema is not None:
            schema_condition = "lower(table_schema) = '%(schema)s'" % {'schema': schema.lower()}
        else:
            schema_condition = "1"

        get_tables_sql = sql.text(dedent("""
                SELECT table_name
                FROM v_catalog.tables
                WHERE %(schema_condition)s
                AND IS_TEMP_TABLE
                ORDER BY table_schema, table_name
            """ % {'schema_condition': schema_condition}))

        c = connection.execute(get_tables_sql)
        return [row[0] for row in c]

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        if schema is not None:
            schema_condition = "lower(table_schema) = '%(schema)s'" % {'schema': schema.lower()}
        else:
            schema_condition = "1"

        get_views_sql = sql.text(dedent("""
            SELECT table_name
            FROM v_catalog.views
            WHERE %(schema_condition)s
            ORDER BY table_schema, table_name
        """ % {'schema_condition': schema_condition}))
        

        c = connection.execute(get_views_sql)
        return [row[0] for row in c]

    @reflection.cache 
    def get_projection_names(self, connection, schema=None, **kw):
        if schema is not None:
            schema_condition = "lower(projection_schema) = '%(schema)s'" % {'schema': schema.lower()}
        else:
            schema_condition = "1"
        
        
        get_projection_sql = sql.text(dedent("""
            SELECT projection_name
            FROM v_catalog.projections
            WHERE %(schema_condition)s
            ORDER BY projection_schema, projection_name
        """ % {'schema_condition': schema_condition}))
        

        c = connection.execute(get_projection_sql)
        # for row in connection.execute(get_projection_sql):
        #     print("",row)
        return [row[0] for row in c]
       
    
    @reflection.cache
    def get_temp_view_names(self, connection, schema=None, **kw):
        return []

    # @reflection.cache
    # def _get_schema_keys(self, connection, db_name, schema=None, **kw):

    #     return None
    
    @reflection.cache
    def _get_extra_tags(self, connection, table, schema=None, **kw):

        if schema is not None:
            schema_condition = "lower(table_schema) = '%(schema)s'" % {'schema': schema.lower()}
        else:
            schema_condition = "1"
        return None

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        if schema is not None:
            schema_condition = "lower(table_schema) = '%(schema)s'" % {'schema': schema.lower()}
        else:
            schema_condition = "1"

        s = sql.text(dedent("""
            SELECT column_name, data_type, column_default, is_nullable
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
                schema,
                primary_key
            )

            
            column_info.update({'primary_key': primary_key})
            columns.append(column_info)
       
        return columns



    @reflection.cache
    def get_projection(self, connection , projection_name, schema=None, **kw):
        if schema is not None:
            schema_condition = "lower(projection_schema) = '%(schema)s'" % {'schema': schema.lower()}
        else:
            schema_condition = "1"
            
        return []
            
        
    @reflection.cache
    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        if schema is None:
            schema = self._get_default_schema_name(connection)

        get_constrains_sql = sql.text(dedent("""
            SELECT constraint_name, column_name
            FROM v_catalog.constraint_columns
            WHERE lower(table_name) = '%(table)s'
            -- AND constraint_type IN ('p', 'u')
            AND lower(table_schema) = '%(schema)s'
        """ % {'schema': schema.lower(), 'table': table_name.lower()}))

        c = connection.execute(get_constrains_sql)
        if c.rowcount <= 0:
            return []

        constraints, columns = zip(*c)
        result_dict = {
            unique_con: [col for con, col in zip(constraints, columns) if con == unique_con]
            for unique_con in set(constraints)
            }

        return [{"name": name, "column_names": cols} for name, cols in result_dict.items()]

    @reflection.cache
    def get_check_constraints(
            self, connection, table_name, schema=None, **kw):
        table_oid = self.get_table_oid(connection, table_name, schema,
                                       info_cache=kw.get('info_cache'))

        constraints_sql = sql.text(dedent("""
            SELECT constraint_name, column_name
            FROM v_catalog.constraint_columns
            WHERE table_id = %(oid)s
            AND constraint_type = 'c'
        """ % {'oid': table_oid}))

        c = connection.execute(constraints_sql)

        return [{'name': name, 'sqltext': col} for name, col in c.fetchall()]

    def normalize_name(self, name):
        name = name and name.rstrip()
        if name is None:
            return None
        return name.lower()

    def denormalize_name(self, name):
        return name

    # methods allows table introspection to work
    @reflection.cache
    def get_pk_constraint(self, bind, table_name, schema=None, **kw):
        return {'constrained_columns': [], 'name': 'undefined'}

    

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    @reflection.cache
    def get_indexes(self, connection, table_name, schema, **kw):
        return []

    # Disable index creation since that's not a thing in Vertica.
    # noinspection PyUnusedLocal
    def visit_create_index(self, create):
        return None

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
                    WHERE %(schema_condition)s
                    """
                    % {"view_name": view_name, "schema_condition": schema_condition}
                )
            )
        )
 
        return view_def





    def _get_column_info(
        self,
        name,
        format_type,
        default,
        nullable,
        schema,
        primary_key
    ):

        # strip (*) from character varying(5), timestamp(5)
        # with time zone, geometry(POLYGON), etc.
        attype = re.sub(r"\(.*\)", "", format_type)

        charlen = re.search(r"\(([\d,]+)\)", format_type)
        if charlen:
            charlen = charlen.group(1)
        args = re.search(r"\((.*)\)", format_type)
        if args and args.group(1):
            args = tuple(re.split(r"\s*,\s*", args.group(1)))
        else:
            args = ()
        kwargs = {}
        
        if attype == "numeric":
            if charlen:
                prec, scale = charlen.split(",")
                args = (int(prec), int(scale))
            else:
                args = ()
        elif attype == "integer":
            args = ()
        elif attype in ("timestamptz", "timetz"):
            kwargs["timezone"] = True
            if charlen:
                kwargs["precision"] = int(charlen)
            args = ()
        elif attype in (
            "timestamp",
            "time",
        ):
            kwargs["timezone"] = False
            if charlen:
                kwargs["precision"] = int(charlen)
            args = ()
        elif attype.startswith("interval"):
            field_match = re.match(r"interval (.+)", attype, re.I)
            if charlen:
                kwargs["precision"] = int(charlen)
            if field_match:
                kwargs["fields"] = field_match.group(1)
            attype = "interval"
            args = ()
        elif charlen:
            args = (int(charlen),)

        while True:
            if attype.upper() in self.ischema_names:
                coltype = self.ischema_names[attype.upper()]
                break
            else:
                coltype = None
                break

        if coltype:
            coltype = coltype(*args, **kwargs)
        else:
            util.warn(
                "Did not recognize type '%s' of column '%s'" % (attype, name)
            )
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
            nullable=nullable,
            default=default,
            primary_key = primary_key,
            autoincrement=autoincrement,
        )
      
        return column_info
