from __future__ import absolute_import
from __future__ import print_function
import os
import glob
import subprocess
import tempfile
from xml.sax.saxutils import escape

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

from .util import row_type
from .util import QueryDict
from .table import Table
import six


class Database(object):
    def __init__(
        self, url, schema=None, row_type=row_type, sql_path=None, multiprocessing=False
    ):
        self.url = url
        u = urlparse(url)
        db, user, password, host, port = (
            u.path[1:],
            u.username,
            u.password,
            u.hostname,
            u.port,
        )
        self.database = db
        self.user = user
        self.password = password
        self.host = host
        self.port = u.port
        self.ogr_string = f"PG:host={host} user={user} dbname={db} port={port}"
        if self.password:
            self.ogr_string = self.ogr_string + f" password={password}"
        self.sql_path = sql_path
        self.multiprocessing = multiprocessing
        # use null pool to ensure the db object can be used by multiprocessing
        # http://docs.sqlalchemy.org/en/latest/faq/connections.html#how-do-i-use-engines-connections-sessions-with-python-multiprocessing-or-os-fork
        if self.multiprocessing:
            self.engine = create_engine(url, poolclass=NullPool)
        else:
            self.engine = create_engine(url)
        self.schema = schema
        self.row_type = row_type
        self.queries = QueryDict(path=self.sql_path)

    @property
    def schemas(self):
        """
        Get a listing of all non-system schemas (prefixed with 'pg_') that
        exist in the database.
        """
        sql = """SELECT schema_name FROM information_schema.schemata
                 ORDER BY schema_name"""
        schemas = self.query(sql).fetchall()
        return [s[0] for s in schemas if s[0][:3] != "pg_"]

    @property
    def tables(self):
        """
        Get a listing of all tables
          - if schema specified on connect, return unqualifed table names in
            that schema
          - in no schema specified on connect, return all tables, with schema
            prefixes
        """
        if self.schema:
            return self.tables_in_schema(self.schema)
        else:
            tables = []
            for schema in self.schemas:
                tables = tables + [
                    schema + "." + t for t in self.tables_in_schema(schema)
                ]
            return tables

    def print_notices(self):
        for notice in self.psycopg2_conn.notices:
            print(notice)

    def __getitem__(self, table):
        if table in self.tables:
            return self.load_table(table)
        # if table doesn't exist, return empty table object
        else:
            return Table(self, "public", None)

    def _valid_table_name(self, table):
        """Check if the table name is obviously invalid.
        """
        if table is None or not len(table.strip()):
            raise ValueError("Invalid table name: %r" % table)
        return table.strip()

    def build_query(self, sql, lookup):
        """
        Modify table and field name variables in a sql string with a dict.
        This seems to be discouraged by psycopg2 docs but it makes small
        adjustments to large sql strings much easier, making prepped queries
        much more versatile.

        USAGE
        sql = 'SELECT $myInputField FROM $myInputTable'
        lookup = {'myInputField':'customer_id', 'myInputTable':'customers'}
        sql = db.build_query(sql, lookup)

        """
        for key, val in six.iteritems(lookup):
            sql = sql.replace("$" + key, val)
        return sql

    def tables_in_schema(self, schema):
        """Get a listing of all tables in given schema
        """
        sql = """SELECT table_name
                 FROM information_schema.tables
                 WHERE table_schema = %s"""
        return [t[0] for t in self.query(sql, (schema,)).fetchall()]

    def parse_table_name(self, table):
        """Parse schema qualified table name
        """
        if "." in table:
            schema, table = table.split(".")
        else:
            schema = None
        return (schema, table)

    def load_table(self, table):
        """Loads a table. Returns None if the table does not already exist in db
        """
        table = self._valid_table_name(table)
        schema, table = self.parse_table_name(table)
        if not schema:
            schema = self.schema
            tables = self.tables
        else:
            tables = self.tables_in_schema(schema)
        if table in tables:
            return Table(self, schema, table)
        else:
            return None

    def mogrify(self, sql, params):
        """Return the query string with parameters added
        """
        conn = self.engine.raw_connection()
        cursor = conn.cursor()
        return cursor.mogrify(sql, params)

    def execute(self, sql, params=None):
        """Just a pointer to engine.execute
        """
        # wrap in a transaction to ensure things are committed
        # https://github.com/smnorris/pgdata/issues/3
        with self.engine.begin() as conn:
            result = conn.execute(sql, params)
        return result

    def execute_many(self, sql, params):
        """Wrapper for executemany.
        """
        self.engine.executemany(sql, params)

    def query(self, sql, params=None):
        """Another word for execute
        """
        return self.engine.execute(sql, params)

    def query_one(self, sql, params=None):
        """Grab just one record
        """
        r = self.engine.execute(sql, params)
        return r.fetchone()

    def create_schema(self, schema):
        """Create specified schema if it does not already exist
        """
        if schema not in self.schemas:
            sql = "CREATE SCHEMA " + schema
            self.execute(sql)

    def drop_schema(self, schema, cascade=False):
        """Drop specified schema
        """
        if schema in self.schemas:
            sql = "DROP SCHEMA " + schema
            if cascade:
                sql = sql + " CASCADE"
            self.execute(sql)

    def wipe_schema(self):
        """Delete all tables from current schema. Use with caution eh?
        """
        for t in self.tables:
            self[t].drop()

    def create_table(self, table, columns):
        """Creates a table
        """
        schema, table = self.parse_table_name(table)
        table = self._valid_table_name(table)
        if not schema:
            schema = self.schema
        if table in self.tables:
            return Table(self, schema, table)
        else:
            return Table(self, schema, table, columns)

    def ogr2pg(
        self,
        in_file,
        in_layer=None,
        out_layer=None,
        schema="public",
        s_srs=None,
        t_srs="EPSG:3005",
        sql=None,
        fid=None,
        fid64=False,
        dim=None,
        index=True,
        append=False,
        geom=True,
        cmd_only=False,
        cmd_as_list=False,
    ):
        """
        Load a layer to provided pgdata database connection using OGR2OGR

        -sql option is like an ESRI where_clause or the ogr2ogr -where option,
        but to increase flexibility, it is in SQLITE dialect:
        SELECT * FROM <in_layer> WHERE <sql>
        """
        # if not provided a layer name, use the name of the input file
        if not in_layer:
            in_layer = os.path.splitext(os.path.basename(in_file))[0]
        if not out_layer:
            out_layer = in_layer.lower()
        command = [
            "ogr2ogr",
            "-f",
            "PostgreSQL",
            "PG:host={h} port={p} user={u} dbname={db} password={pwd}".format(
                h=self.host,
                p=self.port,
                u=self.user,
                db=self.database,
                pwd=self.password,
            ),
            "-lco",
            "OVERWRITE=YES",
            "-overwrite",
            "-lco",
            "SCHEMA={schema}".format(schema=schema),
            "-nlt",
            "PROMOTE_TO_MULTI",
            "-nln",
            out_layer,
        ]
        if t_srs:
            command = command + ["-t_srs", t_srs]
        if geom:
            command = command + ["-lco", "GEOMETRY_NAME=geom"]
        if geom and dim:
            command = command + ["-dim", dim]
        if sql:
            command = command + [
                "-sql",
                "SELECT * FROM {} WHERE {}".format(in_layer, sql),
                "-dialect",
                "SQLITE",
            ]
        if s_srs:
            command = command + ["-s_srs", s_srs]
        if not index:
            command = command + ["-lco", "SPATIAL_INDEX=NONE"]
        if append:
            command = command + ["-update", "-append"]
        if fid:
            command = command + ["-lco", "FID={}".format(fid)]
        if fid64:
            command = command + ["-lco", "FID64=TRUE"]
        # add file name and layer at the end of list
        command = command + [in_file]
        # only add output layer name if sql not included (it gets ignored)
        if not sql:
            command = command + [in_layer]

        if cmd_only and not cmd_as_list:
            return " ".join(command)
        if cmd_only and cmd_as_list:
            return command
        else:
            subprocess.run(command)

    def pg2ogr(
        self,
        sql,
        driver,
        outfile,
        outlayer=None,
        column_remap=None,
        s_srs="EPSG:3005",
        t_srs=None,
        geom_type=None,
        append=False,
    ):
        """
        A wrapper around ogr2ogr, for quickly dumping a postgis query to file.
        Suppported formats are ["ESRI Shapefile", "GeoJSON", "FileGDB", "GPKG"]
           - for GeoJSON, transforms to EPSG:4326
           - for Shapefile, consider supplying a column_remap dict
           - for FileGDB, geom_type is required
             (https://trac.osgeo.org/gdal/ticket/4186)
        """
        if driver == "FileGDB" and geom_type is None:
            raise ValueError("Specify geom_type when writing to FileGDB")
        filename, ext = os.path.splitext(os.path.basename(outfile))
        if not outlayer:
            outlayer = filename
        u = urlparse(self.url)
        pgcred = "host={h} user={u} dbname={db} password={p}".format(
            h=u.hostname, u=u.username, db=u.path[1:], p=u.password
        )
        # use a VRT so we can remap columns if a lookoup is provided
        if column_remap:
            # if specifiying output field names, all fields have to be specified
            # rather than try and parse the input sql, just do a test run of the
            # query and grab column names from that
            columns = [c for c in self.query(sql).keys() if c != "geom"]
            # make sure all columns are represented in the remap
            for c in columns:
                if c not in column_remap.keys():
                    column_remap[c] = c
            field_remap_xml = " \n".join(
                [
                    '<Field name="' + column_remap[c] + '" src="' + c + '"/>'
                    for c in columns
                ]
            )
        else:
            field_remap_xml = ""
        vrt = """<OGRVRTDataSource>
                   <OGRVRTLayer name="{layer}">
                     <SrcDataSource>PG:{pgcred}</SrcDataSource>
                     <SrcSQL>{sql}</SrcSQL>
                   {fieldremap}
                   </OGRVRTLayer>
                 </OGRVRTDataSource>
              """.format(
            layer=outlayer,
            sql=escape(sql.replace("\n", " ")),
            pgcred=pgcred,
            fieldremap=field_remap_xml,
        )
        vrtpath = os.path.join(tempfile.gettempdir(), filename + ".vrt")
        if os.path.exists(vrtpath):
            os.remove(vrtpath)
        with open(vrtpath, "w") as vrtfile:
            vrtfile.write(vrt)
        # GeoJSON writes to EPSG:4326
        if driver == "GeoJSON" and not t_srs:
            t_srs = "EPSG:4326"
        # otherwise, default to BC Albers
        else:
            t_srs = "EPSG:3005"
        command = [
            "ogr2ogr",
            "-s_srs",
            s_srs,
            "-t_srs",
            t_srs,
            "-progress",
            "-f",
            driver,
            outfile,
            vrtpath,
        ]
        # if writing to gdb, specify geom type
        if driver == "FileGDB":
            command.insert(len(command), "-nlt")
            command.insert(len(command), geom_type)
        # automatically update existing multilayer outputs
        if driver in ("FileGDB", "GPKG") and os.path.exists(outfile):
            command.insert(len(command), "-update")
        # if specified, append to existing output
        if append:
            command.insert(len(command), "-append")
        subprocess.run(command)
