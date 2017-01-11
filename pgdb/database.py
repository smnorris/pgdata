from __future__ import absolute_import
from __future__ import print_function
import os
import glob

try:
    from urllib.parse import urlparse
except ImportError:
     from urlparse import urlparse

from sqlalchemy import create_engine
#import psycopg2
#from psycopg2 import extras

from sqlalchemy.pool import NullPool

from .util import row_type
from .table import Table
import six


class Database(object):
    def __init__(self, url, schema=None, row_type=row_type, sql_path='sql'):
        self.url = url
        u = urlparse(url)
        self.database = u.path[1:]
        self.user = u.username
        self.password = u.password
        self.host = u.hostname
        self.port = u.port
        self.sql_path = sql_path
        # use null pool to ensure the db object can be used by multiprocessing
        # http://docs.sqlalchemy.org/en/latest/faq/connections.html#how-do-i-use-engines-connections-sessions-with-python-multiprocessing-or-os-fork
        self.engine = create_engine(url, poolclass=NullPool)
        #self.conn = self._get_connection()
        self.schema = schema
        self.row_type = row_type
        self.queries = self.load_queries(sql_path)

    @property
    def schemas(self):
        """
        Get a listing of all non-system schemas (prefixed with 'pg_') that
        exist in the database.
        """
        sql = """SELECT schema_name FROM information_schema.schemata
                 ORDER BY schema_name"""
        schemas = self.query(sql).fetchall()
        return [s[0] for s in schemas if s[0][:3] != 'pg_']

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
                tables = tables +  \
                          [schema+"."+t for t in self.tables_in_schema(schema)]
            return tables

    """
    def _get_connection(self):
        c = psycopg2.connect(database=self.database,
                             user=self.user,
                             password=self.password,
                             host=self.host,
                             port=self.port,
                             cursor_factory=extras.DictCursor)
        c.autocommit = True
        return c
    """

    def print_notices(self):
        for notice in self.psycopg2_conn.notices:
            print(notice)

    def __getitem__(self, table):
        if table in self.tables:
            return self.load_table(table)
        # if table doesn't exist, return empty table object
        else:
            return Table(self, "public", None)

    #def _get_cursor(self):
    #    return self.conn.cursor()

    def _valid_table_name(self, table):
        """Check if the table name is obviously invalid.
        """
        if table is None or not len(table.strip()):
            raise ValueError("Invalid table name: %r" % table)
        return table.strip()

    def load_queries(self, path):
        """Load stored queries from specified path and return a dict
        """
        if os.path.exists(path):
            sqlfiles = glob.glob(os.path.join(path, "*.sql"))
            queries = {}
            for filename in sqlfiles:
                with open(filename, 'rb') as f:
                    key = os.path.splitext(os.path.basename(filename))[0]
                    queries[key] = six.text_type(f.read())
            return queries
        else:
            return {}

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
            sql = sql.replace('$'+key, val)
        return sql

    def tables_in_schema(self, schema):
        """Get a listing of all tables in given schema
        """
        sql = """SELECT table_name
                 FROM information_schema.tables
                 WHERE table_schema = %s"""
        return [t[0] for t in self.query(sql, (schema,)).fetchall()]

    def parse_table_name(self, table):
        """parse schema qualified table name
        """
        if "." in table:
            schema, table = table.split('.')
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

    def execute(self, sql, params=None):
        """
        Just a pointer to engine.execute
        """
        #return self._get_cursor().execute(sql, params)
        return self.engine.execute(sql, params)

    def execute_many(self, sql, params):
        """Wrapper for executemany.
        """
        #self._get_cursor().executemany(sql, params)
        self.engine.executemany(sql, params)

    def query(self, sql, params=None):
        """Another word for execute
        """
        #cur = self._get_cursor()
        #cur.execute(sql, params)
        #return cur.fetchall()
        return self.engine.execute(sql, params)

    def query_one(self, sql, params=None):
        """Grab just one record
        """
        #cur = self._get_cursor()
        #cur.execute(sql, params)
        #return cur.fetchone()
        r = self.engine.execute(sql, params)
        return r.fetchone()

    def create_schema(self, schema):
        """Create specified schema if it does not already exist
        """
        if schema not in self.schemas:
            sql = "CREATE SCHEMA "+schema
            self.execute(sql)

    def drop_schema(self, schema, cascade=False):
        """Drop specified schema
        """
        if schema in self.schemas:
            sql = "DROP SCHEMA "+schema
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
