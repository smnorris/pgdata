import os
import glob
import urlparse
import csv

import psycopg2
from psycopg2 import extras

from sqlalchemy import create_engine
from sqlalchemy.schema import CreateSchema, DropSchema
from sqlalchemy.schema import MetaData
from sqlalchemy.schema import Table as SQLATable

from util import row_type
from table import Table


class Database(object):
    def __init__(self, url, schema=None, row_type=row_type):
        self.url = url
        u = urlparse.urlparse(url)
        self.database = u.path[1:]
        self.user = u.username
        self.password = u.password
        self.host = u.hostname
        self.port = u.port
        self.sqlPath = os.path.join(os.path.dirname(__file__), 'sql')
        self.queries = self._load_queries()
        self.schema = schema
        self.engine = create_engine(url)
        self.psycopg2 = self._get_connection()
        self.row_type = row_type

    @property
    def schemas(self):
        """
        Get a listing of all schemas that exist in the database.
        """
        sql = """SELECT schema_name FROM information_schema.schemata"""
        return [t[0] for t in self.query(sql)]
        #return self.insp.get_schema_names()

    @property
    def tables(self):
        """
        Get a listing of all tables in schema
        """
        return self.tables_in_schema(self.schema)
        #return self.insp.get_table_names(schema=self.schema)

    def _get_connection(self):
        c = psycopg2.connect(database=self.database,
                             user=self.user,
                             password=self.password,
                             host=self.host,
                             port=self.port)
        c.autocommit = True
        return c

    def __getitem__(self, table):
        return self.load_table(table)

    def _get_cursor(self):
        return self.psycopg2.cursor(cursor_factory=extras.DictCursor)

    def _valid_table_name(self, table):
        """ Check if the table name is obviously invalid. """
        if table is None or not len(table.strip()):
            raise ValueError("Invalid table name: %r" % table)
        return table.strip()

    def _load_queries(self):
        """ load stored queries """
        sqlfiles = glob.glob(os.path.join(self.sqlPath, "*.sql"))
        queries = {}
        for filename in sqlfiles:
            with open(filename, 'rb') as f:
                key = os.path.splitext(os.path.basename(filename))[0]
                queries[key] = unicode(f.read())
        return queries

    def tables_in_schema(self, schema):
        """
        Get a listing of all tables in given schema
        """
        sql = """SELECT table_name
                 FROM information_schema.tables
                 WHERE table_schema = %s"""
        return [t[0] for t in self.query(sql, (schema,))]
        #return self.insp.get_table_names(schema=schema)

    def parse_table_name(self, table):
        """
        parse schema qualified table name
        """
        if "." in table:
            schema, table = table.split('.')
        else:
            schema = None
        return (schema, table)

    def load_table(self, table):
        """
        Loads a table. Returns None if the table does not already exist in db
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
        Execute something against the database where nothing is expected to be
        returned.
        """
        self._get_cursor().execute(sql, params)

    def execute_many(self, sql, params):
        """
        wrapper for executemany.
        """
        self._get_cursor().executemany(sql, params)

    def query(self, sql, params=None):
        """
        Run a statement on the database directly, allowing for the
        execution of arbitrary read/write queries.
        """
        cur = self._get_cursor()
        cur.execute(sql, params)
        return cur.fetchall()

    def query_one(self, sql, params=None):
        """
        Grab just one record
        """
        cur = self._get_cursor()
        cur.execute(sql, params)
        return cur.fetchone()

    def create_schema(self, schema):
        """
        Create specified schema if it does not already exist
        """
        if schema not in self.schemas:
            self.engine.execute(CreateSchema(schema))

    def drop_schema(self, schema):
        """
        Drop specified schema
        """
        if schema in self.schemas:
            self.engine.execute(DropSchema(schema))

    def wipe_schema(self):
        """
        Delete all tables from current schema. Use with caution eh?
        """
        for t in self.tables:
            self[t].drop()

    def create_table(self, table, columns):
        """
        Creates a table.
        """
        schema, table = self.parse_table_name(table)
        table = self._valid_table_name(table)
        if not schema:
            schema = self.schema
        if table in self.tables:
            return Table(self, schema, table)
        else:
            return Table(self, schema, table, columns)

    def to_csv(self, sql, outFile, params=None):
        # this could likely be replaced with this:
        # http://initd.org/psycopg/docs/cursor.html#cursor.copy_expert
        cur = self._get_cursor()
        cur.execute(sql, params)
        colnames = [desc[0] for desc in cur.description]
        data = cur.fetchall()
        with open(outFile, "wb") as csvfile:
            writer = csv.writer(csvfile)
            # write header
            writer.writerow(colnames)
            for row in data:
                writer.writerow(row)
