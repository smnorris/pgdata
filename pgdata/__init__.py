from __future__ import absolute_import
import os
try:
    from urllib.parse import urlparse
except ImportError:
     from urlparse import urlparse

from pgdata.database import Database
from pgdata.table import Table

__version__ = "0.0.9"


def connect(url=None, schema=None, sql_path=None, multiprocessing=False):
    """Open a new connection to postgres via psycopg2/sqlalchemy
    """
    if url is None:
        url = os.environ.get('DATABASE_URL')
    return Database(url, schema, sql_path=sql_path, multiprocessing=multiprocessing)


def create_db(url=None):
    """Create a new database
    """
    if url is None:
        url = os.environ.get('DATABASE_URL')
    parsed_url = urlparse(url)
    db_name = parsed_url.path
    db_name = db_name.strip('/')
    db = connect("postgresql://"+parsed_url.netloc)
    # check that db does not exist
    q = """SELECT 1 as exists
           FROM pg_database
           WHERE datname = '{db_name}'""".format(db_name=db_name)
    if not db.query(q).fetchone():
        # CREATE DATABASE must be run outside of a transaction
        # https://stackoverflow.com/questions/6506578/how-to-create-a-new-database-using-sqlalchemy
        conn = db.engine.connect()
        conn.execute("commit")
        conn.execute("CREATE DATABASE "+db_name)
        conn.close()


def drop_db(url):
    """Drop specified database
    """
    parsed_url = urlparse(url)
    db_name = parsed_url.path
    db_name = db_name.strip('/')
    db = connect("postgresql://"+parsed_url.netloc)
    # check that db exists
    q = """SELECT 1 as exists
           FROM pg_database
           WHERE datname = '{db_name}'""".format(db_name=db_name)
    if db.query(q).fetchone():
        # DROP DATABASE must be run outside of a transaction
        conn = db.engine.connect()
        conn.execute("commit")
        conn.execute("DROP DATABASE "+db_name)
        conn.close()
