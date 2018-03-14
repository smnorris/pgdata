import multiprocessing
import tempfile
import os

#import fiona

from sqlalchemy.schema import Column
from sqlalchemy import Integer, UnicodeText, Float, DateTime, Boolean
from geoalchemy2 import Geometry

from pgdata import connect
from pgdata import create_db
from pgdata import drop_db


URL = "postgresql://postgres:postgres@localhost:5432/pgdata"

AIRPORTS = 'tests/data/bc_airports.json'

DATA = [{"user_id": 1,
         "user_name": 'Fred',
         "email_address": "fred@hotmail.com",
         "password": "fredspwd"},
        {"user_id": 2,
         "user_name": 'Jill',
         "email_address": "jill@gmail.com",
         "password": "mypwd123"},
        {"user_id": 3,
         "user_name": 'Jack',
         "email_address": "jack@emails.uk",
         "password": "jackolope666"}]


def setup():
    create_db(URL)
    db = connect(URL)
    db.execute("CREATE EXTENSION IF NOT EXISTS postgis")


def test_connect():
    db = connect(URL, schema="pgdata")
    assert db.url == "postgresql://postgres:postgres@localhost:5432/pgdata"


def test_drop_schema():
    db = connect(URL, schema="pgdata")
    db.drop_schema("pgdata", cascade=True)


def test_create_schema():
    db = connect(URL, schema="pgdata")
    db.create_schema("pgdata")


def test_list_schema():
    db = connect(URL, schema="pgdata")
    assert set(["information_schema", "pgdata", "public"]) <= set(db.schemas)


def test_create_table():
    db = connect(URL, schema="pgdata")
    columns = [Column('user_id', Integer, primary_key=True),
               Column('user_name', UnicodeText, nullable=False),
               Column('email_address', UnicodeText),
               Column('password', UnicodeText, nullable=False)]
    employees = db.create_table("employees", columns)
    assert employees.table.exists()


def test_create_index():
    db = connect(URL, schema="pgdata")
    indexname = 'employees_user_name_idx'
    db['employees'].create_index(['user_name'], indexname)
    indexes = db['employees'].indexes.keys()
    assert indexname in indexes


def test_tables_in_schema():
    db = connect(URL)
    tables = db.tables_in_schema("pgdata")
    assert set(tables) == set(["employees"])


def test_get_table_cross_schema():
    db = connect(URL)
    assert db["pgdata.employees"] is not None


def test_insert_one():
    db = connect(URL)
    table = db["pgdata.employees"]
    table.insert(DATA[0])


def test_insert_many():
    db = connect(URL)
    table = db["pgdata.employees"]
    table.insert(DATA[1:])


def test_distinct():
    db = connect(URL, schema="pgdata")
    users = [r[0] for r in db["employees"].distinct('user_name')]
    assert len(users) == 3


def test_build_query():
    db = connect(URL)
    sql = "SELECT $UserName FROM pgdata.employees WHERE $UserId = 1"
    lookup = {"UserName": "user_name", "UserId": "user_id"}
    new_sql = db.build_query(sql, lookup)
    assert new_sql == "SELECT user_name FROM pgdata.employees WHERE user_id = 1"


def test_query_params_1():
    db = connect(URL)
    sql = "SELECT user_name FROM pgdata.employees WHERE user_id = %s"
    r = db.query(sql, (1,)).fetchall()
    assert r[0][0] == 'Fred'
    assert r[0]["user_name"] == 'Fred'


def test_query_keys():
    db = connect(URL)
    sql = "SELECT user_name FROM pgdata.employees WHERE user_id = %s"
    assert db.engine.execute(sql, (1,)).keys() == ['user_name']


def test_queryfile():
    sql_path = 'sql'
    db = connect(URL)
    db.execute(db.queries['utmzen2bcalb'])
    sql = """SELECT
                routines.routine_name
             FROM information_schema.routines
             LEFT JOIN information_schema.parameters
               ON routines.specific_name=parameters.specific_name
             WHERE routines.specific_schema='public'
             AND routines.routine_name = 'utmzen2bcalb'
             ORDER BY routines.routine_name, parameters.ordinal_position;
          """
    assert db.query(sql).fetchone()['routine_name'] == 'utmzen2bcalb'


def parallel_query(id):
    sql = "SELECT user_name FROM pgdata.employees WHERE user_id = %s"
    db = connect(URL, multiprocessing=True)
    db.engine.execute(sql, (id,))


def test_parallel():
    pool = multiprocessing.Pool(processes=2)
    pool.map(parallel_query, range(1, 10))
    pool.close()
    pool.join()


def test_null_table():
    db = connect(URL)
    db["table_that_does_not_exist"].drop()
    assert db["table_that_does_not_exist"]._is_dropped is True


#def teardown():
#    drop_db(URL)
