import multiprocessing
import tempfile
import os

#import fiona

from sqlalchemy.schema import Column
from sqlalchemy import Integer, UnicodeText, Float, DateTime, Boolean
from geoalchemy2 import Geometry

from pgdb import connect


URL = "postgresql://postgres:postgres@localhost:5432/pgdb"
DB1 = connect(URL, schema="pgdb")
DB2 = connect(URL)
DB3 = connect(URL, multiprocessing=True)

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


def test_connect():
    db = DB1
    assert db.url == "postgresql://postgres:postgres@localhost:5432/pgdb"


def test_drop_schema():
    db = DB1
    db.drop_schema("pgdb", cascade=True)


def test_create_schema():
    db = DB1
    db.create_schema("pgdb")


def test_list_schema():
    db = DB1
    assert db.schemas == ["information_schema", "pgdb", "public"]


#def test_ogr2pg():
#    db = DB1
#    db.ogr2pg(AIRPORTS, in_layer="OGRGeoJSON", out_layer='bc_airports',
#              schema='pgdb')
#    airports = db['bc_airports']
#    assert 'physical_address' in airports.columns
#    assert sum(1 for _ in airports.all()) == 425


#def test_pg2ogr():
#    db = DB1
#    tempdir = tempfile.mkdtemp()
#    db.pg2ogr(sql="SELECT * FROM pgdb.bc_airports LIMIT 10", driver="GeoJSON",
#              outfile=os.path.join(tempdir, "test_dump.json"))
#    c = fiona.open(os.path.join(tempdir, "test_dump.json"), "r")
#    assert len(c) == 10


def test_create_table():
    db = DB1
    columns = [Column('user_id', Integer, primary_key=True),
               Column('user_name', UnicodeText, nullable=False),
               Column('email_address', UnicodeText),
               Column('password', UnicodeText, nullable=False)]
    employees = db.create_table("employees", columns)
    assert employees.table.exists()


def test_create_index():
    db = DB1
    indexname = 'employees_user_name_idx'
    db['employees'].create_index(['user_name'], indexname)
    indexes = db['employees'].indexes.keys()
    assert indexname in indexes


def test_tables_in_schema():
    db = DB2
    tables = db.tables_in_schema("pgdb")
    assert set(tables) == set(["employees"])#, 'bc_airports'])


def test_get_table_cross_schema():
    db = DB2
    assert db["pgdb.employees"] is not None


def test_insert_one():
    db = DB2
    table = db["pgdb.employees"]
    table.insert(DATA[0])


def test_insert_many():
    db = DB2
    table = db["pgdb.employees"]
    table.insert(DATA[1:])


def test_build_query():
    db = DB2
    sql = "SELECT $UserName FROM pgdb.employees WHERE $UserId = 1"
    lookup = {"UserName": "user_name", "UserId": "user_id"}
    new_sql = db.build_query(sql, lookup)
    assert new_sql == "SELECT user_name FROM pgdb.employees WHERE user_id = 1"


def test_query_params_1():
    db = DB2
    sql = "SELECT user_name FROM pgdb.employees WHERE user_id = %s"
    r = db.query(sql, (1,)).fetchall()
    assert r[0][0] == 'Fred'
    assert r[0]["user_name"] == 'Fred'


def test_query_keys():
    db = DB2
    sql = "SELECT user_name FROM pgdb.employees WHERE user_id = %s"
    assert db.engine.execute(sql, (1,)).keys() == ['user_name']


def parallel_query(id):
    sql = "SELECT user_name FROM pgdb.employees WHERE user_id = %s"
    db = DB3
    db.engine.execute(sql, (id,))


def test_parallel():
    pool = multiprocessing.Pool(processes=2)
    pool.map(parallel_query, range(1, 10))
    pool.close()
    pool.join()


def test_null_table():
    db = DB2
    db["table_that_does_not_exist"].drop()
    assert db["table_that_does_not_exist"]._is_dropped is True


def test_wipe_schema():
    db = DB1
    db.wipe_schema()


def test_n_connections():
    db = DB2
    # ensure tables don't already exist
    db['pgdb.test_n_connections'].drop()
    for i in range(200):
        db['test_'+str(i)].drop()

    for i in range(200):
        db.ogr2pg(AIRPORTS, in_layer="OGRGeoJSON", out_layer='bc_airports_'+str(i), schema='pgdb')
        if i == 0:
            db.execute("CREATE TABLE pgdb.test_n_connections AS SELECT * FROM pgdb.bc_airports_0 LIMIT 0")
        sql = "INSERT INTO pgdb.test_n_connections (SELECT * FROM pgdb.bc_airports_{n} LIMIT 1)".format(n=str(i))
        db.execute(sql)
        db['test_'+str(i)].drop()
    r = db.query("SELECT count(*) FROM pgdb.test_n_connections").fetchone()
    assert r[0] == 200

