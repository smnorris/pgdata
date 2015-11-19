from pgdb import connect
from sqlalchemy.schema import Column
from sqlalchemy import Integer, UnicodeText, Float, DateTime, Boolean
from geoalchemy2 import Geometry

DB = "postgresql://postgres:postgres@localhost:5432/pgdb"


def test_connect():
    db = connect(DB, schema="pgdb")
    assert db.url == "postgresql://postgres:postgres@localhost:5432/pgdb"


def test_wipe_schema():
    db = connect(DB, schema="pgdb")
    db.wipe_schema()


def test_drop_schema():
    db = connect(DB, schema="pgdb")
    db.drop_schema("pgdb")


def test_create_schema():
    db = connect(DB, schema="pgdb")
    db.create_schema("pgdb")


def test_create_table():
    db = connect(DB, schema="pgdb")
    columns = [Column('user_id', Integer, primary_key=True),
               Column('user_name', UnicodeText, nullable=False),
               Column('email_address', UnicodeText),
               Column('password', UnicodeText, nullable=False)]
    employees = db.create_table("employees", columns)
    assert employees.table.exists()


def test_create_index():
    db = connect(DB, schema="pgdb")
    indexname = 'employees_user_name_idx'
    db['employees'].create_index(['user_name'], indexname)
    indexes = db['employees'].indexes.keys()
    assert indexname in indexes


def test_tables_in_schema():
    db = connect(DB)
    tables = db.tables_in_schema("pgdb")
    assert tables == ["employees"]


def test_get_table_cross_schema():
    db = connect(DB)
    assert db["pgdb.employees"] is not None
