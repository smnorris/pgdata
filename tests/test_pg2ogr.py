import os
import tempfile

import fiona

from pgdb import connect


URL = "postgresql://postgres:postgres@localhost:5432/pgdb"
DB1 = connect(URL, schema="pgdb")
AIRPORTS = os.path.join(os.path.dirname(__file__), 'data/bc_airports.json')


def test_ogr2pg():
    db = DB1
    db.ogr2pg(AIRPORTS, in_layer="bc_airports", out_layer='bc_airports',
              schema='pgdb')
    airports = db['bc_airports']
    assert 'physical_address' in airports.columns
    assert sum(1 for _ in airports.all()) == 425


def test_pg2geojson():
    db = DB1
    tempdir = tempfile.mkdtemp()
    db.pg2ogr(sql="SELECT * FROM pgdb.bc_airports LIMIT 10", driver="GeoJSON",
              outfile=os.path.join(tempdir, "test_dump.json"))
    c = fiona.open(os.path.join(tempdir, "test_dump.json"), "r")
    assert len(c) == 10


def test_pg2gpkg():
    db = DB1
    tempdir = tempfile.mkdtemp()
    db.pg2ogr(sql="SELECT * FROM pgdb.bc_airports LIMIT 10", driver="GPKG",
              outfile=os.path.join(tempdir, "test_dump.gpkg"),
              outlayer='bc_airports')
    c = fiona.open(os.path.join(tempdir, "test_dump.gpkg"), "r")
    assert len(c) == 10


def test_pg2gpkg_update():
    db = DB1
    tempdir = tempfile.mkdtemp()
    db.pg2ogr(sql="SELECT * FROM pgdb.bc_airports LIMIT 10", driver="GPKG",
              outfile=os.path.join(tempdir, "test_dump.gpkg"),
              outlayer='bc_airports')
    db.pg2ogr(sql="SELECT * FROM pgdb.bc_airports LIMIT 10", driver="GPKG",
              outfile=os.path.join(tempdir, "test_dump.gpkg"),
              outlayer='bc_airports_2')
    layers = fiona.listlayers(os.path.join(tempdir, "test_dump.gpkg"))
    assert len(layers) == 2


def test_pg2ogr_append():
    db = DB1
    tempdir = tempfile.mkdtemp()
    db.pg2ogr(sql="SELECT * FROM pgdb.bc_airports LIMIT 10", driver="GPKG",
              outfile=os.path.join(tempdir, "test_dump.gpkg"),
              outlayer='bc_airports')
    db.pg2ogr(sql="SELECT * FROM pgdb.bc_airports LIMIT 10", driver="GPKG",
              outfile=os.path.join(tempdir, "test_dump.gpkg"),
              outlayer='bc_airports', append=True)
    c = fiona.open(os.path.join(tempdir, "test_dump.gpkg"), "r")
    assert len(c) == 20
