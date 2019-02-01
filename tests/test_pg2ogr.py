import os
import shutil
import tempfile
import unittest

import fiona

import pgdata


URL = 'postgresql://postgres:postgres@localhost:5432/pgdata'
DB = pgdata.connect(URL)
DB.execute('CREATE SCHEMA IF NOT EXISTS pgdata')


DATA_1 = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
AIRPORTS = os.path.join(DATA_1, 'bc_airports.json')

# also test a path with spaces
DATA_2 = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data space')
AIRPORTS_2 = os.path.join(DATA_2, 'bc_airports_one.json')


class ogrpg(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.spaced_dir = tempfile.mkdtemp("spa ced")

    def test_ogr2pg(self):
        db = DB
        db.ogr2pg(AIRPORTS, in_layer='bc_airports', out_layer='bc_airports',
                  schema='pgdata')
        airports = db['pgdata.bc_airports']
        assert 'physical_address' in airports.columns
        assert sum(1 for _ in airports.all()) == 425

    def test_ogr2pg_sql(self):
        db = DB
        db.ogr2pg(AIRPORTS, in_layer='bc_airports', out_layer='bc_airports_sql', schema='pgdata', sql="AIRPORT_NAME='Terrace (Northwest Regional) Airport'")
        airports = db['pgdata.bc_airports_sql']
        assert 'physical_address' in airports.columns
        assert sum(1 for _ in airports.all()) == 1

    def test_ogr2pg_spaces(self):
        db = DB
        db.ogr2pg(AIRPORTS_2, in_layer='bc_airports', out_layer='bc_airports_spaced',
                  schema='pgdata')
        airports = db['pgdata.bc_airports_spaced']
        assert 'physical_address' in airports.columns
        assert sum(1 for _ in airports.all()) == 1

    def test_pg2ogr_spaces(self):
        db = DB
        db.pg2ogr(sql='SELECT * from pgdata.bc_airports_spaced', driver='GeoJSON', outfile=os.path.join(self.spaced_dir, 'test_dump_spaced.json'))
        c = fiona.open(os.path.join(self.spaced_dir, 'test_dump_spaced.json'), 'r')
        assert len(c) == 1

    def test_pg2geojson(self):
        db = DB
        db.pg2ogr(sql='SELECT * FROM pgdata.bc_airports LIMIT 10', driver='GeoJSON',
                  outfile=os.path.join(self.tempdir, 'test_dump.json'))
        c = fiona.open(os.path.join(self.tempdir, 'test_dump.json'), 'r')
        assert len(c) == 10

    def test_pg2gpkg(self):
        db = DB
        db.pg2ogr(sql='SELECT * FROM pgdata.bc_airports LIMIT 10', driver='GPKG',
                  outfile=os.path.join(self.tempdir, 'test_dump.gpkg'),
                  outlayer='bc_airports')
        c = fiona.open(os.path.join(self.tempdir, 'test_dump.gpkg'), 'r')
        assert len(c) == 10

    def test_pg2gpkg_update(self):
        db = DB
        db.pg2ogr(sql='SELECT * FROM pgdata.bc_airports LIMIT 10', driver='GPKG',
                  outfile=os.path.join(self.tempdir, 'test_dump.gpkg'),
                  outlayer='bc_airports')
        db.pg2ogr(sql='SELECT * FROM pgdata.bc_airports LIMIT 10', driver='GPKG',
                  outfile=os.path.join(self.tempdir, 'test_dump.gpkg'),
                  outlayer='bc_airports_2')
        layers = fiona.listlayers(os.path.join(self.tempdir, 'test_dump.gpkg'))
        assert len(layers) == 2

    def test_pg2ogr_append(self):
        db = DB
        db.pg2ogr(sql='SELECT * FROM pgdata.bc_airports LIMIT 10', driver='GPKG',
                  outfile=os.path.join(self.tempdir, 'test_dump.gpkg'),
                  outlayer='bc_airports')
        db.pg2ogr(sql='SELECT * FROM pgdata.bc_airports LIMIT 10', driver='GPKG',
                  outfile=os.path.join(self.tempdir, 'test_dump.gpkg'),
                  outlayer='bc_airports', append=True)
        c = fiona.open(os.path.join(self.tempdir, 'test_dump.gpkg'), 'r')
        assert len(c) == 20

    def tearDown(self):
        shutil.rmtree(self.tempdir)
        shutil.rmtree(self.spaced_dir)


def test_tearDown():
    DB.drop_schema('pgdata', cascade=True)
