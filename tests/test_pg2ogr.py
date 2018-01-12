import os
import shutil
import tempfile
import unittest

import fiona

from pgdata import connect


URL = 'postgresql://postgres:postgres@localhost:5432/pgdata'
DB = connect(URL)
DB.execute('CREATE SCHEMA IF NOT EXISTS pgdata')
DB1 = connect(URL)

DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
AIRPORTS = os.path.join(DATA, 'bc_airports.json')


class ogrpg(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def test_ogr2pg(self):
        db = DB1
        db.ogr2pg(AIRPORTS, in_layer='bc_airports', out_layer='bc_airports',
                  schema='pgdata')
        airports = db['pgdata.bc_airports']
        assert 'physical_address' in airports.columns
        assert sum(1 for _ in airports.all()) == 425

    def test_pg2geojson(self):
        db = DB1
        db.pg2ogr(sql='SELECT * FROM pgdata.bc_airports LIMIT 10', driver='GeoJSON',
                  outfile=os.path.join(self.tempdir, 'test_dump.json'))
        c = fiona.open(os.path.join(self.tempdir, 'test_dump.json'), 'r')
        assert len(c) == 10

    def test_pg2gpkg(self):
        db = DB1
        db.pg2ogr(sql='SELECT * FROM pgdata.bc_airports LIMIT 10', driver='GPKG',
                  outfile=os.path.join(self.tempdir, 'test_dump.gpkg'),
                  outlayer='bc_airports')
        c = fiona.open(os.path.join(self.tempdir, 'test_dump.gpkg'), 'r')
        assert len(c) == 10

    def test_pg2gpkg_update(self):
        db = DB1
        db.pg2ogr(sql='SELECT * FROM pgdata.bc_airports LIMIT 10', driver='GPKG',
                  outfile=os.path.join(self.tempdir, 'test_dump.gpkg'),
                  outlayer='bc_airports')
        db.pg2ogr(sql='SELECT * FROM pgdata.bc_airports LIMIT 10', driver='GPKG',
                  outfile=os.path.join(self.tempdir, 'test_dump.gpkg'),
                  outlayer='bc_airports_2')
        layers = fiona.listlayers(os.path.join(self.tempdir, 'test_dump.gpkg'))
        assert len(layers) == 2

    def test_pg2ogr_append(self):
        db = DB1
        db.pg2ogr(sql='SELECT * FROM pgdata.bc_airports LIMIT 10', driver='GPKG',
                  outfile=os.path.join(self.tempdir, 'test_dump.gpkg'),
                  outlayer='bc_airports')
        db.pg2ogr(sql='SELECT * FROM pgdata.bc_airports LIMIT 10', driver='GPKG',
                  outfile=os.path.join(self.tempdir, 'test_dump.gpkg'),
                  outlayer='bc_airports', append=True)
        c = fiona.open(os.path.join(self.tempdir, 'test_dump.gpkg'), 'r')
        assert len(c) == 20
