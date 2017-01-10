from __future__ import absolute_import
from collections import OrderedDict
from six import string_types
from inspect import isgenerator
import subprocess

try:
    from urllib.parse import urlparse
except ImportError:
     from urlparse import urlparse

import os
import tempfile
from xml.sax.saxutils import escape


row_type = OrderedDict


class DatasetException(Exception):
    pass


def normalize_column_name(name):
    if not isinstance(name, string_types):
        raise ValueError('%r is not a valid column name.' % name)
    name = name.lower().strip()
    if not len(name) or '.' in name or '-' in name:
        raise ValueError('%r is not a valid column name.' % name)
    return name


def convert_row(row_type, row):
    if row is None:
        return None
    return row_type(list(row.items()))


class ResultIter(object):
    """ SQLAlchemy ResultProxies are not iterable to get a
    list of dictionaries. This is to wrap them. """

    def __init__(self, result_proxies, row_type=row_type):
        self.row_type = row_type
        if not isgenerator(result_proxies):
            result_proxies = iter((result_proxies, ))
        self.result_proxies = result_proxies
        self._iter = None

    def _next_rp(self):
        try:
            rp = next(self.result_proxies)
            self.keys = list(rp.keys())
            self._iter = iter(rp.fetchall())
            return True
        except StopIteration:
            return False

    def __next__(self):
        if self._iter is None:
            if not self._next_rp():
                raise StopIteration
        try:
            return convert_row(self.row_type, next(self._iter))
        except StopIteration:
            self._iter = None
            return self.__next__()

    next = __next__

    def __iter__(self):
        return self


def ogr2pg(db, in_file, in_layer=None, out_layer=None,
           schema='public', t_srs='EPSG:3005', sql=None):
    """
    Load a layer to provided pgdb database connection using OGR2OGR

    SQL provided is like the ESRI where_clause, but in SQLITE dialect:
    SELECT * FROM <in_layer> WHERE <sql>
    """
    # if not provided a layer name, use the name of the input file
    if not in_layer:
        in_layer = os.path.splitext(os.path.basename(in_file))[0]
    if not out_layer:
        out_layer = in_layer.lower()
    command = ['ogr2ogr',
               '--config PG_USE_COPY YES',
               '-t_srs '+t_srs,
               '-f PostgreSQL',
               '''PG:"host={h} user={u} dbname={db} password={pwd}"'''.format(
                          h=db.host,
                          u=db.user,
                          db=db.database,
                          pwd=db.password),
               '-lco OVERWRITE=YES',
               '-overwrite',
               '-lco SCHEMA={schema}'.format(schema=schema),
               '-lco GEOMETRY_NAME=geom',
               '-dim 2',
               '-nln '+out_layer,
               '-nlt PROMOTE_TO_MULTI',
               in_file,
               in_layer]

    if sql:
        command.insert(4,
                       '-sql "SELECT * FROM %s WHERE %s" -dialect SQLITE' %
                       (in_layer, sql))
        # remove layer name, it is ignored in combination with sql
        command.pop()
    subprocess.call(" ".join(command), shell=True)


def pg2ogr(db, sql, driver, outfile, outlayer=None, column_remap=None,
           geom_type=None):
    """
    A wrapper around ogr2ogr, for quickly dumping a postgis query to file.
    Suppported formats are ["ESRI Shapefile", "GeoJSON", "FileGDB"]
       - for GeoJSON, transforms to EPSG:4326
       - for Shapefile, consider supplying a column_remap dict
       - for FileGDB, geom_type is required
         (https://trac.osgeo.org/gdal/ticket/4186)
    """
    filename, ext = os.path.splitext(outfile)
    if not outlayer:
        outlayer = filename
    u = urlparse(db.url)
    pgcred = 'host={h} user={u} dbname={db} password={p}'.format(h=u.hostname,
                                                                 u=u.username,
                                                                 db=u.path[1:],
                                                                 p=u.password)
    # use a VRT so we can remap columns if a lookoup is provided
    if column_remap:
        # if specifiying output field names, all fields have to be specified
        # rather than try and parse the input sql, just do a test run of the
        # query and grab column names from that
        columns = [c for c in db.query(sql).keys() if c != 'geom']
        # make sure all columns are represented in the remap
        for c in columns:
            if c not in column_remap.keys():
                column_remap[c] = c
        field_remap_xml = " \n".join([
            '<Field name="'+column_remap[c]+'" src="'+c+'"/>'
            for c in columns])
    else:
        field_remap_xml = ""
    vrt = """<OGRVRTDataSource>
               <OGRVRTLayer name="{layer}">
                 <SrcDataSource>PG:{pgcred}</SrcDataSource>
                 <SrcSQL>{sql}</SrcSQL>
               {fieldremap}
               </OGRVRTLayer>
             </OGRVRTDataSource>
          """.format(layer=outlayer,
                     sql=escape(sql.replace("\n", " ")),
                     pgcred=pgcred,
                     fieldremap=field_remap_xml)
    vrtpath = os.path.join(tempfile.gettempdir(), filename+".vrt")
    if os.path.exists(vrtpath):
        os.remove(vrtpath)
    with open(vrtpath, "w") as vrtfile:
        vrtfile.write(vrt)
    # allow appending to filegdb and specify the geometry type
    if driver == 'FileGDB':
        nlt = "-nlt "+geom_type
        append = "-append"
    else:
        nlt = ""
        append = ""
    command = """ogr2ogr \
                    -progress \
                    -f "{driver}" {nlt} {append}\
                    {outfile} \
                    {vrt}
              """.format(driver=driver,
                         nlt=nlt,
                         append=append,
                         outfile=outfile,
                         vrt=vrtpath)
    # translate GeoJSON to EPSG:4326
    if driver == 'GeoJSON':
        command = command.replace("""-f "GeoJSON" """,
                                  """-f "GeoJSON" -t_srs EPSG:4326""")
    subprocess.call(command, shell=True)
