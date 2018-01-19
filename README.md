# pgdata

Python PostgreSQL-PostGIS-SQLAlchemy shortcuts.

[![Build Status](https://travis-ci.org/smnorris/pgdata.svg?branch=master)](https://travis-ci.org/smnorris/pgdata) [![Coverage Status](https://coveralls.io/repos/github/smnorris/pgdata/badge.svg?branch=master)](https://coveralls.io/github/smnorris/pgdata?branch=master)

pgdata is a collection of convenience functions for working with PostgreSQL.

Much is copied directly from [dataset](https://dataset.readthedocs.org/) and further inspiration was taken from [pgwrap](https://github.com/paulchakravarti/pgwrap).

## Requirements
- PostgreSQL
- PostGIS
- GDAL (optional, for `pg2ogr` and `ogr2pg`)
- [ESRI File Geodatabase API](http://appsforms.esri.com/products/download/) (optional, for using `pg2ogr` with `FileGDB` option)

## Usage


```
>>> import pgdata
>>> db = pgdata.connect(schema='myschema')
>>> db.tables
['inventory']
>>> db["inventory"].columns
['type', 'supplier', 'cost']
>>> data = db.query("SELECT * FROM inventory WHERE type = %s", ('spam',)).fetchall()
>>> for row in data:
>>>     print (row['type'], row['supplier'], row['cost'])
('spam', 'spamcorp', 100)
>>> for row in db["inventory"].find(type='spam'):
>>>     print (row['type'], row['supplier'], row['cost'])
('spam', 'spamcorp', 100)
```


## Paired with [`bcdata`](https://github.com/smnorris/bcdata)

Try some basic spatial analysis - how many airports are in the CRD?

```
import os

import bcdata
import pgdata

# define data to download
airports = 'bc-airports'
regdist = 'regional-districts-legally-defined-administrative-areas-of-bc'

# connect to default database, as defined by $DATABASE_URL
db = pgdata.connect()

# download and load to postgres
for url in [airports, regdist]:
    # what are the official schema and table names of the data source?
    info = bcdata.info(url)
    schema, table = (info['schema'], info['name'])

    # grab default email address for DataBC downloads
    email = os.environ['BCDATA_EMAIL']

    # download the data and use pgdata's ogr2pg shortcut to load to postgres
    # Note that we assume that the name of the layer in downloaded .gdb 
    # is 'schema_table'
    dl = bcdata.download(url, email)
    db.create_schema(schema)
    db.ogr2pg(dl, in_layer=schema+'_'+table, out_layer=table, schema=schema)

# define the query
sql = """SELECT COUNT(*)
         FROM whse_imagery_and_base_maps.gsr_airports_svw a
         INNER JOIN whse_legal_admin_boundaries.abms_regional_districts_sp rd
         ON ST_Intersects(a.geom, rd.geom)
         WHERE rd.admin_area_name = 'Capital Regional District'
      """

# execute and print results
print(db.query(sql).fetchone()[0])

```
