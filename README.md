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

