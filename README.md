# pgdb

Python PostgreSQL-PostGIS-psycopg2-SQLAlchemy shortcuts.

[![Build Status](https://travis-ci.org/smnorris/pgdb.svg?branch=master)](https://travis-ci.org/smnorris/pgdb) [![Coverage Status](https://coveralls.io/repos/github/smnorris/pgdb/badge.svg?branch=master)](https://coveralls.io/github/smnorris/pgdb?branch=master)

pgdb is a collection of convenience functions for working with postgres.

Much is copied directly from [dataset](https://dataset.readthedocs.org/) and further inspiration was taken from [pgwrap](https://github.com/paulchakravarti/pgwrap).

## Requirements
- PostgreSQL
- PostGIS
- SQLAlchemy
- psycopg2
- Geoalchemy2
- SQLAlchemy-Utils
- Alembic
- GDAL (optional, for `pg2ogr` and `ogr2pg`)

## Usage


```
>>> import pgdb
>>> db = pgdb.connect(schema='myschema')
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

