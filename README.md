# pgdb

Python-PostgreSQL-PostGIS interface shortcuts, copied from [dataset](https://dataset.readthedocs.org/).

[![Build Status](https://travis-ci.org/smnorris/pgdb.svg?branch=master)](https://travis-ci.org/smnorris/pgdb) [![Coverage Status](https://coveralls.io/repos/github/smnorris/pgdb/badge.svg?branch=master)](https://coveralls.io/github/smnorris/pgdb?branch=master)

pgdb is a collection of convenience functions for working with postgres.  The module wraps around psycopg2 and sqlalchemy. 

Primary differences from dataset: 

- handle cross-schema table references (ie, `myschema.table`)
- additional types (via `GeoAlchemy2` and `SQLAlchemy-Utils`)
- additional functions for common queries (mostly spatial)
- many dataset functions are unsupported (autocreate, locking, freezing)

[pgwrap](https://github.com/paulchakravarti/pgwrap) was also used for inspiration.

## Requirements
- PostgreSQL
- PostGIS
- SQLAlchemy
- psycopg2
- Geoalchemy2
- SQLAlchemy-Utils
- Alembic

## Usage

See [dataset](https://dataset.readthedocs.org/) for most usage.

```
>>> import pgdb
>>> db = pgdb.connect(schema='myschema')
>>> db.tables
['inventory']
>>> db["inventory"].columns
['type', 'supplier', 'cost']
>>> data = db.query("SELECT * FROM inventory WHERE type = %s", ('spam',))
>>> for row in data:
>>>     print (row['type'], row['supplier'], row['cost'])
('spam', 'spamcorp', 100)
>>> for row in db["inventory"].find(type='spam'):
>>>     print (row['type'], row['supplier'], row['cost'])
('spam', 'spamcorp', 100)
```

