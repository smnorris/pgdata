# pgdata

Python PostgreSQL-PostGIS-SQLAlchemy shortcuts.

[![Build Status](https://travis-ci.org/smnorris/pgdata.svg?branch=master)](https://travis-ci.org/smnorris/pgdata) [![Coverage Status](https://coveralls.io/repos/github/smnorris/pgdata/badge.svg?branch=master)](https://coveralls.io/github/smnorris/pgdata?branch=master)

pgdata is a collection of convenience functions for working with PostgreSQL:

- provides an dictionary/JSON-like shortcut interface to database objects without dealing directly with an ORM or cursor (see [dataset](https://dataset.readthedocs.io/en/latest/))

        >>> import pgdata
        >>> db = pgdata.connect()
        >>> db.tables
        ['inventory']
        >>> db["inventory"].columns
        ['type', 'supplier', 'cost']

- provides a shortcut to `ogr2ogr` for quickly getting geographic data in and out of your database with sensible defaults and without resorting to shell scripting


        >>> import pgdata
        >>> db = pgdata.connect()
        >>> db.ogr2pg('airports.shp',
                      out_layer='airports_a',
                      schema='airport_project')
        >>> db.execute('do stuff')
        >>> db.pg2ogr('SELECT * FROM airports_project.result','GPKG', 'output.gpkg')


Much is copied directly from [dataset](https://dataset.readthedocs.org/) and further inspiration was taken from [pgwrap](https://github.com/paulchakravarti/pgwrap). See also [records](https://github.com/kennethreitz/records) and many others.

## Requirements

- PostgreSQL
- PostGIS
- GDAL (optional, for `pg2ogr` and `ogr2pg`)
- [ESRI File Geodatabase API](http://appsforms.esri.com/products/download/) (optional, for using `pg2ogr` with `FileGDB` option)

## Installation

```
pip install pgdata
```

## Configuration

Create an environment variable `DATABASE_URL` and set it to the [SQLAlchemy db url](http://docs.sqlalchemy.org/en/latest/core/engines.html) for your database:

MacOS/Linux etc:

`export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/mydb`

Windows:

`SET DATABASE_URL="postgresql://postgres:postgres@localhost:5432/mydb"`


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