# pgdb

A collection of common postgres tasks, wrapped around psycopg2 and sqlalchemy and taken mostly from a fork of [dataset](https://dataset.readthedocs.org/)

Primary differences from dataset:
- schema handling improved (although dataset may be better now)
- a raw psycopg2 connection is used for custom sql (`query` and `execute` functions) rather than going through sqlalchemy
- functions I don't need have been removed (locking, freezing, etc)

https://github.com/paulchakravarti/pgwrap also used for inspiration.

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

