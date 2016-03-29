# pgdb

A collection of common postgres tasks, wrapped around psycopg2 and sqlalchemy.

Raw psycopg2 is used for speedy queries and executes.
The sqlalchemy engine is used for inspection and schema manipulation.

table.py is taken almost verbatim from [dataset](https://dataset.readthedocs.org/), but with many features removed. (I'm unsure how locking works or why it is necessary so it is not included)

https://github.com/paulchakravarti/pgwrap also used for inspiration.

## Usage

```
import pgdb

db = pgdb.connect()
db.tables
db["mytable"].columns
data = db.query("SELECT count(*) FROM inventory WHERE type = %s", ('spam',))
```
