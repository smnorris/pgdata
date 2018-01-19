from __future__ import absolute_import
import itertools
import six
from hashlib import sha1
import logging
from itertools import count

from sqlalchemy import create_engine
from sqlalchemy.schema import Table as SQLATable
from sqlalchemy.schema import MetaData
from sqlalchemy.schema import Column, Index
from sqlalchemy.sql import and_, expression, text
from sqlalchemy import alias

from alembic.migration import MigrationContext
from alembic.operations import Operations

# load custom types to stop sqlalchemy from complaining
from geoalchemy2 import Geometry
from sqlalchemy_utils import LtreeType

from pgdata.util import DatasetException
from pgdata.util import normalize_column_name
from pgdata.util import ResultIter
from six.moves import map

log = logging.getLogger(__name__)


class Table(object):

    def __init__(self, db, schema, table, columns=None):
        self.db = db
        self.schema = schema
        self.name = table
        self.engine = create_engine(db.url)
        self.metadata = MetaData(schema=schema)
        self.metadata.bind = self.engine
        # http://docs.sqlalchemy.org/en/rel_1_0/core/metadata.html
        # if provided columns (SQLAlchemy columns), create the table
        if table:
            if columns:
                self.table = SQLATable(table, self.metadata, schema=self.schema,
                                       *columns)
                self.table.create()
            # otherwise just load from db
            else:
                self.table = SQLATable(table, self.metadata, schema=self.schema,
                                       autoload=True)
            self.indexes = dict((i.name, i) for i in self.table.indexes)
            self._is_dropped = False
        else:
            self._is_dropped = True
            self.table = None

    @property
    def _normalized_columns(self):
        return list(map(normalize_column_name, self.columns))

    @property
    def columns(self):
        """Return list of all columns in table
        """
        return list(self.table.columns.keys())

    @property
    def sqla_columns(self):
        """Return all columns in table as sqlalchemy column types
        """
        return self.table.columns

    @property
    def column_types(self):
        """Return a dict mapping column name to type for all columns in table
        """
        column_types = {}
        for c in self.sqla_columns:
            column_types[c.name] = c.type
        return column_types

    @property
    def primary_key(self):
        """Return a list of columns making up the primary key constraint
        """
        return [c.name for c in self.table.primary_key]

    @property
    def op(self):
        ctx = MigrationContext.configure(self.engine.connect())
        return Operations(ctx)

    def _valid_table_name(self, table_name):
        """Check if the table name is obviously invalid.
        """
        if table_name is None or not len(table_name.strip()):
            raise ValueError("Invalid table name: %r" % table_name)
        return table_name.strip()

    def _update_table(self, table_name):
        self.metadata = MetaData(schema=self.schema)
        self.metadata.bind = self.engine
        return SQLATable(table_name, self.metadata, schema=self.schema)

    def add_primary_key(self, column="id"):
        """Add primary key constraint to specified column
        """
        if not self.primary_key:
            sql = """ALTER TABLE {s}.{t}
                     ADD PRIMARY KEY ({c})
                  """.format(s=self.schema,
                             t=self.name,
                             c=column)
            self.db.execute(sql)

    def drop(self):
        """Drop the table from the database
        """
        if self._is_dropped is False:
            self.table.drop(self.engine)
        self._is_dropped = True

    def _check_dropped(self):
        if self._is_dropped:
            raise DatasetException('the table has been dropped. this object should not be used again.')

    def _args_to_clause(self, args):
        clauses = []
        for k, v in args.items():
            if isinstance(v, (list, tuple)):
                clauses.append(self.table.c[k].in_(v))
            else:
                clauses.append(self.table.c[k] == v)
        return and_(*clauses)

    def create_column(self, name, type):
        """
        Explicitely create a new column ``name`` of a specified type.
        ``type`` must be a `SQLAlchemy column type <http://docs.sqlalchemy.org/en/rel_0_8/core/types.html>`_.
        ::

            table.create_column('created_at', sqlalchemy.DateTime)
        """
        self._check_dropped()
        if normalize_column_name(name) not in self._normalized_columns:
            self.op.add_column(
                self.table.name,
                Column(name, type),
                self.table.schema
            )
            self.table = self._update_table(self.table.name)

    def drop_column(self, name):
        """
        Drop the column ``name``
        ::

            table.drop_column('created_at')
        """
        self._check_dropped()
        if name in list(self.table.columns.keys()):
            self.op.drop_column(
                self.table.name,
                name,
                schema=self.schema
            )
            self.table = self._update_table(self.table.name)

    def create_index(self, columns, name=None, index_type="btree"):
        """
        Create an index to speed up queries on a table.
        If no ``name`` is given a random name is created.
        ::
            table.create_index(['name', 'country'])
        """
        self._check_dropped()
        if not name:
            sig = '||'.join(columns+[index_type])
            # This is a work-around for a bug in <=0.6.1 which would create
            # indexes based on hash() rather than a proper hash.
            key = abs(hash(sig))
            name = 'ix_%s_%s' % (self.table.name, key)
            if name in self.indexes:
                return self.indexes[name]
            key = sha1(sig.encode('utf-8')).hexdigest()[:16]
            name = 'ix_%s_%s' % (self.table.name, key)
        if name in self.indexes:
            return self.indexes[name]
        #self.db._acquire()
        columns = [self.table.c[col] for col in columns]
        idx = Index(name, *columns, postgresql_using=index_type)
        idx.create(self.engine)
        #finally:
        #    self.db._release()
        self.indexes[name] = idx
        return idx

    def create_index_geom(self, column="geom"):
        """Shortcut to create index on geometry
        """
        self.create_index([column], index_type="gist")

    def distinct(self, *columns, **_filter):
        """
        Returns all rows of a table, but removes rows in with duplicate values in ``columns``.
        Interally this creates a `DISTINCT statement <http://www.w3schools.com/sql/sql_distinct.asp>`_.
        ::

            # returns only one row per year, ignoring the rest
            table.distinct('year')
            # works with multiple columns, too
            table.distinct('year', 'country')
            # you can also combine this with a filter
            table.distinct('year', country='China')
        """
        self._check_dropped()
        qargs = []
        try:
            columns = [self.table.c[c] for c in columns]
            for col, val in _filter.items():
                qargs.append(self.table.c[col] == val)
        except KeyError:
            return []

        q = expression.select(columns, distinct=True,
                              whereclause=and_(*qargs),
                              order_by=[c.asc() for c in columns])
        # if just looking at one column, return a simple list
        if len(columns) == 1:
            return itertools.chain.from_iterable(self.engine.execute(q))
        # otherwise return specified row_type
        else:
            return ResultIter(self.engine.execute(q),
                              row_type=self.db.row_type)

    def insert(self, row):
        """
        Add a row (type: dict) by inserting it into the table.
        Columns must exist.
        ::
            data = dict(title='I am a banana!')
            table.insert(data)
        Returns the inserted row's primary key.
        """
        self._check_dropped()
        res = self.engine.execute(self.table.insert(row))
        if len(res.inserted_primary_key) > 0:
            return res.inserted_primary_key[0]

    def insert_many(self, rows, chunk_size=1000):
        """
        Add many rows at a time, which is significantly faster than adding
        them one by one. Per default the rows are processed in chunks of
        1000 per commit, unless you specify a different ``chunk_size``.
        See :py:meth:`insert() <dataset.Table.insert>` for details on
        the other parameters.
        ::
            rows = [dict(name='Dolly')] * 10000
            table.insert_many(rows)
        """
        def _process_chunk(chunk):
            self.table.insert().execute(chunk)
        self._check_dropped()

        chunk = []
        for i, row in enumerate(rows, start=1):
            chunk.append(row)
            if i % chunk_size == 0:
                _process_chunk(chunk)
                chunk = []
        if chunk:
            _process_chunk(chunk)

    def rename(self, name):
        """Rename the table
        """
        sql = """ALTER TABLE {s}.{t} RENAME TO {name}
              """.format(s=self.schema, t=self.name, name=name)
        self.engine.execute(sql)
        self.table = SQLATable(name, self.metadata, schema=self.schema,
                               autoload=True)

    def find_one(self, **kwargs):
        """
        Works just like :py:meth:`find() <dataset.Table.find>` but returns one result, or None.
        ::
            row = table.find_one(country='United States')
        """
        kwargs['_limit'] = 1
        iterator = self.find(**kwargs)
        try:
            return next(iterator)
        except StopIteration:
            return None

    def _args_to_order_by(self, order_by):
        if order_by[0] == '-':
            return self.table.c[order_by[1:]].desc()
        else:
            return self.table.c[order_by].asc()

    def find(self, _limit=None, _offset=0, _step=5000,
             order_by='id', return_count=False, **_filter):
        """
        Performs a simple search on the table. Simply pass keyword arguments as ``filter``.
        ::
            results = table.find(country='France')
            results = table.find(country='France', year=1980)
        Using ``_limit``::
            # just return the first 10 rows
            results = table.find(country='France', _limit=10)
        You can sort the results by single or multiple columns. Append a minus sign
        to the column name for descending order::
            # sort results by a column 'year'
            results = table.find(country='France', order_by='year')
            # return all rows sorted by multiple columns (by year in descending order)
            results = table.find(order_by=['country', '-year'])
        By default :py:meth:`find() <dataset.Table.find>` will break the
        query into chunks of ``_step`` rows to prevent huge tables
        from being loaded into memory at once.
        For more complex queries, please use :py:meth:`db.query()`
        instead."""
        self._check_dropped()
        if not isinstance(order_by, (list, tuple)):
            order_by = [order_by]
        order_by = [o for o in order_by if (o.startswith('-') and o[1:] or o) in self.table.columns]
        order_by = [self._args_to_order_by(o) for o in order_by]

        args = self._args_to_clause(_filter)

        # query total number of rows first
        count_query = alias(self.table.select(whereclause=args, limit=_limit, offset=_offset),
                            name='count_query_alias').count()
        rp = self.engine.execute(count_query)
        total_row_count = rp.fetchone()[0]
        if return_count:
            return total_row_count

        if _limit is None:
            _limit = total_row_count

        if _step is None or _step is False or _step == 0:
            _step = total_row_count

        if total_row_count > _step and not order_by:
            _step = total_row_count
            log.warn("query cannot be broken into smaller sections because it is unordered")

        queries = []

        for i in count():
            qoffset = _offset + (_step * i)
            qlimit = min(_limit - (_step * i), _step)
            if qlimit <= 0:
                break
            queries.append(self.table.select(whereclause=args, limit=qlimit,
                                             offset=qoffset, order_by=order_by))
        return ResultIter((self.engine.execute(q) for q in queries),
                          row_type=self.db.row_type)

    def count(self, **_filter):
        """
        Return the count of results for the given filter set
        (same filter options as with ``find()``).
        """
        return self.find(return_count=True, **_filter)

    def __getitem__(self, item):
        """
        This is an alias for distinct which allows the table to be queried as using
        square bracket syntax.
        ::
            # Same as distinct:
            print list(table['year'])
        """
        if not isinstance(item, tuple):
            item = item,
        return self.distinct(*item)

    def all(self):
        """
        Returns all rows of the table as simple dictionaries. This is simply a shortcut
        to *find()* called with no arguments.
        ::
            rows = table.all()"""
        return self.find()

    def __iter__(self):
        """
        Allows for iterating over all rows in the table without explicetly
        calling :py:meth:`all() <dataset.Table.all>`.
        ::
            for row in table:
                print(row)
        """
        return self.all()

    def __repr__(self):
        return '<Table(%s)>' % self.table.name
