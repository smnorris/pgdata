import itertools
import six
from hashlib import sha1

from sqlalchemy.schema import Table as SQLATable
from sqlalchemy.schema import MetaData
from sqlalchemy.schema import Column, Index
from sqlalchemy.sql import and_, expression, text

from alembic.migration import MigrationContext
from alembic.operations import Operations

# load custom types to stop sqlalchemy from complaining
from geoalchemy2 import Geometry
import ltree

from pgdb.util import DatasetException
from pgdb.util import normalize_column_name
from pgdb.util import ResultIter


class Table(object):

    def __init__(self, db, schema, table, columns=None):
        self.database = db
        self.db = db          # just a shortcut
        self.schema = schema
        self.name = table
        self.metadata = MetaData(schema=schema)
        self.metadata.bind = self.db.engine
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
        return map(normalize_column_name, self.columns)

    @property
    def columns(self):
        """
        Return list of all columns in table
        """
        return list(self.table.columns.keys())

    @property
    def sqla_columns(self):
        """
        Return all columns in table as sqlalchemy column types
        """
        return self.table.columns

    @property
    def column_types(self):
        """
        Return a dict mapping column name to type for all columns in table
        """
        column_types = {}
        for c in self.sqla_columns:
            column_types[c.name] = c.type
        return column_types

    @property
    def primary_key(self):
        """
        return a list of columns making up the primary key constraint
        """
        return [c.name for c in self.table.primary_key]

    @property
    def op(self):
        ctx = MigrationContext.configure(self.db.engine)
        return Operations(ctx)

    def _valid_table_name(self, table_name):
        """ Check if the table name is obviously invalid. """
        if table_name is None or not len(table_name.strip()):
            raise ValueError("Invalid table name: %r" % table_name)
        return table_name.strip()

    def _update_table(self, table_name):
        self.metadata = MetaData(schema=self.schema)
        self.metadata.bind = self.db.engine
        return SQLATable(table_name, self.metadata, schema=self.schema)

    def add_primary_key(self, column="id"):
        """
        add primary key constraint to specified column
        """
        if not self.primary_key:
            sql = """ALTER TABLE {s}.{t}
                     ADD PRIMARY KEY ({c})
                  """.format(s=self.schema,
                             t=self.name,
                             c=column)
            self.db.execute(sql)

    def drop(self):
        """
        Drop the table from the database
        """
        if self._is_dropped is False:
            self.table.drop(self.db.engine)
        self._is_dropped = True

    def _check_dropped(self):
        if self._is_dropped:
            raise DatasetException('the table has been dropped. this object should not be used again.')

    def create_index_geom(self, column="geom"):
        """
        create index on geometry
        """
        self.db.execute('CREATE INDEX "idx_%s_%s" ON "%s"."%s" '
                        'USING GIST ("%s")' % (self.name, column, self.schema,
                                               self.name, column))

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
        if name in self.table.columns.keys():
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
            sig = '||'.join(columns)
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
        try:
            #self.database._acquire()
            columns = [self.table.c[c] for c in columns]
            idx = Index(name, *columns, postgresql_using=index_type)
            idx.create(self.database.engine)
        except:
            idx = None
        #finally:
        #    self.database._release()
        self.indexes[name] = idx
        return idx

    def query(self, query, **kw):
        """
        Run a statement on the table directly, allowing for the
        execution of arbitrary read/write queries. A query can either be
        a plain text string, or a `SQLAlchemy expression <http://docs.sqlalchemy.org/en/latest/core/tutorial.html#selecting>`_.
        If a plain string is passed in, it will be converted to an expression automatically.

        Keyword arguments will be used for parameter binding. See the `SQLAlchemy
        documentation <http://docs.sqlalchemy.org/en/rel_0_9/core/connections.html#sqlalchemy.engine.Connection.execute>`_ for details.

        The returned iterator will yield each result sequentially.
        ::

            res = db.query('SELECT user, COUNT(*) c FROM photos GROUP BY user')
            for row in res:
                print(row['user'], row['c'])
        """
        if isinstance(query, six.string_types):
            query = text(query)
        return ResultIter(self.executable.execute(query, **kw),
                          row_type=self.db.row_type)

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
            return itertools.chain.from_iterable(self.db.engine.execute(q))
        # otherwise return specified row_type
        else:
            return ResultIter(self.db.engine.execute(q),
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
        res = self.database.engine.execute(self.table.insert(row))
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
        sql = """ALTER TABLE {s}.{t} RENAME TO {name}
              """.format(s=self.schema, t=self.name, name=name)
        self.database.engine.execute(sql)
        self.table = SQLATable(name, self.metadata, schema=self.schema,
                               autoload=True)
