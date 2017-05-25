import datetime
import threading
import traceback
from queue import Queue

from sqlalchemy import MetaData, Table
from toolz import memoize

from bonobo_sqlalchemy.constants import SELECT, INSERT, UPDATE

from bonobo import ErrorBag
from bonobo.util.lifecycle import with_context
from bonobo.core.contexts import ComponentExecutionContext


class ProhibitedOperationError(Exception):
    pass


@with_context
class DatabaseWriter:
    engine = None
    table_name = None
    fetch_columns = None
    insert_only_fields = ()
    discriminant = ('id', )
    created_at_field = 'created_at'
    updated_at_field = 'updated_at'
    allowed_operations = (INSERT, UPDATE, )

    def __init__(
        self,
        engine=None,
        table_name=None,
        fetch_columns=None,
        discriminant=None,
        created_at_field=None,
        updated_at_field=None,
        insert_only_fields=None,
        allowed_operations=None
    ):

        self.engine = engine or self.engine
        self.table_name = table_name or self.table_name

        # XXX should take self.fetch_columns into account if provided
        self.fetch_columns = {}
        if isinstance(fetch_columns, (list, tuple)):
            self.add_fetch_columns(*fetch_columns)
        elif isinstance(fetch_columns, dict):
            self.add_fetch_columns(**fetch_columns)

        self.discriminant = discriminant or self.discriminant
        self.created_at_field = created_at_field or self.created_at_field
        self.updated_at_field = updated_at_field or self.updated_at_field
        self.insert_only_fields = insert_only_fields or self.insert_only_fields
        self.allowed_operations = allowed_operations or self.allowed_operations

        self._max_buffer_size = 1000
        self._last_duration = None
        self._last_commit_at = None
        self._query_count = 0

    def get_insert_columns_for(self, hash):
        """List of columns we can use for insert."""
        return self.columns

    def get_update_columns_for(self, hash, row):
        """List of columns we can use for update."""
        return [column for column in self.columns if not column in self.insert_only_fields]

    def get_columns_for(self, hash, row=None):
        """Retrieve list of table column names for which we have a value in given hash.

        """
        if row:
            column_names = self.get_update_columns_for(hash, row)
        else:
            column_names = self.get_insert_columns_for(hash)

        return [key for key in hash if key in column_names]

    def initialize(self, context):
        context.stats.update(dict(SELECT=0, INSERT=0, UPDATE=0))

    def __call__(self, context, bag):
        """Transform method. Stores the input in a buffer, and only unstack buffer content if some limit has been
        exceeded.

        TODO for now buffer limit is hardcoded as 1000, but we may use a few criterias to add intelligence to this:
             time since last commit, duration of last commit, buffer length ...

        """
        yield from context.push(bag)

    def finalize(self, context):
        """Transform's finalize method.

        Empties the remaining lines in buffer by loading them into database and close database connection.

        """
        yield from context.commit()
        context.close()

    def add_fetch_columns(self, *columns, **aliased_columns):
        self.fetch_columns = {
            ** self.fetch_columns,
            ** aliased_columns,
        }

        for column in columns:
            self.fetch_columns[column] = column

    @property
    @memoize(key=lambda args, kwargs: threading.get_ident())
    def columns(self):
        return list(self.table.columns.keys())

    @property
    @memoize(key=lambda args, kwargs: threading.get_ident())
    def metadata(self):
        """SQLAlchemy metadata."""
        return MetaData()

    @property
    @memoize(key=lambda args, kwargs: threading.get_ident())
    def table(self):
        """SQLAlchemy table object, using metadata autoloading from database to avoid the need of column definitions."""
        return Table(self.table_name, self.metadata, autoload=True, autoload_with=self.engine())


class _DatabaseWriterExecutionContext(ComponentExecutionContext):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._buffer = Queue()
        self._connection = None
        self._max_buffer_size = 100

    @property
    @memoize(key=lambda args, kwargs: threading.get_ident())
    def connection(self):
        if self._connection is None:
            engine = self.component.engine() if callable(self.component.engine) else self.component.engine
            self._connection = engine.connect()
        return self._connection

    def push(self, bag):
        self._buffer.put(bag)
        yield from self.maybe_commit()

    def maybe_commit(self):
        if self._buffer.qsize() >= self._max_buffer_size:
            yield from self.commit()

    def commit(self):
        with self.connection.begin():
            while self._buffer.qsize() > 0:
                try:
                    yield self.insert_or_update(self._buffer.get())
                except Exception as exc:
                    yield ErrorBag(exc, traceback.format_exc())

    def close(self):
        self._connection.close()
        self._connection = None

    def insert_or_update(self, bag):
        """Actual database load transformation logic, without the buffering / transaction logic.

        """

        # find line, if it exist
        now = datetime.datetime.now()

        # introspect column names
        # XXX todo move somewhere where it's only called once.
        column_names = self.component.columns
        discriminant = self.component.discriminant
        allowed_operations = self.component.allowed_operations
        created_at_field = self.component.created_at_field
        updated_at_field = self.component.updated_at_field

        # UpdatedAt field configured ? Let's set the value in source hash
        if updated_at_field in column_names:
            bag[updated_at_field] = now  # XXX not pure ...
        # Otherwise, make sure there is no such field
        elif updated_at_field in bag:
            del bag[updated_at_field]  # XXX why ?

        # FIND
        row = self.find(bag)

        # UPDATE
        if row:
            if not UPDATE in allowed_operations:
                raise ProhibitedOperationError('UPDATE operations are not allowed by this transformation.')

            _columns = self.component.get_columns_for(bag, row)

            query = 'UPDATE {table} SET {values} WHERE {criteria}'.format(
                table=self.component.table_name,
                values=', '.join(
                    ('{column} = ?'.format(column=_column) for _column in _columns if not _column in discriminant)
                ),
                criteria=' AND '.join(('{key} = ?'.format(key=_key) for _key in discriminant))
            )
            values = [bag[_column] for _column in _columns if not _column in discriminant] + \
                     [bag[_column] for _column in discriminant]

        # INSERT
        else:
            if not INSERT in allowed_operations:
                raise ProhibitedOperationError('INSERT operations are not allowed by this transformation.')

            if created_at_field in column_names:
                bag[created_at_field] = now
            else:
                if created_at_field in bag:
                    del bag[created_at_field]

            _columns = self.component.get_columns_for(bag)
            query = 'INSERT INTO {table} ({keys}) VALUES ({values})'.format(
                table=self.component.table_name, keys=', '.join(_columns), values=', '.join(['?'] * len(_columns))
            )
            values = [bag[key] for key in _columns]

        # Execute
        self.connection.execute(query, values)

        # Increment stats TODO
        # if row:
        #    self._output._special_stats[UPDATE] += 1
        # else:
        #    self._output._special_stats[INSERT] += 1

        # If user required us to fetch some columns, let's query again to get their actual values.
        if self.component.fetch_columns and len(self.component.fetch_columns):
            if not row:
                row = self.find(bag)
            if not row:
                raise ValueError('Could not find matching row after load.')

            for alias, column in self.component.fetch_columns.items():
                bag[alias] = row[column]

        return bag

    def find(self, dataset, connection=None):
        query = 'SELECT * FROM {table} WHERE {criteria} LIMIT 1'.format(
            table=self.component.table_name,
            criteria=' AND '.join([key_atom + ' = ?' for key_atom in self.component.discriminant]),
        )
        rp = (connection or
              self.connection).execute(query, [dataset.get(key_atom) for key_atom in self.component.discriminant])

        # Increment stats TODO
        # self._input._special_stats[SELECT] += 1

        return rp.fetchone()


DatabaseWriter.Context = _DatabaseWriterExecutionContext
