import datetime
import traceback
from queue import Queue

from sqlalchemy import MetaData, Table, and_
from sqlalchemy.sql import select

from bonobo.config import Configurable, Option, Service, ContextProcessor
from bonobo.structs.bags import Bag
from bonobo.structs.bags import ErrorBag
from bonobo_sqlalchemy.constants import INSERT, UPDATE
from bonobo_sqlalchemy.errors import ProhibitedOperationError


class InsertOrUpdate(Configurable):
    """
    TODO: fields vs columns, choose a name (XXX)
    """
    table_name = Option(str, positional=True, required=True)  # type: str
    fetch_columns = Option(tuple, default=())  # type: tuple
    insert_only_fields = Option(tuple, default=())  # type: tuple
    discriminant = Option(tuple, default=('id', ))  # type: tuple
    created_at_field = Option(str, default='created_at')  # type: str
    updated_at_field = Option(str, default='updated_at')  # type: str
    allowed_operations = Option(tuple, default=(INSERT, UPDATE, ))  # type: tuple
    buffer_size = Option(int, default=1000)  # type: int

    engine = Service('sqlalchemy.engine')  # type: str

    @ContextProcessor
    def create_connection(self, context, engine):
        """
        This context processor creates an sqlalchemy connection for use during the lifetime of this transformation's
        execution.
        
        :param engine: 
        """
        with engine.connect() as connection:
            yield connection

    @ContextProcessor
    def create_table(self, context, engine, connection):
        """SQLAlchemy table object, using metadata autoloading from database to avoid the need of column definitions."""
        yield Table(self.table_name, MetaData(), autoload=True, autoload_with=engine)

    @ContextProcessor
    def create_buffer(self, context, engine, connection, table):
        """
        This context processor creates a "buffer" of yet to be persisted elements, and commits the remaining elements
        when the transformation ends.
        
        :param engine: 
        :param connection: 
        """
        buffer = yield Queue()
        for row in self.commit(table, connection, buffer, force=True):
            context.send(Bag(row))

    def __call__(self, engine, connection, table, buffer, *args, **kwargs):
        """
        Main transformatio method, pushing a row to the "yet to be processed elements" queue and commiting if necessary.
        
        :param engine: 
        :param connection: 
        :param buffer: 
        :param row: 
        """

        if len(args) == 1 and not len(kwargs):
            buffer.put(args[0])
        elif len(kwargs) and not len(args):
            buffer.put(kwargs)
        else:
            raise RuntimeError('Invalid input.')

        yield from self.commit(table, connection, buffer)

    def commit(self, table, connection, buffer, force=False):
        if force or (buffer.qsize() >= self.buffer_size):
            with connection.begin():
                while buffer.qsize() > 0:
                    try:
                        yield self.insert_or_update(table, connection, buffer.get())
                    except Exception as exc:
                        yield ErrorBag(exc, traceback.format_exc())

    def insert_or_update(self, table, connection, row):
        """ Actual database load transformation logic, without the buffering / transaction logic. 
        """

        # find line, if it exist
        dbrow = self.find(connection, table, row)

        # TODO XXX use actual database function instead of this stupid thing
        now = datetime.datetime.now()

        column_names = table.columns.keys()

        # UpdatedAt field configured ? Let's set the value in source hash
        if self.updated_at_field in column_names:
            row[self.updated_at_field] = now  # XXX not pure ...

        # Update logic
        if dbrow:
            if not UPDATE in self.allowed_operations:
                raise ProhibitedOperationError('UPDATE operations are not allowed by this transformation.')

            query = table.update().values(
                **{col: row.get(col)
                   for col in self.get_columns_for(column_names, row, dbrow)}
            ).where(and_(*(getattr(table.c, col) == row.get(col) for col in self.discriminant)))

        # INSERT
        else:
            if not INSERT in self.allowed_operations:
                raise ProhibitedOperationError('INSERT operations are not allowed by this transformation.')

            if self.created_at_field in column_names:
                row[self.created_at_field] = now  # XXX UNPURE
            else:
                if self.created_at_field in row:
                    del row[self.created_at_field]  # UNPURE

            query = table.insert().values(**{col: row.get(col) for col in self.get_columns_for(column_names, row)})

        # Execute
        try:
            connection.execute(query)
        except Exception:
            connection.rollback()
            raise

        # Increment stats TODO
        # if dbrow:
        #    self._output._special_stats[UPDATE] += 1
        # else:
        #    self._output._special_stats[INSERT] += 1

        # If user required us to fetch some columns, let's query again to get their actual values.
        if self.fetch_columns and len(self.fetch_columns):
            if not dbrow:
                dbrow = self.find(row)
            if not dbrow:
                raise ValueError('Could not find matching row after load.')

            for alias, column in self.fetch_columns.items():
                row[alias] = dbrow[column]

        return row

    def find(self, connection, table, row):
        sql = select([table]
                     ).where(and_(*(getattr(table.c, col) == row.get(col) for col in self.discriminant))).limit(1)
        row = connection.execute(sql).fetchone()
        return dict(row) if row else None

    def get_columns_for(self, column_names, row, dbrow=None):
        """Retrieve list of table column names for which we have a value in given hash.

        """
        if dbrow:
            candidates = filter(lambda col: col not in self.insert_only_fields, column_names)
        else:
            candidates = column_names

        return set(candidates).intersection(row.keys())

    def add_fetch_columns(self, *columns, **aliased_columns):
        self.fetch_columns = {
            **self.fetch_columns,
            **aliased_columns,
        }

        for column in columns:
            self.fetch_columns[column] = column
