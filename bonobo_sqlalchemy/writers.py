import datetime
import logging
import traceback
from queue import Queue

from sqlalchemy import MetaData, Table, and_
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql import select

from bonobo.config import Configurable, ContextProcessor, Option, Service, use_context, use_raw_input
from bonobo.errors import UnrecoverableError
from bonobo_sqlalchemy.constants import INSERT, UPDATE
from bonobo_sqlalchemy.errors import ProhibitedOperationError

logger = logging.getLogger(__name__)


@use_context
@use_raw_input
class InsertOrUpdate(Configurable):
    """
    TODO: fields vs columns, choose a name (XXX)
    Maybe the obvious choice is to keep "field" for row fields, as it's already the name used by bonobo, and call the
    database columns "columns".
    """
    table_name = Option(str, positional=True)  # type: str
    fetch_columns = Option(tuple, required=False, default=())  # type: tuple
    insert_only_fields = Option(tuple, required=False, default=())  # type: tuple
    discriminant = Option(tuple, required=False, default=('id', ))  # type: tuple
    created_at_field = Option(str, required=False, default='created_at')  # type: str
    updated_at_field = Option(str, required=False, default='updated_at')  # type: str
    allowed_operations = Option(
        tuple, required=False, default=(
            INSERT,
            UPDATE,
        )
    )  # type: tuple
    buffer_size = Option(int, required=False, default=1000)  # type: int

    engine = Service('sqlalchemy.engine')  # type: str

    @ContextProcessor
    def create_connection(self, context, *, engine):
        """
        This context processor creates an sqlalchemy connection for use during the lifetime of this transformation's
        execution.
        
        :param engine: 
        """
        try:
            connection = engine.connect()
        except OperationalError as exc:
            raise UnrecoverableError('Could not create SQLAlchemy connection: {}.'.format(str(exc).replace('\n', ''))
                                     ) from exc

        with connection:
            yield connection

    @ContextProcessor
    def create_table(self, context, connection, *, engine):
        """SQLAlchemy table object, using metadata autoloading from database to avoid the need of column definitions."""
        yield Table(self.table_name, MetaData(), autoload=True, autoload_with=engine)

    @ContextProcessor
    def create_buffer(self, context, connection, table, *, engine):
        """
        This context processor creates a "buffer" of yet to be persisted elements, and commits the remaining elements
        when the transformation ends.
        
        :param engine: 
        :param connection: 
        """
        buffer = yield Queue()
        try:
            for row in self.commit(table, connection, buffer, force=True):
                context.send(row)
        except Exception as exc:
            logger.exception('Flush fail')
            raise UnrecoverableError('Flushing query buffer failed.') from exc

    def __call__(self, connection, table, buffer, context, row, engine):
        """
        Main transformation method, pushing a row to the "yet to be processed elements" queue and commiting if necessary.
        
        :param engine: 
        :param connection: 
        :param buffer: 
        :param row: 
        """
        buffer.put(row)

        yield from self.commit(table, connection, buffer)

    def commit(self, table, connection, buffer, force=False):
        if force or (buffer.qsize() >= self.buffer_size):
            with connection.begin():
                while buffer.qsize() > 0:
                    yield self.insert_or_update(table, connection, buffer.get())

    def insert_or_update(self, table, connection, row):
        """ Actual database load transformation logic, without the buffering / transaction logic. 
        """

        # find line, if it exist
        dbrow = self.find(connection, table, row)

        # TODO XXX use actual database function instead of this stupid thing
        now = datetime.datetime.now()

        target_row = row._asdict()
        column_names = table.columns.keys()

        # UpdatedAt field configured ? Let's set the value in source hash
        if self.updated_at_field in column_names:
            target_row[self.updated_at_field] = now  # XXX not pure ...

        # Update logic
        if dbrow:
            if not UPDATE in self.allowed_operations:
                raise ProhibitedOperationError('UPDATE operations are not allowed by this transformation.')

            query = table.update().values(
                **{col: target_row.get(col)
                   for col in self.get_columns_for(column_names, target_row, dbrow)}
            ).where(and_(*(getattr(table.c, col) == target_row.get(col) for col in self.discriminant)))

        # INSERT
        else:
            if not INSERT in self.allowed_operations:
                raise ProhibitedOperationError('INSERT operations are not allowed by this transformation.')

            if self.created_at_field in column_names:
                target_row[self.created_at_field] = now  # XXX UNPURE
            else:
                if self.created_at_field in target_row:
                    del target_row[self.created_at_field]  # UNPURE

            query = table.insert().values(**{col: target_row.get(col) for col in self.get_columns_for(column_names, target_row)})

        # Execute
        try:
            connection.execute(query)
        except Exception as exc:
            connection.rollback()
            raise UnrecoverableError('Unable to execute query.') from exc

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
        sql = select([table]).where(and_(*(getattr(table.c, col) == row.get(col)
                                           for col in self.discriminant))).limit(1)

        try:
            row = connection.execute(sql).fetchone()
        except Exception as exc:
            raise UnrecoverableError('Unable to execute query.') from exc

        return dict(row) if row else None

    def get_columns_for(self, column_names, target_row, dbrow=None):
        """Retrieve list of table column names for which we have a value in given hash.

        """
        if dbrow:
            candidates = filter(lambda col: col not in self.insert_only_fields, column_names)
        else:
            candidates = column_names

        return set(candidates).intersection(target_row.keys())

    def add_fetch_columns(self, *columns, **aliased_columns):
        self.fetch_columns = {
            **self.fetch_columns,
            **aliased_columns,
        }

        for column in columns:
            self.fetch_columns[column] = column
