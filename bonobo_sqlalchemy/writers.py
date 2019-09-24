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
    insert_only_fields = Option(tuple, required=False, default=())  # type: tuple
    discriminant = Option(tuple, required=False, default=("id",))  # type: tuple
    created_at_field = Option(str, required=False, default="created_at")  # type: str
    updated_at_field = Option(str, required=False, default="updated_at")  # type: str
    allowed_operations = Option(tuple, required=False, default=(INSERT, UPDATE))  # type: tuple
    buffer_size = Option(int, required=False, default=1000)  # type: int

    engine = Service("sqlalchemy.engine")  # type: str

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
            raise UnrecoverableError(
                "Could not create SQLAlchemy connection: {}.".format(str(exc).replace("\n", ""))
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
            logger.exception("Flush fail")
            raise UnrecoverableError("Flushing query buffer failed.") from exc

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
                    try:
                        yield self.insert_or_update(table, connection, buffer.get())
                    except Exception as exc:
                        yield exc

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
                raise ProhibitedOperationError("UPDATE operations are not allowed by this transformation.")

            query = (
                table.update()
                .values(**{col: row.get(col) for col in self.get_columns_for(column_names, row, dbrow)})
                .where(and_(*(getattr(table.c, col) == row.get(col) for col in self.discriminant)))
            )

        # INSERT
        else:
            if not INSERT in self.allowed_operations:
                raise ProhibitedOperationError("INSERT operations are not allowed by this transformation.")

            if self.created_at_field in column_names:
                row[self.created_at_field] = now  # XXX UNPURE
            else:
                if self.created_at_field in row:
                    del row[self.created_at_field]  # UNPURE

            query = table.insert().values(**{col: row.get(col) for col in self.get_columns_for(column_names, row)})

        # Execute
        try:
            connection.execute(query)
        except Exception as exc:
            connection.rollback()
            raise UnrecoverableError("Unable to execute query.") from exc

        # Increment stats TODO
        # if dbrow:
        #    self._output._special_stats[UPDATE] += 1
        # else:
        #    self._output._special_stats[INSERT] += 1

        return row

    def find(self, connection, table, row):
        sql = (
            select([table]).where(and_(*(getattr(table.c, col) == row.get(col) for col in self.discriminant))).limit(1)
        )

        try:
            row = connection.execute(sql).fetchone()
        except Exception as exc:
            raise UnrecoverableError("Unable to execute query.") from exc

        return dict(row) if row else None

    def get_columns_for(self, column_names, row, dbrow=None):
        """Retrieve list of table column names for which we have a value in given hash.

        """
        if dbrow:
            candidates = filter(lambda col: col not in self.insert_only_fields, column_names)
        else:
            candidates = column_names

        try:
            fields = row._fields
        except AttributeError as exc:
            fields = list(row.keys())

        return set(candidates).intersection(fields)

    def add_fetch_columns(self, *columns, **aliased_columns):
        self.fetch_columns = {**self.fetch_columns, **aliased_columns}

        for column in columns:
            self.fetch_columns[column] = column
