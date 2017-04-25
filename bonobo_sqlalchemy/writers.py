import datetime
import traceback
from queue import Queue

from sqlalchemy import MetaData, Table

from bonobo import Bag, ErrorBag
from bonobo.config import Configurable, ContextProcessor, Option, Service
from bonobo.config.processors import contextual
from bonobo_sqlalchemy.constants import INSERT, UPDATE


class ProhibitedOperationError(Exception):
    pass


@contextual
class InsertOrUpdate(Configurable):
    """
    xxx todo fields vs columns, choose a name
    """
    table_name: str = Option(str, required=True)
    fetch_columns: tuple = Option(tuple, default=())
    insert_only_fields: tuple = Option(tuple, default=())
    discriminant: tuple = Option(tuple, default=('id',))
    created_at_field: str = Option(str, default='created_at')
    updated_at_field: str = Option(str, default='updated_at')
    allowed_operations: tuple = Option(tuple, default=(INSERT, UPDATE,))
    buffer_size: int = Option(int, default=1000)

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
        buffer = Queue()
        yield buffer

        for row in self.commit(table, connection, buffer, force=True):
            context.push(Bag(row))

    def initialize(self, context):
        ### XXX maybe outdated ??? Where does this go ?
        context.stats.update(dict(SELECT=0, INSERT=0, UPDATE=0))

    def __call__(self, engine, connection, table, buffer, row, *args):
        """
        Main transformatio method, pushing a row to the "yet to be processed elements" queue and commiting if necessary.
        
        :param engine: 
        :param connection: 
        :param buffer: 
        :param row: 
        """
        buffer.put(row)
        yield from self.commit(table, connection, buffer)

    def commit(self, table, connection, buffer, force=False):
        if force or buffer.qsize() >= self.buffer_size:
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
        print('before', row)
        dbrow = self.find(connection, row)
        print('after', row)

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

            _columns = self.get_columns_for(column_names, row, dbrow)

            query = 'UPDATE {table} SET {values} WHERE {criteria}'.format(
                table=self.table_name,
                values=', '.join(
                    ('{column} = ?'.format(column=_column) for _column in _columns if not _column in self.discriminant)
                ),
                criteria=' AND '.join(('{key} = ?'.format(key=_key) for _key in self.discriminant))
            )
            values = [row[_column] for _column in _columns if not _column in self.discriminant] + \
                     [row[_column] for _column in self.discriminant]

        # INSERT
        else:
            if not INSERT in self.allowed_operations:
                raise ProhibitedOperationError('INSERT operations are not allowed by this transformation.')

            if self.created_at_field in column_names:
                row[self.created_at_field] = now
            else:
                if self.created_at_field in row:
                    del row[self.created_at_field]

            _columns = self.get_columns_for(column_names, row)
            query = 'INSERT INTO {table} ({keys}) VALUES ({values})'.format(
                table=self.table_name, keys=', '.join(_columns), values=', '.join(['?'] * len(_columns))
            )
            values = [row[key] for key in _columns]

        # Execute
        connection.execute(query, values)

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

    def find(self, connection, dataset):
        query = 'SELECT * FROM {self.table_name} WHERE {criteria} LIMIT 1'.format(
            self=self,
            criteria=' AND '.join([key_atom + ' = ?' for key_atom in self.discriminant]),
        )
        rp = connection.execute(
            query, [dataset.get(key_atom) for key_atom in self.discriminant]
        )

        # Increment stats TODO
        # self._input._special_stats[SELECT] += 1

        return rp.fetchone()

    def get_columns_for(self, column_names, row, dbrow=None):
        """Retrieve list of table column names for which we have a value in given hash.

        """
        if dbrow:
            candidates = filter(
                lambda column: not column in self.insert_only_fields,
                column_names
            )
        else:
            candidates = column_names

        return [key for key in row if key in candidates]

    def add_fetch_columns(self, *columns, **aliased_columns):
        self.fetch_columns = {
            **self.fetch_columns,
            **aliased_columns,
        }

        for column in columns:
            self.fetch_columns[column] = column
