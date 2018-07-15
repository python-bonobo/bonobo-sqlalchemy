from bonobo.config import Option, use_context
from bonobo.config.configurables import Configurable
from bonobo.config.services import Service


@use_context
class Select(Configurable):
    """
    Reads data from a database using a SQL query and a limit-offset based pagination.

    Example:

    .. code-block:: python

        Select('SELECT * from foo;')

    Caveats:

    We're using "limit-offset" pagination, but limit-offset pagination can be inconsistent.

    Suppose a user moves from page n to n+1 while simultaneously a new element is inserted into page n. This will cause
    both a duplication (the previously-final element of page n is pushed into page n+1) and an omission (the new
    element). Alternatively consider an element removed from page n just as the user moves to page n+1. The previously
    initial element of page n+1 will be shifted to page n and be omitted.

    A better implementation could be to use database-side cursors, to have the external system mark the last row
    extracted and "stabilize" pagination. Here is an example of how this can be done (although it's not implemented in
    bonobo-sqlalchemy, for now).

    .. code-block:: sql

        -- We must be in a transaction
        BEGIN;
        -- Open a cursor for a query
        DECLARE select_cursor CURSOR FOR SELECT * FROM foo;
        -- Retrieve ten rows
        FETCH 10 FROM select_cursor;
        -- ...
        -- Retrieve ten more from where we left off
        FETCH 10 FROM select_cursor;
        -- All done
        COMMIT;

    """
    query = Option(str, positional=True, default='SELECT 1', __doc__='The actual SQL query to run.')  # type: str
    pack_size = Option(int, required=False, default=1000, __doc__='How many rows to retrieve at once.')  # type: int
    limit = Option(int, required=False, __doc__='Maximum rows to retrieve, in total.')  # type: int

    engine = Service('sqlalchemy.engine', __doc__='Database connection (an sqlalchemy.engine).')  # type: str

    def formatter(self, context, index, row):
        """
        Formats a result row into whataver you need to send on this transformations' output stream.

        :param context:
        :param index:
        :param row:

        :return: mixed

        """
        if not index:
            context.set_output_fields(row.keys())
        return tuple(row)

    @property
    def parameters(self):
        """
        Provide parameters for input query.

        See https://www.python.org/dev/peps/pep-0249/#paramstyle

        :return: dict

        """
        return {}

    def __call__(self, context, *, engine):
        query = self.query.strip(' \n;')

        assert self.pack_size > 0, 'Pack size must be > 0 for now.'

        offset = 0
        parameters = self.parameters

        while not self.limit or offset * self.pack_size < self.limit:
            real_offset = offset * self.pack_size

            if self.limit:
                _limit = max(min(self.pack_size, self.limit - real_offset), 0)
            else:
                _limit = self.pack_size

            # exit loop if we're gonna LIMIT 0
            if not _limit:
                break

            _offset = real_offset and ' OFFSET {}'.format(real_offset) or ''
            _query = '{query} LIMIT {limit}{offset}'.format(query=query, limit=_limit, offset=_offset)

            results = engine.execute(_query, parameters, use_labels=True).fetchall()

            if not len(results):
                break

            for i, row in enumerate(results):
                formatted_row = self.formatter(context, real_offset + i, row)
                if formatted_row:
                    yield formatted_row

            offset += 1
