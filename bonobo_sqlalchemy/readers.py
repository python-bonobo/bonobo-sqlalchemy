from bonobo.config import Option, use_context, use_raw_input
from bonobo.config.configurables import Configurable
from bonobo.config.services import Service
from bonobo.errors import UnrecoverableError


@use_context
@use_raw_input
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

    query = Option(str, positional=True, default="SELECT 1", __doc__="The actual SQL query to run.")  # type: str
    pack_size = Option(int, required=False, default=1000, __doc__="How many rows to retrieve at once.")  # type: int
    limit = Option(int, required=False, __doc__="Maximum rows to retrieve, in total.")  # type: int
    engine = Service("sqlalchemy.engine", __doc__="Database connection (an sqlalchemy.engine).")  # type: str

    output_fields = Option(list, required=False, default=None)

    def set_output_fields(self, context, input_row, row):
        if self.output_fields:
            context.set_output_fields(self.output_fields)
        else:
            try:
                _fields = input_row._fields
            except AttributeError:
                try:
                    _fields = input_row.keys()
                except AttributeError:
                    _fields = range(len(input_row))

            context.set_output_fields([*_fields, *row.keys()])

    def formatter(self, input_row, row):
        """
        Formats a result row into whataver you need to send on this transformations' output stream.

        :param context:
        :param index:
        :param row:

        :return: mixed

        """
        return input_row + tuple(row)

    @property
    def args(self):
        """
        Provide positional parameters for input query.

        See https://www.python.org/dev/peps/pep-0249/#paramstyle

        :return: dict

        """
        return ()

    @property
    def kwargs(self):
        """
        Provide named parameters for input query.

        See https://www.python.org/dev/peps/pep-0249/#paramstyle

        :return: dict

        """
        return {}

    def __call__(self, context, input_row, *, engine):
        context.setdefault("index", 0)

        query = self.query.strip(" \n;")

        assert self.pack_size > 0, "Pack size must be > 0 for now."

        offset = 0

        # prepare parameters for query...
        args, kwargs = self.args, self.kwargs
        try:
            kwargs = {**kwargs, **input_row._asdict()}
        except AttributeError:
            args = args + input_row

        sqlparams = {**{str(i): v for i, v in enumerate(args)}, **kwargs}

        while not self.limit or offset * self.pack_size < self.limit:
            real_offset = offset * self.pack_size

            if self.limit:
                _limit = max(min(self.pack_size, self.limit - real_offset), 0)
            else:
                _limit = self.pack_size

            # exit loop if we're gonna LIMIT 0
            if not _limit:
                break

            _offset = real_offset and " OFFSET {}".format(real_offset) or ""
            _query = "{query} LIMIT {limit}{offset}".format(query=query, limit=_limit, offset=_offset)

            try:
                results = engine.execute(_query, **sqlparams, use_labels=True).fetchall()
            except Exception as exc:
                raise UnrecoverableError("Unable to execute query.") from exc

            if not len(results):
                break

            for row in results:
                _formatted_row = self.formatter(input_row, row)
                if _formatted_row:
                    if not context.index:
                        self.set_output_fields(context, input_row, row)

                    if len(_formatted_row) != len(context.get_output_fields()):
                        raise ValueError(
                            "Formatted rows contains {} fields while context expects {!r}".format(
                                len(_formatted_row), context.get_output_fields()
                            )
                        )
                    yield _formatted_row
                    context.index += 1

            offset += 1
