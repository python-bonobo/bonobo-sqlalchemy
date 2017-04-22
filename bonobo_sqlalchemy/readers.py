class DatabaseReader:
    """
    Extract data from a database using some raw SQL and yield one output line per query result.

    .. attribute:: engine

        The sqlalchemy engine to use for extraction.

    .. attribute:: query

        The database query that will be used to extract data from database. Should not contain OFFSET/LIMIT, nor ";".

    .. attribute:: pack_size

        The number of records to retrieve at a time (will be used to add OFFSET/LIMIT clauses to SQL).

    """

    query = 'SELECT 1'
    pack_size = 1000

    def __init__(self, engine, query=None, limit=None):
        self.engine = engine
        try:
            self.query = query or self.query
        except AttributeError as e:
            pass
        self.limit = limit

    def extract(self):
        query = self.query.strip()
        if query[-1] == ';':
            query = query[0:-1]

        offset = 0
        while not self.limit or offset * self.pack_size < self.limit:
            _query = query + ' LIMIT ' + str(self.pack_size) + ' OFFSET ' + str(offset * self.pack_size) + ';'
            results = self.engine.execute(_query, use_labels=True).fetchall()
            if not len(results):
                break

            for row in results:
                yield row

            offset += 1
