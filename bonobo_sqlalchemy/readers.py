from bonobo.config import Option
from bonobo.config.configurables import Configurable
from bonobo.config.services import Service


class DatabaseReader(Configurable):
    """
    Extract data from a database using some raw SQL and yield one output line per query result.

    .. attribute:: engine

        The sqlalchemy engine to use for extraction.

    .. attribute:: query

        The database query that will be used to extract data from database. Should not contain OFFSET/LIMIT, nor ";".

    .. attribute:: pack_size

        The number of records to retrieve at a time (will be used to add OFFSET/LIMIT clauses to SQL).

    """

    engine = Service(default='sqlalchemy.engine') # type: sqlalchemy.engine

    query: str = Option(str, required=True, default='SELECT 1')
    pack_size: int = Option(int, default=1000)
    limit: int = Option(int)

    def extract(self, engine):
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
