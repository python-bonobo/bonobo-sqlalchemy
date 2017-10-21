from bonobo.config import Option
from bonobo.config.configurables import Configurable
from bonobo.config.services import Service


class Select(Configurable):
    query = Option(str, positional=True, default='SELECT 1')  # type: str
    pack_size = Option(int, required=False, default=1000)  # type: int
    limit = Option(int, required=False)  # type: int

    engine = Service('sqlalchemy.engine')  # type: str

    def call(self, engine):
        query = self.query.strip()
        if query[-1] == ';':
            query = query[0:-1]

        offset = 0
        while not self.limit or offset * self.pack_size < self.limit:
            results = engine.execute(
                '{query} LIMIT {limit}{offset}'.format(
                    query=query,
                    limit=self.pack_size,
                    offset=' OFFSET {}'.format(offset * self.pack_size) if offset else ''
                ),
                use_labels=True
            ).fetchall()

            if not len(results):
                break

            for row in results:
                yield dict(row)

            offset += 1
