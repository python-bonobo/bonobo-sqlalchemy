import os
import sys

import bonobo
import bonobo_sqlalchemy


def get_graph(**options):
    graph = bonobo.Graph()
    graph.add_chain(
        bonobo_sqlalchemy.Select('SELECT * FROM example', limit=100, pack_size=9),
        bonobo.PrettyPrinter(),
    )

    return graph


# The __main__ block actually execute the graph.
if __name__ == '__main__':
    sys.path.append(os.path.dirname(__file__))
    import commands, services

    with commands.parse_args() as options:
        bonobo.run(get_graph(**options), services=services.get_services())
