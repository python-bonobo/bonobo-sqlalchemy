import os
import sys

import bonobo
import bonobo_sqlalchemy
from bonobo.config import use_context


@use_context
def extract(context):
    context.set_output_fields(["id", "value"])
    for i in range(1000):
        yield i, "value for {}".format(i)


def get_graph(**options):
    graph = bonobo.Graph()
    graph.add_chain(extract, bonobo_sqlalchemy.InsertOrUpdate("example"))

    return graph


# The __main__ block actually execute the graph.
if __name__ == "__main__":
    sys.path.append(os.path.dirname(__file__))
    import commands, services

    with commands.parse_args() as options:
        bonobo.run(get_graph(**options), services=services.get_services())
