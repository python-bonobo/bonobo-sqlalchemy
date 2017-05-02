import logging

from sqlalchemy import *
from sqlalchemy.exc import OperationalError

import bonobo
from bonobo.commands.run import get_default_services
from bonobo_sqlalchemy.examples import postgresql_dsn
from bonobo_sqlalchemy.models.stacks import create_postgres_stack_table
from bonobo_sqlalchemy import Select, InsertOrUpdate

logging.basicConfig()
#logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

metadata = MetaData()
database = 'stacks'
table = create_postgres_stack_table('simple_stack', metadata=metadata)


def get_services():
    engine = create_engine(postgresql_dsn(database=database))
    try:
        metadata.create_all(engine)
    except OperationalError:
        root_engine = create_engine(postgresql_dsn(database='postgres'))
        conn = root_engine.connect()
        conn.execute("commit")
        conn.execute("create database " + database)
        conn.close()
        metadata.create_all(engine)

    return {
        'sqlalchemy.engine': engine
    }

graph = bonobo.Graph(
    Select('SELECT * FROM {!s}'.format(table)),
    bonobo.pprint,
)

if __name__ == '__main__':
    bonobo.run(graph, services=get_default_services(__file__))
