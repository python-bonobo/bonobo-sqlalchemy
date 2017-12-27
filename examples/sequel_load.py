import logging
import os
import sys
import warnings

import bonobo
import bonobo_sqlalchemy
from bonobo.config import use_context


@use_context
def extract(context):
    context.set_output_fields(['id', 'value'])
    for i in range(100):
        yield i, 'value for {}'.format(i)


def get_graph(**options):
    graph = bonobo.Graph()
    graph.add_chain(
        extract,
        bonobo_sqlalchemy.InsertOrUpdate('example')
    )

    return graph


def _execute_sql(engine, sql):
    conn = engine.connect()
    try:
        conn.execute("COMMIT")
        conn.execute(sql)
    except Exception as exc:
        warnings.warn(exc)
    finally:
        conn.close()


# The __main__ block actually execute the graph.
if __name__ == '__main__':
    parser = bonobo.get_argument_parser()
    parser.add_argument('--drop', '-D', action='store_true')
    parser.add_argument('--create', '-C', action='store_true')
    parser.add_argument('--echo', action='store_true')

    with bonobo.parse_args(parser) as options:
        sys.path.append(os.path.dirname(__file__))
        import models
        import settings
        import services

        if options['echo']:
            logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

        if options['drop'] or options['create']:
            root_engine = services.create_engine(superuser=True)
            if options['drop']:
                # drop database/role with super user privileges
                _execute_sql(root_engine, "DROP DATABASE {}".format(settings.DATABASE_NAME))
                _execute_sql(root_engine, "DROP ROLE {}".format(settings.DATABASE_USERNAME))

            if options['create']:
                # create database/role with super user privileges
                _execute_sql(root_engine, 'CREATE ROLE {} WITH LOGIN PASSWORD \'{}\';'.format(
                    settings.DATABASE_USERNAME, settings.DATABASE_PASSWORD
                ))
                _execute_sql(root_engine,
                             'CREATE DATABASE {} WITH OWNER={} TEMPLATE=template0 ENCODING="utf-8";'.format(
                                 settings.DATABASE_NAME, settings.DATABASE_USERNAME
                             ))

                # create tables in userland
                engine = services.create_engine()
                models.metadata.create_all(engine)

        bonobo.run(get_graph(**options), services=services.get_services())
