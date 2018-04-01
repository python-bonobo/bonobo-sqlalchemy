import sqlalchemy

import bonobo
import bonobo_sqlalchemy


def get_services():
    return {
        'sqlalchemy.pgengine': sqlalchemy.create_engine('postgresql+psycopg2://@localhost:5432/example')
    }


def get_graph(**options):
    """This function builds the graph that needs to be executed. :return: bonobo.Graph   """
    return bonobo.Graph(
        bonobo_sqlalchemy.Select('SELECT * FROM table', engine='sqlalchemy.pgengine'),
        bonobo_sqlalchemy.InsertOrUpdate(table_name='table_1', engine='sqlalchemy.pgengine'),
    )


# The __main__ block actually execute the graph.
if __name__ == '__main__':
    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        bonobo.run(
            get_graph(**options),
            services=get_services(**options)
        )
