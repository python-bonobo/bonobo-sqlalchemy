from collections import defaultdict
from os import environ

from sqlalchemy import create_engine

from bonobo_sqlalchemy.logging import get_logger

logger = get_logger()

POSTGRES_DEFAULTS = {
    'driver': 'postgres',
    'host': 'localhost',
    'port': '5432',
    'name': 'postgres',
    'user': 'postgres',
    'pass': '',
}

DSN_TEMPLATE = '{driver}://{user}:{pass}@{host}:{port}/{name}'


def create_postgresql_engine(*, options='client_encoding=utf8', env='POSTGRES', **kwargs):
    config = defaultdict(**POSTGRES_DEFAULTS)
    for var in ('driver', 'user', 'pass', 'host', 'port', 'name'):
        if var in kwargs:
            config[var] = kwargs.pop(var)
        elif env:
            env_var = '{}_{}'.format(env, var).upper()
            if env_var in environ:
                config[var] = environ[env_var]
    dsn = DSN_TEMPLATE.format(**config)
    if options:
        dsn += '?' + options

    logger.info('Creating database engine: ' + dsn)

    return create_engine(dsn, **kwargs)
