from collections import defaultdict
from os import environ

from sqlalchemy import create_engine

from bonobo_sqlalchemy.logging import get_logger

logger = get_logger()

# mysql default config
# DATABASE_CONFIG_DEFAULTS = {
#     'driver': 'mysql',
#     'host': 'localhost',
#     'port': '3306',
#     'name': 'mysql',
#     'user': 'root',
#     'pass': '',
# }

DATABASE_CONFIG_DEFAULTS = {
    'driver': 'postgres',
    'host': 'localhost',
    'port': '5432',
    'name': 'postgres',
    'user': 'postgres',
    'pass': '',
}

DSN_TEMPLATE = '{driver}://{user}:{pass}@{host}:{port}/{name}'


def create_postgresql_engine(*, env='POSTGRES', **kwargs):
    config = defaultdict(**DATABASE_CONFIG_DEFAULTS)
    for var in ('driver', 'user', 'pass', 'host', 'port', 'name'):
        if var in kwargs:
            config[var] = kwargs.pop(var)
        elif env:
            env_var = '{}_{}'.format(env, var).upper()
            if env_var in environ:
                config[var] = environ[env_var]
    dsn = DSN_TEMPLATE.format(**config)
    if DATABASE_CONFIG_DEFAULTS['driver'].upper() == 'POSTGRES':  # or env == 'POSTGRES'
        dsn += '?client_encoding=utf8'

    logger.info('Creating database engine: ' + dsn)

    return create_engine(dsn, **kwargs)
