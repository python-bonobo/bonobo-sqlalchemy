from os import environ

POSTGRES_DEFAULTS = {
    'username': environ.get('POSTGRES_USERNAME', None) or 'postgres',
    'password': environ.get('POSTGRES_PASSWORD', None) or '',
    'hostname': environ.get('POSTGRES_HOSTNAME', None) or 'localhost',
    'port': environ.get('POSTGRES_PORT', None) or '5432',
    'database': environ.get('POSTGRES_DATABASE', None) or 'postgres',
}


def postgresql_dsn(**kwargs):
    params = dict(POSTGRES_DEFAULTS)
    for k, v in kwargs.items():
        if k not in params:
            raise KeyError('Unknown dsn option {}.'.format(k))
    return 'postgresql://{username}:{password}@{hostname}:{port}/{database}?client_encoding=utf8'.format(**params)
