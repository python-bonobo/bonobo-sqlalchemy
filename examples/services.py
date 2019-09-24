from bonobo_sqlalchemy.util import POSTGRES_DEFAULTS, create_postgresql_engine


def create_engine(superuser=False):
    import settings

    if settings.DATABASE_TYPE == "postgres":
        db_config = POSTGRES_DEFAULTS

        if not superuser:
            db_config["name"] = settings.DATABASE_NAME
            db_config["user"] = settings.DATABASE_USERNAME
            db_config["pass"] = settings.DATABASE_PASSWORD

        return create_postgresql_engine(**db_config)
    else:
        raise NotImplementedError("Example not implemented for database type {}.".format(settings.DATABASE_TYPE))


def get_services():
    return {"sqlalchemy.engine": create_engine()}
