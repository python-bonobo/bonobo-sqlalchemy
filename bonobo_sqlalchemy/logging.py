from bonobo.logging import get_logger as _get_logger


def get_logger(name='bonobo_sqlalchemy'):
    return _get_logger(name)


logger = get_logger()
getLogger = get_logger
