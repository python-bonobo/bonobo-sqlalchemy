from sqlalchemy.exc import OperationalError

from bonobo.errors import UnrecoverableError


class ProhibitedOperationError(Exception):
    pass


class UnrecoverableOperationalError(UnrecoverableError, OperationalError):
    pass
