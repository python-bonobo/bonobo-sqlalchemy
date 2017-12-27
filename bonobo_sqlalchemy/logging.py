import logging

import bonobo_sqlalchemy
import mondrian

mondrian.setup(excepthook=True)
logger = logging.getLogger(bonobo_sqlalchemy.__name__)
