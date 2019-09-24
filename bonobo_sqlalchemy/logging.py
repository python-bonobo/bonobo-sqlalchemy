import logging

import mondrian

import bonobo_sqlalchemy

mondrian.setup(excepthook=True)
logger = logging.getLogger(bonobo_sqlalchemy.__name__)
