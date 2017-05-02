import logging

import bonobo
import os
from bonobo.examples.datasets import fablabs as fablabs_example
from bonobo.ext.opendatasoft import OpenDataSoftAPI
from sqlalchemy import Column, MetaData, String, Table, Text, create_engine
from sqlalchemy.exc import OperationalError

from bonobo_sqlalchemy import InsertOrUpdate

API_DATASET = 'fablabs-in-the-world'
API_NETLOC = 'datanova.laposte.fr'

DSN = 'postgresql://postgres@localhost:5432/{}?client_encoding=utf8'

dbname = os.path.splitext(os.path.basename(__file__))[0]
engine = create_engine(DSN.format(dbname))
metadata = MetaData()

table = Table(
    'fablabs',
    metadata,
    Column('name', String(255), primary_key=True),
    Column('address', Text()),
    Column('links', Text()),
)
try:
    metadata.create_all(engine)
except OperationalError:
    root_engine = create_engine(DSN.format('postgres'))
    conn = root_engine.connect()
    conn.execute("commit")
    conn.execute("create database " + dbname)
    conn.close()
    metadata.create_all(engine)

logging.basicConfig()
# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

graph = bonobo.Graph(
    OpenDataSoftAPI(dataset=API_DATASET, netloc=API_NETLOC, timezone='Europe/Paris'),
    fablabs_example.normalize,
    fablabs_example.filter_france,
    InsertOrUpdate(table_name='fablabs', discriminant=('name',), buffer_size=10),

)

if __name__ == '__main__':
    bonobo.run(graph, services={
        'sqlalchemy.engine': engine
    })
