from functools import partial

import os
from sqlalchemy import Column, MetaData, String, Table, create_engine

import bonobo
from bonobo.examples.datasets import fablabs as fablabs_example
from bonobo.ext.opendatasoft import OpenDataSoftAPI
from bonobo_sqlalchemy import InsertOrUpdate
from bonobo_sqlalchemy.strategy import ThreadCollectionStrategy

API_DATASET = 'fablabs-in-the-world'
API_NETLOC = 'datanova.laposte.fr'
ROWS = 100

dbname = os.path.splitext(os.path.basename(__file__))[0]
engine_factory = partial(create_engine, 'sqlite:///{}.db'.format(dbname))

engine = engine_factory()
metadata = MetaData()

table = Table(
    'fablabs',
    metadata,
    Column('name', String(255), primary_key=True),
    Column('address', String(255)),
    Column('links', String(255)),
)
metadata.create_all(engine)
engine.dispose()

graph = bonobo.Graph(
    OpenDataSoftAPI(dataset=API_DATASET, netloc=API_NETLOC, timezone='Europe/Paris'),
    fablabs_example.normalize,
    fablabs_example.filter_france,
    InsertOrUpdate(table_name='fablabs', discriminant=('name',)),
)

if __name__ == '__main__':
    bonobo.run(graph, services={
        'sqlalchemy.engine': engine_factory()
    }, strategy=ThreadCollectionStrategy())
