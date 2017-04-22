import logging
import os
from functools import partial

from sqlalchemy import Column
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import Text
from sqlalchemy import create_engine

from bonobo import console_run, PrettyPrint
from bonobo.core.strategies.executor import ThreadCollectionStrategy
from bonobo_sqlalchemy import DatabaseWriter

dbname = os.path.splitext(os.path.basename(__file__))[0]
engine_factory = partial(create_engine, 'sqlite:///{}.db'.format(dbname))

engine = engine_factory()
metadata = MetaData()

lorem_table = Table('lorem', metadata, Column('name', String(32), primary_key=True), Column('content', Text()))
metadata.create_all(engine)

logging.basicConfig()
# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

console_run(
    [
        {
            'name': 'original',
            'content': 'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut '
            'labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco '
            'laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in '
            'voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat '
            'cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. '
        }, {
            'name': 'cicero',
            'content': 'Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque '
            'laudantium, totam rem aperiam, eaque ipsa quae ab illo inventore veritatis et quasi '
            'architecto beatae vitae dicta sunt explicabo. Nemo enim ipsam voluptatem quia voluptas sit '
            'aspernatur aut odit aut fugit, sed quia consequuntur magni dolores eos qui ratione voluptatem '
            'sequi nesciunt. Neque porro quisquam est, qui dolorem ipsum quia dolor sit amet, consectetur, '
            'adipisci velit, sed quia non numquam eius modi tempora incidunt ut labore et dolore magnam '
            'aliquam quaerat voluptatem. Ut enim ad minima veniam, quis nostrum exercitationem ullam '
            'corporis suscipit laboriosam, nisi ut aliquid ex ea commodi consequatur? Quis autem vel eum '
            'iure reprehenderit qui in ea voluptate velit esse quam nihil molestiae consequatur, '
            'vel illum qui dolorem eum fugiat quo voluptas nulla pariatur? '
        }
    ],
    DatabaseWriter(
        engine_factory, 'lorem', discriminant=('name', )
    ),
    PrettyPrint(),
    strategy=ThreadCollectionStrategy
)
