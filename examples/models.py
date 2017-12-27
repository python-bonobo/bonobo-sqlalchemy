from sqlalchemy import Table, Column, Integer, String, MetaData

metadata = MetaData()

table = Table(
    'example',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('value', String(255)),
)
