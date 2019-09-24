from sqlalchemy import Column, Integer, MetaData, String, Table

metadata = MetaData()

table = Table("example", metadata, Column("id", Integer, primary_key=True), Column("value", String(255)))
