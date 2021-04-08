from datetime import datetime

import peewee
from playhouse.pool import PooledMySQLDatabase

# db = peewee.MySQLDatabase("mydb", host="localhost", port=3306, user="root", passwd="root")

db = PooledMySQLDatabase(
    'mydb',
    max_connections=32,
    stale_timeout=300,  # 5 minutes.
    user='root',
    passwd='root')


class BaseModel(peewee.Model):
    """A base model that will use our MySQL database"""
    created = peewee.DateTimeField(default=datetime.now())

    class Meta:
        database = db


class CIK(BaseModel):
    cik = peewee.IntegerField()
    ticker = peewee.CharField(max_length=15, null=True)
    indexes = (
        (('cik', 'ticker'), True),
    )

    def __enter__(self):
        return self


class EdgarEntry(BaseModel):
    cik = peewee.ForeignKeyField(CIK, backref='entries', on_update='CASCADE')
    form_type = peewee.CharField(max_length=10)
    date = peewee.DateTimeField()
    html_link = peewee.CharField()
    doc_link = peewee.CharField()


# simple utility function to create tables
def create_tbls():
    with db:
        db.create_tables([CIK, EdgarEntry])
