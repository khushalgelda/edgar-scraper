import os
from datetime import datetime

import peewee
from playhouse.db_url import connect

database_proxy = peewee.DatabaseProxy()


class Database:
    def __init__(self, db, user, passwd, max_connections=32, stale_timeout=32):
        self.db = db
        self.user = user
        self.passwd = passwd
        self.max_connection = max_connections
        self.stale_timeout = stale_timeout
        database_proxy.initialize(self.get_db())

    def get_db(self):
        return connect(os.environ.get('DATABASE') or 'mysql://root:root@0.tcp.ngrok.io:13604/mydb')


class BaseModel(peewee.Model):
    """A base model that will use our MySQL database"""
    created = peewee.DateTimeField(default=datetime.now())

    class Meta:
        database = database_proxy


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
    with database_proxy:
        database_proxy.create_tables([CIK, EdgarEntry])
