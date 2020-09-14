from peewee import *

#TODO: come back and replace this with the command line option
#from the user who starts the catalog service
DATABASE = 'catserv.db'

database = SqliteDatabase(DATABASE)

class BaseModel(Model):
    class Meta:
        database = database

class Item(BaseModel):
    version = IntegerField()
    timestamp = DateTimeField()
    user = DeferredForeignKeyField('User', null=True)
    
class UserType(BaseModel):
    name = TextField()
    description = TextField()
    
class User(BaseModel):
    name = TextField()
    item = ForeignKeyField(Item, backref='user_item')
    user_type = ForeignKeyField(UserType, backref='user_type')
    user_schema = TextField()

class AssetType(BaseModel):
    name = TextField()
    description = TextField()

class Asset(BaseModel):
    name = TextField()
    asset_type = ForeignKeyField(AssetType, backref='asset_type')
    item = ForeignKeyField(Item, backref='asset_item')

class WhoProfile(BaseModel):
    item = ForeignKeyField(Item, backref='who_item')
    asset = ForeignKeyField(Asset, backref='who_asset')
    user = ForeignKeyField(User, backref='who_user')
    who_schema = TextField()

class WhatProfile(BaseModel):
    item = ForeignKeyField(Item, backref='what_item')
    asset = ForeignKeyField(Asset, backref='what_asset')
    what_schema = TextField()

class HowProfile(BaseModel):
    item = ForeignKeyField(Item, backref='how_item')
    asset = ForeignKeyField(Asset, backref='how_asset')
    how_schema = TextField()

class WhyProfile(BaseModel):
    item = ForeignKeyField(Item, backref='why_item')
    asset = ForeignKeyField(Asset, backref='why_asset')
    why_schema = TextField()

class WhenProfile(BaseModel):
    item = ForeignKeyField(Item, backref='when_item')
    asset = ForeignKeyField(Asset, backref='when_asset')
    asset_timestamp = DateTimeField()
    expiry_date = DateTimeField()
    start_date = DateTimeField()

class SourceType(BaseModel):
    connector = TextField()
    serde = TextField()
    datamodel = TextField()

class Source(BaseModel):
    name = TextField()
    source_type = ForeignKeyField(SourceType, backref='source_type')
    source_schema = TextField()

class WhereProfile(BaseModel):
    item = ForeignKeyField(Item, backref='where_item')
    asset = ForeignKeyField(Asset, backref='where_asset')
    access_path = TextField()
    source = ForeignKeyField(Source, backref='where_source')
    configuration = TextField()

class Action(BaseModel):
    item = ForeignKeyField(Item, backref='action_item')
    asset = ForeignKeyField(Asset, backref='action_asset')
    who = ForeignKeyField(WhoProfile, backref='action_who')
    how = ForeignKeyField(HowProfile, backref='action_how')
    why = ForeignKeyField(WhyProfile, backref='action_why')
    when = ForeignKeyField(WhenProfile, backref='action_when')


def create_tables():
    with database:
        database.create_tables([Item, UserType, User, AssetType, 
                                Asset, WhoProfile, WhatProfile, 
                                HowProfile, WhyProfile, WhenProfile,
                                SourceType, Source, WhereProfile, Action])
        Item.__schema.create_foreign_key(Item.user)


