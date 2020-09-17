"""
Schema Models are core data structure of Catalog Service. They will be mapped to runtime-instances by ORM
"""

from peewee import *

## Schema Models

database = SqliteDatabase(None)

class BaseModel(Model):
    class Meta:
        database = database


class Item(BaseModel):
    version = IntegerField()
    timestamp = DateTimeField()
    user = DeferredForeignKey('User', null=True)


class UserType(BaseModel):
    name = TextField()
    description = TextField()


class User(BaseModel):
    name = TextField()
    item = ForeignKeyField(Item, backref='user_item')
    user_type = ForeignKeyField(UserType, backref='user_type')
    schema = TextField()


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
    schema = TextField()


class WhatProfile(BaseModel):
    item = ForeignKeyField(Item, backref='what_item')
    asset = ForeignKeyField(Asset, backref='what_asset')
    schema = TextField()


class HowProfile(BaseModel):
    item = ForeignKeyField(Item, backref='how_item')
    asset = ForeignKeyField(Asset, backref='how_asset')
    schema = TextField()


class WhyProfile(BaseModel):
    item = ForeignKeyField(Item, backref='why_item')
    asset = ForeignKeyField(Asset, backref='why_asset')
    schema = TextField()


class WhenProfile(BaseModel):
    item = ForeignKeyField(Item, backref='when_item')
    asset = ForeignKeyField(Asset, backref='when_asset')
    schema = TextField()  # Missing schema field here?
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
    schema = TextField()


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