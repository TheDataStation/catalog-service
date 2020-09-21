"""
Backend API provides low-level data manipulations for catalog service.
Backend API is based on schema model instances mapped by ORM
"""
from backends.schema_models import *


# TODO: come back and replace this with the command line option
# from the user who starts the catalog service

class Backend(object):

    def put(self, schema_id, content):
        pass

    def get(self, ins_name, id: (int, ...) = None, item_id: (int, ...) = None, asset_id: (int, ...) = None, timestamp: (str, str) = None, name: (str, ...) = None, version: (int, int) = None, sub_schema: {} = None):
        """
        Query backends with filters connected by "AND" semantic.
        :param ins_name: schema ins name
        :param id: set filter
        :param item_id:  set filter
        :param asset_id:  set filter
        :param timestamp:  range filter
        :param name: set filter
        :param version: range filter
        :param sub_schema:  provides JSON full text search
        :return:
        """
        pass

    def delete(self, ins_name, id: (int, ...) = None, item_id: (int, ...) = None, asset_id: (int, ...) = None, timestamp: (str, str) = None, name: (str, ...) = None, version: (int, int) = None):
        pass


class SQLiteBackend(Backend):

    def __init__(self, backend_path="catserv.db"):
        # catalog hides backend-specific operations
        self.ins_map = {}
        self.profile_ins_map = {}
        database.init(backend_path)
        self.create_tables()

    def put(self, ins_name, content):
        return self.get_ins(ins_name).insert(content).execute()

    def get(self,ins_name, id: (int, ...) = None, item_id: (int, ...) = None, asset_id: (int, ...) = None, timestamp: (str, str) = None, name: (str, ...) = None, version: (int, int) = None, sub_schema: {} = None):
        sql = self.build_sql(ins_name, "query", id, item_id, asset_id, timestamp, name, version, sub_schema)
        print(sql)
        return list(sql.dicts())

    def delete(self,ins_name, id: (int, ...) = None,  item_id: (int, ...) = None, asset_id: (int, ...) = None, timestamp: (str, str) = None, name: (str, ...) = None, version: (int, int) = None):
        sql = self.build_sql(ins_name, "delete", id, item_id, asset_id, timestamp, name, version)
        print(sql)
        return sql.execute()

    def build_sql(self, ins_name, sql_type, id: (int, ...) = None, item_id: (int, ...) = None, asset_id: (int, ...) = None, timestamp: (str, str) = None, name: (str, ...) = None, version: (int, int) = None, sub_schema: {} = None):

        global sql
        ins = self.get_ins(ins_name)
        if sql_type == "query":
            sql = ins.select().distinct()
        elif sql_type == "delete":
            sql = ins.delete()

        if sub_schema is not None:
            kd = ins.schema.tree().alias('tree')
            sql = sql.from_(ins, kd)
            expr = False
            for k,v in sub_schema.items():
                if k == "*":
                    for wildcard_v in v:
                        expr |= (kd.c.value == wildcard_v)
                elif v == "*":
                    expr |= (kd.c.key == k)
                else:
                    expr |= ((kd.c.key == k) & (kd.c.value == v))
            sql = sql.where(expr)

        if id is not None:
            expr = False
            for i in id:
                expr |= (ins.id == i)
            sql = sql.where(expr)
        if item_id is not None:
            expr = False
            for i in item_id:
                expr |= (ins.item_id == i)
            sql = sql.where(expr)
        if asset_id is not None:
            expr = False
            for i in asset_id:
                expr |= (ins.asset_id == i)
            sql = sql.where(expr)
        if timestamp is not None:
            expr = True
            if timestamp[0] is not None:
                expr &= (ins.timestamp >= timestamp[0])
            if timestamp[1] is not None:
                expr &= (ins.timestamp < timestamp[1])
            sql = sql.where(expr)
        if name is not None:
            expr = False
            for i in name:
                expr |= (ins.name == i)
            sql = sql.where(expr)
            pass
        if version is not None:
            expr = True
            if version[0] is not None:
                expr &= (ins.version >= version[0])
            if version[1] is not None:
                expr &= (ins.version < version[1])
            sql = sql.where(expr)
        return sql

    def get_ins(self, ins_name):
        return self.ins_map[ins_name]

    def create_tables(self):
        with database:
            database.create_tables([Item, UserType, User, AssetType,
                                    Asset, WhoProfile, WhatProfile,
                                    HowProfile, WhyProfile, WhenProfile,
                                    SourceType, Source, WhereProfile, Action])
            # Item.__schema.create_foreign_key(Item.user)
            self.ins_map["Item"] = Item
            self.ins_map["UserType"] = UserType
            self.ins_map["User"] = User
            self.ins_map["AssetType"] = AssetType
            self.ins_map["Asset"] = Asset
            self.ins_map["WhoProfile"] = WhoProfile
            self.ins_map["WhatProfile"] = WhatProfile
            self.ins_map["HowProfile"] = HowProfile
            self.ins_map["WhyProfile"] = WhyProfile
            self.ins_map["WhenProfile"] = WhenProfile
            self.ins_map["SourceType"] = SourceType
            self.ins_map["Source"] = Source
            self.ins_map["WhereProfile"] = WhereProfile
            self.ins_map["Action"] = Action
            self.profile_ins_map["WhoProfile"] = WhoProfile
            self.profile_ins_map["WhatProfile"] = WhatProfile
            self.profile_ins_map["HowProfile"] = HowProfile
            self.profile_ins_map["WhyProfile"] = WhyProfile
            self.profile_ins_map["WhenProfile"] = WhenProfile


if __name__ == "__main__":
    pass
