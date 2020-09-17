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

    def get(self, schema_id, filters):
        pass

    def delete(self, schema_id, filters):
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

    def get(self, ins_name, filters):
        return self.get_ins(ins_name).select().where(filters)

    def get(self, ins, filters):
        return ins.select().where(filters)

    def delete(self, ins_name, filters):
        return self.get_ins(ins_name).delete().where(filters)

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
