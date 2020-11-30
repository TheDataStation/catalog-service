"""
Backend API provides low-level data manipulations for catalog service.
Backend API is based on schema model instances mapped by ORM
"""
import io
import cProfile, pstats
from schema_models import *

#TODO: change the backend functions so they don't depend on schema type-specific
#attributes, like "Item"
class Backend(object):

    def put(self, schema_id, content):
        pass

    def get(self, ins_name, id: (int, ...) = None, asset_id: (int, ...) = None, timestamp: (str, str) = None, name: (str, ...) = None, version: (int, int) = None, sub_schema: {} = None):
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

    def delete(self, ins_name, id: (int, ...) = None, asset_id: (int, ...) = None, timestamp: (str, str) = None, name: (str, ...) = None, version: (int, int) = None):
        pass

class NormalizedBackend(Backend):
    
    def normalized_put(self, schema_id, content):
        pass
    
    def put(self, schema_id, content):
        return self.normalized_put(schema_id, content)
    
    def normalized_get(self, ins_name, id: (int, ...) = None, item_id: (int, ...) = None, asset_id: (int, ...) = None, timestamp: (str, str) = None, name: (str, ...) = None, version: (int, int) = None, sub_schema: {} = None):
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
    
    def get(self, ins_name, id: (int, ...) = None, asset_id: (int, ...) = None, timestamp: (str, str) = None, name: (str, ...) = None, version: (int, int) = None, sub_schema: {} = None):
        #TODO: figure out how to get item_id from all other information! (we're not assuming users will
        #query by item_id)
        return self.normalized_get(ins_name, id, None, asset_id, timestamp, name, version, sub_schema)

    def normalized_delete(self, ins_name, id: (int, ...) = None, item_id: (int, ...) = None, asset_id: (int, ...) = None, timestamp: (str, str) = None, name: (str, ...) = None, version: (int, int) = None):
        pass
    
    def delete(self, ins_name, id: (int, ...) = None, asset_id: (int, ...) = None, timestamp: (str, str) = None, name: (str, ...) = None, version: (int, int) = None):
        self.normalized_delete(ins_name, id, None, asset_id, timestamp, name, version)

class DataVaultBackend(Backend):
    
    def datavault_put(self, schema_id, content):
        pass
    
    def put(self, schema_id, content):
        self.datavault_put(schema_id, content)

    def datavault_get(self, ins_name, id: (int, ...) = None, asset_id: (int, ...) = None, timestamp: (str, str) = None, name: (str, ...) = None, version: (int, int) = None, sub_schema: {} = None):
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
    
    def get(self, ins_name, id: (int, ...) = None, asset_id: (int, ...) = None, timestamp: (str, str) = None, name: (str, ...) = None, version: (int, int) = None, sub_schema: {} = None):
        #TODO: figure out how to get item_id from all other information! (we're not assuming users will
        #query by item_id)
        return self.datavault_get(ins_name, id, None, asset_id, timestamp, name, version, sub_schema)

    def datavault_delete(self, ins_name, id: (int, ...) = None, item_id: (int, ...) = None, asset_id: (int, ...) = None, timestamp: (str, str) = None, name: (str, ...) = None, version: (int, int) = None):
        pass
    
    def delete(self, ins_name, id: (int, ...) = None, asset_id: (int, ...) = None, timestamp: (str, str) = None, name: (str, ...) = None, version: (int, int) = None):
        self.datavault_delete(ins_name, id, None, asset_id, timestamp, name, version)
    
    

class NormalizedSQLiteBackend(NormalizedBackend):

    def __init__(self, backend_path="catserv.db"):
        # catalog hides backend-specific operations
        self.ins_map = {}
        self.profile_ins_map = {}
        self.database = database
        self.database.init(backend_path, pragmas={'journal_mode' : 'wal', 'synchronous' : 0})
        self.create_tables()
    
    def execute(self, query_num, stmt):
        pr = cProfile.Profile()
        pr.enable()
        status = stmt.execute()
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        with open('test_normalized_insert' + str(query_num) + '.txt', 'w+') as f:
            f.write(str(sql))
            f.write(s.getvalue())
        return status
    
    def executeQuery(self, query_num, stmt):
        pr = cProfile.Profile()
        pr.enable()
        query_res = list(stmt.dicts())
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        with open('test_normalized_query' + str(query_num) + '.txt', 'w+') as f:
            f.write(str(sql))
            f.write(s.getvalue())
        return query_res

    def normalized_put(self, ins_name, content):
        return self.get_ins(ins_name).insert(content).execute()
    
    def instr_put(self, ins_name, content, query_num):
        pr = cProfile.Profile()
        pr.enable()
        sql = self.get_ins(ins_name).insert(content)
        status = sql.execute()
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        with open('test_normalized_put' + str(query_num) + '.txt', 'w+') as f:
            f.write(str(sql))
            f.write(s.getvalue())
        return status
        

    def normalized_get(self,ins_name, id: (int, ...) = None, item_id: (int, ...) = None, asset_id: (int, ...) = None, timestamp: (str, str) = None, name: (str, ...) = None, version: (int, int) = None, sub_schema: {} = None):
        sql = self.build_sql(ins_name, "query", id, item_id, asset_id, timestamp, name, version, sub_schema)
        print(sql)
        return list(sql.dicts())
    
    def instr_get(self,ins_name, query_num, id: (int, ...) = None, item_id: (int, ...) = None, asset_id: (int, ...) = None, timestamp: (str, str) = None, name: (str, ...) = None, version: (int, int) = None, sub_schema: {} = None):
        pr = cProfile.Profile()
        pr.enable()
        sql = self.build_sql(ins_name, "query", id, item_id, asset_id, timestamp, name, version, sub_schema)
        query_res = list(sql.dicts())
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        with open('test_normalized_get' + str(query_num) + '.txt', 'w+') as f:
            f.write(str(sql))
            f.write(s.getvalue())
        return query_res

    def normalized_delete(self,ins_name, id: (int, ...) = None,  item_id: (int, ...) = None, asset_id: (int, ...) = None, timestamp: (str, str) = None, name: (str, ...) = None, version: (int, int) = None):
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
            #add following to below if needed later: Item,
            database.create_tables([UserType, User, AssetType,
                                    Asset, WhoProfile, WhatProfile,
                                    HowProfile, WhyProfile, WhenProfile,
                                    SourceType, Source, WhereProfile, 
                                    Action, RelationshipType, Relationship,
                                    Asset_Relationships])
            # Item.__schema.create_foreign_key(Item.user)
            #self.ins_map["Item"] = Item
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
            self.ins_map["RelationshipType"] = RelationshipType
            self.ins_map["Relationship"] = Relationship
            self.ins_map['Asset_Relationships'] = Asset_Relationships
            self.profile_ins_map["WhoProfile"] = WhoProfile
            self.profile_ins_map["WhatProfile"] = WhatProfile
            self.profile_ins_map["HowProfile"] = HowProfile
            self.profile_ins_map["WhyProfile"] = WhyProfile
            self.profile_ins_map["WhenProfile"] = WhenProfile

class DataVaultSQLiteBackend(DataVaultBackend):

    def __init__(self, backend_path="catserv.db"):
        # catalog hides backend-specific operations
        self.ins_map = {}
        self.profile_ins_map = {}
        database.init(backend_path)
        self.create_tables()
    
    def execute(self, query_num, stmt):
        pr = cProfile.Profile()
        pr.enable()
        status = stmt.execute()
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        with open('test_datavault_insert' + str(query_num) + '.txt', 'w+') as f:
            f.write(str(sql))
            f.write(s.getvalue())
        return status
    
    def executeQuery(self, query_num, stmt):
        pr = cProfile.Profile()
        pr.enable()
        query_res = list(stmt.dicts())
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        with open('test_datavault_query' + str(query_num) + '.txt', 'w+') as f:
            f.write(str(sql))
            f.write(s.getvalue())
        return query_res

    def datavault_put(self, ins_name, content):
        return self.get_ins(ins_name).insert(content).execute()

    def datavault_get(self,ins_name, id: (int, ...) = None, asset_id: (int, ...) = None, timestamp: (str, str) = None, name: (str, ...) = None, version: (int, int) = None, sub_schema: {} = None):
        sql = self.build_sql(ins_name, "query", id, None, asset_id, timestamp, name, version, sub_schema)
        print(sql)
        return list(sql.dicts())
    
    def instr_get(self,ins_name, query_num, id: (int, ...) = None, asset_id: (int, ...) = None, timestamp: (str, str) = None, name: (str, ...) = None, version: (int, int) = None, sub_schema: {} = None):
        pr = cProfile.Profile()
        pr.enable()
        sql = self.build_sql(ins_name, "query", id, None, asset_id, timestamp, name, version, sub_schema)
        query_res = list(sql.dicts())
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        with open('test_normalized_get' + str(query_num) + '.txt', 'w+') as f:
            f.write(str(sql))
            f.write(s.getvalue())
        return query_res

    def datavault_delete(self,ins_name, id: (int, ...) = None, asset_id: (int, ...) = None, timestamp: (str, str) = None, name: (str, ...) = None, version: (int, int) = None):
        sql = self.build_sql(ins_name, "delete", id, None, asset_id, timestamp, name, version)
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
            database.create_tables([H_UserType, H_User, H_AssetType,
                                    H_Asset, H_WhoProfile, H_WhatProfile,
                                    H_HowProfile, H_WhyProfile, H_WhenProfile,
                                    H_SourceType, H_Source, H_WhereProfile, 
                                    H_Action, H_RelationshipType, H_Relationship,
                                    L_UserTypeLink, L_AssetTypeLink, L_Asset_WhoProfile,
                                    L_WhoProfileUser, L_Asset_HowProfile, L_Asset_WhyProfile,
                                    L_Asset_WhatProfile, L_Asset_WhenProfile, L_Source2Type,
                                    L_Asset_WhereProfile, L_AssetsInActions, L_Relationship_Type,
                                    L_Asset_Relationships, S_User_schema, S_WhoProfile_schema,
                                    S_HowProfile_schema, S_WhyProfile_schema, S_WhatProfile_schema,
                                    S_WhenProfile_Attributes, S_Configuration, S_SourceTypeAttributes,
                                    S_AssetTypeAttributes, S_UserTypeAttributes,
                                    S_RelationshipTypeAttributes, S_Relationship_schema])
            # Item.__schema.create_foreign_key(Item.user)
            self.ins_map["H_UserType"] = H_UserType
            self.ins_map["H_User"] = H_User
            self.ins_map["H_AssetType"] = H_AssetType
            self.ins_map["H_Asset"] = H_Asset
            self.ins_map["H_WhoProfile"] = H_WhoProfile
            self.ins_map["H_WhatProfile"] = H_WhatProfile
            self.ins_map["H_HowProfile"] = H_HowProfile
            self.ins_map["H_WhyProfile"] = H_WhyProfile
            self.ins_map["H_WhenProfile"] = H_WhenProfile
            self.ins_map["H_SourceType"] = H_SourceType
            self.ins_map["H_Source"] = H_Source
            self.ins_map["H_WhereProfile"] = H_WhereProfile
            self.ins_map["H_Action"] = H_Action
            self.ins_map["H_RelationshipType"] = H_RelationshipType
            self.ins_map["H_Relationship"] = H_Relationship
            self.ins_map["L_UserTypeLink"] = L_UserTypeLink
            self.ins_map["L_AssetTypeLink"] = L_AssetTypeLink
            self.ins_map["L_Asset_WhoProfile"] = L_Asset_WhoProfile
            self.ins_map["L_WhoProfileUser"] = L_WhoProfileUser
            self.ins_map["L_Asset_HowProfile"] = L_Asset_HowProfile
            self.ins_map["L_Asset_WhyProfile"] = L_Asset_WhyProfile
            self.ins_map["L_Asset_WhatProfile"] = L_Asset_WhatProfile
            self.ins_map["L_Asset_WhenProfile"] = L_Asset_WhenProfile
            self.ins_map["L_Source2Type"] = L_Source2Type
            self.ins_map["L_Asset_WhereProfile"] = L_Asset_WhereProfile
            self.ins_map["L_AssetsInActions"] = L_AssetsInActions
            self.ins_map["L_Relationship_Type"] = L_Relationship_Type
            self.ins_map["L_Asset_Relationships"] = L_Asset_Relationships
            self.ins_map["S_User_schema"] = S_User_schema
            self.ins_map["S_WhoProfile_schema"] = S_WhoProfile_schema
            self.ins_map["S_HowProfile_schema"] = S_HowProfile_schema
            self.ins_map["S_WhyProfile_schema"] = S_WhyProfile_schema
            self.ins_map["S_WhatProfile_schema"] = S_WhatProfile_schema
            self.ins_map["S_WhenProfile_Attributes"] = S_WhenProfile_Attributes
            self.ins_map["S_Configuration"] = S_Configuration
            self.ins_map["S_SourceTypeAttributes"] = S_SourceTypeAttributes
            self.ins_map["S_AssetTypeAttributes"] = S_AssetTypeAttributes
            self.ins_map["S_UserTypeAttributes"] = S_UserTypeAttributes
            self.ins_map["S_RelationshipTypeAttributes"] = S_RelationshipTypeAttributes
            self.ins_map["S_Relationship_schema"] = S_Relationship_schema
            self.profile_ins_map["H_WhoProfile"] = H_WhoProfile
            self.profile_ins_map["H_WhatProfile"] = H_WhatProfile
            self.profile_ins_map["H_HowProfile"] = H_HowProfile
            self.profile_ins_map["H_WhyProfile"] = H_WhyProfile
            self.profile_ins_map["H_WhenProfile"] = H_WhenProfile


if __name__ == "__main__":
    pass
