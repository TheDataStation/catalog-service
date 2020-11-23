"""
Catalog service API -- all functionality is offered via this module. This module is designed so that it's accessed
via one of the frontends available
"""
import json


class CatalogService:

    def __init__(self, backend):
        # catalog hides backend-specific operations
        self.bk = backend
        pass

    def asset_id(self, asset_type: str, asset_path: str, user_id=None) -> int:
        """
        TODO: types are temporary, need to think through that
        TODO: we need to add 'asset_path' to Asset before impl this method
        given an asset, returns its ID
        :param asset_type:
        :param asset_path:
        :param user_id:
        :return:
        """
        pass

    def exists(self, asset_id, user_id=None) -> bool:
        """
        TODO: do we need this?
        Returns true if an asset_id already exists in the catalog
        :param asset_id:
        :param user_id:
        :return:
        """
        pass

    def merge_profiles(self, profiles):
        """
        When multiple profiles are available, this function merges them into 1 single profile following a merge
        algorithm. Profiles should be of the same type.
        TODO: we need to design the merge rules
        :param profiles:
        :return:
        """
        pass

    def put(self, ins_name, content):
        """
        Low-level insert with filters
        :param ins_name:
        :param content:
        :return:
        """
        return self.bk.put(ins_name, content)
    
    #TODO: Why do we have item_id here? How would a user ever query anything
    #using its item_id?
    def get(self, ins_name, id: (int, ...) = None, item_id: (int, ...) = None, asset_id: (int, ...) = None, timestamp: (str, str) = None, name: (str, ...) = None, version: (int, int) = None, sub_schema: {} = None):
        """
        Low-level get with filters
        :param ins_name:
        :param id:
        :param item_id:
        :param asset_id:
        :param timestamp:
        :param name:
        :param version:
        :param sub_schema:
        :return:
        """
        #return self.bk.get(ins_name, id, item_id, asset_id, timestamp, name, version, sub_schema)
        return self.bk.get(ins_name, id, asset_id, timestamp, name, version, sub_schema)
    
    #TODO: Why do we have item_id here? How would a user ever delete anything
    #using its item_id?
    def delete(self, ins_name, id: (int, ...) = None, item_id: (int, ...) = None, asset_id: (int, ...) = None, timestamp: (str, str) = None, name: (str, ...) = None, version: (int, int) = None):
        """
        Low-level delete with filters
        :param ins_name:
        :param id:
        :param item_id:
        :param asset_id:
        :param timestamp:
        :param name:
        :param version:
        :return:
        """
        return self.bk.delete(ins_name, id, item_id, asset_id, timestamp, name, version)

    def search_by_keywords(self, keywords):
        """
        Users could run the fuzzy search to get recommended profiles to start
        :param keywords:
        :return: profiles in json format
        """

        # TODO synchronize possible keywords to ES to speed up search
        # current simple version run search loops among some core schemas' fields
        related_profiles = {"WhoProfile": {},"WhatProfile": {},"HowProfile": {},"WhyProfile": {}}
        item_set = set()
        words = tuple(keywords.split())
        item_set.update([_.get("item") for _ in self.get("User", name=words)])
        item_set.update([_.get("item") for _ in self.get("Asset", name=words)])
        for profile_name in related_profiles.keys():
            for profile in self.get(profile_name, item_id = tuple(item_set)): related_profiles[profile_name][str(profile["id"])] = profile
            for profile in self.get(profile_name, sub_schema={"*": words}): related_profiles[profile_name][str(profile["id"])] = profile
        return related_profiles

    def get_profiles(self, profile_name, item_id=None, asset_id=None, sub_schema=None):
        """
        Users could get profiles by filters
        :param profile_name:
        :param item_id:
        :param asset_id:
        :param sub_schema:
        :return:
        """
        return self.get(profile_name,item_id,asset_id,sub_schema)

    def insert_profile(self, profile_name, content):
        """
        Users could insert profiles with json
        :param profile_name:
        :param content:
        :return:
        """
        return self.put(profile_name, content)


if __name__ == "__main__":
    print("Main Catalog Service API")