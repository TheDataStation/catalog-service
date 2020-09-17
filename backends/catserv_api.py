"""
Catalog service API -- all functionality is offered via this module. This module is designed so that it's accessed
via one of the frontends available
"""

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

    def get(self, ins_name, filters):
        """
        Low-level get with filters
        :param ins_name:
        :param filters:
        :return:
        """
        return self.bk.get(ins_name, filters)

    def delete(self, ins_name, filters):
        """
        Low-level delete with filters
        :param ins_name:
        :param filters:
        :return:
        """
        return self.bk.delete(ins_name, filters)

    def search_by_keywords(self, keywords):
        """
        Users could run the fuzzy search to get recommended profiles to start
        :param keywords:
        :return: profiles in json format
        """

        # TODO synchronize possible keywords to ES to speed up search
        # current simple version run search loops among some core schemas' fields

        related_profiles = {k: [] for k in self.bk.profile_ins_map.keys()}
        item_set = set()
        for word in keywords.split():
            ins = self.bk.get_ins("User")
            item_set.update(
                [user.item for user in self.get(ins, (ins.name.contains(word) or ins.schema.contains(word)))])
            ins = self.bk.get_ins("Asset")
            item_set.update([asset.item for asset in self.get(ins, ins.name.contains(word))])

            user = self.bk.get_ins("User")
            usertype = self.bk.get_ins("UserType")
            item_set.update([i.item for i in user.select().join(usertype).where(
                usertype.name.contains(word) or usertype.description.contains(word))])

            for k in related_profiles.keys():
                ins = self.bk.get_ins(k)
                related_profiles[k] += list(
                    self.get(ins, (ins.item in item_set or ins.schema.contains(
                        word))).dicts())  # whether we need to further search the timestamps in WhenProfile?

        return related_profiles

    def get_profiles(self, ins_name=None, item_id=None, asset_id=None):
        """
        Users could get profiles by filters
        :param profile_type:
        :param item_id:
        :param asset_id:
        :return: profiles in json format
        """
        ret = {}
        if not ins_name:
            profiles = {ins_name: self.bk.get_ins(ins_name)}
        else:
            profiles = self.bk.profile_ins_map
        for ins_name, ins in profiles.items():
            ret[ins_name] = list(self.get(ins, (not item_id or ins.item == item_id) and (
                        not asset_id or ins.asset == asset_id)).dicts())
        return ret

    def insert_profile(self, ins_name, profile):
        """
        Users could insert profiles with json
        :param profile_type:
        :param profile:
        :return:
        """
        return self.put(ins_name, profile)


if __name__ == "__main__":
    print("Main Catalog Service API")