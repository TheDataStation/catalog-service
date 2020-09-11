"""
Catalog service API -- all functionality is offered via this module. This module is designed so that it's accessed
via one of the frontends available
"""


class CatalogService:

    def __init__(self, catalog):
        # catalog hides backend-specific operations
        self.catalog = catalog

    def asset_id(self, asset_type: str, asset_path: str, user_id=None) -> int:
        """
        TODO: types are temporary, need to think through that
        given an asset, returns its ID
        :param asset_type:
        :param asset_path:
        :param user_id:
        :return:
        """
        pass

    def exists(self, asset_id, user_id=None) -> bool:
        """
        Returns true if an asset_id already exists in the catalog
        :param asset_id:
        :param user_id:
        :return:
        """
        pass

    def search(self, profile_type, keywords, user_id=None):
        pass

    def new_what_profile(self, asset_id, what_profile, user_id=None):
        pass

    def what_profiles(self, asset_id, user_id=None):
        pass

    def what_profile(self, asset_id, user_id=None):
        pass

    def merge_profiles(self, profiles):
        """
        When multiple profiles are available, this function merges them into 1 single profile following a merge
        algorithm. Profiles should be of the same type.
        :param profiles:
        :return:
        """
        pass


if __name__ == "__main__":
    print("Main Catalog Service API")


