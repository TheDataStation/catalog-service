"""
We use Fire for now https://github.com/google/python-fire
CLI is basically auto-generated from the Base class API.

No need to spend too much on something like `click`,
but we may need to eventually
"""

from ds_catalog_service.api.base import CatalogService
from fire import Fire


class CLIBasedAPI(CatalogService):

    def __contains__(self, item):
        pass

    @property
    def users(self):
        pass

    def add_user(self, user_id, permissions='READ'):
        pass

    def add_data_asset(self):
        pass

    def add_profile(self, asset_id, what, who, where, why, when, how):
        pass

    @property
    def assets(self, user_ids):
        pass


if __name__ == '__main__':
    catalog_service_cli = CLIBasedAPI()
    Fire({
        'list-users': catalog_service_cli.users,
        'add-user': catalog_service_cli.add_user,

        'list-assets': catalog_service_cli.assets,
        'add-data-asset': catalog_service_cli.add_data_asset,

        'add-profile': catalog_service_cli.add_profile,
    })
