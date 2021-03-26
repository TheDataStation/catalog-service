#!/usr/bin/env python

"""
We use Fire for now https://github.com/google/python-fire
CLI is basically auto-generated from the Base class API.

No need to spend too much on something like `click`,
but we may need to eventually
"""

from ds_catalog_service.api.base import CatalogService
from fire import Fire
from ds_catalog_service import DEFAULT_SQLITE_DB_PATH
from sqlite3 import connect

#class CLIBasedAPI(CatalogService):
# Comment-out & implement the ABC implementation when the CLI-driven API is completed
class CLIBasedAPI(object):
    db_path = DEFAULT_SQLITE_DB_PATH
    """
    CLI Utility to emulate a Data Catalog API
    """

    def __contains__(self, item):
        pass

    def add_user(self, email=None, name=None, permissions='ADMIN'):
        with connect(self.db_path) as db:
            db.execute("INSERT INTO user(email, name, permission) VALUES (?, ?, ?)", (email, name, permissions))

    def add_data_asset(self, id_, data_source, metad):
        pass

    def register_profiling_event(self, asset_id, what, who, where, why, when, how):
        pass

    def search(self, query):
        pass

    @property
    def assets(self, user_ids):
        pass


if __name__ == '__main__':
    cli_api = CLIBasedAPI()
    Fire(cli_api)
    catalog_service_cli = CLIBasedAPI()
    Fire({
        'list-users': catalog_service_cli.users,
        'add-user': catalog_service_cli.add_user,

        'list-assets': None, #catalog_service_cli.assets,
        'add-data-asset': catalog_service_cli.add_data_asset,

        'register-profiling': catalog_service_cli.register_profiling_event,

        'search': catalog_service_cli.search
    })
