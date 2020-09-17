import unittest

from backends.backend_api import SQLiteBackend
from backends.catserv_api import CatalogService
import os

test_db_name = "test_catserv.db"


class TestCatalogService(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cs = CatalogService(SQLiteBackend(test_db_name))
        cls.cs.insert_profile("User", {"name": "admin", "user_type": 1, "item": 1, "schema": "test"})
        cls.cs.insert_profile("Item", {"version": 1, "timestamp": "2020-09-16", "user": 1})
        cls.cs.insert_profile("WhoProfile", {"item": 1, "asset": 1, "user": 1, "schema": "test"})
        pass

    @classmethod
    def tearDownClass(cls):
        os.remove(test_db_name)

    def test_get_profile(self):
        self.assertEqual(
            {'WhoProfile': [{'id': 1, 'item': 1, 'asset': 1, 'user': 1, 'schema': 'test'}], 'WhatProfile': [],
             'HowProfile': [], 'WhyProfile': [], 'WhenProfile': []}, self.cs.search_by_keywords("test"),
            "get_profile error")
        #print(self.cs.search_by_keywords("test"))


if __name__ == '__main__':
    unittest.main()
