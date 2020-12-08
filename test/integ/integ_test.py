import unittest

from backends.backend_api import SQLiteBackend
from backends.catserv_api import CatalogService
import os,json


test_db_name = "catserv.db"


class TestCatalogService(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cs = CatalogService(SQLiteBackend(test_db_name))
        cls.cs.insert_profile("User", {"name": "admin", "user_type": 1,
                                       "schema": {"addr": {"home": "GREATER FALL RIVER", "company": "school"}, "phone": 1234567}})
        cls.cs.insert_profile("AssetType", {"name": "csv_file", "description":""})
        cls.cs.insert_profile("Asset", {"name": "mass_tech_report", "asset_type": "csv_file"})
        cls.cs.insert_profile("Asset", {"name": "mass_student", "asset_type": "csv_file"})

        cls.cs.insert_profile("WhoProfile", {"asset": "mass_tech_report", "user": "admin", "schema":{}})
        cls.cs.insert_profile("WhoProfile", {"asset": "mass_student", "user": "admin", "schema":{}})

        with open('profile.json') as json_file:
            data = json.load(json_file)
            for p in data:
                if p["tableName"] == "mass_tech_report.csv":
                    cls.cs.insert_profile("WhatProfile", {"asset": "mass_tech_report", "schema": p})
                elif p["tableName"] == "mass_student.csv":
                    cls.cs.insert_profile("WhatProfile", {"asset": "mass_student", "schema": p})


        # cls.cs.insert_profile("User", {"name": "admin", "user_type": 1, "item": 1, "schema": {"addr":{"home":"westlake","company":"bank"},"phone":1234567}})
        # cls.cs.insert_profile("Item", {"version": 2, "timestamp": "2020-09-16", "user": 1})
        # cls.cs.insert_profile("Item", {"version": 1, "timestamp": "2020-08-16", "user": 1})
        # cls.cs.insert_profile("WhoProfile", {"item": 1, "asset": 1, "user": 1, "schema": {"family":{"father":"john","momther":"jenny"}}})
        # cls.cs.insert_profile("WhatProfile", {"item": 2, "asset": 2, "schema": {"paper":{"field": {"name":"db","field":"metadata"}}}})
        pass

    def test_get_profile(self):
        pass
    # @classmethod
    # def tearDownClass(cls):
    #     os.remove(test_db_name)
    #     pass
    #
    # def test_get_profile(self):
    #     #test search_by_keywords
    #     self.assertEqual({"id": 1,"item": 1, "asset": 1, "user": 1, "schema": {"family":{"father":"john","momther":"jenny"}}},
    #                      self.cs.search_by_keywords("admin")["WhoProfile"]["1"], "get who profile error")
    #
    #     #test json schema search
    #     self.assertEqual(
    #         {"id": 1,"item": 2, "asset": 2, "schema": {"paper":{"field": {"name":"db","field":"metadata"}}}},
    #         self.cs.search_by_keywords("metadata")["WhatProfile"]["1"], "get what profile error")


if __name__ == '__main__':
    unittest.main()
