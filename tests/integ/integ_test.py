import unittest

from ds_catalog_service.backends import SQLiteBackend
from ds_catalog_service.backends import CatalogService
import json


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

        pass

    def test_get_profile(self):
        pass



if __name__ == '__main__':
    unittest.main()
