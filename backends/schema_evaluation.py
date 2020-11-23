from backends.backend_api import NormalizedSQLiteBackend
from backends.backend_api import DataVaultSQLiteBackend
from backends.catserv_api import CatalogService
import os
from backends.schema_models import *
import io
import cProfile, pstats
import pandas as pd
import datetime

class Schema_Evaluation:
    
    def __init__(self, datafile, trim_num, num_reps):
        self.df = self.convert_to_pd(datafile, trim_num)
        self.trim_num = trim_num
        self.num_reps = num_reps
        self.version = 1
        self.asset_idx = 1
        self.normSQL = None
        self.dvSQL = None
        self.normGraph = None
        self.dvGraph = None
        self.inserts = [] #inserts we've performed
        self.queries = [] #queries we've performed
        self.concept_map = {'url' : {'WhereProfile' : 'access_path'},
               'name' : {'Asset' : 'name'},
               'alternateName' : {'Asset' : 'name'},
               'description' : {'WhatProfile' : {'schema' : 'instanceMeaning'}},
               'variablesMeasured' : {'WhatProfile' : {'schema' : 'variablesRecorded'}},
               'measurementTechnique' : {'HowProfile' : {'schema' : 'measurementTechnique'}},
               'sameAs' : {'Relationship' : {'schema' : 'sameImageAs'}},
               'doi' : {'WhereProfile' : {'Source' : {'SourceType' : {'name' : 'Image_Repo',
                                                                      'description' : 'A repository or website of images'},
                                                      'schema' : 'doi'}}},
               'identifier' : {'WhereProfile' : {'Source' : {'SourceType' : {'name' : 'Image_Repo',
                                                                      'description' : 'A repository or website of images'},
                                                      'schema' : 'identifier'}}},
               'author' : {'WhoProfile' : {'schema' : 'author'}},
               'isAccessibleForFree' : {'WhereProfile' : {'Source' : {'SourceType' : {'connector' : 'Image_Repo',
                                                                      'serde' : 'none', 'datamodel' : 'None'},
                                                      'schema' : 'isAccessibleForFree'}}},
               'dateModified' : {'WhenProfile' : 'Asset_timestamp'},
               'distribution' : {'WhereProfile' : {'Configuration' : {'schema' : 'distribution'}}},
               'spatialCoverage' : {'WhatProfile' : {'schema' : 'spatialCoverage'}},
               'provider' : {'WhoProfile' : {'schema' : 'provider'}},
               'funder' : {'WhoProfile' : {'schema' : 'funder'}},
               'temporalCoverage' : {'WhatProfile' : {'schema' : 'temporalCoverage'}}}
    
    def convert_to_pd(self, datafile, trim_num):
        df = pd.read_csv(datafile, 
                     delimiter=',', nrows=trim_num,
                     dtype={'url' : str, 'name' : str,
               'alternateName' : str,
               'description' : str,
               'variablesMeasured' : str,
               'measurementTechnique' : str,
               'sameAs' : str,
               'doi' : str,
               'identifier' : str,
               'author' : str,
               'isAccessibleForFree' : str,
               'dateModified' : str,
               'distribution' : str,
               'spatialCoverage' : str,
               'provider' : str,
               'funder' : str,
               'temporalCoverage' : str})
        print(df.shape)
        return df
    
    def init_NSQLsetup(self):
        self.normSQL = CatalogService(NormalizedSQLiteBackend('normalized_catalog.db'))
        self.normSQL.insert_profile("User", {"name": "admin", "user_type": 1, 
                                     "version": 1, "timestamp" : str(datetime.datetime.now()),
                                     "schema": {"addr":{"home":"westlake","company":"bank"},"phone":1234567}})
    
    def insert_data_normalized_SQLite(self):
        self.init_NSQLsetup()
        for index, row in self.df.iterrows():
            for key in self.concept_map:
                i_val = row[key]
                #I guess we're really doing this...
                #just implement the 17 insert queries...
                if key == 'url':
                    self.normSQL.insert_profile('WhereProfile', {'access_path': i_val, 
                                                       'configuration' : None, 
                                                       'source' : None, 
                                                       'version' : self.version,
                                                       'timestamp' : str(datetime.datetime.now()),
                                                       'user' : 1,
                                                       'asset' : self.asset_idx})
                    self.asset_idx += 1
                    self.inserts.append({'WhereProfile' : {'access_path': i_val, 
                                                       'configuration' : None, 
                                                       'source' : None, 
                                                       'version' : self.version,
                                                       'timestamp' : str(datetime.datetime.now()),
                                                       'user' : 1,
                                                       'asset' : self.asset_idx}})
                

if __name__ == "__main__":
    schema_eval = Schema_Evaluation('/Users/psubramaniam/Documents/Fall2020/testcatalogdata/dataset_metadata_2020_08_17.csv',
                                    1000, 1)
    schema_eval.insert_data_normalized_SQLite()
    
        
