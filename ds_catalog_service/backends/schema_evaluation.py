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
                     delimiter=',', chunksize=trim_num,
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
        #print(df.shape)
        return df
    
    def init_NSQLsetup(self):
        self.normSQL = CatalogService(NormalizedSQLiteBackend('normalized_catalog.db'))
        self.normSQL.insert_profile("User", {"name": "admin", "user_type": 1, 
                                     "version": 1, "timestamp" : str(datetime.datetime.now()),
                                     "schema": {"addr":{"home":"westlake","company":"bank"},"phone":1234567}})
    
    def insert_data_normalized_SQLite(self, chunk):
        insertedAssetType = False
        insertedRelType = False
        insertedSourceType = False
        self.init_NSQLsetup()
        #convert all nan's to nulls
        chunk = chunk.fillna('NULL')
        
        for index, row in chunk.iterrows():
            for key in self.concept_map:
                i_val = row[key]
                if i_val == 'NULL':
                    continue
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
                    self.inserts.append({'WhereProfile' : {'access_path': i_val, 
                                                       'configuration' : None, 
                                                       'source' : None, 
                                                       'version' : self.version,
                                                       'timestamp' : str(datetime.datetime.now()),
                                                       'user' : 1,
                                                       'asset' : self.asset_idx}})
                elif key == 'name':
                    if insertedAssetType:
                        self.normSQL.insert_profile('Asset' , {'name' : i_val,
                                                           'asset_type' : 1,
                                                           'version' : self.version,
                                                           'timestamp' : str(datetime.datetime.now()),
                                                           'user' : 1})
                        self.inserts.append({'Asset' : {'name' : i_val,
                                                           'asset_type' : 1,
                                                           'version' : self.version,
                                                           'timestamp' : str(datetime.datetime.now()),
                                                           'user' : 1}})
                    else:
                        #in this case, there's only one asset type, and that's an image
                        self.normSQL.insert_profile('AssetType', {'name' : 'Image',
                                                                  'description' : 'A file consisting of bytes that represent pixels'})
                        self.inserts.append({'AssetType' : {'name' : 'Image',
                                                                  'description' : 'A file consisting of bytes that represent pixels'}})
                        insertedAssetType = True
                        self.normSQL.insert_profile('Asset', {'name' : i_val,
                                                           'asset_type' : 1,
                                                           'version' : self.version,
                                                           'timestamp' : str(datetime.datetime.now()),
                                                           'user' : 1})
                        self.inserts.append({'Asset' : {'name' : i_val,
                                                           'asset_type' : 1,
                                                           'version' : self.version,
                                                           'timestamp' : str(datetime.datetime.now()),
                                                           'user' : 1}})
                elif key == 'description':
                    self.normSQL.insert_profile('WhatProfile', {'schema' : {'instanceMeaning' : i_val},
                                                       'version' : self.version,
                                                       'timestamp' : str(datetime.datetime.now()),
                                                       'user' : 1,
                                                       'asset' : self.asset_idx})
                    self.inserts.append({'WhatProfile' : {'schema' : {'instanceMeaning' : i_val},
                                                       'version' : self.version,
                                                       'timestamp' : str(datetime.datetime.now()),
                                                       'user' : 1,
                                                       'asset' : self.asset_idx}})
                elif key == 'variablesMeasured':
                    #print("i_val is: " + str(i_val))
                    print(i_val)
                    self.normSQL.insert_profile('WhatProfile', {'schema' : {'variablesRecorded' : i_val},
                                                       'version' : self.version,
                                                       'timestamp' : str(datetime.datetime.now()),
                                                       'user' : 1,
                                                       'asset' : self.asset_idx})
                    self.inserts.append({'WhatProfile' : {'schema' : {'variablesRecorded' : i_val},
                                                       'version' : self.version,
                                                       'timestamp' : str(datetime.datetime.now()),
                                                       'user' : 1,
                                                       'asset' : self.asset_idx}})
                elif key == 'measurementTechnique':
                    self.normSQL.insert_profile('HowProfile', {'schema' : {'measurementTechnique' : i_val},
                                                       'version' : self.version,
                                                       'timestamp' : str(datetime.datetime.now()),
                                                       'user' : 1,
                                                       'asset' : self.asset_idx})
                    self.inserts.append({'HowProfile' : {'schema' : {'measurementTechnique' : i_val},
                                                       'version' : self.version,
                                                       'timestamp' : str(datetime.datetime.now()),
                                                       'user' : 1,
                                                       'asset' : self.asset_idx}})
                elif key == 'sameAs':
                    #NOTE: this part highlights a very important problem the catalog service
                    #will have to do its best to solve: when we're only told that there should
                    #be a relationship between assets, but we're not given clear links to each,
                    #how do we establish the relationship? We need to find the asset that matches
                    #the user's descriptions, otherwise our relationship schema won't be useful.
                    if insertedRelType:
                        rel_key = self.normSQL.insert_profile('Relationship', {'schema' : {'sameImageAs' : i_val},
                                                       'version' : self.version,
                                                       'timestamp' : str(datetime.datetime.now()),
                                                       'user' : 1,
                                                       'relationship_type' : 1})
                        self.inserts.append({'Relationship' : {'schema' : {'sameImageAs' : i_val},
                                                       'version' : self.version,
                                                       'timestamp' : str(datetime.datetime.now()),
                                                       'user' : 1,
                                                       'relationship_type' : 1}})
                        
                        if rel_key == None:
                            print("Rel_key is None!")
                            print(i_val)
                        
                        self.normSQL.insert_profile('Asset_Relationships',
                                                    {'asset' : self.asset_idx,
                                                     'relationship' : rel_key})
                        
                        self.inserts.append({'Asset_Relationships' :
                                                    {'asset' : self.asset_idx,
                                                     'relationship' : rel_key}})
                    else:
                        self.normSQL.insert_profile('RelationshipType', {'name' : 'Identical Images', 
                                                                         'description' : 'The images are of exactly the same thing.'})
                        self.inserts.append({'RelationshipType' : {'name' : 'Identical Images', 
                                                                         'description' : 'The images are of exactly the same thing.'}})
                        insertedRelType = True
                        rel_key = self.normSQL.insert_profile('Relationship', {'schema' : {'sameImageAs' : i_val},
                                                       'version' : self.version,
                                                       'timestamp' : str(datetime.datetime.now()),
                                                       'user' : 1,
                                                       'relationship_type' : 1})
                        self.inserts.append({'Relationship' : {'schema' : {'sameImageAs' : i_val},
                                                       'version' : self.version,
                                                       'timestamp' : str(datetime.datetime.now()),
                                                       'user' : 1,
                                                       'relationship_type' : 1}})
                        
                        if rel_key == None:
                            print("Rel_key is None!")
                            print(i_val)
                        
                        self.normSQL.insert_profile('Asset_Relationships',
                                                    {'asset' : self.asset_idx,
                                                     'relationship' : rel_key})
                        
                        self.inserts.append({'Asset_Relationships' :
                                                    {'asset' : self.asset_idx,
                                                     'relationship' : rel_key}})
                        
                elif key == 'doi':
                    if insertedSourceType:
                        #we know there's only one kind of source here
                        #but the question is: should we assume every row comes from a distinct source,
                        #or should we assume they all come from the same source?
                        #again, if the name is all that's given, we don't know
                        #...let's assume they're all different
                        source_key = self.normSQL.insert_profile('Source', {'name' : 'PNG Repository',
                                                               'source_type' : 1,
                                                               'schema' : {'doi' : i_val},
                                                               'user' : 1,
                                                               'version' : self.version,
                                                               'timestamp' : datetime.datetime.now()})
                        self.normSQL.insert_profile('WhereProfile', {'source' : source_key,
                                                                     'version' : self.version,
                                                                     'timestamp' : datetime.datetime.now(),
                                                                     'user' : 1,
                                                                     'asset' : self.asset_idx})
                        self.inserts.append({'WhereProfile' : {'source' : source_key,
                                                                     'version' : self.version,
                                                                     'timestamp' : datetime.datetime.now(),
                                                                     'user' : 1,
                                                                     'asset' : self.asset_idx}})
                    else:
                        self.normSQL.insert_profile('SourceType', {'connector' : 'web browser',
                                                                   'serde' : 'PNG',
                                                                   'datamodel' : 'Regular Image'})
                        self.inserts.append({'SourceType' : {'connector' : 'web browser',
                                                                   'serde' : 'PNG',
                                                                   'datamodel' : 'Regular Image'}})
                        insertedSourceType = True
                        source_key = self.normSQL.insert_profile('Source', {'name' : 'PNG Repository',
                                                               'source_type' : 1,
                                                               'schema' : {'doi' : i_val},
                                                               'user' : 1,
                                                               'version' : self.version,
                                                               'timestamp' : datetime.datetime.now()})
                        self.normSQL.insert_profile('WhereProfile', {'source' : source_key,
                                                                     'version' : self.version,
                                                                     'timestamp' : datetime.datetime.now(),
                                                                     'user' : 1,
                                                                     'asset' : self.asset_idx})
                        self.inserts.append({'WhereProfile' : {'source' : source_key,
                                                                     'version' : self.version,
                                                                     'timestamp' : datetime.datetime.now(),
                                                                     'user' : 1,
                                                                     'asset' : self.asset_idx}})
                    
                elif key == 'identifier':
                    if insertedSourceType:
                        #we know there's only one kind of source here
                        #but the question is: should we assume every row comes from a distinct source,
                        #or should we assume they all come from the same source?
                        #again, if the name is all that's given, we don't know
                        #...let's assume they're all different
                        source_key = self.normSQL.insert_profile('Source', {'name' : 'PNG Repository',
                                                               'source_type' : 1,
                                                               'schema' : {'identifier' : i_val},
                                                               'user' : 1,
                                                               'version' : self.version,
                                                               'timestamp' : datetime.datetime.now()})
                        self.normSQL.insert_profile('WhereProfile', {'source' : source_key,
                                                                     'version' : self.version,
                                                                     'timestamp' : datetime.datetime.now(),
                                                                     'user' : 1,
                                                                     'asset' : self.asset_idx})
                        self.inserts.append({'WhereProfile' : {'source' : source_key,
                                                                     'version' : self.version,
                                                                     'timestamp' : datetime.datetime.now(),
                                                                     'user' : 1,
                                                                     'asset' : self.asset_idx}})
                    else:
                        self.normSQL.insert_profile('SourceType', {'connector' : 'web browser',
                                                                   'serde' : 'PNG',
                                                                   'datamodel' : 'Regular Image'})
                        self.inserts.append({'SourceType' : {'connector' : 'web browser',
                                                                   'serde' : 'PNG',
                                                                   'datamodel' : 'Regular Image'}})
                        insertedSourceType = True
                        source_key = self.normSQL.insert_profile('Source', {'name' : 'PNG Repository',
                                                               'source_type' : 1,
                                                               'schema' : {'identifier' : i_val},
                                                               'user' : 1,
                                                               'version' : self.version,
                                                               'timestamp' : datetime.datetime.now()})
                        self.normSQL.insert_profile('WhereProfile', {'source' : source_key,
                                                                     'version' : self.version,
                                                                     'timestamp' : datetime.datetime.now(),
                                                                     'user' : 1,
                                                                     'asset' : self.asset_idx})
                        self.inserts.append({'WhereProfile' : {'source' : source_key,
                                                                     'version' : self.version,
                                                                     'timestamp' : datetime.datetime.now(),
                                                                     'user' : 1,
                                                                     'asset' : self.asset_idx}})
                elif key == 'author':
                    self.normSQL.insert_profile('WhoProfile', {'schema' : {'author' : i_val},
                                                               'version' : self.version,
                                                               'timestamp' : datetime.datetime.now(),
                                                               'write_user' : 1,
                                                               'asset' : self.asset_idx})
                    self.inserts.append({'WhoProfile' : {'schema' : {'author' : i_val},
                                                               'version' : self.version,
                                                               'timestamp' : datetime.datetime.now(),
                                                               'write_user' : 1,
                                                               'asset' : self.asset_idx}})
                elif key == 'isAccessibleForFree':
                    if insertedSourceType:
                        #we know there's only one kind of source here
                        #but the question is: should we assume every row comes from a distinct source,
                        #or should we assume they all come from the same source?
                        #again, if the name is all that's given, we don't know
                        #...let's assume they're all different
                        source_key = self.normSQL.insert_profile('Source', {'name' : 'PNG Repository',
                                                               'source_type' : 1,
                                                               'schema' : {'isAccessibleForFree' : i_val},
                                                               'user' : 1,
                                                               'version' : self.version,
                                                               'timestamp' : datetime.datetime.now()})
                        self.normSQL.insert_profile('WhereProfile', {'source' : source_key,
                                                                     'version' : self.version,
                                                                     'timestamp' : datetime.datetime.now(),
                                                                     'user' : 1,
                                                                     'asset' : self.asset_idx})
                        self.inserts.append({'WhereProfile' : {'source' : source_key,
                                                                     'version' : self.version,
                                                                     'timestamp' : datetime.datetime.now(),
                                                                     'user' : 1,
                                                                     'asset' : self.asset_idx}})
                    else:
                        self.normSQL.insert_profile('SourceType', {'connector' : 'web browser',
                                                                   'serde' : 'PNG',
                                                                   'datamodel' : 'Regular Image'})
                        self.inserts.append({'SourceType' : {'connector' : 'web browser',
                                                                   'serde' : 'PNG',
                                                                   'datamodel' : 'Regular Image'}})
                        insertedSourceType = True
                        source_key = self.normSQL.insert_profile('Source', {'name' : 'PNG Repository',
                                                               'source_type' : 1,
                                                               'schema' : {'isAccessibleForFree' : i_val},
                                                               'user' : 1,
                                                               'version' : self.version,
                                                               'timestamp' : datetime.datetime.now()})
                        self.normSQL.insert_profile('WhereProfile', {'source' : source_key,
                                                                     'version' : self.version,
                                                                     'timestamp' : datetime.datetime.now(),
                                                                     'user' : 1,
                                                                     'asset' : self.asset_idx})
                        self.inserts.append({'WhereProfile' : {'source' : source_key,
                                                                     'version' : self.version,
                                                                     'timestamp' : datetime.datetime.now(),
                                                                     'user' : 1,
                                                                     'asset' : self.asset_idx}})
                elif key == 'dateModified':
                    self.normSQL.insert_profile('WhenProfile', {'asset_timestamp' : i_val,
                                                                'version' : self.version,
                                                                'timestamp' : datetime.datetime.now(),
                                                                'user' : 1,
                                                                'asset' : self.asset_idx})
                    self.inserts.append({'WhenProfile' : {'asset_timestamp' : i_val,
                                                                'version' : self.version,
                                                                'timestamp' : datetime.datetime.now(),
                                                                'user' : 1,
                                                                'asset' : self.asset_idx}})
                elif key == 'distribution':
                    self.normSQL.insert_profile('WhereProfile', {'configuration' : {'distribution' : i_val},
                                                                 'version' : self.version,
                                                                 'timestamp' : datetime.datetime.now(),
                                                                 'user' : 1,
                                                                 'asset' : self.asset_idx})
                    self.inserts.append({'WhereProfile' : {'configuration' : {'distribution' : i_val},
                                                           'version' : self.version,
                                                           'timestamp' : datetime.datetime.now(),
                                                           'user' : 1,
                                                           'asset' : self.asset_idx}})
                elif key == 'spatialCoverage':
                    self.normSQL.insert_profile('WhatProfile', 
                                                {'schema' : {'spatialCoverage' : i_val},
                                                 'version' : self.version,
                                                 'timestamp' : datetime.datetime.now(),
                                                 'user' : 1,
                                                 'asset' : self.asset_idx})
                    self.inserts.append({'WhatProfile' : {'schema' : {'spatialCoverage' : i_val},
                                                 'version' : self.version,
                                                 'timestamp' : datetime.datetime.now(),
                                                 'user' : 1,
                                                 'asset' : self.asset_idx}})
                elif key == 'provider':
                    self.normSQL.insert_profile('WhoProfile', {'schema' : {'provider' : i_val},
                                                               'version' : self.version,
                                                               'timestamp' : datetime.datetime.now(),
                                                               'write_user' : 1,
                                                               'asset' : self.asset_idx})
                    self.inserts.append({'WhoProfile' : {'schema' : {'provider' : i_val},
                                                               'version' : self.version,
                                                               'timestamp' : datetime.datetime.now(),
                                                               'write_user' : 1,
                                                               'asset' : self.asset_idx}})
                elif key == 'funder':
                    self.normSQL.insert_profile('WhoProfile', {'schema' : {'funder' : i_val},
                                                               'version' : self.version,
                                                               'timestamp' : datetime.datetime.now(),
                                                               'write_user' : 1,
                                                               'asset' : self.asset_idx})
                    self.inserts.append({'WhoProfile' : {'schema' : {'funder' : i_val},
                                                               'version' : self.version,
                                                               'timestamp' : datetime.datetime.now(),
                                                               'write_user' : 1,
                                                               'asset' : self.asset_idx}})
                elif key == 'temporalCoverage':
                    self.normSQL.insert_profile('WhatProfile', 
                                                {'schema' : {'temporalCoverage' : i_val},
                                                 'version' : self.version,
                                                 'timestamp' : datetime.datetime.now(),
                                                 'user' : 1,
                                                 'asset' : self.asset_idx})
                    self.inserts.append({'WhatProfile' : {'schema' : {'temporalCoverage' : i_val},
                                                 'version' : self.version,
                                                 'timestamp' : datetime.datetime.now(),
                                                 'user' : 1,
                                                 'asset' : self.asset_idx}})
            self.asset_idx += 1
    
    def insert_full_NSonce(self):
        for chunk in self.df:
            self.insert_data_normalized_SQLite(chunk)
                

if __name__ == "__main__":
    schema_eval = Schema_Evaluation('/Users/psubramaniam/Documents/Fall2020/testcatalogdata/dataset_metadata_2020_08_17.csv',
                                    100000, 1)
    #schema_eval = Schema_Evaluation('/home/pranav/dataset_metadata_2020_08_17.csv',
                                    #1000, 1)
    schema_eval.insert_full_NSonce()
    
        
