from backends.schema_models import *
import csv
import datetime
import pandas as pd

class MapToNSQLCSV():

    def __init__(self):
        self.ins_map = {}
        self.create_tables()
        self.csv_map = {}
        self.attrmap = {}
        self.init_attrmap()
        self.fh_map = {}
        self.create_csvs()
        self.version = 1
        self.asset_idx = 1
        self.keymap = {}
        self.init_keymap()
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
    
    def init_keymap(self):
        self.keymap['UserType'] = 1
        self.keymap['User'] = 1
        self.keymap['AssetType'] = 0
        self.keymap['Asset'] = 0
        self.keymap['WhoProfile'] = 0
        self.keymap['WhatProfile'] = 0
        self.keymap['HowProfile'] = 0
        self.keymap['WhyProfile'] = 0
        self.keymap['WhenProfile'] = 0
        self.keymap['SourceType'] = 0
        self.keymap['Source'] = 0
        self.keymap['WhereProfile'] = 0
        self.keymap['RelationshipType'] = 0
        self.keymap['Relationship'] = 0
        self.keymap['Asset_Relationships'] = 0
    
    def init_attrmap(self):
        self.attrmap['UserType'] = ['name', 'description']
        self.attrmap['User'] = ['name', 'user_type', 'schema',
                                'version', 'timestamp', 'user']
        self.attrmap['AssetType'] = ['name', 'description']
        self.attrmap['Asset'] = ['name', 'asset_type', 'version',
                                 'timestamp', 'user']
        self.attrmap['WhoProfile'] = ['version', 'timestamp', 'write_user',
                                      'asset', 'user', 'schema']
        self.attrmap['WhatProfile'] = ['version', 'timestamp', 'user',
                                      'asset', 'schema']
        self.attrmap['HowProfile'] = ['version', 'timestamp', 'user',
                                      'asset', 'schema']
        self.attrmap['WhyProfile'] = ['version', 'timestamp', 'user',
                                      'asset', 'schema']
        self.attrmap['WhenProfile'] = ['version', 'timestamp', 'user',
                                      'asset', 'asset_timestamp',
                                      'expiry_date', 'start_date']
        self.attrmap['SourceType'] = ['connector', 'serde', 'datamodel']
        self.attrmap['Source'] = ['version', 'timestamp', 'user',
                                  'name', 'source_type', 'schema']
        self.attrmap['WhereProfile'] = ['version', 'timestamp', 'user',
                                        'asset', 'access_path', 'source', 'configuration']
        self.attrmap['RelationshipType'] = ['name', 'description']
        self.attrmap['Relationship'] = ['version', 'timestamp', 'user',
                                        'relationship_type', 'schema']
        self.attrmap['Asset_Relationships'] = ['asset', 'relationship']
    
    def create_tables(self):
        database.init('normalized_catalog.db')
        with database:
            #add following to below if needed later: Item,
            database.create_tables([UserType, User, AssetType,
                                    Asset, WhoProfile, WhatProfile,
                                    HowProfile, WhyProfile, WhenProfile,
                                    SourceType, Source, WhereProfile, 
                                    Action, RelationshipType, Relationship,
                                    Asset_Relationships])
            # Item.__schema.create_foreign_key(Item.user)
            #self.ins_map["Item"] = Item
            self.ins_map["UserType"] = UserType
            self.ins_map["User"] = User
            self.ins_map["AssetType"] = AssetType
            self.ins_map["Asset"] = Asset
            self.ins_map["WhoProfile"] = WhoProfile
            self.ins_map["WhatProfile"] = WhatProfile
            self.ins_map["HowProfile"] = HowProfile
            self.ins_map["WhyProfile"] = WhyProfile
            self.ins_map["WhenProfile"] = WhenProfile
            self.ins_map["SourceType"] = SourceType
            self.ins_map["Source"] = Source
            self.ins_map["WhereProfile"] = WhereProfile
            self.ins_map["Action"] = Action
            self.ins_map["RelationshipType"] = RelationshipType
            self.ins_map["Relationship"] = Relationship
            self.ins_map['Asset_Relationships'] = Asset_Relationships
            
            UserType.insert({"name" : "administrator", "description" : "responsible for populating the catalog"}).execute()
            User.insert({"name": "admin", "user_type": 1, 
                         "version": 1, "timestamp" : str(datetime.datetime.now()),
                         "schema": {"addr":{"home":"westlake","company":"bank"},"phone":1234567}}).execute()
    
    def create_csvs(self):
        for key in self.attrmap:
            fh = open(key + '.csv', 'w+')
            
            csvwriter = csv.writer(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            self.fh_map[key] = fh
            self.csv_map[key] = csvwriter
            #self.csv_map[key].writeheader()
    
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
    
    def run_full(self):
        chunks = self.convert_to_pd('/home/pranav/dataset_metadata_2020_08_17.csv', 1000000)
        for c in chunks:
            self.map_to_csv(c)
    
    def map_to_csv(self, df):
        insertedAssetType = False
        insertedRelType = False
        insertedSourceType = False
        rel_key = 0
        #convert all nan's to nulls
        #chunk = chunk.fillna('NULL')
        df = df.fillna('NULL')
        for index, row in df.iterrows():
            self.keymap['Asset'] += 1
            for key in self.concept_map:
                i_val = row[key]
                if i_val == 'NULL':
                    continue
                if key == 'url':
                    self.keymap['WhereProfile'] += 1
                    self.csv_map['WhereProfile'].writerow([self.keymap['WhereProfile'],
                                                           self.version, 
                                                           str(datetime.datetime.now()),
                                                           self.keymap['User'],
                                                           self.keymap['Asset'],
                                                           i_val, 'NULL', 'NULL'])
                elif key == 'name':
                    if insertedAssetType:
                        self.csv_map['Asset'].writerow([self.keymap['Asset'], i_val, 
                                                        self.keymap['AssetType'],
                                                        self.version, str(datetime.datetime.now()),
                                                        self.keymap['User']])
                        
                    else:
                        #in this case, there's only one asset type, and that's an image
                        self.keymap['AssetType'] += 1
                        self.csv_map['AssetType'].writerow([self.keymap['AssetType'],
                                                            'Image',
                                                            'A file consisting of bytes that represent pixels'])
                        insertedAssetType = True
                        self.csv_map['Asset'].writerow([self.keymap['Asset'], i_val, 
                                                        self.keymap['AssetType'],
                                                        self.version, str(datetime.datetime.now()),
                                                        self.keymap['User']])
                elif key == 'description':
                    self.keymap['WhatProfile'] += 1
                    self.csv_map['WhatProfile'].writerow([self.keymap['WhatProfile'],
                                                         self.version,
                                                         str(datetime.datetime.now()),
                                                         self.keymap['User'],
                                                         self.keymap['Asset'],
                                                         "{instanceMeaning :" + str(i_val) + "}"])
                elif key == 'variablesMeasured':
                    #print("i_val is: " + str(i_val))
                    #print(i_val)
                    self.keymap['WhatProfile'] += 1
                    self.csv_map['WhatProfile'].writerow([self.keymap['WhatProfile'],
                                                         self.version,
                                                         str(datetime.datetime.now()),
                                                         self.keymap['User'],
                                                         self.keymap['Asset'],
                                                         "{variablesRecorded :" + str(i_val) + "}"])
                    
                elif key == 'measurementTechnique':
                    self.keymap['HowProfile'] += 1
                    self.csv_map['HowProfile'].writerow([self.keymap['HowProfile'],
                                                         self.version,
                                                         str(datetime.datetime.now()),
                                                         self.keymap['User'],
                                                         self.keymap['Asset'],
                                                         "{measurementTechnique :" + str(i_val) + "}"])
                elif key == 'sameAs':
                    #NOTE: this part highlights a very important problem the catalog service
                    #will have to do its best to solve: when we're only told that there should
                    #be a relationship between assets, but we're not given clear links to each,
                    #how do we establish the relationship? We need to find the asset that matches
                    #the user's descriptions, otherwise our relationship schema won't be useful.
                    if insertedRelType:
                        self.keymap['Relationship'] += 1
                        self.csv_map['Relationship'].writerow([self.keymap['Relationship'],
                                                               self.version,
                                                               str(datetime.datetime.now()),
                                                               self.keymap['User'],
                                                               self.keymap['RelationshipType'],
                                                               "{sameImageAs :" + str(i_val) + "}"])
                        
                        self.keymap['Asset_Relationships'] += 1
                        self.csv_map['Asset_Relationships'].writerow([self.keymap['Asset_Relationships'],
                                                                      self.keymap['Asset'],
                                                                      self.keymap['Relationship']])
                        
                    else:
                        self.keymap['RelationshipType'] += 1
                        self.csv_map['RelationshipType'].writerow([self.keymap['RelationshipType'],
                                                                   'Identical Images',
                                                                   'The images are of exactly the same thing.'])
                        insertedRelType = True
                        self.keymap['Relationship'] += 1
                        self.csv_map['Relationship'].writerow([self.keymap['Relationship'],
                                                               self.version,
                                                               str(datetime.datetime.now()),
                                                               self.keymap['User'],
                                                               self.keymap['RelationshipType'],
                                                               "{sameImageAs :" + str(i_val) + "}"])
                        self.keymap['Asset_Relationships'] += 1
                        self.csv_map['Asset_Relationships'].writerow([self.keymap['Asset_Relationships'],
                                                                      self.keymap['Asset'],
                                                                      self.keymap['Relationship']])
                        
                elif key == 'doi':
                    if insertedSourceType:
                        #we know there's only one kind of source here
                        #but the question is: should we assume every row comes from a distinct source,
                        #or should we assume they all come from the same source?
                        #again, if the name is all that's given, we don't know
                        #...let's assume they're all different
                        self.keymap['Source'] += 1
                        self.csv_map['Source'].writerow([self.keymap['Source'], self.version,
                                                         str(datetime.datetime.now()),
                                                         self.keymap['User'], 
                                                         'PNG Repository',
                                                         self.keymap['SourceType'],
                                                         "{doi : " + str(i_val) + "}"])
                        self.keymap['WhereProfile'] += 1
                        self.csv_map['WhereProfile'].writerow([self.keymap['WhereProfile'],
                                                           self.version, 
                                                           str(datetime.datetime.now()),
                                                           self.keymap['User'],
                                                           self.keymap['Asset'],
                                                           'NULL', self.keymap['Source'], 'NULL'])
                    else:
                        
                        self.keymap['SourceType'] += 1
                        self.csv_map['SourceType'].writerow([self.keymap['SourceType'],
                                                             'web browser', 'PNG', 'Regular Image'])
                        insertedSourceType = True
                        self.keymap['Source'] += 1
                        self.csv_map['Source'].writerow([self.keymap['Source'], self.version,
                                                         str(datetime.datetime.now()),
                                                         self.keymap['User'], 
                                                         'PNG Repository',
                                                         self.keymap['SourceType'],
                                                         "{doi : " + str(i_val) + "}"])
                        self.keymap['WhereProfile'] += 1
                        self.csv_map['WhereProfile'].writerow([self.keymap['WhereProfile'],
                                                           self.version, 
                                                           str(datetime.datetime.now()),
                                                           self.keymap['User'],
                                                           self.keymap['Asset'],
                                                           'NULL', self.keymap['Source'], 'NULL'])
                    
                elif key == 'identifier':
                    if insertedSourceType:
                        #we know there's only one kind of source here
                        #but the question is: should we assume every row comes from a distinct source,
                        #or should we assume they all come from the same source?
                        #again, if the name is all that's given, we don't know
                        #...let's assume they're all different
                        self.keymap['Source'] += 1
                        self.csv_map['Source'].writerow([self.keymap['Source'], self.version,
                                                         str(datetime.datetime.now()),
                                                         self.keymap['User'], 
                                                         'PNG Repository',
                                                         self.keymap['SourceType'],
                                                         "{identifier : " + str(i_val) + "}"])
                        self.keymap['WhereProfile'] += 1
                        self.csv_map['WhereProfile'].writerow([self.keymap['WhereProfile'],
                                                           self.version, 
                                                           str(datetime.datetime.now()),
                                                           self.keymap['User'],
                                                           self.keymap['Asset'],
                                                           'NULL', self.keymap['Source'], 'NULL'])
                    else:
                        self.keymap['SourceType'] += 1
                        self.csv_map['SourceType'].writerow([self.keymap['SourceType'],
                                                             'web browser', 'PNG', 'Regular Image'])
                        insertedSourceType = True
                        self.keymap['Source'] += 1
                        self.csv_map['Source'].writerow([self.keymap['Source'], self.version,
                                                         str(datetime.datetime.now()),
                                                         self.keymap['User'], 
                                                         'PNG Repository',
                                                         self.keymap['SourceType'],
                                                         "{identifier : " + str(i_val) + "}"])
                        self.keymap['WhereProfile'] += 1
                        self.csv_map['WhereProfile'].writerow([self.keymap['WhereProfile'],
                                                           self.version, 
                                                           str(datetime.datetime.now()),
                                                           self.keymap['User'],
                                                           self.keymap['Asset'],
                                                           'NULL', self.keymap['Source'], 'NULL'])
                elif key == 'author':
                    self.keymap['WhoProfile'] += 1
                    self.csv_map['WhoProfile'].writerow([self.keymap['WhoProfile'],
                                                         self.version, str(datetime.datetime.now()),
                                                         self.keymap['User'], self.keymap['Asset'],
                                                         'NULL', "{author : " + str(i_val) + "}"])
                elif key == 'isAccessibleForFree':
                    if insertedSourceType:
                        #we know there's only one kind of source here
                        #but the question is: should we assume every row comes from a distinct source,
                        #or should we assume they all come from the same source?
                        #again, if the name is all that's given, we don't know
                        #...let's assume they're all different
                        self.keymap['Source'] += 1
                        self.csv_map['Source'].writerow([self.keymap['Source'], self.version,
                                                         str(datetime.datetime.now()),
                                                         self.keymap['User'], 
                                                         'PNG Repository',
                                                         self.keymap['SourceType'],
                                                         "{isAccessibleForFree : " + str(i_val) + "}"])
                        self.keymap['WhereProfile'] += 1
                        self.csv_map['WhereProfile'].writerow([self.keymap['WhereProfile'],
                                                           self.version, 
                                                           str(datetime.datetime.now()),
                                                           self.keymap['User'],
                                                           self.keymap['Asset'],
                                                           'NULL', self.keymap['Source'], 'NULL'])
                    else:
                        self.keymap['SourceType'] += 1
                        self.csv_map['SourceType'].writerow([self.keymap['SourceType'],
                                                             'web browser', 'PNG', 'Regular Image'])
                        insertedSourceType = True
                        self.keymap['Source'] += 1
                        self.csv_map['Source'].writerow([self.keymap['Source'], self.version,
                                                         str(datetime.datetime.now()),
                                                         self.keymap['User'], 
                                                         'PNG Repository',
                                                         self.keymap['SourceType'],
                                                         "{isAccessibleForFree : " + str(i_val) + "}"])
                        self.keymap['WhereProfile'] += 1
                        self.csv_map['WhereProfile'].writerow([self.keymap['WhereProfile'],
                                                           self.version, 
                                                           str(datetime.datetime.now()),
                                                           self.keymap['User'],
                                                           self.keymap['Asset'],
                                                           'NULL', self.keymap['Source'], 'NULL'])
                elif key == 'dateModified':
                    self.keymap['WhenProfile'] += 1
                    self.csv_map['WhenProfile'].writerow([self.keymap['WhenProfile'],
                                                          self.version, str(datetime.datetime.now()),
                                                          self.keymap['User'],
                                                          self.keymap['Asset'],
                                                          str(i_val), 'NULL', 'NULL'])
                elif key == 'distribution':
                    self.keymap['WhereProfile'] += 1
                    self.csv_map['WhereProfile'].writerow([self.keymap['WhereProfile'],
                                                           self.version, 
                                                           str(datetime.datetime.now()),
                                                           self.keymap['User'],
                                                           self.keymap['Asset'],
                                                           'NULL', 'NULL', "{distribution : " + str(i_val) + "}"])
                elif key == 'spatialCoverage':
                    
                    self.keymap['WhatProfile'] += 1
                    self.csv_map['WhatProfile'].writerow([self.keymap['WhatProfile'],
                                                         self.version,
                                                         str(datetime.datetime.now()),
                                                         self.keymap['User'],
                                                         self.keymap['Asset'],
                                                         "{spatialCoverage :" + str(i_val) + "}"])
                elif key == 'provider':
                    
                    self.keymap['WhoProfile'] += 1
                    self.csv_map['WhoProfile'].writerow([self.keymap['WhoProfile'],
                                                         self.version, str(datetime.datetime.now()),
                                                         self.keymap['User'], self.keymap['Asset'],
                                                         'NULL', "{provider : " + str(i_val) + "}"])
                    
                elif key == 'funder':
                    self.keymap['WhoProfile'] += 1
                    self.csv_map['WhoProfile'].writerow([self.keymap['WhoProfile'],
                                                         self.version, str(datetime.datetime.now()),
                                                         self.keymap['User'], self.keymap['Asset'],
                                                         'NULL', "{funder : " + str(i_val) + "}"])
                elif key == 'temporalCoverage':
                    self.keymap['WhatProfile'] += 1
                    self.csv_map['WhatProfile'].writerow([self.keymap['WhatProfile'],
                                                         self.version,
                                                         str(datetime.datetime.now()),
                                                         self.keymap['User'],
                                                         self.keymap['Asset'],
                                                         "{temporalCoverage :" + str(i_val) + "}"])
    
    def close_all(self):
        for key in self.fh_map:
            self.fh_map[key].close()

if __name__ == "__main__":
    csv_creator = MapToNSQLCSV()
    #csv_creator.map_to_csv()
    csv_creator.run_full()
    csv_creator.close_all()
    



