from backends.datavault_models import *
import csv
import datetime
import pandas as pd
import sqlite3
import sys

class MapToDSQLCSV():
    
    def __init__(self):
        self.ins_map = {}
        self.version = 1
        self.create_tables()
        self.csv_map = {}
        self.fh_map = {}
        #self.create_csvs()
        #self.keymap = {}
        #self.init_keymap()
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
        
    def create_tables(self):
        database.init('datavault_catalog.db')
        with database:
            database.create_tables([H_UserType, H_User, H_AssetType,
                                    H_Asset, H_WhoProfile, H_WhatProfile,
                                    H_HowProfile, H_WhyProfile, H_WhenProfile,
                                    H_SourceType, H_Source, H_WhereProfile, 
                                    H_Action, H_RelationshipType, H_Relationship,
                                    L_UserTypeLink, L_AssetTypeLink, L_Asset_WhoProfile,
                                    L_WhoProfileUser, L_Asset_HowProfile, L_Asset_WhyProfile,
                                    L_Asset_WhatProfile, L_Asset_WhenProfile, L_Source2Type,
                                    L_Asset_WhereProfile, L_AssetsInActions, L_Relationship_Type,
                                    L_Asset_Relationships, S_User_schema, S_WhoProfile_schema,
                                    S_HowProfile_schema, S_WhyProfile_schema, S_WhatProfile_schema,
                                    S_WhenProfile_Attributes, S_Configuration, S_SourceTypeAttributes,
                                    S_AssetTypeAttributes, S_UserTypeAttributes,
                                    S_RelationshipTypeAttributes, S_Relationship_schema,
                                    S_Source_schema, L_WhereProfile_Source])
            self.ins_map["H_UserType"] = H_UserType
            self.ins_map["H_User"] = H_User
            self.ins_map["H_AssetType"] = H_AssetType
            self.ins_map["H_Asset"] = H_Asset
            self.ins_map["H_WhoProfile"] = H_WhoProfile
            self.ins_map["H_WhatProfile"] = H_WhatProfile
            self.ins_map["H_HowProfile"] = H_HowProfile
            self.ins_map["H_WhyProfile"] = H_WhyProfile
            self.ins_map["H_WhenProfile"] = H_WhenProfile
            self.ins_map["H_SourceType"] = H_SourceType
            self.ins_map["H_Source"] = H_Source
            self.ins_map["H_WhereProfile"] = H_WhereProfile
            self.ins_map["H_Action"] = H_Action
            self.ins_map["H_RelationshipType"] = H_RelationshipType
            self.ins_map["H_Relationship"] = H_Relationship
            self.ins_map["L_UserTypeLink"] = L_UserTypeLink
            self.ins_map["L_AssetTypeLink"] = L_AssetTypeLink
            self.ins_map["L_Asset_WhoProfile"] = L_Asset_WhoProfile
            self.ins_map["L_WhoProfileUser"] = L_WhoProfileUser
            self.ins_map["L_Asset_HowProfile"] = L_Asset_HowProfile
            self.ins_map["L_Asset_WhyProfile"] = L_Asset_WhyProfile
            self.ins_map["L_Asset_WhatProfile"] = L_Asset_WhatProfile
            self.ins_map["L_Asset_WhenProfile"] = L_Asset_WhenProfile
            self.ins_map["L_Source2Type"] = L_Source2Type
            self.ins_map["L_Asset_WhereProfile"] = L_Asset_WhereProfile
            self.ins_map["L_AssetsInActions"] = L_AssetsInActions
            self.ins_map["L_Relationship_Type"] = L_Relationship_Type
            self.ins_map["L_Asset_Relationships"] = L_Asset_Relationships
            self.ins_map["S_User_schema"] = S_User_schema
            self.ins_map["S_WhoProfile_schema"] = S_WhoProfile_schema
            self.ins_map["S_HowProfile_schema"] = S_HowProfile_schema
            self.ins_map["S_WhyProfile_schema"] = S_WhyProfile_schema
            self.ins_map["S_WhatProfile_schema"] = S_WhatProfile_schema
            self.ins_map["S_WhenProfile_Attributes"] = S_WhenProfile_Attributes
            self.ins_map["S_Configuration"] = S_Configuration
            self.ins_map["S_SourceTypeAttributes"] = S_SourceTypeAttributes
            self.ins_map["S_AssetTypeAttributes"] = S_AssetTypeAttributes
            self.ins_map["S_UserTypeAttributes"] = S_UserTypeAttributes
            self.ins_map["S_RelationshipTypeAttributes"] = S_RelationshipTypeAttributes
            self.ins_map["S_Relationship_schema"] = S_Relationship_schema
            self.ins_map["S_Source_schema"] = S_Source_schema
            self.ins_map["L_WhereProfile_Source"] = L_WhereProfile_Source
            
            H_User.insert({"name": "admin", 
                      "version": self.version, "timestamp" : str(datetime.datetime.now())}).execute()
            
            H_UserType.insert({"version" : self.version, "timestamp" : str(datetime.datetime.now())}).execute()
            
            S_UserTypeAttributes.insert({"version" : self.version, "timestamp" : str(datetime.datetime.now()),
                                         "name" : "administrator",
                                         "description" : "responsible for populating the catalog"}).execute()
            
            
            S_User_schema.insert({"schema": {"addr":{"home":"westlake","company":"bank"},"phone":1234567},
                                  "version": self.version, "timestamp" : str(datetime.datetime.now()),
                                  "user" : 1}).execute()
            
            #now, link the tables together
            L_UserTypeLink.insert({"write_user" : 1, "user_type" : 1,
                                   "version" : self.version,
                                   "timestamp" : str(datetime.datetime.now())}).execute()
    
    def init_keymap(self):
        self.keymap["H_UserType"] = 1
        self.keymap["H_User"] = 1
        self.keymap["H_AssetType"] = 0
        self.keymap["H_Asset"] = 0
        self.keymap["H_WhoProfile"] = 0
        self.keymap["H_WhatProfile"] = 0
        self.keymap["H_HowProfile"] = 0
        self.keymap["H_WhyProfile"] = 0
        self.keymap["H_WhenProfile"] = 0
        self.keymap["H_SourceType"] = 0
        self.keymap["H_Source"] = 0
        self.keymap["H_WhereProfile"] = 0
        self.keymap["H_Action"] = 0
        self.keymap["H_RelationshipType"] = 0
        self.keymap["H_Relationship"] = 0
        self.keymap["L_UserTypeLink"] = 1
        self.keymap["L_AssetTypeLink"] = 0
        self.keymap["L_Asset_WhoProfile"] = 0
        self.keymap["L_WhoProfileUser"] = 0
        self.keymap["L_Asset_HowProfile"] = 0
        self.keymap["L_Asset_WhyProfile"] = 0
        self.keymap["L_Asset_WhatProfile"] = 0
        self.keymap["L_Asset_WhenProfile"] = 0
        self.keymap["L_Source2Type"] = 0
        self.keymap["L_Asset_WhereProfile"] = 0
        self.keymap["L_AssetsInActions"] = 0
        self.keymap["L_Relationship_Type"] = 0
        self.keymap["L_Asset_Relationships"] = 0
        self.keymap["S_User_schema"] = 1
        self.keymap["S_WhoProfile_schema"] = 0
        self.keymap["S_HowProfile_schema"] = 0
        self.keymap["S_WhyProfile_schema"] = 0
        self.keymap["S_WhatProfile_schema"] = 0
        self.keymap["S_WhenProfile_Attributes"] = 0
        self.keymap["S_Configuration"] = 0
        self.keymap["S_SourceTypeAttributes"] = 0
        self.keymap["S_AssetTypeAttributes"] = 0
        self.keymap["S_UserTypeAttributes"] = 1
        self.keymap["S_RelationshipTypeAttributes"] = 0
        self.keymap["S_Relationship_schema"] = 0
        self.keymap["S_Source_schema"] = 0
        self.keymap["L_WhereProfile_Source"] = 0
    
    def create_csvs(self):
        for key in self.ins_map:
            fh = open(key + '.csv', 'w+')
            csvwriter = csv.writer(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            self.fh_map[key] = fh
            self.csv_map[key] = csvwriter
    
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
    
    def map_to_csv(self, df):
        insertedAssetType = False
        insertedRelType = False
        insertedSourceType = False
        #convert all nan's to nulls
        #chunk = chunk.fillna('NULL')
        df = df.fillna('NULL')
        
        for index, row in df.iterrows():
            #we need to at least update this
            #anything else?
            self.keymap['H_Asset'] += 1
            seenWhat = False
            seenWhere = False
            seenWho = False
            seenSource = False
            for key in self.concept_map:
                i_val = row[key]
                if i_val == 'NULL':
                    continue
                if key == 'url':
                    self.keymap['H_WhereProfile'] += 1
                    self.csv_map['H_WhereProfile'].writerow([self.keymap['H_WhereProfile'],
                                                             i_val, self.version, 
                                                           str(datetime.datetime.now()),
                                                           self.keymap['H_User']])
                    seenWhere = True
                    
                    self.keymap['L_Asset_WhereProfile'] += 1
                    self.csv_map['L_Asset_WhereProfile'].writerow([self.keymap['L_Asset_WhereProfile'],
                                                                  self.keymap['H_Asset'],
                                                                  self.keymap['H_WhereProfile'],
                                                                  self.version,
                                                                  str(datetime.datetime.now()),
                                                                  self.keymap['H_User']])
                elif key == 'name':
                    if insertedAssetType:
                        self.csv_map['H_Asset'].writerow([self.keymap['H_Asset'], i_val,
                                                        self.version, str(datetime.datetime.now()),
                                                        self.keymap['H_User']])
                        
                        self.keymap['L_AssetTypeLink'] += 1
                        self.csv_map['L_AssetTypeLink'].writerow([self.keymap['L_AssetTypeLink'],
                                                                  self.keymap['H_Asset'],
                                                                  self.keymap['H_AssetType'],
                                                                  self.version, str(datetime.datetime.now()),
                                                                  self.keymap['H_User']])
                    else:
                        #in this case, there's only one asset type, and that's an image
                        self.keymap['H_AssetType'] += 1
                        self.csv_map['H_AssetType'].writerow([self.keymap['H_AssetType'],
                                                              self.version,
                                                              str(datetime.datetime.now()),
                                                              self.keymap['H_User']])
                        
                        self.keymap['S_AssetTypeAttributes'] += 1
                        self.csv_map['S_AssetTypeAttributes'].writerow([self.keymap['S_AssetTypeAttributes'],
                                                                        'Image',
                                                                        'A file consisting of bytes that represent pixels',
                                                                        self.version,
                                                                        str(datetime.datetime.now()),
                                                                        self.keymap['H_User']])
                        insertedAssetType = True
                        self.csv_map['H_Asset'].writerow([self.keymap['H_Asset'], i_val,
                                                        self.version, str(datetime.datetime.now()),
                                                        self.keymap['H_User']])
                        
                        self.keymap['L_AssetTypeLink'] += 1
                        self.csv_map['L_AssetTypeLink'].writerow([self.keymap['L_AssetTypeLink'],
                                                                  self.keymap['H_Asset'],
                                                                  self.keymap['H_AssetType'],
                                                                  self.version, str(datetime.datetime.now()),
                                                                  self.keymap['H_User']])
                elif key == 'description':
                    if seenWhat == False:
                        self.keymap['H_WhatProfile'] += 1
                        self.csv_map['H_WhatProfile'].writerow([self.keymap['H_WhatProfile'],
                                                             self.version,
                                                             str(datetime.datetime.now()),
                                                             self.keymap['H_User']])
                        
                        self.keymap['L_Asset_WhatProfile'] += 1
                        self.csv_map['L_Asset_WhatProfile'].writerow([self.keymap['L_Asset_WhatProfile'],
                                                                  self.keymap['H_Asset'], 
                                                                  self.keymap['H_WhatProfile'],
                                                                  self.version,
                                                                  str(datetime.datetime.now()),
                                                                  self.keymap['H_User']])
                        seenWhat = True
                    
                    self.keymap['S_WhatProfile_schema'] += 1
                    self.csv_map['S_WhatProfile_schema'].writerow([self.keymap['S_WhatProfile_schema'],
                                                                  self.keymap['H_WhatProfile'],
                                                                  "{instanceMeaning :" + str(i_val) + "}",
                                                                  self.version,
                                                                  str(datetime.datetime.now()),
                                                                  self.keymap['H_User']])
                    
                                                         
                    
                    
                elif key == 'variablesMeasured':
                    if seenWhat == False:
                        self.keymap['H_WhatProfile'] += 1
                        self.csv_map['H_WhatProfile'].writerow([self.keymap['H_WhatProfile'],
                                                             self.version,
                                                             str(datetime.datetime.now()),
                                                             self.keymap['H_User']])
                        
                        self.keymap['L_Asset_WhatProfile'] += 1
                        self.csv_map['L_Asset_WhatProfile'].writerow([self.keymap['L_Asset_WhatProfile'],
                                                                  self.keymap['H_Asset'], 
                                                                  self.keymap['H_WhatProfile'],
                                                                  self.version,
                                                                  str(datetime.datetime.now()),
                                                                  self.keymap['H_User']])
                        seenWhat = True
                    
                    self.keymap['S_WhatProfile_schema'] += 1
                    self.csv_map['S_WhatProfile_schema'].writerow([self.keymap['S_WhatProfile_schema'],
                                                                  self.keymap['H_WhatProfile'],
                                                                  "{variablesRecorded :" + str(i_val) + "}",
                                                                  self.version,
                                                                  str(datetime.datetime.now()),
                                                                  self.keymap['H_User']])
                    
                elif key == 'measurementTechnique':
                    
                    self.keymap['H_HowProfile'] += 1
                    self.csv_map['H_HowProfile'].writerow([self.keymap['H_HowProfile'],
                                                         self.version,
                                                         str(datetime.datetime.now()),
                                                         self.keymap['H_User']])
                    
                    self.keymap['L_Asset_HowProfile'] += 1
                    self.csv_map['L_Asset_HowProfile'].writerow([self.keymap['L_Asset_WhatProfile'],
                                                              self.keymap['H_Asset'], 
                                                              self.keymap['H_HowProfile'],
                                                              self.version,
                                                              str(datetime.datetime.now()),
                                                              self.keymap['H_User']])
                    self.keymap['S_HowProfile_schema'] += 1
                    self.csv_map['S_HowProfile_schema'].writerow([self.keymap['S_HowProfile_schema'],
                                                                  self.keymap['H_HowProfile'],
                                                                  "{measurementTechnique :" + str(i_val) + "}",
                                                                  self.version,
                                                                  str(datetime.datetime.now()),
                                                                  self.keymap['H_User']])
                    
                    
                elif key == 'sameAs':
                    #NOTE: this part highlights a very important problem the catalog service
                    #will have to do its best to solve: when we're only told that there should
                    #be a relationship between assets, but we're not given clear links to each,
                    #how do we establish the relationship? We need to find the asset that matches
                    #the user's descriptions, otherwise our relationship schema won't be useful.
                    if insertedRelType:
                        
                        self.keymap['H_Relationship'] += 1
                        self.csv_map['H_Relationship'].writerow([self.keymap['H_Relationship'],
                                                                 self.version,
                                                                 str(datetime.datetime.now()),
                                                                 self.keymap['H_User']])
                        
                        self.keymap['L_Asset_Relationships'] += 1
                        self.csv_map['L_Asset_Relationships'].writerow([self.keymap['L_Asset_Relationships'],
                                                                        self.keymap['H_Asset'],
                                                                        self.keymap['H_Relationship'],
                                                                        self.version,
                                                                        str(datetime.datetime.now()),
                                                                        self.keymap['H_User']])
                        
                        self.keymap['S_Relationship_schema'] += 1
                        self.csv_map['S_Relationship_schema'].writerow([self.keymap['S_Relationship_schema'],
                                                                       self.keymap['H_Relationship'],
                                                                       "{sameImageAs :" + str(i_val) + "}",
                                                                       self.version,
                                                                       str(datetime.datetime.now()),
                                                                       self.keymap['H_User']])
                        
                        self.keymap['L_Relationship_Type'] += 1
                        self.csv_map['L_Relationship_Type'].writerow([self.keymap['L_Relationship_Type'],
                                                                      self.keymap['H_Relationship'],
                                                                      self.keymap['H_RelationshipType'],
                                                                      self.version,
                                                                      str(datetime.datetime.now()),
                                                                      self.keymap['H_User']])
                        
                    else:
                        self.keymap['H_Relationship'] += 1
                        self.csv_map['H_Relationship'].writerow([self.keymap['H_Relationship'],
                                                                 self.version,
                                                                 str(datetime.datetime.now()),
                                                                 self.keymap['H_User']])
                        
                        self.keymap['L_Asset_Relationships'] += 1
                        self.csv_map['L_Asset_Relationships'].writerow([self.keymap['L_Asset_Relationships'],
                                                                        self.keymap['H_Asset'],
                                                                        self.keymap['H_Relationship'],
                                                                        self.version,
                                                                        str(datetime.datetime.now()),
                                                                        self.keymap['H_User']])
                        
                        self.keymap['S_Relationship_schema'] += 1
                        self.csv_map['S_Relationship_schema'].writerow([self.keymap['S_Relationship_schema'],
                                                                       self.keymap['H_Relationship'],
                                                                       "{sameImageAs :" + str(i_val) + "}",
                                                                       self.version,
                                                                       str(datetime.datetime.now()),
                                                                       self.keymap['H_User']])
                        
                        self.keymap['L_Relationship_Type'] += 1
                        self.csv_map['L_Relationship_Type'].writerow([self.keymap['L_Relationship_Type'],
                                                                      self.keymap['H_Relationship'],
                                                                      self.keymap['H_RelationshipType'],
                                                                      self.version,
                                                                      str(datetime.datetime.now()),
                                                                      self.keymap['H_User']])
                        
                        self.keymap['S_RelationshipTypeAttributes'] += 1
                        self.csv_map['S_RelationshipTypeAttributes'].writerow([self.keymap['S_RelationshipTypeAttributes'],
                                                                               'Identical Images',
                                                                               'The images are of exactly the same thing.',
                                                                               self.version,
                                                                               str(datetime.datetime.now()),
                                                                               self.keymap['H_User']])
                        
                elif key == 'doi':
                    if insertedSourceType:
                        #we know there's only one kind of source here
                        #but the question is: should we assume every row comes from a distinct source,
                        #or should we assume they all come from the same source?
                        #again, if the name is all that's given, we don't know
                        #...let's assume they're all different
                        
                        if seenSource == False:
                            self.keymap['H_Source'] += 1
                            self.csv_map['H_Source'].writerow([self.keymap['H_Source'],
                                                              self.version, 
                                                              str(datetime.datetime.now()),
                                                              self.keymap['H_User']])
                            
                            self.keymap['L_Source2Type'] += 1
                            self.csv_map['L_Source2Type'].writerow([self.keymap['L_Source2Type'],
                                                                   self.keymap['H_Source'],
                                                                   self.keymap['H_SourceType'],
                                                                   self.version, 
                                                                   str(datetime.datetime.now()),
                                                                   self.keymap['H_User']])
                            
                            self.keymap['L_WhereProfile_Source'] += 1
                            self.csv_map['L_WhereProfile_Source'].writerow([self.keymap['L_WhereProfile_Source'],
                                                                            self.keymap['H_Source'],
                                                                            self.keymap['H_WhereProfile'],
                                                                            self.version, 
                                                                            str(datetime.datetime.now()),
                                                                            self.keymap['H_User']])
                            seenSource = True
                            
                        self.keymap['S_Source_schema'] += 1
                        self.csv_map['S_Source_schema'].writerow([self.keymap['S_Source_schema'],
                                                                  "{doi : " + str(i_val) + "}",
                                                                  self.version, 
                                                                  str(datetime.datetime.now()),
                                                                  self.keymap['H_User']])
                            
                    else:
                        
                        self.keymap['H_SourceType'] += 1
                        self.csv_map['H_SourceType'].writerow([self.keymap['H_SourceType'],
                                                               self.version, 
                                                               str(datetime.datetime.now()),
                                                               self.keymap['H_User']])
                        
                        self.keymap['S_SourceTypeAttributes'] += 1
                        self.csv_map['S_SourceTypeAttributes'].writerow([self.keymap['S_SourceTypeAttributes'],
                                                                        self.keymap['H_SourceType'],
                                                                        'web browser', 'PNG', 'Regular Image',
                                                                        self.version, 
                                                                        str(datetime.datetime.now()),
                                                                        self.keymap['H_User']])
                        insertedSourceType = True
                        if seenSource == False:
                            #assumes we haven't seen source before 
                            self.keymap['H_Source'] += 1
                            self.csv_map['H_Source'].writerow([self.keymap['H_Source'],
                                                              self.version, 
                                                              str(datetime.datetime.now()),
                                                              self.keymap['H_User']])
                            
                            self.keymap['L_Source2Type'] += 1
                            self.csv_map['L_Source2Type'].writerow([self.keymap['L_Source2Type'],
                                                                   self.keymap['H_Source'],
                                                                   self.keymap['H_SourceType'],
                                                                   self.version, 
                                                                   str(datetime.datetime.now()),
                                                                   self.keymap['H_User']])
                            
                            self.keymap['L_WhereProfile_Source'] += 1
                            self.csv_map['L_WhereProfile_Source'].writerow([self.keymap['L_WhereProfile_Source'],
                                                                            self.keymap['H_Source'],
                                                                            self.keymap['H_WhereProfile'],
                                                                            self.version, 
                                                                            str(datetime.datetime.now()),
                                                                            self.keymap['H_User']])
                            seenSource = True
                            
                        self.keymap['S_Source_schema'] += 1
                        self.csv_map['S_Source_schema'].writerow([self.keymap['S_Source_schema'],
                                                                  "{doi : " + str(i_val) + "}",
                                                                  self.version, 
                                                                  str(datetime.datetime.now()),
                                                                  self.keymap['H_User']])
                    
                elif key == 'identifier':
                    if insertedSourceType:
                        #we know there's only one kind of source here
                        #but the question is: should we assume every row comes from a distinct source,
                        #or should we assume they all come from the same source?
                        #again, if the name is all that's given, we don't know
                        #...let's assume they're all different
                        
                        if seenSource == False:
                            self.keymap['H_Source'] += 1
                            self.csv_map['H_Source'].writerow([self.keymap['H_Source'],
                                                              self.version, 
                                                              str(datetime.datetime.now()),
                                                              self.keymap['H_User']])
                            
                            self.keymap['L_Source2Type'] += 1
                            self.csv_map['L_Source2Type'].writerow([self.keymap['L_Source2Type'],
                                                                   self.keymap['H_Source'],
                                                                   self.keymap['H_SourceType'],
                                                                   self.version, 
                                                                   str(datetime.datetime.now()),
                                                                   self.keymap['H_User']])
                            
                            self.keymap['L_WhereProfile_Source'] += 1
                            self.csv_map['L_WhereProfile_Source'].writerow([self.keymap['L_WhereProfile_Source'],
                                                                            self.keymap['H_Source'],
                                                                            self.keymap['H_WhereProfile'],
                                                                            self.version, 
                                                                            str(datetime.datetime.now()),
                                                                            self.keymap['H_User']])
                            seenSource = True
                            
                        self.keymap['S_Source_schema'] += 1
                        self.csv_map['S_Source_schema'].writerow([self.keymap['S_Source_schema'],
                                                                  "{identifier : " + str(i_val) + "}",
                                                                  self.version, 
                                                                  str(datetime.datetime.now()),
                                                                  self.keymap['H_User']])
                            
                    else:
                        
                        self.keymap['H_SourceType'] += 1
                        self.csv_map['H_SourceType'].writerow([self.keymap['H_SourceType'],
                                                               self.version, 
                                                               str(datetime.datetime.now()),
                                                               self.keymap['H_User']])
                        
                        self.keymap['S_SourceTypeAttributes'] += 1
                        self.csv_map['S_SourceTypeAttributes'].writerow([self.keymap['S_SourceTypeAttributes'],
                                                                        self.keymap['H_SourceType'],
                                                                        'web browser', 'PNG', 'Regular Image',
                                                                        self.version, 
                                                                        str(datetime.datetime.now()),
                                                                        self.keymap['H_User']])
                        insertedSourceType = True
                        if seenSource == False:
                            #assumes we haven't seen source before 
                            self.keymap['H_Source'] += 1
                            self.csv_map['H_Source'].writerow([self.keymap['H_Source'],
                                                              self.version, 
                                                              str(datetime.datetime.now()),
                                                              self.keymap['H_User']])
                            
                            self.keymap['L_Source2Type'] += 1
                            self.csv_map['L_Source2Type'].writerow([self.keymap['L_Source2Type'],
                                                                   self.keymap['H_Source'],
                                                                   self.keymap['H_SourceType'],
                                                                   self.version, 
                                                                   str(datetime.datetime.now()),
                                                                   self.keymap['H_User']])
                            
                            self.keymap['L_WhereProfile_Source'] += 1
                            self.csv_map['L_WhereProfile_Source'].writerow([self.keymap['L_WhereProfile_Source'],
                                                                            self.keymap['H_Source'],
                                                                            self.keymap['H_WhereProfile'],
                                                                            self.version, 
                                                                            str(datetime.datetime.now()),
                                                                            self.keymap['H_User']])
                            seenSource = True
                            
                        self.keymap['S_Source_schema'] += 1
                        self.csv_map['S_Source_schema'].writerow([self.keymap['S_Source_schema'],
                                                                  "{identifier : " + str(i_val) + "}",
                                                                  self.version, 
                                                                  str(datetime.datetime.now()),
                                                                  self.keymap['H_User']])
                elif key == 'author':
                    
                    if seenWho == False:
                        self.keymap['H_WhoProfile'] += 1
                        self.csv_map['H_WhoProfile'].writerow([self.keymap['H_WhoProfile'],
                                                             self.version,
                                                             str(datetime.datetime.now()),
                                                             self.keymap['H_User']])
                        
                        self.keymap['L_Asset_WhoProfile'] += 1
                        self.csv_map['L_Asset_WhoProfile'].writerow([self.keymap['L_Asset_WhoProfile'],
                                                                  self.keymap['H_Asset'], 
                                                                  self.keymap['H_WhoProfile'],
                                                                  self.version,
                                                                  str(datetime.datetime.now()),
                                                                  self.keymap['H_User']])
                        seenWho = True
                    
                    self.keymap['S_WhoProfile_schema'] += 1
                    self.csv_map['S_WhoProfile_schema'].writerow([self.keymap['S_WhoProfile_schema'],
                                                                  self.keymap['H_WhoProfile'],
                                                                  "{author : " + str(i_val) + "}",
                                                                  self.version,
                                                                  str(datetime.datetime.now()),
                                                                  self.keymap['H_User']])
                    
                    
                elif key == 'isAccessibleForFree':
                    if insertedSourceType:
                        #we know there's only one kind of source here
                        #but the question is: should we assume every row comes from a distinct source,
                        #or should we assume they all come from the same source?
                        #again, if the name is all that's given, we don't know
                        #...let's assume they're all different
                        
                        if seenSource == False:
                            self.keymap['H_Source'] += 1
                            self.csv_map['H_Source'].writerow([self.keymap['H_Source'],
                                                              self.version, 
                                                              str(datetime.datetime.now()),
                                                              self.keymap['H_User']])
                            
                            self.keymap['L_Source2Type'] += 1
                            self.csv_map['L_Source2Type'].writerow([self.keymap['L_Source2Type'],
                                                                   self.keymap['H_Source'],
                                                                   self.keymap['H_SourceType'],
                                                                   self.version, 
                                                                   str(datetime.datetime.now()),
                                                                   self.keymap['H_User']])
                            
                            self.keymap['L_WhereProfile_Source'] += 1
                            self.csv_map['L_WhereProfile_Source'].writerow([self.keymap['L_WhereProfile_Source'],
                                                                            self.keymap['H_Source'],
                                                                            self.keymap['H_WhereProfile'],
                                                                            self.version, 
                                                                            str(datetime.datetime.now()),
                                                                            self.keymap['H_User']])
                            seenSource = True
                            
                        self.keymap['S_Source_schema'] += 1
                        self.csv_map['S_Source_schema'].writerow([self.keymap['S_Source_schema'],
                                                                  "{isAccessibleForFree : " + str(i_val) + "}",
                                                                  self.version, 
                                                                  str(datetime.datetime.now()),
                                                                  self.keymap['H_User']])
                            
                    else:
                        
                        self.keymap['H_SourceType'] += 1
                        self.csv_map['H_SourceType'].writerow([self.keymap['H_SourceType'],
                                                               self.version, 
                                                               str(datetime.datetime.now()),
                                                               self.keymap['H_User']])
                        
                        self.keymap['S_SourceTypeAttributes'] += 1
                        self.csv_map['S_SourceTypeAttributes'].writerow([self.keymap['S_SourceTypeAttributes'],
                                                                        self.keymap['H_SourceType'],
                                                                        'web browser', 'PNG', 'Regular Image',
                                                                        self.version, 
                                                                        str(datetime.datetime.now()),
                                                                        self.keymap['H_User']])
                        insertedSourceType = True
                        if seenSource == False:
                            #assumes we haven't seen source before 
                            self.keymap['H_Source'] += 1
                            self.csv_map['H_Source'].writerow([self.keymap['H_Source'],
                                                              self.version, 
                                                              str(datetime.datetime.now()),
                                                              self.keymap['H_User']])
                            
                            self.keymap['L_Source2Type'] += 1
                            self.csv_map['L_Source2Type'].writerow([self.keymap['L_Source2Type'],
                                                                   self.keymap['H_Source'],
                                                                   self.keymap['H_SourceType'],
                                                                   self.version, 
                                                                   str(datetime.datetime.now()),
                                                                   self.keymap['H_User']])
                            
                            self.keymap['L_WhereProfile_Source'] += 1
                            self.csv_map['L_WhereProfile_Source'].writerow([self.keymap['L_WhereProfile_Source'],
                                                                            self.keymap['H_Source'],
                                                                            self.keymap['H_WhereProfile'],
                                                                            self.version, 
                                                                            str(datetime.datetime.now()),
                                                                            self.keymap['H_User']])
                            seenSource = True
                            
                        self.keymap['S_Source_schema'] += 1
                        self.csv_map['S_Source_schema'].writerow([self.keymap['S_Source_schema'],
                                                                  "{isAccessibleForFree : " + str(i_val) + "}",
                                                                  self.version, 
                                                                  str(datetime.datetime.now()),
                                                                  self.keymap['H_User']])
                elif key == 'dateModified':
                    
                    self.keymap['H_WhenProfile'] += 1
                    self.csv_map['H_WhenProfile'].writerow([self.keymap['H_WhenProfile'],
                                                         self.version,
                                                         str(datetime.datetime.now()),
                                                         self.keymap['H_User']])
                    
                    self.keymap['L_Asset_WhenProfile'] += 1
                    self.csv_map['L_Asset_WhenProfile'].writerow([self.keymap['L_Asset_WhenProfile'],
                                                              self.keymap['H_Asset'], 
                                                              self.keymap['H_WhenProfile'],
                                                              self.version,
                                                              str(datetime.datetime.now()),
                                                              self.keymap['H_User']])
                    self.keymap['S_WhenProfile_Attributes'] += 1
                    self.csv_map['S_WhenProfile_Attributes'].writerow([self.keymap['S_WhenProfile_Attributes'],
                                                                  self.keymap['H_WhenProfile'],
                                                                  str(i_val),
                                                                  'NULL', 'NULL',
                                                                  self.version,
                                                                  str(datetime.datetime.now()),
                                                                  self.keymap['H_User']])
                    
                    
                elif key == 'distribution':
                    if seenWhere == False:
                        self.keymap['H_WhereProfile'] += 1
                        self.csv_map['H_WhereProfile'].writerow([self.keymap['H_WhereProfile'],
                                                                 'NULL', self.version, 
                                                               str(datetime.datetime.now()),
                                                               self.keymap['H_User']])
                        
                        self.keymap['L_Asset_WhereProfile'] += 1
                        self.csv_map['L_Asset_WhereProfile'].writerow([self.keymap['L_Asset_WhereProfile'],
                                                                      self.keymap['H_Asset'],
                                                                      self.keymap['H_WhereProfile'],
                                                                      self.version,
                                                                      str(datetime.datetime.now()),
                                                                      self.keymap['H_User']])
                        seenWhere = True
                    
                    self.keymap['S_Configuration'] += 1
                    self.csv_map['S_Configuration'].writerow([self.keymap['S_Configuration'],
                                                              "{distribution : " + str(i_val) + "}",
                                                              self.keymap['H_WhereProfile'],
                                                              self.version,
                                                              str(datetime.datetime.now()),
                                                              self.keymap['H_User']])
                elif key == 'spatialCoverage':
                    
                    if seenWhat == False:
                        self.keymap['H_WhatProfile'] += 1
                        self.csv_map['H_WhatProfile'].writerow([self.keymap['H_WhatProfile'],
                                                             self.version,
                                                             str(datetime.datetime.now()),
                                                             self.keymap['H_User']])
                        
                        self.keymap['L_Asset_WhatProfile'] += 1
                        self.csv_map['L_Asset_WhatProfile'].writerow([self.keymap['L_Asset_WhatProfile'],
                                                                  self.keymap['H_Asset'], 
                                                                  self.keymap['H_WhatProfile'],
                                                                  self.version,
                                                                  str(datetime.datetime.now()),
                                                                  self.keymap['H_User']])
                        seenWhat = True
                    
                    self.keymap['S_WhatProfile_schema'] += 1
                    self.csv_map['S_WhatProfile_schema'].writerow([self.keymap['S_WhatProfile_schema'],
                                                                  self.keymap['H_WhatProfile'],
                                                                  "{spatialCoverage :" + str(i_val) + "}",
                                                                  self.version,
                                                                  str(datetime.datetime.now()),
                                                                  self.keymap['H_User']])
                elif key == 'provider':
                    
                    if seenWho == False:
                        self.keymap['H_WhoProfile'] += 1
                        self.csv_map['H_WhoProfile'].writerow([self.keymap['H_WhoProfile'],
                                                             self.version,
                                                             str(datetime.datetime.now()),
                                                             self.keymap['H_User']])
                        
                        self.keymap['L_Asset_WhoProfile'] += 1
                        self.csv_map['L_Asset_WhoProfile'].writerow([self.keymap['L_Asset_WhoProfile'],
                                                                  self.keymap['H_Asset'], 
                                                                  self.keymap['H_WhoProfile'],
                                                                  self.version,
                                                                  str(datetime.datetime.now()),
                                                                  self.keymap['H_User']])
                        seenWho = True
                    
                    self.keymap['S_WhoProfile_schema'] += 1
                    self.csv_map['S_WhoProfile_schema'].writerow([self.keymap['S_WhoProfile_schema'],
                                                                  self.keymap['H_WhoProfile'],
                                                                  "{provider :" + str(i_val) + "}",
                                                                  self.version,
                                                                  str(datetime.datetime.now()),
                                                                  self.keymap['H_User']])
                    
                elif key == 'funder':
                    if seenWho == False:
                        self.keymap['H_WhoProfile'] += 1
                        self.csv_map['H_WhoProfile'].writerow([self.keymap['H_WhoProfile'],
                                                             self.version,
                                                             str(datetime.datetime.now()),
                                                             self.keymap['H_User']])
                        
                        self.keymap['L_Asset_WhoProfile'] += 1
                        self.csv_map['L_Asset_WhoProfile'].writerow([self.keymap['L_Asset_WhoProfile'],
                                                                  self.keymap['H_Asset'], 
                                                                  self.keymap['H_WhoProfile'],
                                                                  self.version,
                                                                  str(datetime.datetime.now()),
                                                                  self.keymap['H_User']])
                        seenWho = True
                    
                    self.keymap['S_WhoProfile_schema'] += 1
                    self.csv_map['S_WhoProfile_schema'].writerow([self.keymap['S_WhoProfile_schema'],
                                                                  self.keymap['H_WhoProfile'],
                                                                  "{funder :" + str(i_val) + "}",
                                                                  self.version,
                                                                  str(datetime.datetime.now()),
                                                                  self.keymap['H_User']])
                    
                elif key == 'temporalCoverage':
                    if seenWhat == False:
                        self.keymap['H_WhatProfile'] += 1
                        self.csv_map['H_WhatProfile'].writerow([self.keymap['H_WhatProfile'],
                                                             self.version,
                                                             str(datetime.datetime.now()),
                                                             self.keymap['H_User']])
                        
                        self.keymap['L_Asset_WhatProfile'] += 1
                        self.csv_map['L_Asset_WhatProfile'].writerow([self.keymap['L_Asset_WhatProfile'],
                                                                  self.keymap['H_Asset'], 
                                                                  self.keymap['H_WhatProfile'],
                                                                  self.version,
                                                                  str(datetime.datetime.now()),
                                                                  self.keymap['H_User']])
                        seenWhat = True
                    
                    self.keymap['S_WhatProfile_schema'] += 1
                    self.csv_map['S_WhatProfile_schema'].writerow([self.keymap['S_WhatProfile_schema'],
                                                                  self.keymap['H_WhatProfile'],
                                                                  "{temporalCoverage :" + str(i_val) + "}",
                                                                  self.version,
                                                                  str(datetime.datetime.now()),
                                                                  self.keymap['H_User']])
        
    
    def close_all(self):
        for key in self.fh_map:
            self.fh_map[key].close()
            
    def run_full(self):
        chunks = self.convert_to_pd('/home/pranav/dataset_metadata_2020_08_17.csv', 1000000)
        for c in chunks:
            self.map_to_csv(c)
    
    def perform_inserts(self):
        #first, it looks like we have some big fields, so...
        csv.field_size_limit(int(sys.maxsize/10))
        con = sqlite3.connect('datavault_catalog.db') 
        for key in self.ins_map:
            inserts = []
            rowlen = -1
            numrows = 0
            with open(key + '.csv', 'r') as fh:
                csvreader = csv.reader(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                for row in csvreader:
                    rowlen = len(row)
                    numrows += 1
                    if rowlen == 0:
                        continue
                    inserts.append(row)
            #construct the query
            if numrows == 0:
                continue
            query = 'INSERT INTO ' + key.lower() + ' VALUES ('
            for i in range(rowlen):
                query += '?,'
            query = query[:-1] + ');'
            print("Executing: " + query)
            cur = con.cursor()
            cur.executemany(query, inserts)
            con.commit()
        con.close()
            
            
                

if __name__ == "__main__":
    csv_creator = MapToDSQLCSV()
    #df = csv_creator.convert_to_pd('/home/pranav/dataset_metadata_2020_08_17.csv', 1000)
    #csv_creator.map_to_csv(df)
    #csv_creator.run_full()
    #csv_creator.close_all()
    
    #run this if you already have the csvs
    csv_creator.create_tables()
    csv_creator.perform_inserts()