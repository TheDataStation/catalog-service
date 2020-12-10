from flask import Flask
from flask import request
from flask import Flask, Blueprint
from flask_restplus import Api, Namespace, Resource, Swagger, fields
import json
import config

app = Flask(__name__)

api = Api(app, prefix="/v1", title="catalog service", description="catserv api.")

from frontends.api import create_api

# app = Flask(__name__)
db_path = "../backends/catserv.db"
cs = create_api("sqlite", db_path)


@api.route('/search')
@api.doc("wildcard search by keywords in catalog")
@api.param('keywords')
class SearchApi(Resource):
    def get(self):
        print(cs.bk.ins_map)
        return cs.search_by_keywords(request.args.get('keywords', default="", type=str))


@api.route('/get')
@api.doc("low-level get data")
@api.param('table_name', 'str')
@api.param('id', "int list")
@api.param('asset_name', "str list")
@api.param('timestamp', 'str list')
@api.param('name', 'str list')
@api.param('version', 'int list')
@api.param('sub_schema', 'json str')
class get(Resource):
    def get(self):
        cs.get(ins_name=request.args.get('table_name', default=None, type=str),
               id=request.args.get('id', default=None, type=list),
               asset_id=request.args.get('asset_name', default=None, type=list),
               timestamp=request.args.get('timestamp', default=None, type=list),
               name=request.args.get('name', default=None, type=list),
               version=request.args.get('version', default=None, type=list),
               sub_schema=json.loads(request.args.get('sub_schema', default=None, type=str)))


@api.route('/put')
@api.doc("low-level put data")
@api.param('table_name', 'str')
@api.param('content', "json str")
class put(Resource):
    def put(self):
        return cs.put(request.args.get('table_name', default=None, type=str),
                      json.loads(request.args.get('content', default=None, type=str)))


@api.route('/get_what_profiles')
@api.doc("get what profiles by asset_id or sub_schema")
@api.param('asset_name', 'asset name split by ","')
@api.param('sub_schema', 'json str')
class get_what_profiles(Resource):
    def get(self):
        asset_name = request.args.get('asset_name', default=None, type=str)
        sub_schema = request.args.get('sub_schema', default=None, type=str)
        if asset_name and sub_schema:
            return cs.get_profiles('WhatProfile',
                                   asset_name.split(","),
                                   json.loads(sub_schema))
        elif sub_schema:
            return cs.get_profiles('WhatProfile',
                                   None,
                                   json.loads(sub_schema))
        else:
            return cs.get_profiles('WhatProfile',
                                   asset_name.split(","),
                                   None)


@api.route('/get_who_profiles')
@api.doc("get who profiles by asset_id or sub_schema")
@api.param('asset_name', 'asset name split by ","')
@api.param('user_name', 'user name split by ","')
class get_who_profiles(Resource):
    def get(self):
        asset_name = request.args.get('asset_name', default=None, type=str)
        user_name = request.args.get('user_name', default=None, type=str)
        if user_name and asset_name:
            return cs.get_profiles('WhoProfile',
                                   asset_id=asset_name.split(","),
                                   user_id=user_name.split(","))
        elif user_name:
            return cs.get_profiles('WhoProfile',
                                   user_id=user_name.split(","))
        else:
            return cs.get_profiles('WhoProfile',
                                   asset_id=asset_name.split(","))


@api.route('/insert_what_profiles')
@api.doc("insert what profiles")
@api.param('content', 'json str')
class insert_what_profiles(Resource):
    def put(self):
        content = request.args.get('content', default=None, type=str)
        if content:
            return cs.insert_profile('WhatProfile',
                                     json.loads(content))
        else:
            return False


@api.route('/get_asset')
@api.doc("get asset by asset_id")
@api.param('asset_name', 'asset name split by ","')
class get_asset(Resource):
    def get(self):
        asset_name = request.args.get('asset_name', default=None, type=str)
        if asset_name:
            return cs.get("Asset", asset_id=asset_name.split(","))

@api.route('/insert_who_profiles')
@api.doc("insert who profiles")
@api.param('content', 'json object')
class insert_who_profiles(Resource):
    def put(self):
        content = request.args.get('content', default=None, type=str)
        if content:
            return cs.insert_profile('WhoProfile',
                                     json.loads(content))
        else:
            return False


if __name__ == '__main__':
    app.run()
