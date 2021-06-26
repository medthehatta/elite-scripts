#!/usr/bin/env python


from pymongo import MongoClient


mongo_url = os.environ.get("MONGO_URL", default="mongodb://localhost:27017/")
mongo = MongoClient(mongo_url)

dump_meta = mongo.dumpmetadb.dumps
market = mongo.elite.market
station = mongo.elite.station
commodity = mongo.elite.commodities
