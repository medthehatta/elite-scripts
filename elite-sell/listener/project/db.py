#!/usr/bin/env python


import os

from cytoolz import dissoc
from diskcache import Cache
from pymongo import MongoClient


edsm_cache = Cache("edsm-cache")


mongo_url = os.environ.get("MONGO_URL", default="mongodb://localhost:27017/")
print(f"Using mongo at: {mongo_url}")
mongo = MongoClient(mongo_url)

dump_meta = mongo.dumpmetadb.dumps
market = mongo.elite.market
station = mongo.elite.station
commodity = mongo.elite.commodities


def strip_id(result):
    if result is not None:
        return dissoc(result, "_id")
    else:
        return None
