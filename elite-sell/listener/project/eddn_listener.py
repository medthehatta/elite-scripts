#!/usr/bin/env python


import zlib
import zmq
import json
import sys
import time
import os

from pymongo import MongoClient

from conversion import from_eddn
import db


relayEDDN = "tcp://eddn.edcd.io:9500"
timeoutEDDN = 600000

def save_to_mongo(data):
    if data["$schemaRef"] != "https://eddn.edcd.io/schemas/commodity/3":
        return

    converted = from_eddn(data)
    system = converted["system"]
    station = converted["station"]
    update_time = converted["update_time"]
    commodities = converted["commodities"]
    db.market.update_one(
        {"system": system, "station": station},
        {
            "$set": {
                "system": system,
                "station": station,
                "update_time": update_time,
                "commodities": commodities,
                "source": "eddn",
            },
        },
        upsert=True,
    )
    print(json.dumps(data))
    sys.stdout.flush()


def main():
    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)

    subscriber.setsockopt(zmq.SUBSCRIBE, b"")
    subscriber.setsockopt(zmq.RCVTIMEO, timeoutEDDN)

    while True:
        try:
            subscriber.connect(relayEDDN)

            while True:
                message = subscriber.recv()

                if message == False:
                    subscriber.disconnect(relayEDDN)
                    break

                json_ = json.loads(zlib.decompress(message))
                save_to_mongo(json_)

        except zmq.ZMQError as e:
            print("ZMQSocketException: " + str(e))
            sys.stdout.flush()
            subscriber.disconnect(relayEDDN)
            time.sleep(5)


if __name__ == "__main__":
    main()
