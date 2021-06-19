#!/usr/bin/env python

import zlib
import zmq
import json
import sys
import time
from diskcache import Cache

relayEDDN = "tcp://eddn.edcd.io:9500"
timeoutEDDN = 600000

dirty = Cache("edsm-dirty")

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

                data = json.loads(zlib.decompress(message))

                market_schema = "https://eddn.edcd.io/schemas/commodity/3"
                if data["$schemaRef"] == market_schema:
                    system = data["message"]["systemName"]
                    station = data["message"]["stationName"]
                    cache_key = f"{system}{station}"
                    print(f"Dirtying market for {station} @ {system}")
                    dirty.set(cache_key, True)

        except zmq.ZMQError as e:
            print("ZMQSocketException: " + str(e))
            sys.stdout.flush()
            subscriber.disconnect(relayEDDN)
            time.sleep(5)


if __name__ == "__main__":
    main()
