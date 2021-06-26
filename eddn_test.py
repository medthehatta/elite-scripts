import zlib
import zmq
import json
import sys
import time

"""
 "  Configuration
"""
relayEDDN = "tcp://eddn.edcd.io:9500"
timeoutEDDN = 600000


"""
 "  Start
"""


def trace(data):
    print(json.dumps(data))
    sys.stdout.flush()


callbacks = [
    trace,
]


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
                for callback in callbacks:
                    callback(json_)

        except zmq.ZMQError as e:
            print("ZMQSocketException: " + str(e))
            sys.stdout.flush()
            subscriber.disconnect(relayEDDN)
            time.sleep(5)


if __name__ == "__main__":
    main()
