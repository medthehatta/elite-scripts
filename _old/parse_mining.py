import re
import json


def parse(lines):
    for line in lines:
        try:
            (left, right) = line.split(" | ", 1)
        except Exception:
            pass
        (name, items) = (left, [x.strip() for x in right.split(",")])
        yield {"name": name, "items": items}


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("infile", default="-", nargs="?", type=argparse.FileType("r"))
    parsed = parser.parse_args()
    for x in parse(parsed.infile):
        print(json.dumps(x))


if __name__ == "__main__":
    main()
