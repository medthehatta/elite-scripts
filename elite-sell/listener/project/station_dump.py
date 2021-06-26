#!/usr/bin/env python


import json
import re
from datetime import datetime
from pprint import pprint
import time
import os
import subprocess

from bs4 import BeautifulSoup
from pymongo import MongoClient
import requests

from conversion import from_edsm
import db


def save_to_mongo(data):
    print(f"{data=}")
    converted = from_edsm(data)
    system = converted["system"]
    station = converted["station"]
    update_time = converted["update_time"]
    type_ = converted["type"]
    sc_distance = converted["sc_distance"]
    commodities = converted["commodities"]
    for commodity in commodities:
        db.commodity.update_one(
            {"name": commodity["name"]},
            {
                "$set": {
                    "name": commodity["name"],
                    "readable": commodity["readable"],
                },
            },
            upsert=True,
        )
        # We don't want this in the DB, EDDN does not have this key and we want
        # the DB entries to have identical fields
        commodity.pop("readable")
    db.station.update_one(
        {
            "system": system,
            "station": station,
        },
        {
            "$set": {
                "system": system,
                "station": station,
                "type": type_,
                "sc_distance": sc_distance,
                "source": "edsm-dump",
            },
        },
        upsert=True,
    )
    db.market.update_one(
        {
            "system": system,
            "station": station,
            # EDDN data is fresher than the dump, we're constantly reading it
            "source": {"$ne": "eddn"},
        },
        {
            "$set": {
                "system": system,
                "station": station,
                "update_time": update_time,
                "commodities": commodities,
                "source": "edsm-dump",
            },
        },
        upsert=True,
    )


def fetch_dump_page(url="https://www.edsm.net/en/nightly-dumps"):
    res = requests.get(url)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, features="lxml")
    return soup


def process_soup(soup):
    table = (
        soup
            .find(class_="card-header", string=re.compile(r"Stations"))
            .find_parent(class_="card")
            .find("table")
    )
    dump_url = (
        table
            .find("td", string=re.compile(r"Url:"))
            .find_next_sibling()
            .find("strong")
            .text
            .strip()
    )
    gen_time = (
        table
            .find("td", string=re.compile(r"Generated:"))
            .find_next_sibling()
            .text
            .strip()
    )
    dt = datetime.strptime(gen_time + " +00:00", "%b %d, %Y, %H:%M:%S %p %z")
    result = {"url": dump_url, "updated": dt.isoformat()}
    return result


def stream_zipped_from_url(url):
    cmd = (
        f"curl '{url}' | "
        f"zcat - | "
        f"jq -nc --stream 'fromstream(1|truncate_stream(inputs))'"
    )
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
    for line in p.stdout:
        json_ = json.loads(line)
        save_to_mongo(json_)


def main():
    while True:
        page = fetch_dump_page()
        meta = process_soup(page)
        url = meta["url"]
        prev_meta = db.dump_meta.find_one({"url": url}) or {"url": None, "updated": None}
        if prev_meta.get("updated") != meta["updated"]:
            print(f"{prev_meta.get('updated')=} != {meta['updated']=}, loading!")
            stream_zipped_from_url(url)
            db.dump_meta.update_one(
                {"url": url},
                {"$set": {"url": url, "updated": meta["updated"]}},
                upsert=True,
            )
            prev_meta = meta
        else:
            print(f"Got {meta=}")
            print(f"{prev_meta=}")
            print(f"No update found, no need to load.")
        time.sleep(1800)


if __name__ == "__main__":
    main()
