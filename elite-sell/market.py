#!/usr/bin/env python


import datetime
import json
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from typing import Dict

import redis
import requests
from celery.result import AsyncResult
from cytoolz import dissoc
from cytoolz import get_in
from cytoolz import groupby
from cytoolz import partition_all
from fastapi import Body
from fastapi import FastAPI
from fastapi import Form
from fastapi import Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from retrying import retry

from worker import celery_worker

UNSET = object()

app = FastAPI()


class DB:
    def __init__(self, db, namespace="default"):
        self.db = db
        self.namespace = namespace

    def _key(self, key):
        return json.dumps([self.namespace, key])

    def invalidate(self, key):
        key = self._key(key)
        if self.db.exists(key):
            self.db.delete(key)
            return {"ok": True, "present_before": True, "key": key}
        else:
            return {"ok": True, "present_before": False, "key": key}

    def set(self, key, value):
        key = self._key(key)
        value = json.dumps(value)
        self.db.set(key, value)
        return {"ok": True, "value": value, "key": key}

    def peek(self, key):
        key = self._key(key)
        if self.db.exists(key):
            value = json.loads(self.db.get(key))
            return {"ok": True, "value": value, "key": key}
        else:
            return {"ok": False, "error": "unpopulated", "key": key}

    def get(self, key, default=UNSET):
        result = self.peek(key)
        if result["ok"]:
            return result["value"]
        elif default is not UNSET:
            return default
        else:
            raise LookupError(key)

    def exists(self, key):
        key = self._key(key)
        return True if self.db.exists(key) else False

    def memoize(self, func):
        @wraps(func)
        def _wrapped(*args, **kwargs):
            key = (args, kwargs)
            if self.exists(key):
                return self.get(key)
            else:
                value = func(*args, **kwargs)
                self.set(key, value)
                return value

        return _wrapped

    def which(self, keys):
        db_keys = set(self.db.keys())
        keys = set(self._key(k) for k in keys)
        present = keys.intersection(db_keys)
        missing = keys.difference(db_keys)
        return {"ok": True, True: present, False: missing}


class RedisDB:
    @classmethod
    def from_environment(cls, variable="DB_URL", namespace="default"):
        url = os.environ.get(variable, "redis://localhost:6379/0")
        print(f"Connecting to redis at url: {url} for namespace {namespace}")
        client = redis.from_url(url)
        return DB(client, namespace)

    def __init__(self, client):
        self.client = client

    def __getattr__(self, attr):
        return getattr(self.client, attr)


station_db = RedisDB.from_environment("DB_URL", namespace="stations")
market_db = RedisDB.from_environment("DB_URL", namespace="markets")
request_db = RedisDB.from_environment("DB_URL", namespace="requests")


@retry(stop_max_attempt_number=3)
def _get_with_http(url, params):
    print(f"GET {url} ({params})...")
    r = requests.get(url, params=params)
    rate_limit = int(r.headers.get("X-Rate-Limit-Limit", 720))
    rate_reset = int(r.headers.get("X-Rate-Limit-Reset", 0))
    retry_after = int(r.headers.get("Retry-After", 0))
    request_interval = rate_reset / rate_limit
    request_preroll = 50
    if r.status_code == 429:
        print(f"Rate-limited: {r.headers}")
        # Sleep for long enough to get a few prerolled requests
        time.sleep(retry_after + request_preroll * request_interval)
        # Raise so we get retried
        r.raise_for_status()
    else:
        try:
            r.raise_for_status()
        except Exception:
            print(f"<<<< REQUEST\n{r.request.__dict__}")
            print(f">>>> RESPONSE\n{r.__dict__}")
            raise
    time.sleep(request_interval)
    return r


def _get_raw(url, params):
    return _get_with_http(url, params).json()


def location_raw(name, api_key):
    return _get_raw(
        "https://www.edsm.net/api-logs-v1/get-position",
        params={
            "commanderName": name,
            "apiKey": api_key,
        },
    )


def cargo_raw(name, api_key):
    return _get_raw(
        "https://www.edm.net/api-commander-v1/get-materials",
        params={
            "commanderName": name,
            "apiKey": api_key,
            "type": "cargo",
        },
    )


def systems_in_sphere_raw(current_system, radius=50, min_radius=0):
    """Get systems in a sphere of radius 50 of another system."""
    return _get_raw(
        "https://www.edsm.net/api-v1/sphere-systems",
        params={
            "systemName": current_system,
            "radius": radius,
            "minRadius": min_radius,
            "showInformation": 1,
            "showPrimaryStar": 1,
            "showCoordinates": 1,
        },
    )


def stations_in_system_raw(system):
    return _get_raw(
        "https://www.edsm.net/api-system-v1/stations",
        params={"systemName": system},
    )


def station_names_in_system_onlycache(system_name):
    return station_db.get(system_name, default=None)


def stations_in_system(system):
    result = stations_in_system_raw(system["name"])
    wanted_keys = [
        "name",
        "distanceToArrival",
        "updateTime",
        "type",
    ]
    station_db.set(system["name"], [s["name"] for s in result["stations"]])
    return [{k: s[k] for k in wanted_keys} for s in result["stations"]]


def market_in_station_raw(system_name, station_name):
    """Get the market data for a station in a system."""
    return _get_raw(
        "https://www.edsm.net/api-system-v1/stations/market",
        params={"systemName": system_name, "stationName": station_name},
    )


def market_in_station_onlycache(system_name, station_name):
    key = (system_name, station_name)
    return market_db.get(key, default=None)


def markets_in_system(system):
    disallowed_types = [
        "Odyssey Settlement",
        "Fleet Carrier",
    ]
    stations = [
        station
        for station in stations_in_system(system)
        if station["type"] not in disallowed_types
    ]

    def _market(station):
        key = (system["name"], station["name"])
        if not market_db.exists(key):
            raw = market_in_station_raw(system["name"], station["name"])
            result = {"market": raw, "station": station, "system": system}
            market_db.set(key, result)
        return market_db.get((system["name"], station["name"]))

    with ThreadPoolExecutor(max_workers=6) as exe:
        return list(exe.map(_market, stations))


def invalidate(system_name, station_name):
    market_db.invalidate((system_name, station_name))
    market_db.set(("dirty", system_name), True)


def request_near(location, radius=50):
    request_id = str(uuid.uuid1())
    systems = systems_in_sphere_raw(location, radius=radius)
    system_names = [system["name"] for system in systems]

    def _dirty(system):
        return market_db.get(("dirty", system["name"]), default=None)

    dirty = groupby(_dirty, systems)

    need_update = dirty.get(True, []) + dirty.get(None, [])

    need_update_names = sorted(
        need_update,
        key=lambda x: x["distance"],
    )

    task_systems = list(partition_all(10, need_update_names))

    tasks_ = [
        (i, populate_markets.delay(batch))
        for (i, batch) in enumerate(task_systems)
    ]

    request = {
        "request_id": request_id,
        "location": location,
        "radius": radius,
        "system_names": system_names,
        "tasks": {
            task.id: {
                "task_id": task.id,
                "shell": i,
                "system_names": [system["name"] for system in batch],
            }
            for ((i, task), batch) in zip(tasks_, task_systems)
        },
    }

    request_db.set(request_id, request)

    return request


def request_status(request_id):
    request = request_db.get(request_id)

    def _dirty(system_name):
        return market_db.get(("dirty", system_name), default=None)

    dirty = groupby(_dirty, request["system_names"])
    system_completion = {
        (
            "Partial" if k is True else "Complete" if k is False else "Pending"
        ): len(v)
        for (k, v) in dirty.items()
    }

    num_systems = len(request["system_names"])
    partial_or_pending = (
        system_completion.get("Partial", 0) +
        system_completion.get("Pending", 0)
    )

    system_completion_percent = 100 * (
        (num_systems - partial_or_pending) / num_systems
    )

    def _shell_status(item):
        return AsyncResult(item["task_id"]).state

    shell_completion = groupby(_shell_status, request["tasks"].values())

    return {
        "request_id": request["request_id"],
        "location": request["location"],
        "radius": request["radius"],
        "system_names": request["system_names"],
        "system_completion": system_completion,
        "system_completion_percent": system_completion_percent,
        "tasks": shell_completion,
        "unfinished_shells": dissoc(shell_completion, "SUCCESS"),
    }


@celery_worker.task
def populate_markets(systems):
    start = time.time()
    with ThreadPoolExecutor(max_workers=4) as exe:
        result = list(exe.map(markets_in_system, systems))
    end = time.time()
    duration = end - start
    return {
        "ok": True,
        "timing": {
            "start": start,
            "end": end,
            "elapsed": duration,
        },
        "result": result,
    }


def without_false(seq):
    return (s for s in seq if s)


def filter_sell_commodity(name, min_demand=1000, min_price=100000):
    def _filter_sell_commodity(market):
        commodities = get_in(
            ["market", "commodities"],
            market,
            default=None,
        )
        commodities = commodities or []
        return next(
            (
                c
                for c in commodities
                if c["name"].lower() == name.lower()
                and c["demand"] >= min_demand
                and c["sellPrice"] >= min_price
            ),
            None,
        )

    return _filter_sell_commodity


def multi_sell_filter(min_demand=1000, min_price=100000, commodities=None):
    commodities = commodities or []
    all_filters = [
        filter_sell_commodity(
            name,
            min_demand=min_demand,
            min_price=min_price,
        )
        for name in commodities
    ]

    def _multi_sell_filter(market):
        return list(without_false(cf(market) for cf in all_filters))

    return _multi_sell_filter


def filter_markets(markets, commodity_filter):
    results = []
    for mkt in markets:
        station = mkt["station"]
        system = mkt["system"]
        # or [] because this could return None instead
        # of omitting the commodities
        all_commodities = (
            get_in(["market", "commodities"], mkt, default=None) or []
        )
        relevant_commodities = commodity_filter(mkt)
        if relevant_commodities:
            results.append(
                {
                    "system": system,
                    "station": station,
                    "commodities": all_commodities,
                    "relevant": relevant_commodities,
                }
            )
    return results


def time_since(timestr):
    tm = datetime.datetime.strptime(timestr + " Z", "%Y-%m-%d %H:%M:%S %z")
    delta = datetime.datetime.now().astimezone() - tm
    return delta


def readable_time_since(delta):
    days = delta.days
    hours = delta.seconds // 3600
    minutes = (delta.seconds - 3600 * hours) // 60
    return f"{days}d {hours}h {minutes}m"


def digest_relevant_markets_near(relevant):
    return [
        {
            "system": r["system"]["name"],
            "jump_distance": r["system"]["distance"],
            "station": r["station"]["name"],
            "supercruise_distance": r["station"]["distanceToArrival"],
            "relevant": [
                {
                    "name": c["name"],
                    "sellPrice": c["sellPrice"],
                    "demand": c["demand"],
                }
                for c in r["relevant"]
            ],
            "updated": {
                "when": r["station"]["updateTime"]["market"],
                "elapsed": readable_time_since(
                    time_since(
                        r["station"]["updateTime"]["market"],
                    ),
                ),
                "seconds_ago": time_since(
                    r["station"]["updateTime"]["market"],
                ).total_seconds(),
            },
        }
        for r in relevant
    ]


def cascading_lookup(paths, data):
    NOT_FOUND = object()
    for path in paths:
        result = get_in(path, data, default=NOT_FOUND)
        if result is not NOT_FOUND:
            return result
    else:
        return None


def hypothetical_sale(commodities, market):
    mkt_commodities = cascading_lookup(
        [
            ["relevant"],
            ["station", "market", "commodities"],
            ["market", "commodities"],
            ["commodities"],
            [],
        ],
        market,
    )
    mkt = {c["name"]: c for c in mkt_commodities}
    matches = [
        {
            "name": name,
            "sellPrice": mkt[name]["sellPrice"],
            "revenue": mkt[name]["sellPrice"] * quantity,
        }
        for (name, quantity) in commodities.items()
        if name in mkt
    ]
    return {
        "total": sum(m["revenue"] for m in matches),
        "matched": matches,
        "missing": [
            name for (name, quantity) in commodities.items() if name not in mkt
        ],
    }


def best_sell_stations(
    system_names,
    cargo,
    min_price=1,
    min_demand=1,
    topk=20,
):
    sell_filter_args = {
        "min_price": min_price,
        "min_demand": min_demand,
    }
    sell_filter = multi_sell_filter(
        commodities=list(cargo.keys()), **sell_filter_args
    )
    markets = []
    for system_name in system_names:
        station_names_ = station_names_in_system_onlycache(system_name) or []
        for station_name in station_names_:
            if market_ := market_in_station_onlycache(
                system_name, station_name
            ):
                markets.append(market_)
    filtered = filter_markets(markets, commodity_filter=sell_filter)
    digested = digest_relevant_markets_near(filtered)
    sales = [
        {"sale": hypothetical_sale(cargo, n), "market": n} for n in digested
    ]
    sales_sorted = sorted(
        sales, key=lambda x: x["sale"]["total"], reverse=True
    )
    return sales_sorted[:topk]


class SellStationRequest(BaseModel):
    location: str
    radius: float = 40.0
    min_price: int = 100000
    min_demand: int = 1
    cargo: Dict[str, int]
    topk: int = 20


@app.get("/")
def _():
    return {"ok": True, "api_docs": "/docs"}


@app.post("/scan")
def _new(location: str, radius: float = 40.0):
    return request_near(location, radius=radius)


@app.get("/scan/{scan_id}")
def _check(scan_id: str):
    return request_status(scan_id)


@app.post("/sales")
def _sales(payload: SellStationRequest, scan_id: str = ""):
    if scan_id:
        req = request_status(scan_id)
    else:
        req1 = request_near(payload.location, radius=payload.radius)
        req = request_status(req1["request_id"])

    best = best_sell_stations(
        req["system_names"],
        payload.cargo,
        min_price=payload.min_price,
        min_demand=payload.min_demand,
        topk=payload.topk,
    )

    return {"scan": req, "best": best}
