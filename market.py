#!/usr/bin/env python


import datetime
import itertools
import json
import math
import os
import pickle
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from pprint import pprint

import redis
import requests
from cytoolz import groupby
from cytoolz import mapcat
from cytoolz import merge
from cytoolz import partial
from cytoolz import partition_all
from cytoolz import sliding_window
from cytoolz import dissoc
from diskcache import Cache
from retrying import retry
from celery.result import AsyncResult

import tasks

UNSET = object()


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
            return {"ok": False, "error": f"unpopulated", "key": key}

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
    rate_remain = int(r.headers.get("X-Rate-Limit-Remaining", 720))
    rate_reset = int(r.headers.get("X-Rate-Limit-Reset", 0))
    retry_after = int(r.headers.get("Retry-After", 0))
    request_interval = rate_reset/rate_limit
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


def _location_raw(name, api_key):
    return _get_raw(
        "https://www.edsm.net/api-logs-v1/get-position",
        params={
            "commanderName": name,
            "apiKey": api_key,
        },
    )


def _cargo_raw(name, api_key):
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


def equal_volume_shells(r):
    yield (0, r)
    r1 = r
    r0 = 0
    while True:
        r2 = (2 * r1**3 - r0**3)**(1/3)
        yield (r1, r2)
        r0 = r1
        r1 = r2


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


def log(x):
    pprint(x)
    return x


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


def _which_bin(system, shells):
    return next(
        i
        for (i, (r0, r1))
        in enumerate(shells)
        if r0 <= system["distance"] <= r1
    )


def request_near(location, initial_radius=15, max_radius=50):
    request_id = str(uuid.uuid1())
    systems = systems_in_sphere_raw(location, radius=max_radius)
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
        (i, tasks.populate_markets.delay(batch))
        for (i, batch) in enumerate(task_systems)
    ]

    request = {
        "request_id": request_id,
        "location": location,
        "max_radius": max_radius,
        "initial_radius": initial_radius,
        "systems": systems,
        "system_names": system_names,
        "tasks": {
            task.id: {"task_id": task.id, "shell": i, "systems": systems_batched}
            for ((i, task), systems_batched) in zip(tasks_, task_systems)
        },
    }

    request_db.set(request_id, request)

    return request


def request_status(request_id):
    request = request_db.get(request_id)
    
    def _dirty(system):
        return market_db.get(("dirty", system["name"]), default=None)

    dirty = groupby(_dirty, request["systems"])
    system_completion = {
        (
            "Partial" if k is True else
            "Complete" if k is False else "Pending"
        ): len(v)
        for (k, v) in dirty.items()
    }

    def _compl(status):
        return system_completion.get(status, 0)

    system_completion_percent = 100 * (
        _compl("Complete") / (_compl("Complete") + _compl("Partial") + _compl("Pending"))
    )

    def _shell_status(item):
        return AsyncResult(item["task_id"]).state

    shell_completion = groupby(_shell_status, request["tasks"].values())

    return {
        "request_id": request["request_id"],
        "location": request["location"],
        "max_radius": request["max_radius"],
        "initial_radius": request["initial_radius"],
        "systems": request["systems"],
        "system_names": request["system_names"],
        "system_completion": system_completion,
        "system_completion_percent": system_completion_percent,
        "tasks": shell_completion,
        "unfinished_shells": dissoc(shell_completion, "SUCCESS"),
    }
