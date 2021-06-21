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


def systems_in_sphere(current_system, radius=50, min_radius=0):
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


def queue_markets_near(system, max_radius=50, initial_radius=15):
    request_id = str(uuid.uuid1())
    shells = itertools.takewhile(
        lambda x: x[1] < max_radius,
        equal_volume_shells(initial_radius),
    )
    task_systems = []
    tasks_ = []
    for (r0, r1) in shells:
        batch = systems_in_sphere(system, min_radius=r0, radius=r1)
        names = [s["name"] for s in batch]
        task_systems.append(names)
        tasks_.append(tasks.populate_markets.delay(names))

    return {
        "request_id": request_id,
        "system": system,
        "max_radius": max_radius,
        "initial_radius": initial_radius,
        "tasks": [
            {"task_id": task.id, "systems": names_batched}
            for (task, names_batched) in zip(tasks_, task_systems)
        ],
    }


def stations_in_system_raw(system):
    return _get_raw(
        "https://www.edsm.net/api-system-v1/stations",
        params={"systemName": system},
    )


def stations_in_system(system):
    result = stations_in_system_raw(system)
    wanted_keys = [
        "name",
        "distanceToArrival",
        "updateTime",
        "type",
    ]
    return [{k: s[k] for k in wanted_keys} for s in result["stations"]]


def market_in_station_raw(system, station):
    """Get the market data for a station in a system."""
    return _get_raw(
        "https://www.edsm.net/api-system-v1/stations/market",
        params={"systemName": system, "stationName": station},
    )


def market_in_station(system, station):
    key = (system, station)
    if not market_db.exists(key):
        market_db.set(key, market_in_station_raw(system, station))
    return market_db.get(key)


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
        return {
            "market": market_in_station(system, station["name"]),
            "station": station,
        }

    with ThreadPoolExecutor(max_workers=6) as exe:
        return list(exe.map(_market, stations))


def populate_system_markets(system):
    result = [
        market_db.set((system, m["station"]["name"]), m)
        for m in markets_in_system(system)
    ]
    market_db.set(("dirty", system), False)
    return result


def invalidate(system, station):
    market_db.invalidate((system, station))
    market_db.set(("dirty", system), True)


def get_system_markets(system):
    return [
        market_db.peek((system, m["station"]["name"]))
        for m in markets_in_system(system)
    ]


def relevant_markets_near(location, commodity_filter, initial_radius=15, max_radius=50):
    request_id = str(uuid.uuid1())
    systems = systems_in_sphere(location, radius=max_radius)
    system_names = [system["name"] for system in systems]

    shells = list(
        itertools.takewhile(
            lambda x: x[1] < max_radius,
            equal_volume_shells(initial_radius),
        )
    )

    def _which_bin(system):
        return next(
            i
            for (i, (r0, r1))
            in reversed(list(enumerate(shells)))
            if system["distance"] < r1
        )

    bins = groupby(_which_bin, systems)

    def _dirty(system):
        return market_db.get(("dirty", system), default=None)

    dirty = groupby(_dirty, system_names)

    task_systems = [
        [
            s["name"] for s in bins[i]
            if s["name"] not in dirty.get(False, [])
        ]
        for i in range(len(shells))
        if i in bins
    ]

    tasks_ = [
        (i, tasks.populate_markets.delay(names))
        for (i, names) in enumerate(task_systems)
    ]

    request = {
        "request_id": request_id,
        "location": location,
        "max_radius": max_radius,
        "initial_radius": initial_radius,
        "systems": system_names,
        "tasks": {
            task.id: {"task_id": task.id, "shell": i, "systems": names_batched}
            for ((i, task), names_batched) in zip(tasks_, task_systems)
        },
    }

    request_db.set(request_id, request)

    return request


def request_status(request_id):
    request = request_db.get(request_id)
    
    def _dirty(system):
        return market_db.get(("dirty", system), default=None)

    dirty = groupby(_dirty, request["systems"])
    system_completion = {
        (
            "Partial" if k is True else
            "Complete" if k is False else "Pending"
        ): len(v)
        for (k, v) in dirty.items()
    }

    def _shell_status(item):
        return AsyncResult(item["task_id"]).state

    shell_completion = groupby(_shell_status, request["tasks"].values())

    return {
        "request_id": request["request_id"],
        "location": request["location"],
        "max_radius": request["max_radius"],
        "initial_radius": request["initial_radius"],
        "systems": request["systems"],
        "system_completion": system_completion,
        "tasks": shell_completion,
    }
