#!/usr/bin/env python


import datetime
import itertools
import json
import math
import os
import pickle
from concurrent.futures import ThreadPoolExecutor
from pprint import pprint
import threading
import time

import requests
from cytoolz import mapcat
from cytoolz import merge
from cytoolz import partial
from cytoolz import partition_all
from cytoolz import sliding_window
from diskcache import Cache
from retrying import retry


cache = Cache("edsm-cache")


def batched(num):
    """Run a function over its list argument in batches of size `num`."""

    def _batched(func):
        def _wrapped(total):
            batches = partition_all(num, total)
            combined = mapcat(func, batches)
            return list(combined)

        return _wrapped

    return _batched


@retry(stop_max_attempt_number=3)
def _get_raw(url, params):
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
    # Sleep for 80% of the amortized rate limit
    time.sleep(0.8 * request_interval)
    return r.json()


_get = cache.memoize()(_get_raw)


@batched(100)
def systems_get(systems):
    """Get a batch of systems, up to 100 (per the API)."""
    return _get(
        "https://www.edsm.net/api-v1/systems",
        params={
            "systemName[]": systems,
            "showInformation": 1,
            "showPrimaryStar": 1,
            "showCoordinates": 1,
        },
    )


def systems_in_sphere(current_system, radius=50):
    """Get systems in a sphere of radius 50 of another system."""
    return _get(
        "https://www.edsm.net/api-v1/sphere-systems",
        params={
            "systemName": current_system,
            "radius": radius,
            "showInformation": 1,
            "showPrimaryStar": 1,
            "showCoordinates": 1,
        },
    )


def systems_in_cube_at(coords, edge=20):
    """Get systems in a cube of the given edge length at coords."""
    (x, y, z) = coords
    return _get(
        "https://www.edsm.net/api-v1/cube-systems",
        params={
            "x": x,
            "y": y,
            "z": z,
            "size": edge,
            "showInformation": 1,
            "showPrimaryStar": 1,
            "showCoordinates": 1,
        },
    )


def systems_in_radius_of(coords, radius=50):
    """Get systems in a sphere of radius 50 of a coordinate tuple."""
    return _get(
        "https://www.edsm.net/api-v1/sphere-systems",
        params={
            "x": coords[0],
            "y": coords[1],
            "z": coords[2],
            "radius": radius,
            "showInformation": 1,
            "showPrimaryStar": 1,
            "showCoordinates": 1,
        },
    )


def log(x):
    pprint(x)
    return x


def stations_in_system(system):
    """Get the stations in a given system."""
    stations = _get(
        "https://www.edsm.net/api-system-v1/stations",
        params={"systemName": system},
    )
    return [
        station
        for station in stations.get("stations", [])
        if station["type"] != "Fleet Carrier"
    ]


def bodies_in_system(system):
    """Get bodies in a given system."""
    return _get(
        "https://www.edsm.net/api-system-v1/bodies",
        params={"systemName": system},
    )


@cache.memoize(expire=1000)
def traffic_in_system(system):
    """Get traffic in a given system."""
    return _get_raw(
        "https://www.edsm.net/api-system-v1/traffic",
        params={"systemName": system},
    )


@cache.memoize(expire=24*3600)
def market_in_station(system, station):
    """Get the market data for a station in a system."""
    return _get_raw(
        "https://www.edsm.net/api-system-v1/stations/market",
        params={"systemName": system, "stationName": station},
    )


def markets_in_system(system):
    """Get all the market data for a system."""
    disallowed_types = [
        "Odyssey Settlement",
        "Fleet Carrier",
    ]
    return [
        {
            "market": market_in_station(system, station["name"]),
            "station": station,
        }
        for station in stations_in_system(system)
        if station["type"] not in disallowed_types
    ]


def commodity_from_system(system, commodity):
    stations = stations_in_system(system)
    markets = (
        {
            "system": system,
            "station": station,
            "market": market_in_station(system, station["name"]),
        }
        for station in stations
    )
    return itertools.chain.from_iterable(
        [
            merge(
                com,
                {
                    "timestamp": datetime.datetime.now().isoformat(),
                    "system": market["system"],
                    "station": market["station"]["name"],
                    "stationType": market["station"]["type"],
                },
            )
            for com in market["market"].get("commodities", []) or []
            if commodity.lower() in (com["name"].lower(), com["id"].lower())
        ]
        for market in markets
    )


def commodity_in_sphere(center_system, commodity, radius=10):
    systems = systems_in_sphere(center_system, radius)
    system_names = [system["name"] for system in systems]
    this_commodity_from_system = partial(commodity_from_system, commodity=commodity)
    with ThreadPoolExecutor(max_workers=16) as exe:
        commodities_batched = exe.map(this_commodity_from_system, system_names)
        return list(itertools.chain.from_iterable(commodities_batched))


def system_coords(systems):
    """Return a dict of system name to coords."""
    system_data = systems_get(systems)
    return {sys["name"]: sys["coords"] for sys in system_data}


def distance(pt2, pt1):
    """Compute the distance between coord dicts with x,y,z keys."""
    displ = [pt2[i] - pt1[i] for i in ["x", "y", "z"]]
    return math.sqrt(sum(p ** 2 for p in displ))
