#!/usr/bin/env python


import datetime
import json
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from typing import Dict

import requests
from cytoolz import curry
from cytoolz import dissoc
from cytoolz import get_in
from cytoolz import groupby
from cytoolz import partition_all
from fastapi import Body
from fastapi import FastAPI
from fastapi import Form
from fastapi import HTTPException
from fastapi import Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from retrying import retry

import db


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


def time_since(timestr):
    try:
        tm = datetime.datetime.strptime(timestr, "%Y-%m-%dT%H:%M:%S.%f%z")
    except ValueError:
        tm = datetime.datetime.strptime(timestr, "%Y-%m-%dT%H:%M:%S%z")
    delta = datetime.datetime.now().astimezone() - tm
    return delta


def readable_time_since(delta):
    days = delta.days
    hours = delta.seconds // 3600
    minutes = (delta.seconds - 3600 * hours) // 60
    return f"{days}d {hours}h {minutes}m"


def hypothetical_sale(commodities, market):
    market_commodities = market.get("commodities", [])
    by_name = {c["name"]: c for c in market_commodities}
    matches = [
        {
            "name": name,
            "sellPrice": by_name[name]["sellPrice"],
            "revenue": by_name[name]["sellPrice"] * quantity,
        }
        for (name, quantity) in commodities.items()
        if name in by_name
    ]
    return {
        "total": sum(m["revenue"] for m in matches),
        "matched": matches,
        "missing": [
            name for (name, quantity) in commodities.items()
            if name not in by_name
        ],
    }


def log(x):
    print(x)
    return x


def filter_markets(
    markets,
    desired_commodities,
    min_price=1,
    min_demand=1,
    max_update_seconds=24*3600,
):
    result = []
    if not markets:
        return []

    desired = [
        db.commodity.find_one({"readable": k})["name"]
        for k in desired_commodities
    ]

    for market in markets:
        commodities1 = market.get("commodities", [])

        commodities = [c for c in commodities1 if c["name"] in desired]

        price_demand_ok = any(
            (
                c["name"] in desired and
                c["sellPrice"] >= min_price and
                c["demand"] >= min_demand
            )
            for c in commodities
        )
        if not price_demand_ok:
            continue

        market_update = market.get("update_time")
        if market_update is None:
            continue
        delta = time_since(market_update)
        update_ok = delta.total_seconds() < max_update_seconds
        if not update_ok:
            continue

        # otherwise
        result.append(market)

    return result


def best_sell_stations(
    system,
    cargo,
    radius=30,
    min_price=1,
    min_demand=1,
    max_update_seconds=24*3600,
    topk=20,
):
    commodities = list(cargo.keys())

    markets = []
    systems = systems_in_sphere_raw(system, radius=radius)
    for system in systems:
        markets.extend(list(db.market.find({"system": system["name"]})))

    filtered = filter_markets(
        markets,
        commodities,
        min_price=min_price,
        min_demand=min_demand,
        max_update_seconds=max_update_seconds,
    )
    sales = [hypothetical_sale(cargo, n) for n in filtered]
    pre_sort = [
        {
            "sale": sale,
            "market": db.station.find_one({
                "system": mkt["system"],
                "station": mkt["station"],
            }),
            "updated": mkt["update_time"],
        }
        for (sale, mkt) in zip(sales, filtered)
    ]
    sales_sorted = sorted(
        pre_sort, key=lambda x: x["sale"]["total"], reverse=True
    )
    return sales_sorted[:topk]


class SellStationRequest(BaseModel):
    system: str
    radius: float = 30.0
    min_price: int = 100000
    min_demand: int = 1
    not_planetary: bool = False
    large_station: bool = False
    max_update_seconds: int = -1
    cargo: Dict[str, int]
    topk: int = 20


#@app.get("/")
#def _():
#    """API index, just has a welcome message."""
#    return {"ok": True, "api_docs": "/docs"}
