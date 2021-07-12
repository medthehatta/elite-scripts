#!/usr/bin/env python


import datetime
import itertools
from typing import Dict
from typing import List

from fastapi import FastAPI
from pydantic import BaseModel
from cytoolz import topk as get_topk
from cytoolz import partition_all

import db
from edsm import systems_in_sphere_raw
from edsm import location_raw
from edsm import cargo_raw


app = FastAPI()


def without_none(seq):
    return (x for x in seq if x is not None)


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


def hypothetical_sale(cargo, market):
    market_commodities = market.get("commodities", [])
    by_name = {c["name"]: c for c in market_commodities}

    matches = []
    for (name, quantity) in cargo.items():
        if name in by_name:
            matches.append({
                "name": name,
                "sellPrice": by_name[name]["sellPrice"],
                "demand": by_name[name]["demand"],
                "quantity": quantity,
                "revenue": by_name[name]["sellPrice"] * quantity,
            })

    return {
        "total": sum(m["revenue"] for m in matches),
        "matched": sorted(matches, key=lambda x: x["sellPrice"], reverse=True),
        "missing": [
            name for (name, quantity) in cargo.items()
            if name not in by_name
        ],
    }


def log(x, desc=None):
    if desc:
        print(f"{desc} {x}")
    else:
        print(x)
    return x


def filter_markets(markets, desired, **kwargs):
    if not markets:
        return []
    else:
        def _filter_market(market):
            return filter_market(market, desired, **kwargs)

        return list(without_none(_filter_market(m) for m in markets))


def filter_market(
    market,
    desired,
    min_price=1,
    min_demand=1,
    max_update_seconds=24*3600,
    disallowed_types=None,
):
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
        return None

    market_update = market.get("update_time")
    if market_update is None:
        return None
    delta = time_since(market_update)
    update_ok = delta.total_seconds() < max_update_seconds
    if not update_ok:
        return None

    if disallowed_types:
        station_data = db.strip_id(
            db.station.find_one(
                {
                    "system": market["system"],
                    "station": market["station"],
                },
            )
        )
        if station_data is None:
            return None

        if station_data["type"] in disallowed_types:
            return None

    # Made it through the gauntlet of conditions
    return market


def _translate_commodity(readable):
    entry = db.commodity.find_one({"readable": readable})
    if entry:
        return entry["name"]
    else:
        return readable


@db.edsm_cache.memoize()
def systems_in_sphere(location, radius=30):
    return systems_in_sphere_raw(location, radius=radius)


def _format_market(systems, market):
    market_data = db.strip_id(
        db.market.find_one(
            {
                "system": market["system"],
                "station": market["station"],
            },
        )
    )
    if market_data is None:
        return {
            "system": market["system"],
            "station": market["station"],
            "source": "NO DATA",
        }

    station_data = db.strip_id(
        db.station.find_one(
            {
                "system": market["system"],
                "station": market["station"],
            },
        )
    )
    if station_data is None:
        type_ = "NO DATA"
        sc_distance = "NO DATA"
    else:
        type_ = station_data["type"]
        sc_distance = station_data["sc_distance"]

    system = systems[market["system"]]
    try:
        return {
            "system": system["name"],
            "jump_distance": system["distance"],
            "station": market_data["station"],
            "sc_distance": sc_distance,
            "type": type_,
            "updated": readable_time_since(
                time_since(market_data["update_time"])
            ),
            "source": market_data["source"],
        }
    except KeyError:
        return {
            "system": system["name"],
            "station": market_data["station"],
            "source": "ERROR READING DB ENTRY",
        }


def best_sell_stations(
    system,
    cargo,
    radius=30,
    min_price=1,
    min_demand=1,
    max_update_seconds=24*3600,
    topk=20,
    disallowed_types=None,
):
    cargo = {
        _translate_commodity(k): v
        for (k, v) in cargo.items()
    }
    commodities = list(cargo.keys())

    systems = systems_in_sphere(system, radius=radius)
    system_data = {system["name"]: system for system in systems}

    batch_size = 100
    markets = itertools.chain.from_iterable(
        db.market.find({"system": {"$in": system_batch}})
        for system_batch in partition_all(batch_size, systems)
    )

    filtered = filter_markets(
        markets,
        commodities,
        min_price=min_price,
        min_demand=min_demand,
        max_update_seconds=max_update_seconds,
        disallowed_types=disallowed_types,
    )
    sales = [hypothetical_sale(cargo, n) for n in filtered]
    pre_sort = [
        {
            "sale": sale,
            "market": _format_market(system_data, mkt),
        }
        for (sale, mkt) in zip(sales, filtered)
    ]
    sales_sorted = get_topk(
        topk,
        pre_sort,
        key=lambda x: x["sale"]["total"],
    )
    return sales_sorted


def commander_info(name, api_key):
    location = location_raw(name, api_key)
    cargo_ = cargo_raw(name, api_key)
    system = location["system"]
    cargo = {c["name"]: c["qty"] for c in cargo_["cargo"] if c["qty"]}
    return {
        "system": system,
        "cargo": cargo,
    }


class SellStationRequest(BaseModel):
    commander: str = None
    api_key: str = None
    system: str = None
    radius: float = 30.0
    min_price: int = 100000
    min_demand: int = 1
    max_update_seconds: int = 1e8
    omit_station_types: List[str] = ["Fleet Carrier", "Odyssey Settlement"]
    cargo: Dict[str, int] = None
    topk: int = 20


@app.get("/")
def _():
    """API index, just has a welcome message."""
    return {"ok": True, "api_docs": "/docs"}


@app.post("/sales")
def _sales(request: SellStationRequest):
    if request.system and request.cargo:
        system = request.system
        cargo = request.cargo
    else:
        data = commander_info(request.commander, request.api_key)
        system = request.system or data["system"]
        cargo = request.cargo or data["cargo"]

    best = best_sell_stations(
        system,
        cargo,
        radius=request.radius,
        min_price=request.min_price,
        min_demand=request.min_demand,
        max_update_seconds=request.max_update_seconds,
        topk=request.topk,
        disallowed_types=request.omit_station_types,
    )
    return best
