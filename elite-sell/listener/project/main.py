#!/usr/bin/env python


import datetime
from typing import Dict

from fastapi import FastAPI
from pydantic import BaseModel
from cytoolz import topk as get_topk

import db
from edsm import systems_in_sphere_raw


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
                "quantity": quantity,
                "revenue": by_name[name]["sellPrice"] * quantity,
            })

    return {
        "total": sum(m["revenue"] for m in matches),
        "matched": matches,
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

    # Made it through the gauntlet of conditions
    return market


def _translate_commodity(readable):
    entry = db.commodity.find_one({"readable": readable})
    if entry:
        return entry["name"]
    else:
        raise LookupError(readable)


@db.edsm_cache.memoize()
def systems_in_sphere(location, radius=30):
    return systems_in_sphere_raw(location, radius=radius)


def best_sell_stations(
    system,
    cargo,
    radius=30,
    min_price=1,
    min_demand=1,
    max_update_seconds=24*3600,
    topk=20,
):
    cargo = {
        _translate_commodity(k): v
        for (k, v) in cargo.items()
    }
    commodities = list(cargo.keys())

    markets = []
    systems = systems_in_sphere(system, radius=radius)
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
    sales_sorted = get_topk(
        topk,
        pre_sort,
        key=lambda x: x["sale"]["total"],
    )
    return sales_sorted


class SellStationRequest(BaseModel):
    system: str
    radius: float = 30.0
    min_price: int = 100000
    min_demand: int = 1
    max_update_seconds: int = 1e8
    cargo: Dict[str, int]
    topk: int = 20


@app.get("/")
def _():
    """API index, just has a welcome message."""
    return {"ok": True, "api_docs": "/docs"}


@app.get("/sales")
def _sales(request: SellStationRequest):
    return best_sell_stations(
        request.system,
        request.cargo,
        radius=request.radius,
        min_price=request.min_price,
        min_demand=request.min_demand,
        max_update_seconds=request.max_update_seconds,
        topk=request.topk,
    )
