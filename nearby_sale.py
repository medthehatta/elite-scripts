#!/usr/bin/env python

"""
Finds the most profitable nearby station to sell a cargo haul.
"""

import datetime
import json
from concurrent.futures import ThreadPoolExecutor
from pprint import pprint
from math import ceil
import itertools

from cytoolz import get_in

import edsm
import market


def without_false(seq):
    return (s for s in seq if s)


def markets_near(location, radius=30):
    systems = edsm.systems_in_sphere(location, radius)
    return list(
        without_false(
            edsm.markets_in_system(system["name"]) for system in systems
        )
    )


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
                c for c in commodities
                if c["name"].lower() == name.lower() and
                c["demand"] >= min_demand and
                c["sellPrice"] >= min_price
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


def log(x):
    pprint(x)
    return x


def filter_markets(markets, commodity_filter):
    results = []
    for market in markets:
        station = market["station"]
        all_commodities = get_in(["market", "commodities"], market, default=[])
        relevant_commodities = commodity_filter(market)
        results.extend(
            [
                {
                    "station": station,
                    "commodities": commodities,
                    "relevant": relevant,
                }
                for (commodities, relevant) in
                zip(all_commodities, relevant_commodities)
                if relevant
            ]
        )
    return log(results)


def filter_markets_bak(markets, commodity_filter):
    results = []
    for market in markets:
        stations = [
            market["station"] for market in markets
        ]
        all_commodities = [
            get_in(["market", "commodities"], market, default=[])
            for market in markets
        ]
        relevant_commodities = [
            commodity_filter(market) for market in markets
        ]
        results.extend(
            [
                {
                    "station": station,
                    "commodities": commodities,
                    "relevant": relevant,
                }
                for (station, commodities, relevant) in
                zip(stations, all_commodities, relevant_commodities)
                if relevant
            ]
        )
    return results


def time_since(timestr):
    tm = datetime.datetime.strptime(timestr + " Z", "%Y-%m-%d %H:%M:%S %z")
    delta = datetime.datetime.now().astimezone() - tm
    return delta


def readable_time_since(delta):
    days = delta.days
    hours = delta.seconds // 3600
    minutes = (delta.seconds - 3600*hours) // 60
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
            "revenue": mkt[name]["sellPrice"]*quantity,
        }
        for (name, quantity) in commodities.items()
        if name in mkt
    ]
    return {
        "total": sum(m["revenue"] for m in matches),
        "matched": matches,
        "missing": [
            name for (name, quantity) in commodities.items()
            if name not in mkt
        ],
    }


def best_sell_stations(cargo, system, sell_filter_args=None, radius=30):
    sell_filter_args = sell_filter_args or {
        "min_price": 200000,
        "min_demand": 100,
    }
    sell_filter = multi_sell_filter(
        commodities=list(cargo.keys()),
        **sell_filter_args
    )
    nearby = relevant_markets_near(system, sell_filter, radius=radius)
    digested = digest_relevant_markets_near(nearby)
    sales = [
        {"sale": hypothetical_sale(cargo, n), "market": n}
        for n in digested
    ]
    sales_sorted = sorted(
        sales,
        key=lambda x: x["sale"]["total"],
        reverse=True
    )
    return sales_sorted


def best_sell_stations_celery(cargo, location, sell_filter_args=None, radius=30):
    sell_filter_args = sell_filter_args or {
        "min_price": 200000,
        "min_demand": 100,
    }
    sell_filter = multi_sell_filter(
        commodities=list(cargo.keys()),
        **sell_filter_args
    )
    market_request = market.request_near(
        location,
        initial_radius=15,
        max_radius=radius,
    )
    markets = []
    for system in market_request["system_names"]:
        print(f"{system=}")
        stations_ = market.station_names_in_system_onlycache(system) or []
        print(f"{stations_=}")
        for station in stations_:
            print(f"{station=}")
            if market_ := market.market_in_station_onlycache(system, station):
                print(f"{market_=}")
                markets.append(market_)
    import pdb; pdb.set_trace()
    filtered = filter_markets(markets, commodity_filter=sell_filter)
    digested = digest_relevant_markets_near(filtered)
    sales = [
        {"sale": hypothetical_sale(cargo, n), "market": n}
        for n in digested
    ]
    sales_sorted = sorted(
        sales,
        key=lambda x: x["sale"]["total"],
        reverse=True
    )
    return sales_sorted


def pretty_print_sales(sales):
    lines = []
    for sale in sales:
        total = sale["sale"]["total"]
        station = sale["market"]["station"]
        system = sale["market"]["system"]
        updated = sale["market"]["updated"]["elapsed"]
        unsold = sale["sale"]["missing"]
        jump_dist = ceil(sale["market"]["jump_distance"])
        sc_dist = ceil(sale["market"]["supercruise_distance"])
        lines.append(
            f"{total:,} "
            f"from {station} ({sc_dist} Ls) "
            f"in {system} ({jump_dist} Ly) "
            f"({updated})"
        )
        if unsold:
            lines.append(f"    (missing: {', '.join(unsold)})")

    return lines


def main():
    """Entry point."""
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-P", "--pretty", action="store_true")
    parser.add_argument("-s", "--system", required=True)
    parser.add_argument("-r", "--radius", type=int, default=30)
    parser.add_argument("-d", "--demand", "--min-demand", type=int, default=100)
    parser.add_argument("-p", "--price", "--min-price", type=int, default=100000)
    parser.add_argument(
        "cargo",
        type=argparse.FileType("r"),
        default="-",
        nargs="?",
    )
    parsed = parser.parse_args()

    system = parsed.system
    radius = parsed.radius
    sell_filter_args = {
        "min_demand": parsed.demand,
        "min_price": parsed.price,
    }
    pretty = parsed.pretty

    cargo = {}
    for line in parsed.cargo:
        line = line.strip()
        if not line:
            continue
        (name, quantity) = line.rsplit(" ", 1)
        quantity = int(quantity)
        cargo[name] = quantity

    results = best_sell_stations(
        cargo,
        system,
        sell_filter_args=sell_filter_args,
        radius=radius,
    )

    if pretty:
        print("\n".join(pretty_print_sales(results)))
    else:
        print(json.dumps(results))


if __name__ == "__main__":
    main()
