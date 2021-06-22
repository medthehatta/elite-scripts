import numpy as np
import edsm
import eddb_tables as et
from importlib import reload
from cytoolz import get
from cytoolz import get_in
from cytoolz import groupby
from cytoolz import mapcat
from itertools import chain
from concurrent.futures import ThreadPoolExecutor
import json
from pprint import pprint
import datetime
import market as mkt
import nearby_sale


def without_false(seq):
    return (s for s in seq if s)


def coords(system_info):
    return np.array(get(['x', 'y', 'z'], system_info['coords']))


def centroid(systems):
    coords_ = [coords(s) for s in systems]
    return sum(coords_) / len(coords_)


def ray(start, point_along):
    displace = point_along - start
    norm = displace / np.sqrt(point_along.dot(point_along))
    def _ray(t):
        return start + norm * t
    return _ray


def systems_along_ray_dist(start, point_along, dist):
    r = ray(start, point_along)
    bubbles = [tuple(r(x)) for x in np.arange(0, dist, 50)]
    with ThreadPoolExecutor(max_workers=6) as exe:
        results = list(exe.map(edsm.systems_in_radius_of, bubbles))
    rd = {s['name']: s for s in sum(results, [])}
    return list(rd.values())


def bodies_for(systems):
    names = [s['name'] for s in systems]
    with ThreadPoolExecutor(max_workers=6) as exe:
        results = list(exe.map(edsm.bodies_in_system, names))
    return results


def best_sell_price_in_system(commodity, system):
    try:
        return max(
            (x["sellPrice"], x["station"])
            for x in edsm.commodity_from_system(system, commodity)
        )
    except ValueError:
        return (-1, "")


def sell_near_system(location, radius=30):
    locations = json.load(open("mining_locations.json"))
    nearby_systems = edsm.systems_in_sphere(location, radius)
    commodities = next(loc["items"] for loc in locations if loc["name"].lower() == location.lower())
    for commodity in commodities:
        for system in nearby_systems:
            (price, station) = best_sell_price_in_system(commodity, system["name"])
            if price != -1:
                yield (price, commodity, station, system)


def markets_near(location, radius=30):
    systems = edsm.systems_in_sphere(location, radius)
    found = (
        edsm.markets_in_system(system["name"]) for system in systems
    )
    return [f for f in found if f]


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


def relevant_markets_near(
    location,
    commodity_filter=None,
    radius=30,
):
    results = []
    if commodity_filter is None:
        return markets_near(location, radius)
    else:
        systems = edsm.systems_in_sphere(location, radius)
        system_names = [system["name"] for system in systems]
        with ThreadPoolExecutor(max_workers=6) as exe:
            all_markets = list(exe.map(edsm.markets_in_system, system_names))
        for (system, markets) in zip(systems, all_markets):
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
                        "system": system,
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
            "station": r["station"]["name"],
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


ml = json.load(open("mining_locations.json"))

mining_commodities = [
    "Alexandrite",
    "Musgravite",
    "Monazite",
    "Rhodplumsite",
    "Serendibite",
]


def mining_sell_filter(**kwargs):
    return multi_sell_filter(commodities=mining_commodities, **kwargs)


test_haul = {"Alexandrite": 12, "Serendibite": 15, "Monazite": 8}


def get_deep_market(entry):
    lookups = [
        ["market"]*n
        for n in reversed(range(0, 11))
    ]
    return cascading_lookup(lookups, entry)


def do_sale():
    try:
        result = nearby_sale.best_sell_stations_celery({"Alexandrite": 12}, "Ebor", sell_filter_args={"min_price": 100000, "min_demand": 100}, radius=30) #kk
    except Exception:
        result = None
    d = groupby(lambda x: x[0], nearby_sale.bad)
    return (result, d)
