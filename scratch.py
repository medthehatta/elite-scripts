import numpy as np
import edsm
import eddb_tables as et
from importlib import reload
from cytoolz import get
from concurrent.futures import ThreadPoolExecutor
import json


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


ml = json.load(open("mining_locations.json"))
