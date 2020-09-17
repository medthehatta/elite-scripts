#!/usr/bin/env python


import json
import math
import pickle
import requests
import os
from itertools import permutations
from cytoolz import partition_all
from cytoolz import mapcat
from cytoolz import sliding_window


def cachefile_for(system):
    """Return the presumed cache file location for the `system`."""
    cache_prefix = "/tmp/elite-cache"
    os.makedirs(cache_prefix)
    return os.path.join(cache_prefix, system.lower()[:2])


def retrieve_from_cache(system, data_key=None):
    """Retrieve system data from cache."""
    try:
        with open(cachefile_for(system), "rb") as f:
            data = pickle.load(f)
            if data_key:
                return data[system][data_key]
            else:
                return data[system]
    except OSError:
        raise LookupError(system)


def write_to_cache(system, data_key, data):
    """Write system data to the cache."""
    shard = cachefile_for(system)
    with open(shard, "rb") as f:
        existing = pickle.load(f)
        updated = {**existing, system: {data_key: data}}
    with open(shard, "wb") as f:
        pickle.dump(updated, f)
    return updated


def greedy_path(positions, initial):
    """Find a reasonably short path through the dict of positions."""
    unfound = set(positions.keys())

    itin = [initial]
    unfound.remove(initial)

    while unfound:
        next_loc = min(
            unfound,
            key=lambda x: distance(positions[itin[-1]], positions[x]),
        )
        unfound.remove(next_loc)
        itin.append(next_loc)

    return itin


def exact_path(positions, initial):
    """SLOWLY find the optimal path through a dict of positions."""
    unfound = list(positions.keys())
    unfound.remove(initial)
    possible_itineraries = (
        [initial] + list(rest)
        for rest in permutations(unfound)
    )
    return min(
        possible_itineraries,
        key=lambda it: total_dist(itinerary_dists(positions, it)),
    )


def itinerary_dists(positions, itinerary):
    """Return list of pairs, ['dest', 'dist']."""
    distances = [
        distance(positions[b], positions[a])
        for (a, b) in sliding_window(2, itinerary)
    ]
    labeled = list(zip(itinerary[1:], distances))
    return labeled


def total_dist(itinerary):
    """The total distance covered by an itinerary from `itinerary_dists`."""
    return sum(dist for (_, dist) in itinerary)


def batched(num):
    """Run a function over its list argument in batches of size `num`."""

    def _batched(func):

        def _wrapped(total):
            batches = partition_all(num, total)
            combined = mapcat(func, batches)
            return list(combined)

        return _wrapped
    return _batched


@batched(100)
def systems_get(systems):
    """Get a batch of systems, up to 100 (per the API)."""
    system_dict = {"systemName[]": systems}
    r = requests.get(
        "https://www.edsm.net/api-v1/systems",
        params={"showCoordinates": 1, **system_dict},
    )
    r.raise_for_status()
    return r.json()


def systems_in_sphere(current_system, radius=50):
    """Get systems in a sphere of radius 100."""
    r = requests.get(
        "https://www.edsm.net/api-v1/sphere-systems",
        params={"systemName": current_system, "radius": radius},
    )
    r.raise_for_status()
    return r.json()


def stations_in_system(system):
    """Get the stations in a given system."""
    r = requests.get(
        "https://www.edsm.net/api-system-v1/stations",
        params={"systemName": system},
    )
    r.raise_for_status()
    return r.json()


def distance(pt2, pt1):
    """Compute the distance between coord dicts with x,y,z keys."""
    displ = [pt2[i] - pt1[i] for i in ["x", "y", "z"]]
    return math.sqrt(sum(p**2 for p in displ))


def main():
    """Entry point."""
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--exact", action="store_true")
    parser.add_argument("base")
    parser.add_argument(
        "others",
        type=argparse.FileType("r"),
        nargs="?",
        default="-",
    )
    parsed = parser.parse_args()

    exact = parsed.exact
    base = parsed.base
    others = [line.strip() for line in parsed.others]

    system_data = systems_get([base] + others)
    systems = {
        sys["name"]: sys["coords"]
        for sys in system_data
    }

    if exact:
        itinerary = exact_path(systems, initial=base)
    else:
        itinerary = greedy_path(systems, initial=base)

    labeled = itinerary_dists(systems, itinerary)

    result = {
        "total": total_dist(labeled),
        "exact": exact,
        "itinerary": labeled,
    }

    print(json.dumps(result))


if __name__ == "__main__":
    main()
