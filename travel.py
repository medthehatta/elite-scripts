#!/usr/bin/env python


import json
import math
import requests
from itertools import permutations
from cytoolz import partition_all
from cytoolz import mapcat
from cytoolz import sliding_window


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


def systems_get(systems):
    """Get data for systems."""
    batches = partition_all(100, systems)
    combined = mapcat(_systems_get, batches)
    return list(combined)


def _systems_get(systems):
    """Get a batch of systems, up to 100 (per the API)."""
    system_dict = {"systemName[]": systems}
    r = requests.get(
        "https://www.edsm.net/api-v1/systems",
        params={"showCoordinates": 1, **system_dict},
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
