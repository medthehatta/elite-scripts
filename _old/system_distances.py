#!/usr/bin/env python


import json
import math
import requests
from cytoolz import partition_all
from cytoolz import mapcat


def system_coords(systems):
    """Return a dict of system name to coords."""
    system_data = systems_get(systems)
    return {
        sys["name"]: sys["coords"]
        for sys in system_data
    }


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
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("base")
    parser.add_argument(
        "others",
        type=argparse.FileType("r"),
        nargs="?",
        default="-",
    )
    parsed = parser.parse_args()

    base = parsed.base
    others = [line.strip() for line in parsed.others]

    system_data = systems_get([base] + others)
    systems = {
        sys["name"]: sys["coords"]
        for sys in system_data
    }

    base_coords = systems[base]

    system_distances = {
        name: distance(coords, base_coords)
        for (name, coords) in systems.items()
        if name != base
    }

    distances = {
        "base": base,
        "distances": system_distances,
        "closest": min(
            (dist, name) for (name, dist) in system_distances.items()
        ),
        "sorted": sorted(
            (dist, name) for (name, dist) in system_distances.items()
        ),
    }

    print(json.dumps(distances))


if __name__ == "__main__":
    main()
