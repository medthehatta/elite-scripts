#!/usr/bin/env python


import json
import itertools
import sys

from travel import system_coords
from travel import distance
from travel import stations_in_system


def vo2file(path):
    with open(path) as f:
        return [json.loads(line) for line in f]


def sellstations(path):
    with open(path) as f:
        return sell_stations_from_data(json.load(f))


def sell_stations_from_data(sell_stations):
    sell_header = sell_stations[0]
    return [
        dict(zip(sell_header, entry))
        for entry in sell_stations[1:]
    ]


aspx = dict(
    seconds_per_jump=50,
    ly_per_jump_laden=25,
    ly_per_jump_unladen=30,
    escape_mass_lock_station_seconds=90,
    escape_mass_lock_ring_seconds=90,
    avg_sc_speed_vs_c=13,
    docking_seconds=90,
    tons=70,
    min_profit_per_hour=60e6,
    seconds_per_ton=60,
)


def _loop_sort_key(config):
    seconds_per_jump = config["seconds_per_jump"]
    ly_per_jump_laden = config["ly_per_jump_laden"]
    ly_per_jump_unladen = config["ly_per_jump_unladen"]
    escape_mass_lock_station_seconds = config["escape_mass_lock_station_seconds"]
    escape_mass_lock_ring_seconds = config["escape_mass_lock_ring_seconds"]
    avg_sc_speed_vs_c = config["avg_sc_speed_vs_c"]
    docking_seconds = config["docking_seconds"]
    tons = config["tons"]
    min_profit_per_hour = config["min_profit_per_hour"]
    seconds_per_ton = config["seconds_per_ton"]

    def _lsk(loop):
        jump_seconds_out = loop["dist"] / ly_per_jump_unladen * seconds_per_jump
        jump_seconds_back = loop["dist"] / ly_per_jump_laden* seconds_per_jump
        to_ring_seconds = loop["mine"]["dist"] / avg_sc_speed_vs_c
        to_sell_seconds = loop["sell"]["dist"] / avg_sc_speed_vs_c

        transit_seconds = sum([
            escape_mass_lock_station_seconds,
            jump_seconds_out,
            to_ring_seconds,
            escape_mass_lock_ring_seconds,
            jump_seconds_back,
            to_sell_seconds,
            docking_seconds,
        ])

        profit_per_ton = loop["sell"]["price"]
        profit = tons * profit_per_ton

        collect_seconds = tons * seconds_per_ton

        min_profit_per_second = min_profit_per_hour / 3600

        return profit - min_profit_per_second * (transit_seconds + collect_seconds)
    return _lsk


def sort_loops(loops, sort_config):
    return sorted(loops, key=_loop_sort_key(sort_config), reverse=True)


def mining_loops(mine_data, sell_data):
    pairs = itertools.product(mine_data, sell_data)
    print("Getting coordinates...", file=sys.stderr)
    systems = system_coords(
        [m["System"] for m in mine_data] +
        [s["System"] for s in sell_data]
    )
    for (mine, sell) in pairs:
        if mine["System"] not in systems:
            print(f"ERROR: Unable to find system {mine['System']}, skipping.")
            continue
        if sell["System"] not in systems:
            print(f"ERROR: Unable to find system {sell['System']}, skipping.")
            continue
        notes = (
            f"{mine['Extras']}; {mine['Confirmation']}; "
            f"{mine['Notes']}; {mine['Notes2']}; {mine['Notes3']}"
        ).replace(" ;","").strip().rstrip(";")
        price = int(sell["Sell"].replace(",",""))
        dist = distance(
            systems[mine["System"]],
            systems[sell["System"]],
        )
        stations = stations_in_system(sell["System"])["stations"]
        station_dist = next(
            station["distanceToArrival"]
            for station in stations
        )
        mine_dist = int(mine["Dist ls"].replace(",",""))
        result = {
            "mine": {
                "system": mine["System"],
                "ring": mine["Ring"],
                "notes": notes,
                "dist": mine_dist,
            },
            "sell": {
                "system": sell["System"],
                "station": sell["Station"],
                "dist": station_dist,
                "price": price,
            },
            "dist": dist,
        }
        yield result
