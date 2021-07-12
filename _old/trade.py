#!/usr/bin/env python


import json
import itertools
import sys

from travel import system_coords
from travel import distance
from travel import stations_in_system


aspx = dict(
    name="aspx",
    seconds_per_jump=50,
    ly_per_jump_laden=15,
    ly_per_jump_unladen=25,
    escape_mass_lock_station_seconds=90,
    avg_sc_speed_vs_c=13,
    docking_seconds=90,
    tons=70,
    min_profit_per_hour=60e6,
    large_pad_only=False,
)


type9 = dict(
    name="type9",
    seconds_per_jump=50,
    ly_per_jump_laden=14,
    ly_per_jump_unladen=25,
    escape_mass_lock_station_seconds=100,
    avg_sc_speed_vs_c=13,
    docking_seconds=100,
    tons=750,
    min_profit_per_hour=60e6,
    large_pad_only=True,
)


def _loop_sort_key_data(loop):
    def _lsk(config):
        seconds_per_jump = config["seconds_per_jump"]
        ly_per_jump_laden = config["ly_per_jump_laden"]
        ly_per_jump_unladen = config["ly_per_jump_unladen"]
        escape_mass_lock_station_seconds = config["escape_mass_lock_station_seconds"]
        avg_sc_speed_vs_c = config["avg_sc_speed_vs_c"]
        docking_seconds = config["docking_seconds"]
        tons = config["tons"]
        min_profit_per_hour = config["min_profit_per_hour"]
        large_pad_only = config["large_pad_only"]

        jump_seconds_out = loop["dist"] / ly_per_jump_unladen * seconds_per_jump
        jump_seconds_back = loop["dist"] / ly_per_jump_laden* seconds_per_jump
        to_buy_seconds = loop["buy"]["dist"] / avg_sc_speed_vs_c
        to_sell_seconds = loop["sell"]["dist"] / avg_sc_speed_vs_c

        transit_seconds = sum([
            escape_mass_lock_station_seconds,
            jump_seconds_out,
            to_buy_seconds,
            docking_seconds,
            escape_mass_lock_station_seconds,
            jump_seconds_back,
            to_sell_seconds,
            docking_seconds,
        ])

        profit_per_ton = loop["sell"]["price"] - loop["buy"]["price"]
        profit = tons * profit_per_ton

        min_profit_per_second = min_profit_per_hour / 3600

        penalties = 0
        if (
            large_pad_only and 
            (loop["buy"]["pad"], loop["sell"]["pad"]) != ("L", "L")
        ):
            penalties += 500e6

        return {
            "route_value": profit - min_profit_per_second * transit_seconds - penalties,
            "profit_mil": profit / 1e6,
            "profit_per_ton": profit_per_ton,
            "profit_per_hour_mil": profit / (transit_seconds / 3600) / 1e6,
            "timings": {
                "transit_minutes": transit_seconds / 60,
                "unladen_jump_minutes": jump_seconds_out / 60,
                "laden_jump_minutes": jump_seconds_back / 60,
                "to_buy_minutes": to_buy_seconds / 60,
                "to_sell_minutes": to_sell_seconds / 60,
            },
            "ship_parameters": config,
            "penalties": penalties,
        }
    return _lsk


def _loop_sort_key(config):
    def _lsk(loop):
        result = _loop_sort_key_data(loop)(config)
        return result["route_value"]
    return _lsk


def sort_loops(loops, sort_config):
    annotated = (
        {**loop, **_loop_sort_key_data(loop)(sort_config)}
        for loop in loops
    )
    return sorted(
        annotated,
        key=lambda loop: loop["route_value"],
        reverse=True,
    )


def loops(buy_data, sell_data):
    pairs = itertools.product(buy_data, sell_data)
    print("Getting coordinates...", file=sys.stderr)
    systems = system_coords(
        [m["system"] for m in buy_data] +
        [s["system"] for s in sell_data]
    )
    for (buy, sell) in pairs:
        if buy["system"] not in systems:
            print(f"ERROR: Unable to find system {buy['system']}, skipping.")
            continue
        if sell["system"] not in systems:
            print(f"ERROR: Unable to find system {sell['System']}, skipping.")
            continue
        sell_price = sell["sellPrice"]
        buy_price = buy["buyPrice"]
        supply = buy["stock"]
        dist = distance(
            systems[buy["system"]],
            systems[sell["system"]],
        )
        sell_station = next(
            station for station in
            stations_in_system(sell["system"])["stations"]
            if station["name"] == sell["station"]
        )
        buy_station = next(
            station for station in
            stations_in_system(buy["system"])["stations"]
            if station["name"] == buy["station"]
        )
        sell_station_dist = sell_station["distanceToArrival"]
        buy_station_dist = buy_station["distanceToArrival"]
        buy_pad = buy.get("Pad") or buy.get("stationType")
        result = {
            "buy": {
                "system": buy["system"],
                "station": buy["station"],
                "dist": buy_station_dist,
                "price": buy_price,
                "pad": pad,
                "supply": supply,
                "time": buy["time"],
            },
            "sell": {
                "system": sell["system"],
                "station": sell["station"],
                "dist": sell_station_dist,
                "price": sell_price,
                "pad": pad,
            },
            "dist": dist,
            "profit_per_ton": sell_price - buy_price,
        }
        yield result
