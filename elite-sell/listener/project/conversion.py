import datetime
from cytoolz import get_in


def from_eddn(entry):
    def g(path):
        return get_in(path, entry, default=None)

    market_id = g(["message", "marketId"])
    system = g(["message", "systemName"])
    station = g(["message", "stationName"])
    update_time_raw = g(["header", "gatewayTimestamp"])
    # EDDN uses ISO format, which is what we'll use at rest.  We don't get
    # microsecond precision out of EDSM though, so we will truncate the EDDN
    # microseconds.
    update_time_dt = datetime.datetime.strptime(
        # EDDN entries already have the Z for UTC, but strptime only knows how
        # to make timezone-aware datetimes if you use the +00:00 designation
        # and capture with %z
        update_time_raw + " +00:00",
        "%Y-%m-%dT%H:%M:%S.%fZ %z",
    )
    update_time = update_time_dt.isoformat()
    commodities = [
        {
            "name": com["name"],
            "sellPrice": com["sellPrice"],
            "buyPrice": com["buyPrice"],
            "stock": com["stock"],
            "demand": com["demand"],
        }
        for com in g(["message", "commodities"])
    ]

    return {
        "system": system,
        "station": station,
        "market": market_id,
        "update_time": update_time,
        "commodities": commodities,
    }


def from_edsm(entry):
    def g(path):
        return get_in(path, entry, default=None)

    market_id = g(["marketId"])
    system = g(["systemName"])
    station = g(["name"])
    update_time_raw = g(["updateTime", "market"])
    type_ = g(["type"]) or "Unknown"
    sc_dist = g(["distanceToArrival"]) or 0
    commodities_raw = g(["commodities"]) or []

    commodities = [
        {
            "name": com["id"],
            "sellPrice": com["sellPrice"],
            "buyPrice": com["buyPrice"],
            "stock": com["stock"],
            "demand": com["demand"],
            "readable": com["name"],
        }
        for com in commodities_raw
    ]

    if update_time_raw is None:
        return {
            "system": system,
            "station": station,
            "market": market_id,
            "update_time": None,
            "type": type_,
            "sc_distance": sc_dist,
            "commodities": [],
        }

    update_time_dt = datetime.datetime.strptime(
        # EDSM entries are in the UTC timezone; I have to fake the timezone
        # being in the string so datetime produces a timezone-aware object
        # instead of a naive one
        update_time_raw + " +00:00",
        "%Y-%m-%d %H:%M:%S %z",
    )
    update_time = update_time_dt.isoformat()

    return {
        "system": system,
        "station": station,
        "market": market_id,
        "update_time": update_time,
        "type": type_,
        "sc_distance": sc_dist,
        "commodities": commodities,
    }
