import time

import requests
from retrying import retry


@retry(stop_max_attempt_number=3)
def _get_with_http(url, params):
    print(f"GET {url} ({params})...")
    r = requests.get(url, params=params)
    rate_limit = int(r.headers.get("X-Rate-Limit-Limit", 720))
    rate_reset = int(r.headers.get("X-Rate-Limit-Reset", 0))
    retry_after = int(r.headers.get("Retry-After", 0))
    request_interval = rate_reset / rate_limit
    request_preroll = 50
    if r.status_code == 429:
        print(f"Rate-limited: {r.headers}")
        # Sleep for long enough to get a few prerolled requests
        time.sleep(retry_after + request_preroll * request_interval)
        # Raise so we get retried
        r.raise_for_status()
    else:
        try:
            r.raise_for_status()
        except Exception:
            print(f"<<<< REQUEST\n{r.request.__dict__}")
            print(f">>>> RESPONSE\n{r.__dict__}")
            raise
    time.sleep(request_interval)
    return r


def _get_raw(url, params):
    return _get_with_http(url, params).json()


def location_raw(name, api_key):
    return _get_raw(
        "https://www.edsm.net/api-logs-v1/get-position",
        params={
            "commanderName": name,
            "apiKey": api_key,
        },
    )


def cargo_raw(name, api_key):
    return _get_raw(
        "https://www.edsm.net/api-commander-v1/get-materials",
        params={
            "commanderName": name,
            "apiKey": api_key,
            "type": "cargo",
        },
    )


def systems_in_sphere_raw(current_system, radius=50, min_radius=0):
    """Get systems in a sphere of radius 50 of another system."""
    return _get_raw(
        "https://www.edsm.net/api-v1/sphere-systems",
        params={
            "systemName": current_system,
            "radius": radius,
            "minRadius": min_radius,
            "showInformation": 1,
            "showPrimaryStar": 1,
            "showCoordinates": 1,
        },
    )


def stations_in_system_raw(system):
    return _get_raw(
        "https://www.edsm.net/api-system-v1/stations",
        params={"systemName": system},
    )
