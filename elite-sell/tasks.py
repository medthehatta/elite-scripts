import time
from concurrent.futures import ThreadPoolExecutor


from worker import celery_worker
from nearby_sale import best_sell_stations
import market


@celery_worker.task
def create_task(duration):
    start = time.time()
    time.sleep(duration)
    end = time.time()
    return {"slept": duration, "start": start, "end": end}


@celery_worker.task
def best_sell_stations_task(cargo, system, min_price, min_demand, radius):
    start = time.time()
    result = best_sell_stations(
        cargo,
        system,
        sell_filter_args={"min_price": min_price, "min_demand": min_demand},
        radius=radius,
    )
    end = time.time()
    duration = end - start
    return {
        "ok": True,
        "timing": {
            "start": start,
            "end": end,
            "elapsed": duration,
        },
        "result": result,
    }


@celery_worker.task
def populate_markets(systems):
    start = time.time()
    with ThreadPoolExecutor(max_workers=4) as exe:
        result = list(exe.map(market.markets_in_system, systems))
    end = time.time()
    duration = end - start
    return {
        "ok": True,
        "timing": {
            "start": start,
            "end": end,
            "elapsed": duration,
        },
        "result": result,
    }
