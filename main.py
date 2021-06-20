import time

from celery.result import AsyncResult
from fastapi import Body, FastAPI, Form, Request
from pydantic import BaseModel
from typing import Dict

import tasks


app = FastAPI()


class SellStationRequest(BaseModel):
    system: str
    min_price: int = 100000
    min_demand: int = 1
    radius: int = 40
    cargo: Dict[str, int]


@app.post("/nearby_sell_stations", status_code=201)
def _(request: SellStationRequest):
    task = tasks.best_sell_stations_task.delay(
        request.cargo,
        request.system,
        request.min_price,
        request.min_demand,
        request.radius,
    )
    return {"task_id": task.id}


@app.get("/nearby_sell_stations/{task_id}")
def _(task_id):
    task_result = AsyncResult(task_id)
    return {
        "task_id": task_id,
        "task_status": task_result.status,
        "task_result": task_result.result
    }


@app.post("/tasks", status_code=201)
def run_task(payload = Body(...)):
    task_type = payload["type"]
    task = tasks.create_task.delay(int(task_type))
    return {"task_id": task.id}


@app.get("/tasks/{task_id}")
def get_status(task_id):
    task_result = AsyncResult(task_id)
    result = {
        "task_id": task_id,
        "task_status": task_result.status,
        "task_result": task_result.result
    }
    return result
