import time

from celery.result import AsyncResult
from fastapi import Body, FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import Dict

import tasks
from nearby_sale import pretty_print_sales


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


def as_html_table(sell_results):
    header = [
        "Revenue",
        "Station",
        "SC Dist (Ls)",
        "System",
        "Jump Dist (Ly)",
        "Updated",
        "Missing?",
    ]

    def row(sale):
        return [
            "{:,}".format(sale["sale"]["total"]),
            sale["market"]["station"],
            "{:.02f}".format(sale["market"]["supercruise_distance"]),
            sale["market"]["system"],
            "{:.02f}".format(sale["market"]["jump_distance"]),
            sale["market"]["updated"]["elapsed"],
            ", ".join(sale["sale"]["missing"]),
        ]

    header_html = "<tr>" + "".join(f"<th>{x}</th>" for x in header) + "</tr>"
    row_html = "".join(
        ("<tr>" + "".join(f"<td>{x}</td>" for x in row(sale)) + "</tr>")
        for sale in sell_results
    )

    top = """
<html>
<head>
<style>
table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
    padding: 5px;
}
</style>
</head>
<body>
<table>
"""

    bot = """
</body>
</html>
"""

    return "".join([
        top,
        header_html,
        row_html,
    ])


@app.get("/nearby_sell_stations/{task_id}")
def _(task_id, pretty: bool = False):
    task_result = AsyncResult(task_id)
    if task_result.successful():
        if pretty:
            return HTMLResponse(as_html_table(task_result.result["result"]))
    return {
        "task_id": task_id,
        "task_status": task_result.status,
        "task_result": task_result.result
    }
