"""Serving layer: merges speed-layer and batch-layer outputs at query time.

Endpoints:
  GET /live/utilization       - latest per-zone utilization metrics (speed layer)
  GET /live/alerts            - recent alerts (idle threshold + no-data health check)
  GET /reports/profitability/{sim_day} - per-vehicle profitability for a given sim day (batch layer)
  GET /health                 - basic liveness check
"""
import os

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "common"))
sys.path.append(os.path.join(os.path.dirname(__file__), "common"))
from logging_config import get_logger  # noqa: E402

log = get_logger("serving.api")

DB_DSN = os.environ.get(
    "PIPELINE_DB_DSN", "dbname=fleet user=fleet password=fleet host=postgres"
)

app = FastAPI(title="Fleet Ops Serving API")


def get_conn():
    return psycopg2.connect(DB_DSN, cursor_factory=psycopg2.extras.RealDictCursor)


@app.get("/", response_class=HTMLResponse)
def serve_dashboard():
    dashboard_path = os.path.join(os.path.dirname(__file__), "index.html")
    with open(dashboard_path, "r") as f:
        return f.read()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/live/utilization")
def live_utilization(zone: str | None = None, limit: int = 50):
    query = """
        SELECT window_start, window_end, zone, active_vehicles, idle_ratio,
               trips_in_window, total_fare
        FROM fleet_live_metrics
        {where}
        ORDER BY window_start DESC
        LIMIT %s
    """
    where = "WHERE zone = %s" if zone else ""
    params = [zone, limit] if zone else [limit]
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(query.format(where=where), params)
        rows = cur.fetchall()
    log.info("live_utilization queried", extra={"fields": {"rows": len(rows), "zone": zone}})
    return rows


@app.get("/live/alerts")
def live_alerts(limit: int = 50):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT rule, vehicle_id, detail, created_at FROM pipeline_alerts "
            "ORDER BY created_at DESC LIMIT %s",
            (limit,),
        )
        rows = cur.fetchall()
    return rows


@app.get("/reports/profitability/{sim_day}")
def profitability_report(sim_day: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT vehicle_id, total_fare, fuel_cost, maintenance_cost, "
            "total_cost, net_profit, unprofitable FROM vehicle_profitability "
            "WHERE sim_day = %s ORDER BY net_profit ASC",
            (sim_day,),
        )
        rows = cur.fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail=f"No profitability report for sim_day={sim_day}")
    return rows
