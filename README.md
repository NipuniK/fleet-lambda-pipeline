# Fleet Operations Lambda Pipeline (EC8203 Mini-Project)

Use Case 1 — Ride-Hailing Fleet Operations, implemented as a **Lambda architecture**.

## Architecture

```
Telemetry producer ─┐
                     ├─▶ Kafka (topics: telemetry, daily-cost) ─┬─▶ Spark Structured Streaming (speed layer) ─┐
Daily-cost producer ─┘                                          └─▶ Spark batch job, run by Airflow (batch layer) ─┤
                                                                                                                    ▼
                                                                                                        Postgres serving store
                                                                                                                    │
                                                                                                                    ▼
                                                                                                        FastAPI serving/API layer
```

- **Speed layer**: consumes the `telemetry` Kafka topic continuously with Spark Structured Streaming, computes rolling per-zone utilization, idle ratios, and trips/hour, and writes to `fleet_live_metrics` in Postgres. Also emits a threshold alert when a vehicle is idle beyond `IDLE_ALERT_SECONDS`.
- **Batch layer**: an Airflow DAG runs once per simulated day. It reads the day's completed trips (aggregated from raw telemetry stored to `raw_telemetry`) and joins them against the day's cost file (from the `daily-cost` topic / file drop) to produce a per-vehicle profitability report in `vehicle_profitability`.
- **Serving layer**: FastAPI reads both tables and exposes `/live/utilization`, `/live/alerts`, and `/reports/profitability/{date}`.

## Simulated clock
One simulated "day" = 5 minutes of wall-clock time (configurable via `SIM_DAY_SECONDS` in `.env`). The batch producer drops one cost file every `SIM_DAY_SECONDS`.

## Run it

```bash
cp .env.example .env
docker compose up -d --build
```

This brings up Zookeeper, Kafka, Spark (master + 1 worker), Postgres, Airflow (webserver + scheduler), and the FastAPI serving layer.

- Airflow UI: http://localhost:8080 (admin/admin) — enable the `daily_profitability_reconciliation` DAG.
- API: http://localhost:8000/docs
- To start the simulators: `docker compose exec producers python telemetry_producer.py` and `docker compose exec producers python daily_cost_producer.py` (or they auto-start as services — see `docker-compose.yml`).

## Repo layout
```
producers/     - telemetry_producer.py, daily_cost_producer.py (simulated sources)
processing/    - speed_layer.py (Spark Structured Streaming), batch_reconciliation.py (Spark batch)
airflow/dags/  - daily_profitability_reconciliation.py
serving/       - api.py (FastAPI)
common/        - schemas.py, logging_config.py (shared structured logging + alert rule)
storage/init/  - Postgres DDL
```

## Observability
- Structured JSON logs (via `common/logging_config.py`) at ingestion, processing, and storage stages — grep-able / shippable to any log aggregator.
- Health-check rule: speed layer logs a `health_check` alert if no telemetry event is processed within `NO_DATA_ALERT_SECONDS` (default 30s of simulated stream silence).
- Threshold alert: vehicle idle beyond `IDLE_ALERT_SECONDS` (default 600s) triggers a structured `alert` log line the API surfaces under `/live/alerts`.

## Team contributions
_Fill in before submission — who owns ingestion, processing, serving, observability, report._
