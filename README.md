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

- **Speed layer**: consumes the `telemetry` Kafka topic continuously with Spark Structured Streaming, computes rolling per-zone utilization, idle ratios, and trips/hour, and writes to `fleet_live_metrics` in Postgres.
- **Alert Watcher**: A dedicated service (`idle_alert_watcher.py`) that monitors the Kafka stream in real time. It emits a `vehicle_idle_threshold` alert if a vehicle sits idle for too long, and a `no_data` alert if the pipeline receives zero telemetry for 30 seconds. Alerts are written to `pipeline_alerts` in Postgres.
- **Batch layer**: an Airflow DAG runs once per simulated day. It reads the day's completed trips (aggregated from raw telemetry stored to `raw_telemetry`) and joins them against the day's cost file (from the `daily-cost` topic / file drop) to produce a per-vehicle profitability report in `vehicle_profitability`.
- **Serving layer**: FastAPI exposes a beautiful HTML dashboard at `/` as well as the underlying JSON endpoints (`/live/utilization`, `/live/alerts`, and `/reports/profitability/{date}`).

## Simulated clock
One simulated "day" = 5 minutes of wall-clock time (configurable via `SIM_DAY_SECONDS` in `.env`). The batch producer drops one cost file every `SIM_DAY_SECONDS`.

## Run it

```bash
cp .env.example .env
docker compose up -d --build
```

This brings up Zookeeper, Kafka, Spark (master + 1 worker), Postgres, Airflow (webserver + scheduler), the Producers, the Alert Watcher, and the FastAPI dashboard.

- **Dashboard**: http://localhost:8000 (Beautiful dark mode UI auto-refreshing live data)
- Airflow UI: http://localhost:8080 (admin/admin) — enable the `daily_profitability_reconciliation` DAG.
- API Docs: http://localhost:8000/docs
- Note: The simulated producers (`telemetry_producer.py` and `daily_cost_producer.py`) auto-start as a combined service in `docker-compose.yml`.

## Repo layout
```
producers/     - telemetry_producer.py, daily_cost_producer.py (simulated sources)
processing/    - speed_layer.py (Spark Streaming), batch_reconciliation.py (Spark batch), idle_alert_watcher.py (Alerts)
airflow/dags/  - daily_profitability_reconciliation.py
serving/       - api.py (FastAPI), index.html (Dashboard UI)
common/        - business_logic.py (Pure functions), logging_config.py (Structured logging)
storage/init/  - Postgres DDL
tests/         - Pytest suite
```

## Observability
- Structured JSON logs (via `common/logging_config.py`) at ingestion, processing, and storage stages — grep-able / shippable to any log aggregator.
- Health-check rule: `alert-watcher` triggers a `no_data` alert if no telemetry event is processed within 10s of wall-clock time.
- Threshold alert: `alert-watcher` triggers a `vehicle_idle_threshold` alert if a vehicle is idle beyond `IDLE_ALERT_SECONDS`.


