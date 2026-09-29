# Implementation Plan — Fleet Ops Lambda Pipeline (EC8203)

Tracks the gap between the scaffolded skeleton and a submittable, working project.
Check items off as your group completes them. Assign owners where marked `[owner: ?]`.

---

## Status snapshot

| Component | Status |
|---|---|
| Kafka topics, producers (telemetry + daily cost) | Done |
| Speed layer windowed aggregation logic | Done |
| Idle / no-data alert logic | Done |
| Batch join logic (costs × trips → profitability) | Done |
| Postgres schema | Done |
| FastAPI serving endpoints | Done |
| Raw telemetry archive sink | **Missing — blocks batch layer** |
| Airflow → Spark connectivity | **Missing — DAG will fail as-is** |
| sim_day stamping | **Placeholder only** |
| Drop-file volume mount for Airflow | **Missing** |
| Dashboard / report artifact | **Not built (API only)** |
| Tests | **None** |
| Report (8–15 pages) | **Not started** |
| Demo video | **Not started** |

---

## Phase 1 — Close the plumbing gaps (Days 1–3) `[owner: ?]`

- [x] Add a `raw_telemetry` archive sink in `speed_layer.py` (second `foreachBatch` write, or a separate cheap Kafka→Postgres consumer) so every raw event is stored untransformed
- [x] Add shared `sim_day` tracking between `telemetry_producer.py` and `daily_cost_producer.py`; stamp `sim_day` onto every telemetry event and store it as a column on `raw_telemetry`
- [x] Replace the date-arithmetic placeholder filter in `batch_reconciliation.py` with `WHERE sim_day = :sim_day`
- [x] Fix Airflow → Spark connectivity — choose one:
  - [ ] Option A (simpler): install `pyspark` in the Airflow image, call the batch job as a Python function directly in the DAG
  - [x] Option B (more realistic): use `SparkSubmitOperator` from `apache-airflow-providers-apache-spark`, pointed at `spark://spark-master:7077`
- [x] Mount `./producers/drops` into the Airflow scheduler container in `docker-compose.yml` so the file sensor can see dropped cost files

## Phase 2 — Get it running end-to-end (Days 3–5) `[owner: ?]`

- [x] `docker compose up -d --build`, bring services up incrementally, check logs per service
- [x] Confirm producer messages land in Kafka (`kafka-console-consumer` sanity check)
- [x] Confirm `speed-layer` populates both `fleet_live_metrics` and `raw_telemetry`
- [x] Run one full sim-day cycle, confirm Airflow DAG populates `vehicle_profitability`
- [x] Hit all API endpoints, confirm real data returned
- [ ] Budget real debugging time — this phase is the highest risk

## Phase 3 — Consolidated report/dashboard deliverable (Days 5–7) `[owner: ?]`

- [ ] Build a minimal HTML page or scheduled script that calls the API and renders current utilization + latest profitability report
- [ ] OR: add an Airflow task that dumps a daily HTML/PDF snapshot after `run_batch_job`
- [ ] Confirm it visibly answers the business question (fleet utilization now + which vehicles are unprofitable)

## Phase 4 — Observability completeness (Day 7–8) `[owner: ?]`

- [ ] Confirm structured JSON logs appear per stage (ingestion / processing / storage) in `docker compose logs`
- [ ] Force-test the idle-threshold alert (let a simulated vehicle sit idle past `IDLE_ALERT_SECONDS`)
- [ ] Force-test the no-data health check (briefly stop a producer)
- [ ] Confirm both show up via `/live/alerts`

## Phase 5 — Tests + code quality (Day 8–9) `[owner: ?]`

- [ ] Unit test: profitability calculation logic (pure function, no Spark needed)
- [ ] Unit test: idle-duration alert threshold logic
- [ ] Unit test: API endpoints against a mocked DB
- [ ] Clean up placeholder/dead code
- [ ] Verify `docker compose up` works from a clean clone on a teammate's machine
- [ ] Finalize README run instructions

## Phase 6 — Report, 8–15 pages (Days 9–12) `[owner: ?]`

- [ ] Use case & business requirements
- [ ] Architecture decision: Lambda vs Kappa, with rejected-alternative argument
- [ ] Architecture diagram(s)
- [ ] Tech stack justification per layer (incl. Spark vs Storm for speed layer)
- [ ] Observability design
- [ ] Results: sample dashboard/report output with screenshots
- [ ] Limitations & production trade-offs (e.g. Flink vs Spark at scale, geo-indexing vs flat zones, data lake vs convenience table, consumer-lag monitoring)

## Phase 7 — Demo + submission (Days 12–14) `[owner: ?]`

- [ ] Record 5–10 min demo once system is stable (not last-minute)
- [ ] Write individual contributions statement
- [ ] Final checklist: repo link works from clean clone, report PDF attached, demo video linked, assumptions/simplifications clearly stated (sim-day compression, zone list, etc.)

---

## Assumptions & simplifications to state explicitly in the report

- One simulated day = `SIM_DAY_SECONDS` (default 300s / 5 min)
- Zones are a fixed hardcoded list, not real geo-indexing (H3/S2)
- Live metrics served directly from Postgres rather than a low-latency store (Redis, etc.) — acceptable at this data scale
- No raw-archive-specific sink separate from the speed layer (archiving is piggybacked on the streaming job for simplicity)
