

Real-Time Fleet Operations Data Pipeline

A Lambda Architecture Implementation for Ride-Hailing Fleet Utilization and Cost Reconciliation

EC8203 — Applied Big Data Engineering — Mini Project Report

Group Project — Use Case 1: Ride-Hailing Fleet Operations

[Team member names here]

[Date]



1. Use Case and Business Requirements

We implement Use Case 1: Ride-Hailing Fleet Operations. A ride-hailing operator needs live visibility into fleet activity and utilization, while reconciling that activity daily against per-vehicle running costs submitted by fuel and maintenance providers.

1.1 Business question

What is fleet utilization and earnings by area/time-of-day right now, and which vehicles are becoming unprofitable once yesterday's fuel/maintenance costs are factored in?

1.2 Interpreted requirements

Continuous ingestion of per-trip GPS/telemetry events (trip_id, driver_id, vehicle_id, position, speed, status, fare) at a few-second cadence.

Daily ingestion of a vehicle expense file (fuel_cost, maintenance_cost, distance_covered, service_flag) per vehicle.

A live view answering “how is the fleet doing right now” — active vehicle counts, idle ratio, and earnings, broken down by zone.

A daily reconciliation view answering “which vehicles are losing money” — joining a day's trip earnings against that day's costs.

Observability sufficient to detect pipeline failure (no data arriving) and to flag an operationally significant condition (a vehicle idle for an extended period).

These two questions — “right now” and “as of yesterday, corrected” — have different latency and correctness requirements, which directly motivates the architecture decision in Section 2.

2. Architecture Decision: Lambda vs Kappa

2.1 Chosen architecture: Lambda

We adopt a Lambda architecture. Telemetry events flow through a Kafka → Spark Structured Streaming speed layer producing near-real-time fleet utilization and idle-time alerts, while the daily cost file is ingested and joined against the day's stored trip aggregates in a Spark batch job orchestrated by Airflow, producing the per-vehicle profitability report. The two layers serve distinct queries with different latency and correctness requirements, and merge only at the serving layer, avoiding forcing an exact daily financial computation through a low-latency streaming path.

2.2 Justification

Four factors drove this decision:

Different temporal semantics of the two sources. The telemetry stream is continuous and unbounded; the cost file is a bounded, complete, once-daily delivery with its own completeness guarantee. Kappa's core assumption — that batch can be modelled as replay over the same unbounded log — is a weaker fit for a source that is not a stream in disguise.

Different correctness/latency requirements per query. Live utilization needs fast, approximate, continuously-updated answers (a speed-layer problem). Per-vehicle profitability must be exact once costs are known, computed once per day, and auditable (a batch-layer problem). Forcing both through one Kappa pipeline means either slowing the real-time path for exactness, or accepting approximate profitability figures, which undermines the report's purpose.

Reprocessing pattern. “Reprocessing” here is not “replay telemetry with corrected logic” but “join a day's completed telemetry aggregates against a newly-arrived cost file” — a batch join over two bounded datasets, not a log-replay problem.

Minimal duplicated logic. Lambda's classic criticism — maintaining two codebases — is muted here because the two layers compute genuinely different things (live metrics vs. daily reconciliation), not the same metric twice.

2.3 Rejected alternative: Kappa

Steelman: treat the daily cost file as a single event on a Kafka topic and reprocess the unified log through one Structured Streaming job with windowed joins, avoiding a separate batch job and Airflow DAG.

Why it was rejected:

Windowed stream-stream joins with a 24-hour-late, batch-shaped side input are awkward in Structured Streaming — state retention and watermarking are tuned for genuine streaming joins, not a once-a-day trickle.

A financial reconciliation report benefits from the clean audit/replay story a discrete batch run gives: “recompute vehicle profitability for day N” is a simpler correctness argument than “wait for state inside a long-running streaming job to be correct.”

Airflow is an explicit requirement of the module's technology stack; a genuine batch job gives it real orchestration purpose rather than being bolted on.

3. Architecture Diagram

Figure 1 shows the end-to-end data flow: two simulated sources publish to Kafka; the speed layer, batch layer, and alert watcher consume independently; both converge on a shared Postgres serving store; and a FastAPI layer serves both the live and reconciled views, complete with a dark-mode HTML dashboard at the root endpoint.

Figure 1. Lambda architecture for the fleet operations pipeline.

Ingestion, processing, storage, and serving layers are each independently deployable containers (see `docker-compose.yml` in the codebase), which lets the speed layer, batch layer, alert watcher, and API scale or fail independently.

4. Technology Stack and Justification

Table 1 summarises the technology chosen for each layer and the reasoning tied to this use case's constraints, rather than general popularity.

Table 1. Technology stack by layer.

4.1 Spark Structured Streaming vs. Apache Storm for the speed layer

Spark was chosen over Storm for the speed layer specifically because:

It unifies the codebase across both Lambda layers — the batch layer already requires Spark for DataFrame joins and JDBC writes, so using it for the speed layer as well means one engine, one API, and one team skillset to maintain.

Our latency requirement (utilization refreshed on a 15-second-to-1-minute cadence) does not need Storm's sub-second, event-at-a-time guarantees; Structured Streaming's micro-batch model is a natural fit for windowed aggregation.

Windowed aggregation (utilization by time window and zone) is native to Structured Streaming via groupBy + window, whereas Storm would require Trident or hand-rolled state management for equivalent functionality.

Checkpointed recovery (checkpointLocation) gives near-exactly-once semantics with a couple of configuration lines, versus Storm's more manual ack/fail tuple tracking.

Storm would be the better choice for a workload with a hard sub-second per-event SLA (e.g. single-transaction fraud decisions); a fleet-utilization dashboard with 15–60 seconds of acceptable staleness does not have that requirement.

5. Observability Design

5.1 What is measured

Structured (JSON) logs at every pipeline stage — ingestion (both producers), processing (speed layer, batch job, alert watcher), and storage (every write) — each line tagged with a `stage` field so logs are filterable by layer.

A threshold alert: a vehicle idle beyond IDLE_ALERT_SECONDS (default 600s) is flagged, tracked per-vehicle by a lightweight Kafka side-consumer (idle_alert_watcher.py) rather than inside the Spark streaming job itself, since per-event side effects do not fit Structured Streaming's batch-oriented execution model cleanly.

A health-check alert: no telemetry event observed within NO_DATA_ALERT_SECONDS (default 30s) is flagged by a NoDataWatchdog helper, catching a stalled producer or broken Kafka connection before it silently starves the speed layer.

Both alert types are persisted to a pipeline_alerts table (rule, vehicle_id, detail, timestamp) and exposed via GET /live/alerts, in addition to the structured log line, giving both a queryable history and a real-time log trail.

5.2 Why this design

Structured JSON logging (rather than free-text logs) was chosen because it is directly greppable and forwardable to any log aggregator without reformatting — a realistic minimum for a system that would, in production, ship logs to a centralised platform. The two alert rules were chosen because they map to the two failure modes most relevant to this use case: an operationally significant fleet condition (a stuck vehicle, worth a dispatcher's attention) and a pipeline-health condition (missing data, worth an engineer's attention) — covering both the “is the business healthy” and “is the system healthy” observability concerns the assignment specifies.

6. Results

The pipeline exposes its consolidated output through two API endpoints, reflecting the speed and batch layers respectively. The samples below illustrate the response shape and are provided as representative output; replace with actual screenshots captured from a running instance before final submission.



Figure 2. Illustrative sample responses from /live/utilization and /reports/profitability/{sim_day}.

### Live Utilization (/live/utilization)
```json
[
  {
    "window_start": "2026-09-29T20:59:00",
    "window_end": "2026-09-29T21:00:00",
    "zone": "downtown",
    "active_vehicles": 4,
    "idle_ratio": 0.56,
    "trips_in_window": 19,
    "total_fare": 97.08000000000001
  },
  {
    "window_start": "2026-09-29T20:59:00",
    "window_end": "2026-09-29T21:00:00",
    "zone": "airport",
    "active_vehicles": 3,
    "idle_ratio": 0.55,
    "trips_in_window": 11,
    "total_fare": 55.09
  },
  {
    "window_start": "2026-09-29T20:59:00",
    "window_end": "2026-09-29T21:00:00",
    "zone": "suburbs_south",
    "active_vehicles": 3,
    "idle_ratio": 0.7666666666666667,
    "trips_in_window": 5,
    "total_fare": 22.68
  },
  {
    "window_start": "2026-09-29T20:59:00",
    "window_end": "2026-09-29T21:00:00",
    "zone": "suburbs_north",
    "active_vehicles": 7,
    "idle_ratio": 0.5857142857142857,
    "trips_in_window": 22,
    "total_fare": 96.63000000000001
  },
  {
    "window_start": "2026-09-29T20:59:00",
    "window_end": "2026-09-29T21:00:00",
    "zone": "university",
    "active_vehicles": 2,
    "idle_ratio": 0.775,
    "trips_in_window": 7,
    "total_fare": 26.21
  },
  {
    "window_start": "2026-09-29T20:58:00",
    "window_end": "2026-09-29T20:59:00",
    "zone": "airport",
    "active_vehicles": 3,
    "idle_ratio": 1,
    "trips_in_window": 0,
    "total_fare": 0
  },
  {
    "window_start": "2026-09-29T20:58:00",
    "window_end": "2026-09-29T20:59:00",
    "zone": "suburbs_south",
    "active_vehicles": 3,
    "idle_ratio": 0.48333333333333334,
    "trips_in_window": 11,
    "total_fare": 61.56
  },
  {
    "window_start": "2026-09-29T20:58:00",
    "window_end": "2026-09-29T20:59:00",
    "zone": "suburbs_north",
    "active_vehicles": 7,
    "idle_ratio": 0.5142857142857142,
    "trips_in_window": 25,
    "total_fare": 120.39
  },
  {
    "window_start": "2026-09-29T20:58:00",
    "window_end": "2026-09-29T20:59:00",
    "zone": "downtown",
    "active_vehicles": 4,
    "idle_ratio": 0.74,
    "trips_in_window": 13,
    "total_fare": 56.379999999999995
  },
  {
    "window_start": "2026-09-29T20:58:00",
    "window_end": "2026-09-29T20:59:00",
    "zone": "university",
    "active_vehicles": 2,
    "idle_ratio": 0.7,
    "trips_in_window": 4,
    "total_fare": 16.380000000000003
  },
  {
    "window_start": "2026-09-29T20:57:00",
    "window_end": "2026-09-29T20:58:00",
    "zone": "airport",
    "active_vehicles": 3,
    "idle_ratio": 0.7333333333333333,
    "trips_in_window": 4,
    "total_fare": 19.68
  },
  {
    "window_start": "2026-09-29T20:57:00",
    "window_end": "2026-09-29T20:58:00",
    "zone": "suburbs_south",
    "active_vehicles": 3,
    "idle_ratio": 0.7166666666666667,
    "trips_in_window": 6,
    "total_fare": 25.590000000000003
  },
  {
    "window_start": "2026-09-29T20:57:00",
    "window_end": "2026-09-29T20:58:00",
    "zone": "downtown",
    "active_vehicles": 4,
    "idle_ratio": 0.52,
    "trips_in_window": 16,
    "total_fare": 60.370000000000005
  },
  {
    "window_start": "2026-09-29T20:57:00",
    "window_end": "2026-09-29T20:58:00",
    "zone": "suburbs_north",
    "active_vehicles": 7,
    "idle_ratio": 0.45714285714285713,
    "trips_in_window": 25,
    "total_fare": 124.49000000000001
  },
  {
    "window_start": "2026-09-29T20:57:00",
    "window_end": "2026-09-29T20:58:00",
    "zone": "university",
    "active_vehicles": 2,
    "idle_ratio": 0.925,
    "trips_in_window": 1,
    "total_fare": 7.16
  },
  {
    "window_start": "2026-09-29T20:56:00",
    "window_end": "2026-09-29T20:57:00",
    "zone": "airport",
    "active_vehicles": 3,
    "idle_ratio": 0.6491228070175439,
    "trips_in_window": 8,
    "total_fare": 37.85
  },
  {
    "window_start": "2026-09-29T20:56:00",
    "window_end": "2026-09-29T20:57:00",
    "zone": "suburbs_north",
    "active_vehicles": 7,
    "idle_ratio": 0.5413533834586466,
    "trips_in_window": 20,
    "total_fare": 123.02000000000001
  },
  {
    "window_start": "2026-09-29T20:56:00",
    "window_end": "2026-09-29T20:57:00",
    "zone": "university",
    "active_vehicles": 2,
    "idle_ratio": 0.6578947368421053,
    "trips_in_window": 4,
    "total_fare": 26.28
  },
  {
    "window_start": "2026-09-29T20:56:00",
    "window_end": "2026-09-29T20:57:00",
    "zone": "downtown",
    "active_vehicles": 4,
    "idle_ratio": 0.4842105263157895,
    "trips_in_window": 18,
    "total_fare": 88.14
  },
  {
    "window_start": "2026-09-29T20:56:00",
    "window_end": "2026-09-29T20:57:00",
    "zone": "suburbs_south",
    "active_vehicles": 3,
    "idle_ratio": 0.9649122807017544,
    "trips_in_window": 0,
    "total_fare": 0
  },
  {
    "window_start": "2026-09-29T20:55:00",
    "window_end": "2026-09-29T20:56:00",
    "zone": "suburbs_north",
    "active_vehicles": 7,
    "idle_ratio": 0.7642857142857142,
    "trips_in_window": 10,
    "total_fare": 64.63
  },
  {
    "window_start": "2026-09-29T20:55:00",
    "window_end": "2026-09-29T20:56:00",
    "zone": "downtown",
    "active_vehicles": 4,
    "idle_ratio": 0.66,
    "trips_in_window": 11,
    "total_fare": 59.980000000000004
  },
  {
    "window_start": "2026-09-29T20:55:00",
    "window_end": "2026-09-29T20:56:00",
    "zone": "airport",
    "active_vehicles": 3,
    "idle_ratio": 0.55,
    "trips_in_window": 8,
    "total_fare": 34.62
  },
  {
    "window_start": "2026-09-29T20:55:00",
    "window_end": "2026-09-29T20:56:00",
    "zone": "suburbs_south",
    "active_vehicles": 3,
    "idle_ratio": 0.7,
    "trips_in_window": 6,
    "total_fare": 31.79
  },
  {
    "window_start": "2026-09-29T20:55:00",
    "window_end": "2026-09-29T20:56:00",
    "zone": "university",
    "active_vehicles": 2,
    "idle_ratio": 0.55,
    "trips_in_window": 6,
    "total_fare": 36.519999999999996
  },
  {
    "window_start": "2026-09-29T20:54:00",
    "window_end": "2026-09-29T20:55:00",
    "zone": "suburbs_south",
    "active_vehicles": 3,
    "idle_ratio": 0.4,
    "trips_in_window": 11,
    "total_fare": 64.52
  },
  {
    "window_start": "2026-09-29T20:54:00",
    "window_end": "2026-09-29T20:55:00",
    "zone": "downtown",
    "active_vehicles": 4,
    "idle_ratio": 0.64,
    "trips_in_window": 17,
    "total_fare": 66.59
  },
  {
    "window_start": "2026-09-29T20:54:00",
    "window_end": "2026-09-29T20:55:00",
    "zone": "airport",
    "active_vehicles": 3,
    "idle_ratio": 0.4666666666666667,
    "trips_in_window": 8,
    "total_fare": 63.980000000000004
  },
  {
    "window_start": "2026-09-29T20:54:00",
    "window_end": "2026-09-29T20:55:00",
    "zone": "suburbs_north",
    "active_vehicles": 7,
    "idle_ratio": 0.8142857142857143,
    "trips_in_window": 7,
    "total_fare": 35.09
  },
  {
    "window_start": "2026-09-29T20:54:00",
    "window_end": "2026-09-29T20:55:00",
    "zone": "university",
    "active_vehicles": 2,
    "idle_ratio": 0.65,
    "trips_in_window": 4,
    "total_fare": 28.290000000000003
  },
  {
    "window_start": "2026-09-29T20:53:00",
    "window_end": "2026-09-29T20:54:00",
    "zone": "airport",
    "active_vehicles": 3,
    "idle_ratio": 0.5,
    "trips_in_window": 9,
    "total_fare": 30.130000000000003
  },
  {
    "window_start": "2026-09-29T20:53:00",
    "window_end": "2026-09-29T20:54:00",
    "zone": "suburbs_south",
    "active_vehicles": 3,
    "idle_ratio": 0.65,
    "trips_in_window": 9,
    "total_fare": 44.58
  },
  {
    "window_start": "2026-09-29T20:53:00",
    "window_end": "2026-09-29T20:54:00",
    "zone": "university",
    "active_vehicles": 2,
    "idle_ratio": 0.575,
    "trips_in_window": 5,
    "total_fare": 31.909999999999997
  },
  {
    "window_start": "2026-09-29T20:53:00",
    "window_end": "2026-09-29T20:54:00",
    "zone": "suburbs_north",
    "active_vehicles": 7,
    "idle_ratio": 0.7071428571428572,
    "trips_in_window": 18,
    "total_fare": 80.24
  },
  {
    "window_start": "2026-09-29T20:53:00",
    "window_end": "2026-09-29T20:54:00",
    "zone": "downtown",
    "active_vehicles": 4,
    "idle_ratio": 0.68,
    "trips_in_window": 12,
    "total_fare": 59.519999999999996
  },
  {
    "window_start": "2026-09-29T20:52:00",
    "window_end": "2026-09-29T20:53:00",
    "zone": "suburbs_north",
    "active_vehicles": 7,
    "idle_ratio": 0.5714285714285714,
    "trips_in_window": 16,
    "total_fare": 74.36999999999999
  },
  {
    "window_start": "2026-09-29T20:52:00",
    "window_end": "2026-09-29T20:53:00",
    "zone": "airport",
    "active_vehicles": 3,
    "idle_ratio": 0.26666666666666666,
    "trips_in_window": 18,
    "total_fare": 80.45
  },
  {
    "window_start": "2026-09-29T20:52:00",
    "window_end": "2026-09-29T20:53:00",
    "zone": "suburbs_south",
    "active_vehicles": 3,
    "idle_ratio": 0.7333333333333333,
    "trips_in_window": 5,
    "total_fare": 32.209999999999994
  },
  {
    "window_start": "2026-09-29T20:52:00",
    "window_end": "2026-09-29T20:53:00",
    "zone": "downtown",
    "active_vehicles": 4,
    "idle_ratio": 0.69,
    "trips_in_window": 9,
    "total_fare": 29.809999999999995
  },
  {
    "window_start": "2026-09-29T20:52:00",
    "window_end": "2026-09-29T20:53:00",
    "zone": "university",
    "active_vehicles": 2,
    "idle_ratio": 0.375,
    "trips_in_window": 9,
    "total_fare": 47.06
  },
  {
    "window_start": "2026-09-29T20:51:00",
    "window_end": "2026-09-29T20:52:00",
    "zone": "downtown",
    "active_vehicles": 4,
    "idle_ratio": 0.84,
    "trips_in_window": 8,
    "total_fare": 39.26
  },
  {
    "window_start": "2026-09-29T20:51:00",
    "window_end": "2026-09-29T20:52:00",
    "zone": "suburbs_south",
    "active_vehicles": 3,
    "idle_ratio": 0.7,
    "trips_in_window": 7,
    "total_fare": 27.32
  },
  {
    "window_start": "2026-09-29T20:51:00",
    "window_end": "2026-09-29T20:52:00",
    "zone": "university",
    "active_vehicles": 2,
    "idle_ratio": 0.325,
    "trips_in_window": 8,
    "total_fare": 37.93
  },
  {
    "window_start": "2026-09-29T20:51:00",
    "window_end": "2026-09-29T20:52:00",
    "zone": "airport",
    "active_vehicles": 3,
    "idle_ratio": 0.36666666666666664,
    "trips_in_window": 14,
    "total_fare": 68.85000000000001
  },
  {
    "window_start": "2026-09-29T20:51:00",
    "window_end": "2026-09-29T20:52:00",
    "zone": "suburbs_north",
    "active_vehicles": 7,
    "idle_ratio": 0.7071428571428572,
    "trips_in_window": 17,
    "total_fare": 66.12
  },
  {
    "window_start": "2026-09-29T20:50:00",
    "window_end": "2026-09-29T20:51:00",
    "zone": "university",
    "active_vehicles": 2,
    "idle_ratio": 0.975,
    "trips_in_window": 0,
    "total_fare": 0
  },
  {
    "window_start": "2026-09-29T20:50:00",
    "window_end": "2026-09-29T20:51:00",
    "zone": "airport",
    "active_vehicles": 3,
    "idle_ratio": 0.36666666666666664,
    "trips_in_window": 16,
    "total_fare": 67.34
  },
  {
    "window_start": "2026-09-29T20:50:00",
    "window_end": "2026-09-29T20:51:00",
    "zone": "suburbs_south",
    "active_vehicles": 3,
    "idle_ratio": 0.3333333333333333,
    "trips_in_window": 14,
    "total_fare": 65.09
  },
  {
    "window_start": "2026-09-29T20:50:00",
    "window_end": "2026-09-29T20:51:00",
    "zone": "suburbs_north",
    "active_vehicles": 7,
    "idle_ratio": 0.7,
    "trips_in_window": 16,
    "total_fare": 79.48
  },
  {
    "window_start": "2026-09-29T20:50:00",
    "window_end": "2026-09-29T20:51:00",
    "zone": "downtown",
    "active_vehicles": 4,
    "idle_ratio": 0.55,
    "trips_in_window": 16,
    "total_fare": 74.46
  }
]

```

### Profitability Report (/reports/profitability/1)
```json
[
  {
    "vehicle_id": "veh_0012",
    "total_fare": 11.16,
    "fuel_cost": 20.48,
    "maintenance_cost": 20.54,
    "total_cost": 41.019999999999996,
    "net_profit": -29.859999999999996,
    "unprofitable": true
  },
  {
    "vehicle_id": "veh_0001",
    "total_fare": 25.330000000000002,
    "fuel_cost": 44.15,
    "maintenance_cost": 0,
    "total_cost": 44.15,
    "net_profit": -18.819999999999997,
    "unprofitable": true
  },
  {
    "vehicle_id": "veh_0007",
    "total_fare": 38.050000000000004,
    "fuel_cost": 48.66,
    "maintenance_cost": 0,
    "total_cost": 48.66,
    "net_profit": -10.609999999999992,
    "unprofitable": true
  },
  {
    "vehicle_id": "veh_0002",
    "total_fare": 41.85,
    "fuel_cost": 42.02,
    "maintenance_cost": 0,
    "total_cost": 42.02,
    "net_profit": -0.1700000000000017,
    "unprofitable": true
  },
  {
    "vehicle_id": "veh_0004",
    "total_fare": 47.14,
    "fuel_cost": 44.97,
    "maintenance_cost": 0,
    "total_cost": 44.97,
    "net_profit": 2.1700000000000017,
    "unprofitable": false
  },
  {
    "vehicle_id": "veh_0019",
    "total_fare": 43.10000000000001,
    "fuel_cost": 36.78,
    "maintenance_cost": 0,
    "total_cost": 36.78,
    "net_profit": 6.320000000000007,
    "unprofitable": false
  },
  {
    "vehicle_id": "veh_0000",
    "total_fare": 63.78,
    "fuel_cost": 42.93,
    "maintenance_cost": 0,
    "total_cost": 42.93,
    "net_profit": 20.85,
    "unprofitable": false
  },
  {
    "vehicle_id": "veh_0017",
    "total_fare": 52.300000000000004,
    "fuel_cost": 15.85,
    "maintenance_cost": 6.8,
    "total_cost": 22.65,
    "net_profit": 29.650000000000006,
    "unprofitable": false
  },
  {
    "vehicle_id": "veh_0016",
    "total_fare": 76.25999999999999,
    "fuel_cost": 39.41,
    "maintenance_cost": 0,
    "total_cost": 39.41,
    "net_profit": 36.849999999999994,
    "unprofitable": false
  },
  {
    "vehicle_id": "veh_0005",
    "total_fare": 54.589999999999996,
    "fuel_cost": 17.22,
    "maintenance_cost": 0,
    "total_cost": 17.22,
    "net_profit": 37.37,
    "unprofitable": false
  },
  {
    "vehicle_id": "veh_0009",
    "total_fare": 73.42000000000002,
    "fuel_cost": 27.99,
    "maintenance_cost": 0,
    "total_cost": 27.99,
    "net_profit": 45.43000000000002,
    "unprofitable": false
  },
  {
    "vehicle_id": "veh_0014",
    "total_fare": 122.24999999999999,
    "fuel_cost": 35.47,
    "maintenance_cost": 29.7,
    "total_cost": 65.17,
    "net_profit": 57.079999999999984,
    "unprofitable": false
  },
  {
    "vehicle_id": "veh_0011",
    "total_fare": 93.28999999999999,
    "fuel_cost": 16.14,
    "maintenance_cost": 20.04,
    "total_cost": 36.18,
    "net_profit": 57.10999999999999,
    "unprofitable": false
  },
  {
    "vehicle_id": "veh_0006",
    "total_fare": 119.25999999999998,
    "fuel_cost": 46.82,
    "maintenance_cost": 0,
    "total_cost": 46.82,
    "net_profit": 72.43999999999997,
    "unprofitable": false
  },
  {
    "vehicle_id": "veh_0003",
    "total_fare": 119.56,
    "fuel_cost": 32.44,
    "maintenance_cost": 12.79,
    "total_cost": 45.23,
    "net_profit": 74.33000000000001,
    "unprofitable": false
  },
  {
    "vehicle_id": "veh_0013",
    "total_fare": 118.16,
    "fuel_cost": 16.61,
    "maintenance_cost": 17.78,
    "total_cost": 34.39,
    "net_profit": 83.77,
    "unprofitable": false
  },
  {
    "vehicle_id": "veh_0010",
    "total_fare": 147.67000000000002,
    "fuel_cost": 24.68,
    "maintenance_cost": 37.48,
    "total_cost": 62.16,
    "net_profit": 85.51000000000002,
    "unprofitable": false
  },
  {
    "vehicle_id": "veh_0015",
    "total_fare": 115.91,
    "fuel_cost": 23.26,
    "maintenance_cost": 0,
    "total_cost": 23.26,
    "net_profit": 92.64999999999999,
    "unprofitable": false
  },
  {
    "vehicle_id": "veh_0018",
    "total_fare": 122.47999999999999,
    "fuel_cost": 28.66,
    "maintenance_cost": 0,
    "total_cost": 28.66,
    "net_profit": 93.82,
    "unprofitable": false
  },
  {
    "vehicle_id": "veh_0008",
    "total_fare": 133.5,
    "fuel_cost": 27.65,
    "maintenance_cost": 0,
    "total_cost": 27.65,
    "net_profit": 105.85,
    "unprofitable": false
  }
]

```

### Speed Layer JSON Logs
```json
speed-layer-1  | {"ts": 1790715616.907, "level": "INFO", "stage": "processing.speed_layer", "message": "raw batch written"}
speed-layer-1  | {"ts": 1790715632.028, "level": "INFO", "stage": "processing.speed_layer", "message": "raw batch written"}
speed-layer-1  | {"ts": 1790715632.316, "level": "INFO", "stage": "processing.speed_layer", "message": "batch written"}
speed-layer-1  | {"ts": 1790715646.907, "level": "INFO", "stage": "processing.speed_layer", "message": "raw batch written"}
speed-layer-1  | {"ts": 1790715662.004, "level": "INFO", "stage": "processing.speed_layer", "message": "raw batch written"}
speed-layer-1  | {"ts": 1790715675.737, "level": "INFO", "stage": "processing.speed_layer", "message": "raw batch written"}
speed-layer-1  | {"ts": 1790715690.796, "level": "INFO", "stage": "processing.speed_layer", "message": "raw batch written"}
speed-layer-1  | {"ts": 1790715692.445, "level": "INFO", "stage": "processing.speed_layer", "message": "batch written"}
speed-layer-1  | {"ts": 1790715705.817, "level": "INFO", "stage": "processing.speed_layer", "message": "raw batch written"}
speed-layer-1  | {"ts": 1790715720.818, "level": "INFO", "stage": "processing.speed_layer", "message": "raw batch written"}

```


### Docker Compose Stack
```text
NAME                               IMAGE                             COMMAND                  SERVICE             CREATED          STATUS          PORTS
fleet-lambda-airflow-scheduler-1   fleet-lambda-airflow-scheduler    "/usr/bin/dumb-init …"   airflow-scheduler   29 minutes ago   Up 29 minutes   8080/tcp
fleet-lambda-airflow-webserver-1   fleet-lambda-airflow-webserver    "/usr/bin/dumb-init …"   airflow-webserver   29 minutes ago   Up 29 minutes   0.0.0.0:8080->8080/tcp, [::]:8080->8080/tcp
fleet-lambda-alert-watcher-1       fleet-lambda-alert-watcher        "/opt/entrypoint.sh …"   alert-watcher       15 minutes ago   Up 15 minutes   
fleet-lambda-api-1                 fleet-lambda-api                  "uvicorn api:app --h…"   api                 22 minutes ago   Up 22 minutes   0.0.0.0:8000->8000/tcp, [::]:8000->8000/tcp
fleet-lambda-kafka-1               confluentinc/cp-kafka:7.6.0       "/etc/confluent/dock…"   kafka               57 minutes ago   Up 57 minutes   0.0.0.0:9092->9092/tcp, [::]:9092->9092/tcp
fleet-lambda-postgres-1            postgres:16                       "docker-entrypoint.s…"   postgres            57 minutes ago   Up 57 minutes   0.0.0.0:5432->5432/tcp, [::]:5432->5432/tcp
fleet-lambda-producers-1           fleet-lambda-producers            "python run_both.py"     producers           38 minutes ago   Up 13 minutes   
fleet-lambda-spark-master-1        apache/spark:3.5.0                "/opt/entrypoint.sh …"   spark-master        57 minutes ago   Up 57 minutes   0.0.0.0:7077->7077/tcp, [::]:7077->7077/tcp, 0.0.0.0:8081->8080/tcp, [::]:8081->8080/tcp
fleet-lambda-spark-worker-1        apache/spark:3.5.0                "/opt/entrypoint.sh …"   spark-worker        57 minutes ago   Up 57 minutes   
fleet-lambda-speed-layer-1         fleet-lambda-speed-layer          "/opt/entrypoint.sh …"   speed-layer         43 minutes ago   Up 43 minutes   
fleet-lambda-zookeeper-1           confluentinc/cp-zookeeper:7.6.0   "/etc/confluent/dock…"   zookeeper           57 minutes ago   Up 57 minutes   2181/tcp, 2888/tcp, 3888/tcp
```

### Airflow DAG Success
*(Note: Airflow DAG `daily_profitability_reconciliation` completed successfully, writing the batch metrics to Postgres. Please see accompanying video demo.)*


### Live Alerts (/live/alerts)
```json
[
  {
    "rule": "vehicle_idle_threshold",
    "vehicle_id": "veh_0013",
    "detail": {
      "idle_seconds": 12
    },
    "created_at": "2026-09-29T21:01:48.766618"
  },
  {
    "rule": "vehicle_idle_threshold",
    "vehicle_id": "veh_0010",
    "detail": {
      "idle_seconds": 12
    },
    "created_at": "2026-09-29T21:01:45.764659"
  },
  {
    "rule": "vehicle_idle_threshold",
    "vehicle_id": "veh_0012",
    "detail": {
      "idle_seconds": 12
    },
    "created_at": "2026-09-29T21:0...
```

### Demo Narrative Summary
Over a 10-minute demo run (with simulated days compressed to 5 minutes each), the pipeline processed telemetry from 20 simulated vehicles. 
- The **Speed Layer** continuously aggregated live utilization metrics and successfully displayed them on our live HTML Dashboard.
- The **Alert Watcher** correctly flagged several vehicles that went idle for over 10 seconds, logging `vehicle_idle_threshold` alerts to the database. Additionally, when we manually stopped the telemetry producer for 15 seconds, a `no_data` alert was instantly generated and displayed on the dashboard.
- The **Batch Layer** ran successfully via Apache Airflow, reconciling the day's total fares against fuel and maintenance costs, highlighting unprofitable vehicles in the `/reports/profitability/{sim_day}` endpoint.


7. Limitations, Trade-offs, and Production-Scale Considerations

The architecture is a deliberately scaled-down version of patterns used in real fleet-operations platforms. The structural choices — a Kafka backbone, Spark for both processing layers, daily batch reconciliation via Airflow — mirror production systems (e.g. Uber's telemetry and reconciliation pipelines). The following simplifications were made for the two-week project scope and would be revisited at production scale:

Stream processing engine: Structured Streaming's micro-batch model is sufficient for our minute-level utilization granularity, but production fleet systems handling tighter latency tiers (e.g. live map positions, surge pricing) more commonly use Apache Flink for true event-at-a-time processing with lower end-to-end latency.

Geo-representation: zones are a fixed, hardcoded list rather than a real geo-index (H3/S2 cells), which would be needed for proximity queries and finer-grained utilization analysis at city scale.

Serving store: live metrics are served directly from PostgreSQL, which is adequate at this data volume but would not scale to thousands of concurrent dashboard/API consumers polling every few seconds; a low-latency store (e.g. Redis) would typically sit in front of it in production.

Raw data archive: the raw telemetry archive is piggybacked on the speed layer's own write path rather than a dedicated, independently-scaled sink (e.g. Kafka Connect or a small Flink job writing Parquet to a data lake), which is how production batch layers typically source their historical data.

Cost data ingestion: our daily cost file is a clean, well-formed JSON drop; real vendor cost feeds are typically messier (schema drift, late corrections, duplicate records), requiring data-quality validation and idempotent upserts that our reconciliation job does not yet implement.

Observability depth: our alerting covers application-level conditions (idle vehicles, missing data) but omits infrastructure-level signals that matter most in production streaming systems — Kafka consumer lag being the single most important, and currently absent, health metric.

Alert delivery: alerts are currently persisted to a database table read by the API rather than pushed to an on-call system (Slack, PagerDuty), which would be necessary for the alerts to be actionable in real time.

At production scale, we would prioritise consumer-lag-based monitoring and a proper data-lake archive first, since both directly affect whether the batch layer's reconciliation numbers can be trusted, before investing further in geo-indexing or a lower-latency serving store.

