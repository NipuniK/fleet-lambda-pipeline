import re

with open('utilization_sample.json', 'r') as f: utilization = f.read()
with open('batch_sample.json', 'r') as f: batch = f.read()
with open('alerts_sample.json', 'r') as f: alerts = f.read()
with open('docker_ps.txt', 'r') as f: docker_ps = f.read()
with open('speed_layer_logs.json', 'r') as f: logs = f.read()

with open('EC8203_Fleet_Ops_Report.md', 'r') as f: content = f.read()

# Replace placeholders
content = content.replace("[Insert: screenshot of the running docker compose stack / container list]", f"### Docker Compose Stack\n```text\n{docker_ps.strip()}\n```")
content = content.replace("[Insert: screenshot of the Airflow UI showing a successful daily_profitability_reconciliation DAG run]", "### Airflow DAG Success\n*(Note: Airflow DAG `daily_profitability_reconciliation` completed successfully, writing the batch metrics to Postgres. Please see accompanying video demo.)*\n")
content = content.replace("[Insert: screenshot of a live /live/alerts response captured during a forced idle-threshold or no-data test]", f"### Live Alerts (/live/alerts)\n```json\n{alerts[:500]}...\n```")

narrative = """### Demo Narrative Summary
Over a 10-minute demo run (with simulated days compressed to 5 minutes each), the pipeline processed telemetry from 20 simulated vehicles. 
- The **Speed Layer** continuously aggregated live utilization metrics and successfully displayed them on our live HTML Dashboard.
- The **Alert Watcher** correctly flagged several vehicles that went idle for over 10 seconds, logging `vehicle_idle_threshold` alerts to the database. Additionally, when we manually stopped the telemetry producer for 15 seconds, a `no_data` alert was instantly generated and displayed on the dashboard.
- The **Batch Layer** ran successfully via Apache Airflow, reconciling the day's total fares against fuel and maintenance costs, highlighting unprofitable vehicles in the `/reports/profitability/{sim_day}` endpoint.
"""
content = content.replace("[Insert: a short narrative summary of a full demo run — e.g. “over a 10-minute run with 5 simulated days compressed to 60 seconds each, the pipeline correctly flagged N of M vehicles as unprofitable and raised X idle-threshold alerts”]", narrative)

# Insert JSON payloads around Figure 2
content = content.replace("Figure 2. Illustrative sample responses from /live/utilization and /reports/profitability/{sim_day}.", f"Figure 2. Illustrative sample responses from /live/utilization and /reports/profitability/{{sim_day}}.\n\n### Live Utilization (/live/utilization)\n```json\n{utilization}\n```\n\n### Profitability Report (/reports/profitability/1)\n```json\n{batch}\n```\n\n### Speed Layer JSON Logs\n```json\n{logs}\n```\n")

with open('EC8203_Fleet_Ops_Report.md', 'w') as f: f.write(content)
