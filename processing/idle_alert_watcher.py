"""Lightweight observability side-consumer, run alongside speed_layer.py.

Tracks per-vehicle idle duration from the raw telemetry topic (cheap, no Spark needed)
and fires:
  - a threshold alert when a vehicle is idle beyond IDLE_ALERT_SECONDS
  - a health-check alert when no telemetry event has arrived within NO_DATA_ALERT_SECONDS

Alerts are written to the `pipeline_alerts` table so the API's /live/alerts endpoint can
surface them, in addition to the structured JSON log line.
"""
import json
import os
import sys
import time

import psycopg2
from kafka import KafkaConsumer

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "common"))
sys.path.append(os.path.join(os.path.dirname(__file__), "common"))
from logging_config import get_logger, log_alert, NoDataWatchdog  # noqa: E402
from schemas import TELEMETRY_TOPIC  # noqa: E402

log = get_logger("processing.idle_alert_watcher")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
DB_URL = os.environ.get("PIPELINE_DB_URL_RAW", "dbname=fleet user=fleet password=fleet host=localhost")
IDLE_ALERT_SECONDS = int(os.environ.get("IDLE_ALERT_SECONDS", 600))
NO_DATA_ALERT_SECONDS = int(os.environ.get("NO_DATA_ALERT_SECONDS", 30))


def write_alert(conn, rule: str, vehicle_id: str, detail: dict):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pipeline_alerts (rule, vehicle_id, detail, created_at) "
            "VALUES (%s, %s, %s, NOW())",
            (rule, vehicle_id, json.dumps(detail)),
        )
    conn.commit()


def main():
    consumer = KafkaConsumer(
        TELEMETRY_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        consumer_timeout_ms=5000,
    )
    conn = psycopg2.connect(DB_URL)
    watchdog = NoDataWatchdog(log, NO_DATA_ALERT_SECONDS)
    idle_since: dict[str, float] = {}
    already_alerted: set[str] = set()

    log.info("idle alert watcher started")

    while True:
        got_message = False
        for msg in consumer:
            got_message = True
            watchdog.beat()
            e = msg.value
            vid = e["vehicle_id"]
            now = e["timestamp"]

            if e["status"] == "idle":
                idle_since.setdefault(vid, now)
                idle_for = now - idle_since[vid]
                if idle_for > IDLE_ALERT_SECONDS and vid not in already_alerted:
                    log_alert(log, "vehicle_idle_threshold", vehicle_id=vid, idle_seconds=round(idle_for, 1))
                    write_alert(conn, "vehicle_idle_threshold", vid, {"idle_seconds": round(idle_for, 1)})
                    already_alerted.add(vid)
            else:
                idle_since.pop(vid, None)
                already_alerted.discard(vid)

        if not got_message:
            watchdog.check()
        time.sleep(1)


if __name__ == "__main__":
    main()
