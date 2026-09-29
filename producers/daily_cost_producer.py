"""Simulated daily-batch source: once per simulated day, drops a JSON record per vehicle
with fuel/maintenance costs (vehicle_id, fuel_cost, maintenance_cost, distance_covered,
service_flag). Published to the `daily-cost` Kafka topic so it can be picked up by the
Airflow-orchestrated batch job, and also written as a JSON file under ./drops/ to satisfy
the "drops a file" framing literally (either can be used as the ingestion point).

Run standalone: python daily_cost_producer.py
"""
import json
import os
import random
import sys
import time

from kafka import KafkaProducer

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "common"))
sys.path.append(os.path.join(os.path.dirname(__file__), "common"))
from logging_config import get_logger  # noqa: E402
from schemas import DAILY_COST_TOPIC  # noqa: E402
from sim_clock import get_sim_day  # noqa: E402

log = get_logger("ingestion.daily_cost")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
NUM_VEHICLES = int(os.environ.get("NUM_VEHICLES", 20))
SIM_DAY_SECONDS = int(os.environ.get("SIM_DAY_SECONDS", 300))
DROP_DIR = os.path.join(os.path.dirname(__file__), "drops")


def make_day_file(sim_day: int):
    os.makedirs(DROP_DIR, exist_ok=True)
    records = []
    for i in range(NUM_VEHICLES):
        vehicle_id = f"veh_{i:04d}"
        record = {
            "vehicle_id": vehicle_id,
            "fuel_cost": round(random.uniform(15, 60), 2),
            "maintenance_cost": round(random.uniform(0, 40), 2) if random.random() < 0.15 else 0.0,
            "distance_covered": round(random.uniform(50, 300), 1),
            "service_flag": random.random() < 0.05,
            "sim_day": sim_day,
        }
        records.append(record)

    path = os.path.join(DROP_DIR, f"costs_day_{sim_day:04d}.json")
    with open(path, "w") as f:
        json.dump(records, f, indent=2)
    return records, path


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    log.info("daily cost producer started", extra={"fields": {"sim_day_seconds": SIM_DAY_SECONDS}})

    last_day_processed = -1
    while True:
        sim_day = get_sim_day()
        if sim_day > last_day_processed:
            records, path = make_day_file(sim_day)
            for r in records:
                producer.send(DAILY_COST_TOPIC, value=r)
            producer.flush()
            log.info(
                "daily cost file dropped",
                extra={"fields": {"sim_day": sim_day, "path": path, "num_records": len(records)}},
            )
            last_day_processed = sim_day
        time.sleep(1)


if __name__ == "__main__":
    main()
