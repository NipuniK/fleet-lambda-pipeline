"""Simulated streaming source: emits one GPS/telemetry event per active vehicle every
few seconds, formatted per the assignment spec (trip_id, driver_id, vehicle_id, lat, lon,
speed, status, fare, timestamp) plus a `zone` field used for the live utilization breakdown.

Run standalone: python telemetry_producer.py
"""
import json
import os
import random
import sys
import time
import uuid

from kafka import KafkaProducer

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "common"))
sys.path.append(os.path.join(os.path.dirname(__file__), "common"))
from logging_config import get_logger  # noqa: E402
from schemas import TELEMETRY_TOPIC, ZONES  # noqa: E402
from sim_clock import get_sim_day  # noqa: E402

log = get_logger("ingestion.telemetry")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
NUM_VEHICLES = int(os.environ.get("NUM_VEHICLES", 20))
EMIT_INTERVAL_SECONDS = float(os.environ.get("EMIT_INTERVAL_SECONDS", 3))
# A vehicle occasionally goes idle for a long stretch, to exercise the idle-time alert.
IDLE_STREAK_PROB = 0.02
IDLE_STREAK_LEN = (20, 60)  # in emit ticks


class Vehicle:
    def __init__(self, vehicle_id: str):
        self.vehicle_id = vehicle_id
        self.driver_id = f"drv_{vehicle_id[-4:]}"
        self.zone = random.choice(ZONES)
        self.lat = round(random.uniform(6.85, 6.95), 5)
        self.lon = round(random.uniform(79.83, 79.93), 5)
        self.status = "idle"
        self.trip_id = None
        self.idle_streak_remaining = 0

    def tick(self):
        # Random walk position
        self.lat = round(self.lat + random.uniform(-0.001, 0.001), 5)
        self.lon = round(self.lon + random.uniform(-0.001, 0.001), 5)

        if self.idle_streak_remaining > 0:
            self.status = "idle"
            self.idle_streak_remaining -= 1
        elif random.random() < IDLE_STREAK_PROB:
            self.status = "idle"
            self.idle_streak_remaining = random.randint(*IDLE_STREAK_LEN)
        else:
            self.status = random.choices(
                ["idle", "enroute", "on_trip"], weights=[0.3, 0.3, 0.4]
            )[0]

        if self.status == "on_trip" and not self.trip_id:
            self.trip_id = str(uuid.uuid4())
        elif self.status != "on_trip":
            self.trip_id = None

        speed = 0.0 if self.status == "idle" else round(random.uniform(10, 60), 1)
        fare = round(random.uniform(2, 4), 2) if self.status == "on_trip" else 0.0

        return {
            "trip_id": self.trip_id,
            "driver_id": self.driver_id,
            "vehicle_id": self.vehicle_id,
            "lat": self.lat,
            "lon": self.lon,
            "speed": speed,
            "status": self.status,
            "fare": fare,
            "zone": self.zone,
            "timestamp": time.time(),
            "sim_day": get_sim_day(),
        }


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    vehicles = [Vehicle(f"veh_{i:04d}") for i in range(NUM_VEHICLES)]
    log.info("telemetry producer started", extra={"fields": {"num_vehicles": NUM_VEHICLES}})

    sent = 0
    while True:
        for v in vehicles:
            event = v.tick()
            producer.send(TELEMETRY_TOPIC, value=event)
            sent += 1
        producer.flush()
        if sent % 200 == 0:
            log.info("events sent", extra={"fields": {"total_sent": sent}})
        time.sleep(EMIT_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
