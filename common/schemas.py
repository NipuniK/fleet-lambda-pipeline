"""Shared field definitions so producers, processors, and the API stay in sync."""

TELEMETRY_TOPIC = "telemetry"
DAILY_COST_TOPIC = "daily-cost"

TELEMETRY_FIELDS = [
    "trip_id", "driver_id", "vehicle_id", "lat", "lon", "speed",
    "status", "fare", "zone", "timestamp", "sim_day",
]
# status is one of: idle | enroute | on_trip

DAILY_COST_FIELDS = [
    "vehicle_id", "fuel_cost", "maintenance_cost", "distance_covered",
    "service_flag", "sim_day",
]

ZONES = ["downtown", "airport", "suburbs_north", "suburbs_south", "university"]
