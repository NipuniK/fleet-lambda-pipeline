-- Raw telemetry archive (also used by the batch layer to derive daily trip aggregates)
CREATE TABLE IF NOT EXISTS raw_telemetry (
    id SERIAL PRIMARY KEY,
    trip_id TEXT,
    driver_id TEXT,
    vehicle_id TEXT NOT NULL,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    speed DOUBLE PRECISION,
    status TEXT,
    fare DOUBLE PRECISION,
    zone TEXT,
    "timestamp" DOUBLE PRECISION
);

-- Speed layer output: windowed utilization metrics per zone
CREATE TABLE IF NOT EXISTS fleet_live_metrics (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    zone TEXT,
    active_vehicles INT,
    idle_ratio DOUBLE PRECISION,
    trips_in_window INT,
    total_fare DOUBLE PRECISION
);

-- Batch layer output: daily per-vehicle profitability
CREATE TABLE IF NOT EXISTS vehicle_profitability (
    id SERIAL PRIMARY KEY,
    vehicle_id TEXT NOT NULL,
    total_fare DOUBLE PRECISION,
    fuel_cost DOUBLE PRECISION,
    maintenance_cost DOUBLE PRECISION,
    total_cost DOUBLE PRECISION,
    net_profit DOUBLE PRECISION,
    unprofitable BOOLEAN,
    sim_day INT
);

-- Observability: threshold + health-check alerts
CREATE TABLE IF NOT EXISTS pipeline_alerts (
    id SERIAL PRIMARY KEY,
    rule TEXT NOT NULL,
    vehicle_id TEXT,
    detail JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_telemetry_vehicle ON raw_telemetry(vehicle_id);
CREATE INDEX IF NOT EXISTS idx_profitability_day ON vehicle_profitability(sim_day);
CREATE INDEX IF NOT EXISTS idx_alerts_created ON pipeline_alerts(created_at);
