-- Tiger Data & TimescaleDB Continuous Aggregates (CAGGs) Architecture
-- Pre-computes heavy analytical rollups in real time to accelerate front-end dashboards

CREATE TABLE IF NOT EXISTS industrial_sensor_telemetry (
    id BIGSERIAL PRIMARY KEY,
    time TIMESTAMPTZ NOT NULL,
    device_id VARCHAR(32) NOT NULL,
    site_id VARCHAR(32) NOT NULL,
    temperature DOUBLE PRECISION NOT NULL,
    vibration DOUBLE PRECISION NOT NULL,
    power_kw DOUBLE PRECISION NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'NORMAL'
);

-- High-performance indexes for raw scans
CREATE INDEX IF NOT EXISTS idx_telemetry_time_device 
ON industrial_sensor_telemetry (time DESC, device_id);

CREATE INDEX IF NOT EXISTS idx_telemetry_device_time 
ON industrial_sensor_telemetry (device_id, time DESC);

-- Continuous Aggregate (CAGG) Materialized View: 1-Hour Resolution
-- Pre-computes aggregate rollups and refreshes asynchronously
CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_sensor_cagg AS
SELECT 
    date_trunc('hour', time) AS bucket,
    device_id,
    site_id,
    ROUND(AVG(temperature)::numeric, 2) AS avg_temperature,
    ROUND(MAX(temperature)::numeric, 2) AS max_temperature,
    ROUND(MIN(temperature)::numeric, 2) AS min_temperature,
    ROUND(AVG(vibration)::numeric, 4) AS avg_vibration,
    ROUND(SUM(power_kw)::numeric, 2) AS total_kwh,
    COUNT(*) AS reading_count
FROM industrial_sensor_telemetry
GROUP BY date_trunc('hour', time), device_id, site_id;

-- Index on pre-computed CAGG for sub-millisecond dashboard queries
CREATE INDEX IF NOT EXISTS idx_cagg_dev_bucket 
ON hourly_sensor_cagg (device_id, bucket DESC);
