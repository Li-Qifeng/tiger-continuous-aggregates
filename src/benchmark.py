"""
Continuous Aggregates (CAGGs) Benchmarking & Data Generation Engine
Compares raw analytical table scans vs. pre-computed Continuous Aggregate queries.
"""

import time
import random
import datetime
import json
import psycopg2
from psycopg2.extras import execute_values

DB_URL = "host=localhost port=54329 user=postgres password=tigerpass dbname=postgres"

DEVICES = [
    ("TURBINE-01", "SITE-A", 68.5, 0.012, 120.0),
    ("TURBINE-02", "SITE-A", 71.2, 0.015, 125.0),
    ("GENERATOR-01", "SITE-A", 82.0, 0.022, 210.0),
    ("PUMP-X1", "SITE-B", 55.4, 0.008, 45.0),
    ("PUMP-X2", "SITE-B", 58.1, 0.009, 48.0),
    ("COMPRESSOR-01", "SITE-B", 88.5, 0.035, 310.0),
    ("CONVEYOR-01", "SITE-C", 42.0, 0.005, 30.0),
    ("COOLING-TWR-1", "SITE-C", 38.5, 0.004, 25.0),
    ("MOTOR-DRIVE-1", "SITE-C", 64.0, 0.011, 75.0),
    ("FEEDER-LINE-1", "SITE-A", 49.5, 0.006, 50.0)
]

def seed_sensor_telemetry(total_records=25000):
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    now = datetime.datetime.now(datetime.timezone.utc)
    print(f"Generating {total_records} sensor telemetry records across 14-day window...")
    records = []

    for _ in range(total_records):
        dev_id, site_id, base_t, base_v, base_p = random.choice(DEVICES)
        hours_ago = random.uniform(0, 14 * 24)
        t_stamp = now - datetime.timedelta(hours=hours_ago)

        t_val = round(base_t + random.uniform(-4.0, 6.0), 2)
        v_val = round(base_v + random.uniform(-0.003, 0.008), 4)
        p_val = round(base_p + random.uniform(-10.0, 15.0), 2)
        status = "ALERT" if v_val > 0.025 or t_val > 85.0 else "NORMAL"

        records.append((t_stamp, dev_id, site_id, t_val, v_val, p_val, status))

    sql = """
        INSERT INTO industrial_sensor_telemetry (
            time, device_id, site_id, temperature, vibration, power_kw, status
        ) VALUES %s;
    """
    print("Executing bulk insertion into industrial_sensor_telemetry...")
    execute_values(cur, sql, records, page_size=2500)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM industrial_sensor_telemetry;")
    count = cur.fetchone()[0]
    print(f"Seeding complete! Total rows in industrial_sensor_telemetry: {count}")

    cur.close()
    conn.close()

def run_benchmarks():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    print("\n" + "=" * 70)
    print("BENCHMARK 1: Raw Dynamic HashAggregation across 25,000 rows")
    print("=" * 70)
    raw_sql = """
        EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
        SELECT 
            date_trunc('hour', time) AS bucket,
            device_id,
            ROUND(AVG(temperature)::numeric, 2) AS avg_temp,
            ROUND(MAX(temperature)::numeric, 2) AS max_temp,
            ROUND(MIN(temperature)::numeric, 2) AS min_temp,
            COUNT(*) AS readings
        FROM industrial_sensor_telemetry
        WHERE device_id = 'TURBINE-01' AND time >= NOW() - INTERVAL '7 days'
        GROUP BY date_trunc('hour', time), device_id
        ORDER BY bucket DESC
        LIMIT 20;
    """
    cur.execute(raw_sql)
    raw_plan = cur.fetchone()[0][0]
    raw_exec_ms = raw_plan.get("Execution Time")
    raw_plan_ms = raw_plan.get("Planning Time")
    print(f"Raw Scan Planning Time:  {raw_plan_ms:.3f} ms")
    print(f"Raw Scan Execution Time: {raw_exec_ms:.3f} ms")

    print("\n" + "=" * 70)
    print("BENCHMARK 2: Continuous Aggregate Query (`hourly_sensor_summary`)")
    print("=" * 70)
    cagg_sql = """
        EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
        SELECT 
            bucket,
            device_id,
            avg_temperature,
            max_temperature,
            min_temperature,
            reading_count
        FROM hourly_sensor_summary
        WHERE device_id = 'TURBINE-01' AND bucket >= NOW() - INTERVAL '7 days'
        ORDER BY bucket DESC
        LIMIT 20;
    """
    cur.execute(cagg_sql)
    cagg_plan = cur.fetchone()[0][0]
    cagg_exec_ms = cagg_plan.get("Execution Time")
    cagg_plan_ms = cagg_plan.get("Planning Time")
    print(f"CAGG Query Planning Time:  {cagg_plan_ms:.3f} ms")
    print(f"CAGG Query Execution Time: {cagg_exec_ms:.3f} ms")

    speedup = raw_exec_ms / max(cagg_exec_ms, 0.001)
    print(f"\n⚡ DASHBOARD ACCELERATION FACTOR: {speedup:.1f}x FASTER!")

    results = {
        "raw_execution_time_ms": raw_exec_ms,
        "cagg_execution_time_ms": cagg_exec_ms,
        "speedup_factor": round(speedup, 1),
        "total_records": 25000
    }
    with open("/root/tiger-continuous-aggregates/benchmark_results.json", "w") as f:
        json.dump(results, f, indent=2)

    cur.close()
    conn.close()

if __name__ == "__main__":
    seed_sensor_telemetry(25000)
    run_benchmarks()
