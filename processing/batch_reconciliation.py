"""Batch layer (Lambda's batch path), invoked once per simulated day by the Airflow DAG.

Reads:
  - the day's completed trip aggregates from `raw_telemetry` (written by speed_layer's
    raw archive, or re-derived here from stored events) -> per-vehicle distance/earnings
  - the day's cost file dropped by daily_cost_producer (./producers/drops/costs_day_N.json,
    or the `daily-cost` Kafka topic if you prefer to read it from there instead)

Joins them on vehicle_id and writes `vehicle_profitability` rows: a vehicle is flagged
`unprofitable` when (fuel_cost + maintenance_cost) > total_fare for that day.

Usage: spark-submit batch_reconciliation.py --sim-day 3 --cost-file /path/to/costs_day_0003.json
"""
import argparse
import json
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "common"))
from logging_config import get_logger  # noqa: E402

log = get_logger("processing.batch_reconciliation")

DB_URL = os.environ.get("PIPELINE_DB_URL", "postgresql://fleet:fleet@localhost:5432/fleet")
JDBC_URL = "jdbc:" + DB_URL.replace("postgresql://", "postgresql://")


def main(sim_day: int, cost_file: str):
    spark = (
        SparkSession.builder.appName("fleet-batch-reconciliation")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    log.info("batch reconciliation starting", extra={"fields": {"sim_day": sim_day, "cost_file": cost_file}})

    # 1. Day's trip aggregates, read back from the raw telemetry table the speed layer
    #    (or a separate raw-archive sink) writes every event to.
    trips = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "raw_telemetry")
        .option("user", "fleet").option("password", "fleet")
        .option("driver", "org.postgresql.Driver")
        .load()
        .filter(F.to_date(F.to_timestamp("timestamp")) == F.date_add(F.current_date(), -1 * (0)))
        # NOTE: replace the filter above with your actual sim-day -> calendar-day mapping;
        # for the demo, sim_day is usually easiest to store as its own column on ingest.
    )

    trip_agg = trips.groupBy("vehicle_id").agg(
        F.sum("fare").alias("total_fare"),
        F.count("*").alias("num_events"),
    )

    # 2. Day's cost file (meaningful join input, not pass-through)
    with open(cost_file) as f:
        cost_records = json.load(f)
    costs = spark.createDataFrame(cost_records)

    # 3. Join + profitability flag -- the meaningful transformation for this layer.
    joined = (
        trip_agg.join(costs, on="vehicle_id", how="outer")
        .fillna(0, subset=["total_fare", "fuel_cost", "maintenance_cost"])
        .withColumn("total_cost", F.col("fuel_cost") + F.col("maintenance_cost"))
        .withColumn("net_profit", F.col("total_fare") - F.col("total_cost"))
        .withColumn("unprofitable", F.col("net_profit") < 0)
        .withColumn("sim_day", F.lit(sim_day))
    )

    (
        joined.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "vehicle_profitability")
        .option("user", "fleet").option("password", "fleet")
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

    unprofitable_count = joined.filter(F.col("unprofitable")).count()
    log.info(
        "batch reconciliation complete",
        extra={"fields": {"sim_day": sim_day, "vehicles": joined.count(), "unprofitable": unprofitable_count}},
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sim-day", type=int, required=True)
    parser.add_argument("--cost-file", type=str, required=True)
    args = parser.parse_args()
    main(args.sim_day, args.cost_file)
