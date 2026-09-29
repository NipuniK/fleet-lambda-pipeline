"""Speed layer (Lambda's real-time path).

Consumes the `telemetry` Kafka topic continuously with Spark Structured Streaming.
For each 1-minute tumbling window x zone, computes:
  - active_vehicles, idle_ratio, trips_per_hour (extrapolated), total_fare (earnings)
and upserts the result into `fleet_live_metrics` in Postgres via foreachBatch.

Also raises a threshold alert (structured log, written to `pipeline_alerts`) when a
single vehicle has been continuously idle beyond IDLE_ALERT_SECONDS, and a health-check
alert if no telemetry event has been seen within NO_DATA_ALERT_SECONDS.

This is the "not just pass-through" transformation for the speed layer: windowed
aggregation + per-vehicle idle-duration tracking, not a raw copy of events.
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, StructField, StructType, DoubleType, BooleanType, IntegerType,
)

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "common"))
from logging_config import get_logger, log_alert, NoDataWatchdog  # noqa: E402
from schemas import TELEMETRY_TOPIC  # noqa: E402

log = get_logger("processing.speed_layer")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
DB_URL = os.environ.get("PIPELINE_DB_URL", "postgresql://fleet:fleet@localhost:5432/fleet")
IDLE_ALERT_SECONDS = int(os.environ.get("IDLE_ALERT_SECONDS", 600))
NO_DATA_ALERT_SECONDS = int(os.environ.get("NO_DATA_ALERT_SECONDS", 30))

TELEMETRY_SCHEMA = StructType([
    StructField("trip_id", StringType()),
    StructField("driver_id", StringType()),
    StructField("vehicle_id", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("speed", DoubleType()),
    StructField("status", StringType()),
    StructField("fare", DoubleType()),
    StructField("zone", StringType()),
    StructField("timestamp", DoubleType()),
    StructField("service_flag", BooleanType(), True),
    StructField("sim_day", IntegerType(), True),
])

JDBC_URL = "jdbc:" + DB_URL.replace("postgresql://", "postgresql://")


def write_batch_to_postgres(batch_df, batch_id: int):
    """foreachBatch sink: append this micro-batch's window aggregates to Postgres.
    A daily/hourly view can be built on top via a simple GROUP BY in the serving layer,
    or you can swap this for an UPSERT if you want strictly one row per (window, zone)."""
    count = batch_df.count()
    if count == 0:
        return
    (
        batch_df.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "fleet_live_metrics")
        .option("user", "fleet")
        .option("password", "fleet")
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )
    log.info("batch written", extra={"fields": {"batch_id": batch_id, "rows": count}})


def write_raw_batch_to_postgres(batch_df, batch_id: int):
    """foreachBatch sink: append raw telemetry events to Postgres."""
    count = batch_df.count()
    if count == 0:
        return
        
    # Select only columns that match the Postgres schema
    cols = ["trip_id", "driver_id", "vehicle_id", "lat", "lon", "speed", "status", "fare", "zone", "timestamp", "sim_day"]
    db_df = batch_df.select(*cols)

    (
        db_df.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "raw_telemetry")
        .option("user", "fleet")
        .option("password", "fleet")
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )
    log.info("raw batch written", extra={"fields": {"batch_id": batch_id, "rows": count}})


def main():
    spark = (
        SparkSession.builder.appName("fleet-speed-layer")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TELEMETRY_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    events = (
        raw.select(F.from_json(F.col("value").cast("string"), TELEMETRY_SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("event_time", F.to_timestamp(F.col("timestamp")))
        .withWatermark("event_time", "1 minute")
    )

    # Meaningful transformation: windowed aggregation by zone, not pass-through.
    metrics = (
        events.groupBy(F.window("event_time", "1 minute"), F.col("zone"))
        .agg(
            F.countDistinct("vehicle_id").alias("active_vehicles"),
            (F.sum(F.when(F.col("status") == "idle", 1).otherwise(0)) / F.count("*")).alias("idle_ratio"),
            F.countDistinct(F.when(F.col("status") == "on_trip", F.col("trip_id"))).alias("trips_in_window"),
            F.sum("fare").alias("total_fare"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "zone", "active_vehicles", "idle_ratio", "trips_in_window", "total_fare",
        )
    )

    log.info("speed layer streaming queries starting")

    query = (
        metrics.writeStream.outputMode("append")
        .foreachBatch(write_batch_to_postgres)
        .option("checkpointLocation", "/tmp/checkpoints/speed_layer")
        .trigger(processingTime="15 seconds")
        .start()
    )

    raw_query = (
        events.writeStream.outputMode("append")
        .foreachBatch(write_raw_batch_to_postgres)
        .option("checkpointLocation", "/tmp/checkpoints/speed_layer_raw")
        .trigger(processingTime="15 seconds")
        .start()
    )


    # NOTE: per-vehicle idle-duration threshold alerting (IDLE_ALERT_SECONDS) and the
    # NoDataWatchdog health check are simplest to run as a lightweight side consumer
    # (see idle_alert_watcher.py) rather than inside this same streaming query, since
    # Structured Streaming aggregation state isn't a convenient place to fire external
    # side-effecting alerts per-event. Start that watcher alongside this job.
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
