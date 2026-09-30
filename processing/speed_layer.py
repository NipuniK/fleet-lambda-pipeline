"""Speed layer (Lambda's real-time path).

Consumes the `telemetry` Kafka topic continuously with Spark Structured Streaming.
For each micro-batch, computes per-zone:
  - active_vehicles, idle_ratio, trips_in_window, total_fare
and writes the result into `fleet_live_metrics` in Postgres via foreachBatch.

This approach aggregates inside foreachBatch rather than using Spark's windowed
aggregation, which avoids watermark emission delays and gives immediate UI updates.
"""
import os
import sys
import time

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, StructField, StructType, DoubleType, BooleanType, IntegerType,
)

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "common"))
sys.path.append(os.path.join(os.path.dirname(__file__), "common"))
from logging_config import get_logger  # noqa: E402
from schemas import TELEMETRY_TOPIC  # noqa: E402

log = get_logger("processing.speed_layer")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
DB_URL = os.environ.get("PIPELINE_DB_URL", "postgresql://fleet:fleet@localhost:5432/fleet")

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

import urllib.parse
parsed = urllib.parse.urlparse(DB_URL)
JDBC_URL = f"jdbc:postgresql://{parsed.hostname}:{parsed.port}{parsed.path}"
JDBC_PROPS = {
    "user": "fleet",
    "password": "fleet",
    "driver": "org.postgresql.Driver",
}


def write_metrics_batch(batch_df: DataFrame, batch_id: int):
    """Aggregate per micro-batch by zone and write live metrics to Postgres."""
    count = batch_df.count()
    if count == 0:
        return

    now = time.time()
    window_start = now - 15
    window_end = now

    agg = (
        batch_df
        .groupBy("zone")
        .agg(
            F.approx_count_distinct("vehicle_id").alias("active_vehicles"),
            (F.sum(F.when(F.col("status") == "idle", 1).otherwise(0)) / F.count("*")).alias("idle_ratio"),
            F.approx_count_distinct(F.when(F.col("status") == "on_trip", F.col("trip_id"))).alias("trips_in_window"),
            F.sum("fare").alias("total_fare"),
        )
        .withColumn("window_start", F.lit(window_start).cast("timestamp"))
        .withColumn("window_end", F.lit(window_end).cast("timestamp"))
        .select("window_start", "window_end", "zone", "active_vehicles", "idle_ratio", "trips_in_window", "total_fare")
    )

    (
        agg.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "fleet_live_metrics")
        .option("user", "fleet")
        .option("password", "fleet")
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )
    log.info("metrics batch written", extra={"fields": {"batch_id": batch_id, "events": count}})


def write_raw_batch_to_postgres(batch_df: DataFrame, batch_id: int):
    """foreachBatch sink: append raw telemetry events to Postgres."""
    count = batch_df.count()
    if count == 0:
        return

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
    )

    log.info("speed layer streaming queries starting")

    # Write aggregated live metrics every 10 seconds
    metrics_query = (
        events.writeStream
        .outputMode("append")
        .foreachBatch(write_metrics_batch)
        .option("checkpointLocation", "/tmp/checkpoints/speed_layer_metrics")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # Write raw events for batch layer
    raw_query = (
        events.writeStream
        .outputMode("append")
        .foreachBatch(write_raw_batch_to_postgres)
        .option("checkpointLocation", "/tmp/checkpoints/speed_layer_raw")
        .trigger(processingTime="15 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
