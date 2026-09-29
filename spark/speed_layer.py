"""
MEMBER B -- Speed layer: Spark Structured Streaming job.

Consumes the `meter_readings` topic from Kafka, computes live grid load
and renewable-mix metrics over 1-minute tumbling windows, and writes
results to PostgreSQL using foreachBatch (Structured Streaming has no
native JDBC sink).

Tables written (see postgres/init.sql):
    - live_grid_metrics    windowed metrics per zone (active meters,
                            total consumption, total solar, net grid load,
                            renewable ratio)
    - meter_last_state     latest known reading per meter, upserted via
                            psycopg2 (meter_id is the primary key)
    - raw_meter_readings   append-only log of every valid reading --
                            the batch layer's source of truth for the
                            day's actual consumption per household
    - dlq_events           malformed/unparseable Kafka messages
    - pipeline_alerts      alert log (high error rate, low renewable share)
    - pipeline_health      heartbeat row updated every micro-batch

Architecture note (Lambda): this is the SPEED layer only. It reads the
live Kafka stream and produces fast, continuously-updating views. It
never reads the daily tariff/billing file -- that reconciliation happens
in the separate batch layer job (Airflow-orchestrated, Member C),
consistent with Lambda's separation of speed and batch layers.
"""

import logging
import os

import psycopg2
import psycopg2.extras

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, sum as spark_sum, when, current_timestamp, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

logging.basicConfig(
    level=logging.INFO,
    format='{"ts": "%(asctime)s", "level": "%(levelname)s", "component": "spark_streaming", "msg": "%(message)s"}',
)
logger = logging.getLogger("spark_streaming")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.environ.get("METER_TOPIC", "meter_readings")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "grid_db")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "grid")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "grid_pw")
PG_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

WINDOW_DURATION = os.environ.get("WINDOW_DURATION", "1 minute")
WATERMARK_DELAY = os.environ.get("WATERMARK_DELAY", "2 minutes")
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/tmp/spark-checkpoints")
LOW_RENEWABLE_THRESHOLD = float(os.environ.get("LOW_RENEWABLE_THRESHOLD", "0.05"))  # 5%
DLQ_ALERT_THRESHOLD = int(os.environ.get("DLQ_ALERT_THRESHOLD", "5"))

EVENT_SCHEMA = StructType([
    StructField("meter_id", StringType(), True),
    StructField("household_id", StringType(), True),
    StructField("power_consumption_kwh", DoubleType(), True),
    StructField("solar_generation_kwh", DoubleType(), True),
    StructField("grid_zone", StringType(), True),
    StructField("timestamp", StringType(), True),
])


def get_spark():
    return (
        SparkSession.builder
        .appName("GridSpeedLayer")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def pg_connect():
    return psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD, connect_timeout=5,
    )


def heartbeat(status, detail):
    """Best-effort health heartbeat; failures are logged, never raised, so a
    heartbeat problem never takes down the main streaming query."""
    try:
        conn = pg_connect()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_health (component, last_heartbeat, status, detail)
                VALUES ('spark_streaming', now(), %s, %s)
                ON CONFLICT (component) DO UPDATE
                SET last_heartbeat = now(), status = EXCLUDED.status, detail = EXCLUDED.detail
                """,
                (status, detail),
            )
        conn.close()
    except Exception as e:
        logger.warning(f"heartbeat write failed (non-fatal): {e}")


def jdbc_append(df, table_name):
    (
        df.write
        .format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", table_name)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )


def write_grid_metrics_batch(df, epoch_id):
    n = df.count()
    if n == 0:
        logger.info(f"batch {epoch_id}: no grid metrics rows, skipping")
        return
    try:
        jdbc_append(df, "live_grid_metrics")
        logger.info(f"batch {epoch_id}: wrote {n} rows to live_grid_metrics")

        # Low-renewable-share alert: check each zone's renewable_ratio in
        # this batch and raise an alert for any zone below threshold.
        rows = df.collect()
        low_renewable_zones = [r for r in rows if r["renewable_ratio"] is not None and r["renewable_ratio"] < LOW_RENEWABLE_THRESHOLD]
        if low_renewable_zones:
            conn = pg_connect()
            with conn, conn.cursor() as cur:
                for r in low_renewable_zones:
                    cur.execute(
                        """
                        INSERT INTO pipeline_alerts (alert_type, severity, component, message, details)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            "low_renewable_share", "WARNING", "spark_streaming",
                            f"Zone {r['zone']} renewable ratio {r['renewable_ratio']:.1%} below {LOW_RENEWABLE_THRESHOLD:.0%} threshold",
                            psycopg2.extras.Json({"zone": r["zone"], "renewable_ratio": r["renewable_ratio"]}),
                        ),
                    )
            conn.close()
            logger.info(f"batch {epoch_id}: raised {len(low_renewable_zones)} low-renewable-share alerts")

        heartbeat("OK", f"batch {epoch_id}: wrote {n} grid metrics rows")
    except Exception as e:
        logger.error(f"batch {epoch_id}: FAILED writing live_grid_metrics: {e}")
        heartbeat("ERROR", str(e))
        raise


def write_meter_state_and_readings_batch(df, epoch_id):
    """Upsert per-meter latest state via psycopg2 (meter_id is PK), and
    append every valid reading to raw_meter_readings (the batch layer's
    source of truth)."""
    rows = df.collect()
    if not rows:
        logger.info(f"batch {epoch_id}: no meter reading rows, skipping")
        return
    try:
        conn = pg_connect()
        with conn, conn.cursor() as cur:
            for r in rows:
                cur.execute(
                    """
                    INSERT INTO meter_last_state
                        (meter_id, household_id, grid_zone, last_consumption_kwh,
                         last_solar_kwh, last_event_ts, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, now())
                    ON CONFLICT (meter_id) DO UPDATE SET
                        household_id = EXCLUDED.household_id,
                        grid_zone = EXCLUDED.grid_zone,
                        last_consumption_kwh = EXCLUDED.last_consumption_kwh,
                        last_solar_kwh = EXCLUDED.last_solar_kwh,
                        last_event_ts = EXCLUDED.last_event_ts,
                        updated_at = now()
                    """,
                    (r.meter_id, r.household_id, r.grid_zone,
                     r.power_consumption_kwh, r.solar_generation_kwh, r.event_time),
                )
                cur.execute(
                    """
                    INSERT INTO raw_meter_readings
                        (meter_id, household_id, grid_zone, power_consumption_kwh,
                         solar_generation_kwh, event_time)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (r.meter_id, r.household_id, r.grid_zone,
                     r.power_consumption_kwh, r.solar_generation_kwh, r.event_time),
                )
        conn.close()
        logger.info(f"batch {epoch_id}: upserted {len(rows)} meter states and appended {len(rows)} raw readings")
        heartbeat("OK", f"batch {epoch_id}: {len(rows)} readings processed")
    except Exception as e:
        logger.error(f"batch {epoch_id}: FAILED updating meter_last_state/raw_meter_readings: {e}")
        heartbeat("ERROR", str(e))
        raise


def write_dlq_batch(df, epoch_id):
    """Write malformed events to the DLQ and raise an alert if the error
    volume in this micro-batch crosses a threshold."""
    n = df.count()
    if n == 0:
        return
    try:
        jdbc_append(df, "dlq_events")
        logger.warning(f"batch {epoch_id}: {n} malformed events routed to DLQ")
        if n >= DLQ_ALERT_THRESHOLD:
            try:
                conn = pg_connect()
                with conn, conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO pipeline_alerts (alert_type, severity, component, message, details)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            "high_error_rate", "ERROR", "spark_streaming",
                            f"{n} malformed messages in one micro-batch (threshold: {DLQ_ALERT_THRESHOLD})",
                            psycopg2.extras.Json({"malformed_count": n, "batch": epoch_id}),
                        ),
                    )
                conn.close()
            except Exception as e:
                logger.warning(f"DLQ alert write failed (non-fatal): {e}")
        heartbeat("OK" if n < DLQ_ALERT_THRESHOLD else "WARN", f"batch {epoch_id}: {n} malformed events")
    except Exception as e:
        logger.error(f"batch {epoch_id}: FAILED writing dlq_events: {e}")
        heartbeat("ERROR", str(e))
        raise


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"starting speed layer: topic={TOPIC}, bootstrap={KAFKA_BOOTSTRAP}, pg={PG_URL}")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_raw = (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
        .withColumn("data", from_json(col("json_str"), EVENT_SCHEMA))
    )

    # A message that fails schema validation parses to a row of all-null
    # fields -- meter_id is required in every legitimate event, so its
    # absence flags a bad record.
    malformed = parsed_raw.filter(col("data.meter_id").isNull()).select(
        lit(TOPIC).alias("source_topic"),
        col("json_str").alias("raw_payload"),
        lit("schema_validation_failed").alias("error_reason"),
    )

    malformed_query = (
        malformed.writeStream
        .outputMode("append")
        .foreachBatch(write_dlq_batch)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/dlq")
        .trigger(processingTime="30 seconds")
        .start()
    )

    parsed = (
        parsed_raw.filter(col("data.meter_id").isNotNull())
        .select("data.*")
        .withColumn("event_time", col("timestamp").cast(TimestampType()))
        .withWatermark("event_time", WATERMARK_DELAY)
    )

    # ---- 1. Live grid metrics per zone, tumbling windows ----
    grid_metrics = (
        parsed.groupBy(window(col("event_time"), WINDOW_DURATION), col("grid_zone"))
        .agg(
            count("meter_id").alias("active_meters"),
            spark_sum("power_consumption_kwh").alias("total_consumption_kwh"),
            spark_sum("solar_generation_kwh").alias("total_solar_kwh"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("grid_zone").alias("zone"),
            col("active_meters"),
            col("total_consumption_kwh"),
            col("total_solar_kwh"),
            (col("total_consumption_kwh") - col("total_solar_kwh")).alias("net_grid_load_kwh"),
            (col("total_solar_kwh") / col("total_consumption_kwh")).alias("renewable_ratio"),
            current_timestamp().alias("ingested_at"),
        )
    )

    grid_metrics_query = (
        grid_metrics.writeStream
        .outputMode("update")
        .foreachBatch(write_grid_metrics_batch)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/grid_metrics")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # ---- 2. Per-meter latest-state upsert + raw reading log ----
    meter_snapshot = parsed.select(
        col("meter_id"), col("household_id"), col("grid_zone"),
        col("power_consumption_kwh"), col("solar_generation_kwh"),
        col("event_time"),
    )

    meter_state_query = (
        meter_snapshot.writeStream
        .outputMode("append")
        .foreachBatch(write_meter_state_and_readings_batch)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/meter_state")
        .trigger(processingTime="30 seconds")
        .start()
    )

    logger.info(
        "speed layer streaming queries started: live_grid_metrics, "
        "meter_last_state/raw_meter_readings, dlq_events"
    )
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
