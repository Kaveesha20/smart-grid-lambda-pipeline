

from __future__ import annotations

import csv
import glob
import logging
import os
from datetime import datetime, timedelta

import psycopg2
import psycopg2.extras

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

logger = logging.getLogger("daily_billing_dag")

POSTGRES_HOST = os.environ.get("GRID_POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("GRID_POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("GRID_POSTGRES_DB", "grid_db")
POSTGRES_USER = os.environ.get("GRID_POSTGRES_USER", "grid")
POSTGRES_PASSWORD = os.environ.get("GRID_POSTGRES_PASSWORD", "grid_pw")
TARIFF_DIR = os.environ.get("GRID_TARIFF_DIR", "/opt/airflow/data/raw/tariffs")


def pg_connect():
    return psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD, connect_timeout=10,
    )


def log_health(component: str, status: str, detail: str):
    try:
        conn = pg_connect()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_health (component, last_heartbeat, status, detail)
                VALUES (%s, now(), %s, %s)
                ON CONFLICT (component) DO UPDATE
                SET last_heartbeat = now(), status = EXCLUDED.status, detail = EXCLUDED.detail
                """,
                (component, status, detail),
            )
        conn.close()
    except Exception as e:
        logger.warning(f"health heartbeat write failed (non-fatal): {e}")


def raise_alert(alert_type: str, severity: str, message: str, details: dict | None = None):
    try:
        conn = pg_connect()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_alerts (alert_type, severity, component, message, details)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (alert_type, severity, "airflow_daily_dag", message,
                 psycopg2.extras.Json(details or {})),
            )
        conn.close()
    except Exception as e:
        logger.warning(f"alert write failed (non-fatal): {e}")


@dag(
    dag_id="daily_household_billing",
    description="Batch layer: join daily consumption against tariff data per household",
    schedule="*/5 * * * *",  # every 5 real minutes = every simulated day
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
    },
    tags=["grid", "batch-layer", "billing"],
)
def daily_household_billing():

    @task
    def find_tariff_file(**context) -> str:
        """
        Find the most recent tariff CSV that hasn't been processed yet.
        Picking the newest file in the directory (rather than matching
        Airflow's own execution_date exactly) is robust to the sim-day /
        DAG-schedule cadences not lining up perfectly.
        """
        pattern = os.path.join(TARIFF_DIR, "tariffs_*.csv")
        files = sorted(glob.glob(pattern))
        if not files:
            msg = f"no tariff files found matching {pattern}"
            logger.warning(msg)
            raise_alert("tariff_file_missing", "WARNING", msg, {"pattern": pattern})
            log_health("airflow_daily_dag", "WARN", msg)
            raise AirflowException(msg)

        latest = files[-1]
        logger.info(f"using tariff file: {latest}")
        return latest

    @task
    def load_tariffs(filepath: str) -> list[dict]:
        rows = []
        with open(filepath, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append({
                    "household_id": row["household_id"],
                    "tariff_rate": float(row["tariff_rate"]),
                    "billing_tier": row["billing_tier"],
                    "subsidy_flag": row["subsidy_flag"],
                })
        logger.info(f"loaded {len(rows)} tariff records from {filepath}")
        return rows

    @task
    def extract_sim_date(filepath: str) -> str:
        # tariffs_YYYY-MM-DD.csv
        basename = os.path.basename(filepath)
        date_str = basename.replace("tariffs_", "").replace(".csv", "")
        return date_str

    @task
    def aggregate_consumption(sim_date: str) -> list[dict]:
        """Aggregate raw_meter_readings per household for the given sim_date."""
        conn = pg_connect()
        results = []
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT household_id,
                           COALESCE(SUM(power_consumption_kwh), 0) AS total_consumption_kwh,
                           COALESCE(SUM(solar_generation_kwh), 0) AS total_solar_kwh
                    FROM raw_meter_readings
                    WHERE event_time::date = %s::date
                    GROUP BY household_id
                    """,
                    (sim_date,),
                )
                for household_id, total_consumption, total_solar in cur.fetchall():
                    results.append({
                        "household_id": household_id,
                        "total_consumption_kwh": float(total_consumption),
                        "total_solar_kwh": float(total_solar),
                    })
        finally:
            conn.close()
        logger.info(f"aggregated consumption for {len(results)} households on {sim_date}")
        return results

    @task
    def reconcile_and_upsert(sim_date: str, tariffs: list[dict], consumption: list[dict]):
        consumption_by_household = {c["household_id"]: c for c in consumption}
        conn = pg_connect()
        upserted = 0
        try:
            with conn, conn.cursor() as cur:
                for t in tariffs:
                    hid = t["household_id"]
                    c = consumption_by_household.get(
                        hid, {"total_consumption_kwh": 0.0, "total_solar_kwh": 0.0}
                    )
                    net_consumption = max(0.0, c["total_consumption_kwh"] - c["total_solar_kwh"])
                    bill_amount = net_consumption * t["tariff_rate"]

                    cur.execute(
                        """
                        INSERT INTO daily_household_billing
                            (sim_date, household_id, total_consumption_kwh, total_solar_kwh,
                             net_consumption_kwh, tariff_rate, billing_tier, subsidy_flag,
                             bill_amount)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (sim_date, household_id) DO UPDATE SET
                            total_consumption_kwh = EXCLUDED.total_consumption_kwh,
                            total_solar_kwh = EXCLUDED.total_solar_kwh,
                            net_consumption_kwh = EXCLUDED.net_consumption_kwh,
                            tariff_rate = EXCLUDED.tariff_rate,
                            billing_tier = EXCLUDED.billing_tier,
                            subsidy_flag = EXCLUDED.subsidy_flag,
                            bill_amount = EXCLUDED.bill_amount,
                            processed_at = now()
                        """,
                        (sim_date, hid, c["total_consumption_kwh"], c["total_solar_kwh"],
                         net_consumption, t["tariff_rate"], t["billing_tier"],
                         t["subsidy_flag"], bill_amount),
                    )
                    upserted += 1
        finally:
            conn.close()

        msg = f"billed {upserted} households for {sim_date}"
        logger.info(msg)
        log_health("airflow_daily_dag", "OK", msg)

    tariff_file = find_tariff_file()
    sim_date = extract_sim_date(tariff_file)
    tariffs = load_tariffs(tariff_file)
    consumption = aggregate_consumption(sim_date)
    reconcile_and_upsert(sim_date, tariffs, consumption)


daily_household_billing()
