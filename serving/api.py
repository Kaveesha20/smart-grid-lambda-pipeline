
import os
from datetime import date, datetime
from typing import Optional

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "grid_db")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "grid")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "grid_pw")

app = FastAPI(
    title="Smart Grid Pipeline Serving API",
    description="Read-only API over the Lambda-architecture smart grid data pipeline",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def pg_connect():
    return psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD, connect_timeout=5,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


def to_jsonable(rows):
    out = []
    for row in rows:
        d = dict(row)
        for k, v in d.items():
            if isinstance(v, (datetime, date)):
                d[k] = v.isoformat()
        out.append(d)
    return out


@app.get("/api/health")
def get_health():
    """Pipeline component heartbeats -- the basis of the 'no data received' health check."""
    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT component, last_heartbeat, status, detail,
                       EXTRACT(EPOCH FROM (now() - last_heartbeat)) AS seconds_since_heartbeat
                FROM pipeline_health
                ORDER BY component
                """
            )
            rows = to_jsonable(cur.fetchall())
    finally:
        conn.close()


    STALE_THRESHOLD_SECONDS = {
        "airflow_daily_dag": 360,
        "spark_streaming": 120,
    }
    for r in rows:
        threshold = STALE_THRESHOLD_SECONDS.get(r["component"], 120)
        if r["seconds_since_heartbeat"] is not None and r["seconds_since_heartbeat"] > threshold:
            r["status"] = "STALE"

    overall = "HEALTHY" if all(r["status"] in ("OK", "HEALTHY") for r in rows) else "DEGRADED"
    return {"overall_status": overall, "components": rows}


@app.get("/api/grid/live")
def get_live_grid_metrics(limit: int = Query(20, ge=1, le=200)):
    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT window_start, window_end, zone, active_meters,
                       total_consumption_kwh, total_solar_kwh,
                       net_grid_load_kwh, renewable_ratio
                FROM live_grid_metrics
                ORDER BY window_start DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = to_jsonable(cur.fetchall())
    finally:
        conn.close()
    return {"count": len(rows), "windows": rows}


@app.get("/api/grid/by-zone")
def get_grid_by_zone(minutes: int = Query(10, ge=1, le=1440)):
    """Grid metrics summed/averaged over the last N minutes, one row per zone --
    this is what the dashboard's zone cards render."""
    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT zone,
                       AVG(active_meters) AS avg_active_meters,
                       SUM(total_consumption_kwh) AS total_consumption_kwh,
                       SUM(total_solar_kwh) AS total_solar_kwh,
                       SUM(net_grid_load_kwh) AS net_grid_load_kwh,
                       AVG(renewable_ratio) AS avg_renewable_ratio
                FROM live_grid_metrics
                WHERE window_start >= now() - (%s || ' minutes')::interval
                GROUP BY zone
                ORDER BY zone
                """,
                (minutes,),
            )
            rows = to_jsonable(cur.fetchall())
    finally:
        conn.close()
    return {"window_minutes": minutes, "zones": rows}


@app.get("/api/meters")
def get_meters():
    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT meter_id, household_id, grid_zone, last_consumption_kwh,
                       last_solar_kwh, last_event_ts, updated_at
                FROM meter_last_state
                ORDER BY meter_id
                """
            )
            rows = to_jsonable(cur.fetchall())
    finally:
        conn.close()
    return {"count": len(rows), "meters": rows}


@app.get("/api/alerts")
def get_alerts(limit: int = Query(50, ge=1, le=500), unresolved_only: bool = Query(False)):
    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            where_clause = "WHERE resolved_at IS NULL" if unresolved_only else ""
            cur.execute(
                f"""
                SELECT id, alert_type, severity, component, message, details,
                       triggered_at, resolved_at
                FROM pipeline_alerts
                {where_clause}
                ORDER BY triggered_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = to_jsonable(cur.fetchall())
    finally:
        conn.close()
    return {"count": len(rows), "alerts": rows}


@app.get("/api/dlq")
def get_dlq_events(limit: int = Query(50, ge=1, le=500)):
    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, source_topic, error_reason, ingested_at
                FROM dlq_events
                ORDER BY ingested_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = to_jsonable(cur.fetchall())
    finally:
        conn.close()
    return {"count": len(rows), "events": rows}


@app.get("/api/billing/dates")
def get_billing_dates():
    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT sim_date FROM daily_household_billing ORDER BY sim_date DESC"
            )
            rows = to_jsonable(cur.fetchall())
    finally:
        conn.close()
    return {"dates": [r["sim_date"] for r in rows]}


@app.get("/api/billing/daily")
def get_daily_billing(sim_date: Optional[str] = Query(None)):
    """Per-household bill for a given sim_date (defaults to the most recent)."""
    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            if sim_date is None:
                cur.execute("SELECT MAX(sim_date) AS d FROM daily_household_billing")
                latest = cur.fetchone()
                if not latest or not latest["d"]:
                    return {"sim_date": None, "households": []}
                sim_date = latest["d"].isoformat()

            cur.execute(
                """
                SELECT sim_date, household_id, total_consumption_kwh, total_solar_kwh,
                       net_consumption_kwh, tariff_rate, billing_tier, subsidy_flag,
                       bill_amount, processed_at
                FROM daily_household_billing
                WHERE sim_date = %s
                ORDER BY bill_amount DESC
                """,
                (sim_date,),
            )
            rows = to_jsonable(cur.fetchall())
    finally:
        conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail=f"no billing data for {sim_date}")

    total_billed = sum(r["bill_amount"] for r in rows)
    return {
        "sim_date": sim_date,
        "household_count": len(rows),
        "total_billed": round(total_billed, 2),
        "households": rows,
    }


STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
def dashboard():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))
