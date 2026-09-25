# Observability Design

This document describes what the pipeline measures, how, and why.

## 1. Structured Logging

Every component (both simulated sources, the Spark speed layer, and the
Airflow batch DAG) logs in a consistent JSON shape:

```json
{"ts": "...", "level": "INFO", "component": "meter_producer", "msg": "..."}
```

Uniform format across `docker logs` for any container, useful for
local debugging and, in a production deployment, for feeding into a log
aggregator.

## 2. Metrics / Storage-backed Observability

Observability signals are written to PostgreSQL tables, exposed by the
serving API as JSON, and rendered live on the dashboard.

| Table | Written by | Purpose |
|---|---|---|
| `pipeline_health` | Spark speed layer, Airflow DAG | Per-component heartbeat: last-seen timestamp, status, detail string. Basis of the "no data received" health check. |
| `pipeline_alerts` | Spark speed layer, Airflow DAG | Append-only alert log: low-renewable-share, high-error-rate. Each row has severity, message, and a JSON `details` blob. |
| `dlq_events` | Spark speed layer | Dead-letter queue for Kafka messages that failed schema validation. |

## 3. Health Check: "No Data Received in N Minutes"

`GET /api/health` computes `seconds_since_heartbeat` per component and
flags `STALE` if it exceeds a **component-specific** threshold:

- `spark_streaming`: 120s (writes a heartbeat every 30s micro-batch; two
  missed cycles indicates the stream has genuinely gone quiet).
- `airflow_daily_dag`: 360s (only runs every 5 real minutes; a shorter
  threshold would falsely flag it as stale between scheduled runs).

A single fixed threshold across components would either produce false
positives on Airflow (constant "stale" between its normal runs) or be too
lenient to catch a genuinely stalled Spark job.

## 4. Alert Rules

| Alert type | Trigger | Severity | Raised by |
|---|---|---|---|
| `low_renewable_share` | A zone's renewable ratio (solar / consumption) in a window falls below `LOW_RENEWABLE_THRESHOLD` (default 5%) | WARNING | Spark speed layer |
| `high_error_rate` | A single micro-batch has ≥ `DLQ_ALERT_THRESHOLD` (default 5) malformed/unparseable Kafka messages | ERROR | Spark speed layer |

## 5. Ingestion Robustness (Dead-Letter Queue)

The speed layer splits every incoming Kafka message into two paths based
on schema validation (`meter_id IS NULL` after `from_json` indicates a
parse/schema failure):

- **Valid readings** flow into the normal processing pipeline (grid
  metrics windows, meter state, raw reading log).
- **Malformed events** are routed to `dlq_events` with their raw payload
  and reason, never silently dropped. A `high_error_rate` alert fires if
  malformed volume in one micro-batch crosses the threshold.

## 6. Where to See All of This

- **Dashboard** (`http://localhost:8001`): live view of health, zone grid
  metrics, recent alerts, and the daily billing table.
- **API docs** (`http://localhost:8001/docs`): interactive Swagger UI.
- **Container logs**: `docker logs -f grid-spark-streaming` /
  `docker logs -f grid-airflow`.
- **Airflow UI** (`http://localhost:8083`): task-level graph, logs, retry
  history for the batch DAG.
