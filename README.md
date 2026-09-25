# Smart Grid Pipeline — Energy Monitoring & Billing (Lambda Architecture)

A data engineering mini-project implementing an end-to-end **Lambda architecture**
pipeline for a utility company: live grid load / renewable-mix monitoring
via a streaming speed layer, and daily per-household billing reconciliation
via a batch layer — built on Kafka, Spark Structured Streaming, Airflow,
and PostgreSQL, fully containerized with Docker Compose.

## Architecture Summary

**Chosen architecture: Lambda.** Two heterogeneous data sources feed this
system: a continuous stream of smart-meter readings, and a daily batch
file of tariff/billing reference data. The two outputs (live grid metrics
vs. daily billing) have different consistency needs — live metrics can be
approximate and self-correcting, but a household's bill must be an
accurate, auditable computation. See `docs/architecture-decision.md`.

```
   meter_producer.py         ┌──────────────────┐
   (every 2s per meter)      │  Kafka            │
   ───────────────────────▶  │  meter_readings   │
                              └────────┬──────────┘
                                       │
                    ┌──────────────────┴──────────────────┐
                    ▼                                       ▼
        ┌──────────────────────┐                ┌───────────────────────────┐
        │  SPEED LAYER         │                │  raw_meter_readings (PG)  │
        │  Spark Structured    │───────────────▶│  (batch layer source      │
        │  Streaming           │                │   of truth)               │
        │                      │                └────────────┬──────────────┘
        │  • live_grid_metrics │                             │
        │  • meter_last_state  │                             ▼
        │  • pipeline_alerts   │                ┌───────────────────────────┐
        │  • dlq_events        │                │  BATCH LAYER              │
        │  • pipeline_health   │                │  Airflow DAG (every       │
        └──────────┬────────────┘                │  simulated day)          │
                   │                             │                            │
                   │                             │  joins raw_meter_readings │
                   │                             │  against the daily tariff │
                   │                             │  CSV → daily_household_   │
                   │                             │  billing                  │
                   │                             └────────────┬──────────────┘
                   │                                          │
                   └─────────────────────┬────────────────────┘
                                         ▼
                            ┌─────────────────────────┐
                            │  SERVING LAYER          │
                            │  FastAPI + dashboard    │
                            │  (localhost:8001)       │
                            └─────────────────────────┘

   tariff_batch_source.py
   (1 CSV per simulated day) ──────▶ data/raw/tariffs/ ──▶ read by Airflow
```

## Tech Stack

| Layer | Technology | Why |
|---|---|---|
| Ingestion (stream) | Apache Kafka | Ordered, replayable, partitioned per-meter event stream |
| Ingestion (batch) | Python script → CSV | Simulates a utility billing system's daily tariff export |
| Speed layer | Spark Structured Streaming | Windowed aggregation with watermarking |
| Batch layer | Airflow (TaskFlow API) | Scheduled, retryable, auditable daily recompute |
| Storage | PostgreSQL | Single queryable store for both live and batch outputs |
| Serving | FastAPI + static HTML/JS dashboard | Read-only REST API + lightweight live dashboard |
| Observability | Structured JSON logs, `pipeline_health`, `pipeline_alerts`, `dlq_events` | See `docs/observability.md` |

## Project Structure

```
smart-grid-pipeline/
├── docker-compose.yml          # Full stack: Kafka, Postgres, Spark, Airflow, serving API
├── postgres/init.sql           # Schema for all tables
├── sources/                    # Simulated data sources (Member A) — run locally
│   ├── meter_producer.py
│   ├── tariff_batch_source.py
│   └── requirements.txt
├── spark/                      # Speed layer (Member B)
│   ├── speed_layer.py
│   └── Dockerfile
├── airflow/                    # Batch layer (Member C)
│   └── dags/daily_billing_dag.py
├── serving/                    # Serving layer (Member C)
│   ├── api.py
│   ├── static/index.html
│   └── Dockerfile
└── data/                       # Runtime data (gitignored except folder structure)
    └── raw/tariffs/            # Daily tariff CSVs land here
```

## Setup & Run

### 1. Python environment for the sources

```bash
python -m venv venv
# Windows: .\venv\Scripts\Activate.ps1
# Mac/Linux: source venv/bin/activate

cd sources
pip install -r requirements.txt
cd ..
```

### 2. Bring up the full container stack

```bash
docker compose up -d --build
docker ps --filter "name=grid-"
```

Expect 5 healthy containers: `grid-kafka`, `grid-postgres`,
`grid-spark-streaming`, `grid-airflow`, `grid-serving-api`.

### 3. Start the simulated data sources

Terminal 1:
```bash
cd sources
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
python meter_producer.py
```

Terminal 2:
```bash
cd sources
$env:TARIFF_OUTPUT_DIR = "<full path to>/smart-grid-pipeline/data/raw/tariffs"
python tariff_batch_source.py
```

**Simulated clock:** 1 simulated day = 5 real minutes.

### 4. View the results

| What | URL |
|---|---|
| Live dashboard | http://localhost:8001 |
| API docs | http://localhost:8001/docs |
| Airflow UI | http://localhost:8083 |
| Postgres shell | `docker exec -it grid-postgres psql -U grid -d grid_db` |

## Assumptions & Simplifications

- Simulated time compression: 1 day = 5 real minutes.
- 20 households/meters, 4 fixed zones.
- Tariff rates and billing tiers are fabricated for simulation purposes
  only — not real utility pricing for any actual provider or region.
- Single-node Spark (`local[2]`) and single-node Kafka broker (KRaft mode).
- Airflow runs in `standalone` mode for simplicity.
- No authentication on the serving API (read-only, local-only scope).

## Documentation

- `docs/architecture-decision.md` — Lambda vs Kappa justification
- `docs/observability.md` — logging, metrics, alerting, DLQ design
