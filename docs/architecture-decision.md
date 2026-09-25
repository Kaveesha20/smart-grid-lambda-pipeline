# Architecture Decision: Lambda vs Kappa

## Use Case

**Smart Grid Energy Monitoring & Billing.** A utility company wants
real-time visibility into grid load and renewable (solar) contribution
from smart meters, reconciled daily against tariff and billing data
produced once a day.

Two data sources:
1. **Streaming**: smart-meter readings emitted every few seconds
   (`meter_id`, `household_id`, `power_consumption_kwh`,
   `solar_generation_kwh`, `grid_zone`, `timestamp`).
2. **Daily batch**: one CSV per simulated day of tariff/billing reference
   data (`household_id`, `tariff_rate`, `billing_tier`, `subsidy_flag`).

Business question: *What is the current grid load and renewable
contribution by zone, and what will each household's bill look like once
daily tariff data is applied to their consumption?*

## Decision: Lambda Architecture

### Why not Kappa

Kappa unifies all processing around a single stream, with batch views
produced by replaying that stream — appropriate when there is one true
source of data that both live and historical views derive from.

This use case does not have that property. The two sources are
**genuinely heterogeneous**:

- The meter stream is naturally continuous, per-household telemetry.
- The tariff/billing data is a **daily batch extract from the utility's
  own billing system** — it has no natural streaming form. It represents
  a reference-data snapshot valid for one day, not an event sequence.

Forcing the tariff file into Kafka just to unify under Kappa would gain
nothing: tariff rates don't arrive as a stream of individual events in
any real system, and doing so would blur the distinction between "live
grid state" and "the billing figures for a closed day" — a distinction
this business question explicitly needs (a bill must reflect one day's
tariff, not a replayed/re-interpreted stream).

### Why Lambda fits

| | Speed layer | Batch layer |
|---|---|---|
| **Input** | `meter_readings` Kafka stream | `raw_meter_readings` (accumulated from the stream) + daily tariff CSV |
| **Output** | `live_grid_metrics` (load, renewable ratio per zone), meter state, alerts | `daily_household_billing` (per-household bill) |
| **Latency** | Seconds (30s micro-batch trigger) | Once per simulated day |
| **Correctness model** | Approximate, continuously self-correcting | Full daily recompute — auditable, reproducible |
| **Failure mode** | A missed window is superseded within 30s | A failed run retries (Airflow: 3 retries) and is fully re-runnable |

The consistency requirements genuinely differ: a slightly stale live grid
load figure for one 1-minute window is a non-issue — the next window
corrects it. A household's bill is not something you want computed
incrementally from partial streaming state; it should be a clean,
auditable join over the full day's consumption, recomputed from source
each time, exactly what Lambda's batch layer provides.

### Rejected Alternative: Pure streaming reconciliation

Could billing be computed as a stateful streaming join instead of a daily
batch job? Rejected because the tariff file is not a stream — it is one
atomic daily snapshot. Treating it as streaming input would require
managing long-lived join state for a source with no continuous
throughput, adding complexity without benefit. A daily batch join is
trivially replayable (re-run the DAG, get the same answer), which matters
for a billing computation that customers may dispute and that must be
auditable.

## Trade-offs and Limitations

- **Aggregation-logic duplication** between the speed layer (windowed
  zone metrics) and batch layer (per-household daily join) — a known
  Lambda criticism, minimal at this project's scale.
- **Two systems to operate** (Spark + Airflow) instead of one processing
  paradigm — mitigated by shared observability (`pipeline_health`,
  `pipeline_alerts` used by both layers).
- **Eventual-consistency window**: the batch layer depends on
  `raw_meter_readings` being fully written by the speed layer before the
  batch job runs — mitigated by the batch job's 5-minute schedule being
  well-spaced from the 30-second micro-batch writes.

## What Would Change at Production Scale

- Kafka: multiple brokers, higher partition count, replication factor > 1.
- Spark: multi-executor cluster instead of `local[2]`.
- Airflow: split `standalone` into separate webserver/scheduler/worker
  containers with `CeleryExecutor` or `KubernetesExecutor`.
- Postgres: a dedicated time-series store for grid load metrics if query
  volume grew significantly.
- Add a Schema Registry for the Kafka topic instead of an inline schema
  definition in the Spark job.
- Real tariff-tier logic would come from the utility's actual billing
  system rather than randomly generated reference data.
