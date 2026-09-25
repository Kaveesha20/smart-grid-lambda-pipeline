-- Smart Grid pipeline database schema.
-- Applied automatically on first Postgres container startup (mounted into
-- /docker-entrypoint-initdb.d/) or run manually via psql if the volume
-- already existed before this file was added.

-- ============================================================
-- SPEED LAYER (Spark writes here)
-- ============================================================

CREATE TABLE IF NOT EXISTS live_grid_metrics (
    id                  BIGSERIAL PRIMARY KEY,
    window_start        TIMESTAMP NOT NULL,
    window_end          TIMESTAMP NOT NULL,
    zone                VARCHAR(64) NOT NULL,
    active_meters       BIGINT,
    total_consumption_kwh DOUBLE PRECISION,
    total_solar_kwh     DOUBLE PRECISION,
    net_grid_load_kwh   DOUBLE PRECISION,
    renewable_ratio     DOUBLE PRECISION,
    ingested_at         TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_live_grid_metrics_window ON live_grid_metrics (window_start DESC);
CREATE INDEX IF NOT EXISTS idx_live_grid_metrics_zone ON live_grid_metrics (zone);

-- Latest known reading per meter, upserted by Spark so consumption/solar
-- trends and "no reading received" per-meter checks are possible.
CREATE TABLE IF NOT EXISTS meter_last_state (
    meter_id             VARCHAR(32) PRIMARY KEY,
    household_id         VARCHAR(32),
    grid_zone            VARCHAR(64),
    last_consumption_kwh DOUBLE PRECISION,
    last_solar_kwh       DOUBLE PRECISION,
    last_event_ts        TIMESTAMP,
    updated_at           TIMESTAMP NOT NULL DEFAULT now()
);

-- Raw per-meter readings, appended by Spark. This is the batch layer's
-- source of truth for "how much did each household actually consume
-- today" -- Airflow aggregates this and joins it against the tariff CSV.
CREATE TABLE IF NOT EXISTS raw_meter_readings (
    id                     BIGSERIAL PRIMARY KEY,
    meter_id               VARCHAR(32) NOT NULL,
    household_id           VARCHAR(32) NOT NULL,
    grid_zone              VARCHAR(64),
    power_consumption_kwh  DOUBLE PRECISION,
    solar_generation_kwh   DOUBLE PRECISION,
    event_time             TIMESTAMP NOT NULL,
    ingested_at            TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_raw_meter_readings_household_date
    ON raw_meter_readings (household_id, (event_time::date));
CREATE INDEX IF NOT EXISTS idx_raw_meter_readings_time ON raw_meter_readings (event_time);

-- ============================================================
-- BATCH LAYER (Airflow writes here)
-- ============================================================

CREATE TABLE IF NOT EXISTS daily_household_billing (
    id                  BIGSERIAL PRIMARY KEY,
    sim_date            DATE NOT NULL,
    household_id        VARCHAR(32) NOT NULL,
    total_consumption_kwh DOUBLE PRECISION,
    total_solar_kwh     DOUBLE PRECISION,
    net_consumption_kwh DOUBLE PRECISION,   -- consumption minus solar offset
    tariff_rate         DOUBLE PRECISION,
    billing_tier        VARCHAR(32),
    subsidy_flag        VARCHAR(4),
    bill_amount         DOUBLE PRECISION,
    processed_at         TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (sim_date, household_id)
);
CREATE INDEX IF NOT EXISTS idx_daily_billing_date ON daily_household_billing (sim_date DESC);

-- ============================================================
-- OBSERVABILITY (shared across speed + batch layers)
-- ============================================================

CREATE TABLE IF NOT EXISTS pipeline_health (
    component       VARCHAR(64) PRIMARY KEY,
    last_heartbeat  TIMESTAMP NOT NULL DEFAULT now(),
    status          VARCHAR(16) NOT NULL DEFAULT 'OK',
    detail          TEXT
);

CREATE TABLE IF NOT EXISTS pipeline_alerts (
    id            BIGSERIAL PRIMARY KEY,
    alert_type    VARCHAR(64) NOT NULL,
    severity      VARCHAR(16) NOT NULL,   -- WARNING / ERROR
    component     VARCHAR(64) NOT NULL,
    message       TEXT,
    details       JSONB,
    triggered_at  TIMESTAMP NOT NULL DEFAULT now(),
    resolved_at   TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_pipeline_alerts_time ON pipeline_alerts (triggered_at DESC);

CREATE TABLE IF NOT EXISTS dlq_events (
    id           BIGSERIAL PRIMARY KEY,
    source_topic VARCHAR(128) NOT NULL,
    raw_payload  TEXT,
    error_reason VARCHAR(256),
    ingested_at  TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_dlq_events_time ON dlq_events (ingested_at DESC);
