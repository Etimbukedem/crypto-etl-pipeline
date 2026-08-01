-- ============================================================
-- schema.sql
-- Target: PostgreSQL (primary). SQLite-compatible version noted
-- inline where syntax diverges — see comments marked [SQLITE].
-- ============================================================

-- Drop-and-recreate is commented out on purpose: never ship a
-- schema file that silently destroys production data.
-- DROP TABLE IF EXISTS crypto_market_data;

CREATE TABLE IF NOT EXISTS crypto_market_data (
    -- Surrogate key. [SQLITE]: replace SERIAL with
    -- INTEGER PRIMARY KEY AUTOINCREMENT and drop the PRIMARY KEY line below.
    id                              SERIAL PRIMARY KEY,

    -- Natural / business keys
    coin_id                         VARCHAR(100)    NOT NULL,   -- e.g. "bitcoin"
    symbol                          VARCHAR(20)     NOT NULL,   -- e.g. "btc"
    name                            VARCHAR(150)    NOT NULL,   -- e.g. "Bitcoin"

    -- Market metrics
    current_price_usd               NUMERIC(24, 10),
    market_cap_usd                   NUMERIC(28, 2),
    market_cap_rank                 INTEGER,
    total_volume_usd                 NUMERIC(28, 2),
    price_change_pct_24h            NUMERIC(10, 4),
    circulating_supply              NUMERIC(28, 2),
    total_supply                    NUMERIC(28, 2),
    ath_usd                          NUMERIC(24, 10),          -- all-time high

    -- Snapshot dimension: one row per coin per day
    snapshot_date                   DATE            NOT NULL,

    -- Audit columns
    created_at                      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at                      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- Idempotency guard: one record per coin per day, no matter
    -- how many times the pipeline reruns for that date.
    CONSTRAINT uq_coin_snapshot UNIQUE (coin_id, snapshot_date)
);

-- Helpful indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_crypto_snapshot_date
    ON crypto_market_data (snapshot_date);

CREATE INDEX IF NOT EXISTS idx_crypto_coin_id
    ON crypto_market_data (coin_id);

CREATE INDEX IF NOT EXISTS idx_crypto_market_cap_rank
    ON crypto_market_data (market_cap_rank);

-- ------------------------------------------------------------
-- Optional: a small pipeline_runs audit table. Shows employers
-- you think about observability, not just happy-path ETL.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id                  SERIAL PRIMARY KEY,      -- [SQLITE]: INTEGER PRIMARY KEY AUTOINCREMENT
    run_started_at      TIMESTAMPTZ NOT NULL,
    run_finished_at     TIMESTAMPTZ,
    status               VARCHAR(20) NOT NULL,   -- SUCCESS | FAILED
    rows_extracted       INTEGER,
    rows_loaded          INTEGER,
    error_message        TEXT
);

-- ------------------------------------------------------------
-- [SQLITE] equivalent CREATE TABLE, for local dev / cheap hosting:
--
-- CREATE TABLE IF NOT EXISTS crypto_market_data (
--     id                     INTEGER PRIMARY KEY AUTOINCREMENT,
--     coin_id                TEXT NOT NULL,
--     symbol                 TEXT NOT NULL,
--     name                   TEXT NOT NULL,
--     current_price_usd      REAL,
--     market_cap_usd         REAL,
--     market_cap_rank        INTEGER,
--     total_volume_usd       REAL,
--     price_change_pct_24h   REAL,
--     circulating_supply     REAL,
--     total_supply           REAL,
--     ath_usd                REAL,
--     snapshot_date          TEXT NOT NULL,
--     created_at             TEXT NOT NULL DEFAULT (datetime('now')),
--     updated_at             TEXT NOT NULL DEFAULT (datetime('now')),
--     UNIQUE (coin_id, snapshot_date)
-- );
-- ------------------------------------------------------------
