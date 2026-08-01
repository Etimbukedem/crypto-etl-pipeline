"""
etl.py
End-to-end ETL pipeline: CoinGecko API -> pandas transform -> PostgreSQL/SQLite.

Design notes:
- Works against SQLite out of the box (zero setup) and PostgreSQL if
  DATABASE_URL is set (e.g. in GitHub Actions secrets / Railway / Supabase).
- Idempotent: reruns for the same day UPSERT instead of duplicating rows.
- Every run is logged to both stdout and logs/etl.log, and recorded in
  the pipeline_runs audit table.

Usage:
    python etl.py
    python etl.py --pages 2          # fetch more than 250 coins
    python etl.py --db sqlite:///data/crypto.db
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

API_URL = "https://api.coingecko.com/api/v3/coins/markets"
DEFAULT_DB_URL = "sqlite:///data/crypto.db"
DEFAULT_PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 250,
    "page": 1,
    "sparkline": "false",
    "price_change_percentage": "24h",
}
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5
REQUEST_TIMEOUT_SECONDS = 15

os.makedirs("logs", exist_ok=True)
os.makedirs("data", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.FileHandler("logs/etl.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("crypto_etl")


# --------------------------------------------------------------------------
# EXTRACT
# --------------------------------------------------------------------------

def extract(page: int = 1, per_page: int = 250) -> list[dict]:
    """
    Pull raw market data from the CoinGecko API with retry + backoff.
    Returns the raw JSON payload (list of coin dicts).
    """
    params = {**DEFAULT_PARAMS, "page": page, "per_page": per_page}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"Extract attempt {attempt}/{MAX_RETRIES} (page={page})")
            response = requests.get(API_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)

            if response.status_code == 429:
                wait = RETRY_BACKOFF_SECONDS * attempt
                logger.warning(f"Rate limited (429). Backing off {wait}s.")
                time.sleep(wait)
                continue

            response.raise_for_status()
            payload = response.json()

            if not isinstance(payload, list) or len(payload) == 0:
                raise ValueError("API returned an empty or unexpected payload.")

            logger.info(f"Extracted {len(payload)} records.")
            return payload

        except requests.exceptions.Timeout:
            logger.warning(f"Request timed out (attempt {attempt}).")
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"Connection error (attempt {attempt}): {e}")
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error (attempt {attempt}): {e}")
        except (ValueError, requests.exceptions.RequestException) as e:
            logger.error(f"Extract failed (attempt {attempt}): {e}")

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)

    raise RuntimeError(f"Extraction failed after {MAX_RETRIES} attempts.")


# --------------------------------------------------------------------------
# TRANSFORM
# --------------------------------------------------------------------------

def transform(raw_data: list[dict]) -> pd.DataFrame:
    """
    Clean and reshape the raw API payload into a DataFrame matching
    the crypto_market_data schema.
    """
    df = pd.DataFrame(raw_data)

    column_map = {
        "id": "coin_id",
        "symbol": "symbol",
        "name": "name",
        "current_price": "current_price_usd",
        "market_cap": "market_cap_usd",
        "market_cap_rank": "market_cap_rank",
        "total_volume": "total_volume_usd",
        "price_change_percentage_24h": "price_change_pct_24h",
        "circulating_supply": "circulating_supply",
        "total_supply": "total_supply",
        "ath": "ath_usd",
    }

    missing = [c for c in column_map if c not in df.columns]
    if missing:
        logger.warning(f"API response missing expected columns: {missing}")

    df = df[[c for c in column_map if c in df.columns]].rename(columns=column_map)

    # Type cleanup
    numeric_cols = [
        "current_price_usd", "market_cap_usd", "total_volume_usd",
        "price_change_pct_24h", "circulating_supply", "total_supply", "ath_usd",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "market_cap_rank" in df.columns:
        df["market_cap_rank"] = pd.to_numeric(df["market_cap_rank"], errors="coerce").astype("Int64")

    df["symbol"] = df["symbol"].str.upper().str.strip()
    df["name"] = df["name"].str.strip()

    # Drop rows with no usable identity or price — junk data, not real coins
    before = len(df)
    df = df.dropna(subset=["coin_id", "current_price_usd"])
    dropped = before - len(df)
    if dropped:
        logger.info(f"Dropped {dropped} rows with missing coin_id/price.")

    # De-dupe defensively (API shouldn't repeat, but pipelines should assume it might)
    df = df.drop_duplicates(subset=["coin_id"])

    # Snapshot dimension — same value for every row in this run
    snapshot_date = datetime.now(timezone.utc).date().isoformat()
    df["snapshot_date"] = snapshot_date

    now_iso = datetime.now(timezone.utc).isoformat()
    df["created_at"] = now_iso
    df["updated_at"] = now_iso

    logger.info(f"Transformed {len(df)} clean rows for snapshot_date={snapshot_date}.")
    return df


# --------------------------------------------------------------------------
# LOAD
# --------------------------------------------------------------------------

def get_engine(db_url: str):
    return create_engine(db_url)


def load(df: pd.DataFrame, engine) -> int:
    """
    Idempotent UPSERT into crypto_market_data, keyed on (coin_id, snapshot_date).
    Works for both PostgreSQL and SQLite via each dialect's native
    "ON CONFLICT" upsert syntax.
    """
    is_sqlite = engine.url.get_backend_name() == "sqlite"
    rows_loaded = 0

    columns = [
        "coin_id", "symbol", "name", "current_price_usd", "market_cap_usd",
        "market_cap_rank", "total_volume_usd", "price_change_pct_24h",
        "circulating_supply", "total_supply", "ath_usd", "snapshot_date",
        "created_at", "updated_at",
    ]
    columns = [c for c in columns if c in df.columns]

    if is_sqlite:
        insert_cols = ", ".join(columns)
        placeholders = ", ".join(f":{c}" for c in columns)
        update_cols = ", ".join(f"{c} = excluded.{c}" for c in columns if c not in ("coin_id", "snapshot_date", "created_at"))
        upsert_sql = text(f"""
            INSERT INTO crypto_market_data ({insert_cols})
            VALUES ({placeholders})
            ON CONFLICT (coin_id, snapshot_date)
            DO UPDATE SET {update_cols}
        """)
    else:  # PostgreSQL
        insert_cols = ", ".join(columns)
        placeholders = ", ".join(f":{c}" for c in columns)
        update_cols = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c not in ("coin_id", "snapshot_date", "created_at"))
        upsert_sql = text(f"""
            INSERT INTO crypto_market_data ({insert_cols})
            VALUES ({placeholders})
            ON CONFLICT (coin_id, snapshot_date)
            DO UPDATE SET {update_cols}, updated_at = EXCLUDED.updated_at
        """)

    records = df[columns].to_dict(orient="records")

    with engine.begin() as conn:
        for record in records:
            try:
                conn.execute(upsert_sql, record)
                rows_loaded += 1
            except SQLAlchemyError as e:
                logger.error(f"Failed to load row for coin_id={record.get('coin_id')}: {e}")

    logger.info(f"Loaded/updated {rows_loaded} rows into crypto_market_data.")
    return rows_loaded


def ensure_schema(engine):
    """Create tables if they don't exist yet (safe on every run)."""
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    is_sqlite = engine.url.get_backend_name() == "sqlite"

    with open(schema_path, "r") as f:
        raw_sql = f.read()

    if is_sqlite:
        # Use the SQLite-flavored statements defined inline here rather than
        # parsing the Postgres-flavored schema.sql (SERIAL, TIMESTAMPTZ, NOW()
        # aren't valid SQLite syntax).
        statements = [
            """
            CREATE TABLE IF NOT EXISTS crypto_market_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                coin_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                name TEXT NOT NULL,
                current_price_usd REAL,
                market_cap_usd REAL,
                market_cap_rank INTEGER,
                total_volume_usd REAL,
                price_change_pct_24h REAL,
                circulating_supply REAL,
                total_supply REAL,
                ath_usd REAL,
                snapshot_date TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE (coin_id, snapshot_date)
            );
            """,
            "CREATE INDEX IF NOT EXISTS idx_crypto_snapshot_date ON crypto_market_data (snapshot_date);",
            "CREATE INDEX IF NOT EXISTS idx_crypto_coin_id ON crypto_market_data (coin_id);",
            """
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_started_at TEXT NOT NULL,
                run_finished_at TEXT,
                status TEXT NOT NULL,
                rows_extracted INTEGER,
                rows_loaded INTEGER,
                error_message TEXT
            );
            """,
        ]
        with engine.begin() as conn:
            for stmt in statements:
                conn.execute(text(stmt))
    else:
        # Postgres: run the real schema.sql, stripping comment-only lines.
        with engine.begin() as conn:
            for statement in raw_sql.split(";"):
                clean = "\n".join(
                    line for line in statement.splitlines() if not line.strip().startswith("--")
                ).strip()
                if clean:
                    conn.execute(text(clean))

    logger.info("Schema verified/created.")


def log_run(engine, started_at, status, rows_extracted=None, rows_loaded=None, error_message=None):
    is_sqlite = engine.url.get_backend_name() == "sqlite"
    finished_at = datetime.now(timezone.utc).isoformat()
    started_iso = started_at.isoformat()

    sql = text("""
        INSERT INTO pipeline_runs
            (run_started_at, run_finished_at, status, rows_extracted, rows_loaded, error_message)
        VALUES
            (:started, :finished, :status, :rows_extracted, :rows_loaded, :error_message)
    """)
    try:
        with engine.begin() as conn:
            conn.execute(sql, {
                "started": started_iso,
                "finished": finished_at,
                "status": status,
                "rows_extracted": rows_extracted,
                "rows_loaded": rows_loaded,
                "error_message": error_message,
            })
    except SQLAlchemyError as e:
        logger.warning(f"Could not write to pipeline_runs audit table: {e}")


# --------------------------------------------------------------------------
# ORCHESTRATION
# --------------------------------------------------------------------------

def run_pipeline(db_url: str, pages: int = 1, per_page: int = 250):
    started_at = datetime.now(timezone.utc)
    logger.info("=" * 60)
    logger.info(f"Pipeline run started at {started_at.isoformat()}")

    engine = get_engine(db_url)
    ensure_schema(engine)

    try:
        all_raw = []
        for page in range(1, pages + 1):
            all_raw.extend(extract(page=page, per_page=per_page))

        df = transform(all_raw)
        rows_loaded = load(df, engine)

        log_run(
            engine, started_at, status="SUCCESS",
            rows_extracted=len(all_raw), rows_loaded=rows_loaded,
        )
        logger.info("Pipeline run completed successfully.")
        return 0

    except Exception as e:
        logger.exception(f"Pipeline run FAILED: {e}")
        log_run(engine, started_at, status="FAILED", error_message=str(e))
        return 1


def parse_args():
    parser = argparse.ArgumentParser(description="Crypto market data ETL pipeline.")
    parser.add_argument(
        "--db", default=os.environ.get("DATABASE_URL") or DEFAULT_DB_URL,
        help="SQLAlchemy database URL (defaults to DATABASE_URL env var, then local SQLite).",
    )
    parser.add_argument("--pages", type=int, default=1, help="Number of API pages to fetch (250 coins/page).")
    parser.add_argument("--per-page", type=int, default=250, help="Coins per page (max 250).")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    exit_code = run_pipeline(db_url=args.db, pages=args.pages, per_page=args.per_page)
    sys.exit(exit_code)
