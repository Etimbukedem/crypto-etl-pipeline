# Crypto Market ETL Pipeline

An end-to-end, production-style ETL pipeline that extracts daily cryptocurrency
market data from the [CoinGecko API](https://www.coingecko.com/en/api), cleans
and models it with pandas, loads it into PostgreSQL/SQLite with idempotent
UPSERTs, and runs automatically every day via GitHub Actions.

Built to demonstrate: API extraction with retry/error handling, data
transformation and type-safety, relational schema design, idempotent loading
patterns, and CI/CD-driven workflow automation.

## Architecture

```mermaid
flowchart LR
    A[CoinGecko API] -->|requests + retry/backoff| B[Extract]
    B --> C[Transform<br/>pandas: clean, type-cast, dedupe]
    C --> D[Load<br/>SQLAlchemy UPSERT]
    D --> E[(PostgreSQL / SQLite)]
    F[GitHub Actions<br/>daily cron] -.triggers.-> B
    D --> G[pipeline_runs<br/>audit log]
```

**Flow:**
1. **Extract** — pulls the top N coins by market cap from CoinGecko, with
   automatic retries and exponential backoff on timeouts/rate limits.
2. **Transform** — renames/casts columns to match the schema, drops junk
   rows (missing price/id), de-duplicates, and stamps each row with a
   `snapshot_date`.
3. **Load** — UPSERTs into `crypto_market_data` keyed on
   `(coin_id, snapshot_date)`, so re-running the pipeline for the same day
   updates existing rows instead of duplicating them.
4. **Automate** — a GitHub Actions workflow runs this daily on a cron
   schedule, storing logs as build artifacts and (optionally) writing to a
   hosted Postgres database via a repo secret.

## Key Features

- **Idempotent by design** — safe to re-run any number of times per day;
  no duplicate rows, no manual cleanup.
- **Dual-database support** — works out of the box with local SQLite
  (zero setup) and switches to PostgreSQL automatically when `DATABASE_URL`
  is set.
- **Resilient extraction** — retry logic with backoff, explicit handling
  of timeouts, connection errors, HTTP errors, and API rate limits (429).
- **Structured logging** — every run logs to both stdout and
  `logs/etl.log`, and is recorded in a `pipeline_runs` audit table
  (status, row counts, error messages).
- **CI/CD native** — fully automated via GitHub Actions on a daily cron,
  with manual trigger support and log artifacts per run.
- **Time-series ready** — the schema stores one row per coin per day,
  so the dataset naturally supports trend/volatility analysis later.

## Project Structure

```
crypto-etl-pipeline/
├── etl.py                       # Main pipeline: extract, transform, load
├── schema.sql                   # PostgreSQL DDL (SQLite variant inline)
├── requirements.txt
├── .env.example
├── .gitignore
├── .github/
│   └── workflows/
│       └── etl.yml              # Daily cron automation
├── data/                        # Local SQLite DB (gitignored)
└── logs/                        # Run logs (gitignored)
```

## Setup Instructions

### 1. Clone and install

```bash
git clone https://github.com/Uny1me/crypto-etl-pipeline.git
cd crypto-etl-pipeline
pip install -r requirements.txt
```

### 2. Run locally (SQLite, no config needed)

```bash
python etl.py
```

This creates `data/crypto.db`, fetches the top 250 coins, and loads them.
Check `logs/etl.log` for the run output.

### 3. (Optional) Point it at PostgreSQL

Get a free Postgres instance from [Supabase](https://supabase.com),
[Neon](https://neon.tech), or [Railway](https://railway.app), then:

```bash
cp .env.example .env
# edit .env and set DATABASE_URL=postgresql://user:pass@host:5432/dbname
python etl.py --db "$DATABASE_URL"
```

### 4. Enable daily automation on GitHub

1. Push this repo to GitHub.
2. In **Settings → Secrets and variables → Actions**, add a secret named
   `DATABASE_URL` pointing to your hosted Postgres instance.
3. The workflow in `.github/workflows/etl.yml` will run automatically every
   day at 03:00 UTC. You can also trigger it manually from the **Actions**
   tab (`workflow_dispatch`).

### 5. Fetch more coins (optional)

```bash
python etl.py --pages 2 --per-page 250   # top 500 coins
```

## Example Query

Once you've got a few days of snapshots, this is the kind of question the
schema is built to answer:

```sql
SELECT coin_id, snapshot_date, current_price_usd
FROM crypto_market_data
WHERE coin_id = 'bitcoin'
ORDER BY snapshot_date DESC
LIMIT 30;
```

## Tech Stack

`Python` · `pandas` · `SQLAlchemy` · `PostgreSQL` / `SQLite` · `GitHub Actions` · `CoinGecko API`

## License

MIT
