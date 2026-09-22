# Automated Cloud Data Warehouse & BI Suite — Car-Sharing Simulation

[![tests](https://github.com/jurajpijakdata/automated-cloud-data-warehouse-bolt/actions/workflows/tests.yml/badge.svg)](https://github.com/jurajpijakdata/automated-cloud-data-warehouse-bolt/actions/workflows/tests.yml)

![Dashboard Preview](dashboard_preview.png)

An end-to-end ELT pipeline and dimensional data warehouse for a simulated car-sharing business, built on PostgreSQL (Supabase). It takes messy, multi-currency ride records and turns them into a clean, queryable star schema that feeds a BI dashboard.

All data in this project is synthetically generated to model realistic scale and messiness, without touching any real customer data.

## Live dashboard

**[Open the dashboard](https://datastudio.google.com/u/1/reporting/489ba77f-5b10-4aea-a723-47137253b3d6/page/FNw6F?hl=en)**

Try changing the date range or filtering by city (Prague, Bratislava, Frankfurt) to see the numbers recalculate.

## What problem this solves

Raw ride records come in messy: prices are logged in three currencies (CZK, USD, EUR) with inconsistent formatting, some price fields contain corrupted text (like a stray order ID stuck in the price string), and there's no reliable duration field for utilization analysis. This pipeline:

1. Normalizes every price into EUR, using the exchange rate that was actually in effect on the day of the ride, not today's rate.
2. Detects and quarantines malformed price data instead of silently turning it into zero or a wrong number.
3. Computes ride duration from the raw timestamps so fleet utilization can be analyzed.

## How it's built

**Idempotent loads.** The pipeline uses `INSERT ... ON CONFLICT (ride_id) DO UPDATE` instead of `replace` or blind `append`. You can re-run it as many times as you want on the same data without creating duplicates or wiping history.

**One source of truth for the transformation logic.** Currency parsing, decimal handling, and EUR conversion all live in a single tested Python module (`currency_parser.py`), used by the ETL script. The database has one lightweight trigger, and its only job is to keep the date dimension populated — it doesn't duplicate the parsing logic, so there's no risk of the SQL and Python versions of the transformation drifting apart from each other.

**Decimal-safe money handling.** Prices are parsed with Python's `Decimal` type and stored as `NUMERIC(10,2)` in Postgres, so there's no floating-point rounding drift on financial figures.

**Explicit parsing, not blind string replacement.** A regex (`^-?\d+(?:\.\d+)?$`) validates every price string after normalizing EU/US decimal formats. Anything that doesn't match is rejected and flagged rather than mangled into a wrong number. Negative values (refunds) are preserved rather than accidentally flipped positive.

**Quarantine over silent failure.** Rows with unparseable prices get `NULL` and a `data_quality_status = 'UNKNOWN'` flag instead of defaulting to zero. If more than 25% of a run's rows fail validation, the pipeline stops and exits with a non-zero status, so a bad batch never gets loaded quietly.

**Schema validation.** `pandera` checks the shape and types of the data (required columns, non-negative durations, etc.) before anything is written to the warehouse.

**Tested business logic.** The currency conversion logic is isolated in its own module and covered by a parametrized pytest suite, so it can be tested without a database connection. Tests run automatically in CI on every push (see the badge above).

**Slowly Changing Dimensions.** `dim_exchange_rates` and `production_cars` both use SCD Type 2 (`valid_from` / `valid_to`), so historical reporting always uses the values that were correct at the time of the transaction, not the current ones.

**Bulk loads.** The load step batches rows into chunked bulk upserts rather than issuing one database round-trip per row, so it stays fast as the dataset grows.

**Analytics pushed into SQL.** The reporting view (`v_bi_enterprise_reporting_marts`) uses CTEs and window functions (`SUM() OVER`, `ROW_NUMBER() OVER`) to compute running revenue totals and customer segmentation directly in Postgres, so Looker Studio just renders results instead of doing heavy client-side aggregation.

## Repository structure

```text
automated-cloud-data-warehouse-bolt/
├── currency_parser.py          # Currency parsing & EUR conversion logic (unit tested)
├── etl_bolt_drive.py           # Main ETL: extract, transform, validate, bulk upsert
├── test_currency.py            # Pytest suite for currency_parser.py
├── create_tables.sql           # Star schema: staging, dimensions, fact table, indexes
├── database_architecture.sql   # Reporting view with CTEs & window functions
├── requirements.txt            # Pinned dependencies
├── .github/workflows/tests.yml # CI: runs the test suite on every push/PR
├── LICENSE
└── README.md
```

## Quick start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Run the test suite

```bash
pytest test_currency.py -v
```

### 3. (Optional) Configure database credentials

Copy `.env.example` to `.env` and fill in your Supabase/Postgres connection details:

```text
DB_USER=your_project_reference_user
DB_PASSWORD=your_secure_password
DB_HOST=your_supabase_host_string
DB_PORT=6543
DB_NAME=postgres
```

If you skip this step, the pipeline automatically falls back to a local SQLite database, so you can run it end to end without any cloud credentials.

### 4. Run the pipeline

```bash
python etl_bolt_drive.py
```

## What the dashboard shows

The warehouse feeds a Looker Studio dashboard over roughly 1,500 synthetically generated ride records, covering:

- **Gross revenue (EUR)** — cash flow trends by city, normalized to a single currency.
- **Fleet utilization** — total minutes on the road, broken down by brand (Tesla, Škoda, BMW, Toyota, Audi, Volkswagen).
- **Ride quality** — average rating by car, to flag underperforming vehicles.

## Data protection note

This project uses only synthetic, randomly generated data — no real customer information is processed anywhere in the pipeline. For real deployments handling personal data, UpDataLogic operates as a data processor under a standard Data Processing Agreement, with anonymization/tokenization applied to any PII before it reaches this kind of pipeline.
