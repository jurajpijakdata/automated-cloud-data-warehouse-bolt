# 🚗 Automated Cloud Data Warehouse & BI Suite - Mobility Simulation Framework

![Dashboard Preview](dashboard_preview.png)

A self-directed data engineering framework modeling an end-to-end cloud ELT data pipeline and dimensional data warehouse based on a high-scale car-sharing scenario. Built entirely on cloud infrastructure (Supabase/PostgreSQL) via a secure Connection Pooler layer to bypass local dependencies and demonstrate robust database architecture, data quality constraints, and automated currency normalization.

## 🔗 Live Interactive Dashboard
👉 **[CLICK HERE TO OPEN THE LIVE MANAGEMENT DASHBOARD](https://datastudio.google.com/reporting/489ba77f-5b10-4aea-a723-47137253b3d6)**  
*(Feel free to interact with date ranges and look up specific simulated metrics for cities like Prague, Bratislava, or Frankfurt to watch the analytics recalculate).*

## 📊 Project Scope & Simulated Business Scenario
This portfolio project simulates the infrastructure required to solve data fragmentation from mobile application streams and telematics. The architecture is engineered to resolve three core challenges:
1. **Historical Currency Inconsistency:** Processing raw ride records ingested concurrently in multiple currencies (CZK, USD, EUR) to establish a unified financial reporting layer in EUR based on explicit transactional execution dates.
2. **Adversarial Text Ingestion Malfunctions:** Preventing data quality degradation by intercepting corrupted text noise inside numerical price attributes (e.g., stopping nested order context IDs from distorting core metrics).
3. **Missing Telematics Analytics:** Pre-computing precise trip runtime duration metrics from transactional timestamps to analyze asset idling and utilization.

*Note: All transactional data utilized in this project is synthetically generated to model real-world scale and anomalies safely within a test environment.*

## 🏗️ Architecture & Engineered Solutions
This repository demonstrates a production-grade 5-stage pipeline approach to resolve data quality and temporal normalization challenges directly inside a PostgreSQL instance while strictly maintaining financial integrity standard constraints:

1. **The Ingestion Layer (`raw_rides`):** Direct append-only storage staging layer simulating transactional inflows from car telematics.
2. **High-Precision Numeric Vectoring:** Binary `float` data types have been entirely deprecated across all monetary pipelines. The ingestion engine enforces strict `decimal.Decimal` inside Python and `NUMERIC(10,2)` inside the cloud database to eliminate fractional drifts (`://30000000000000004.com`).
3. **Explicit Character Parsing (Regex Shield):** Blind string replacement filters have been replaced with a strict, non-destructive Regular Expression parser (`^-?\\d+(?:\\.\\d+)?$`). Malformed alphanumeric payloads (e.g., `"45 (order 5000)"`) are safely rejected and isolated instead of being incorrectly concatenated.
4. **Negative Vector Safety (Refund Shield):** The parsing grammar explicitly validates and preserves leading negative flags (`-`), ensuring financial refunds remain negative data points instead of silently mutating into false revenue spikes.
5. **Slowly Changing Dimensions (SCD Type 2):** The dimensional exchange matrix (`dim_exchange_rates`) incorporates active date horizons (`valid_from`, `valid_to`). Conversions automatically utilize the precise historical exchange rate applicable on the exact calendar date of the ride transaction.
6. **The Metrics Semantic View (`view_clean_reporting`):** Blends normalized transaction records with dimensional registries (`production_cars`, `production_locations`). Textual quality flags (`UNKNOWN`) are completely decoupled into a sibling column (`reporting_quality_flag`), ensuring the core price metric (`reporting_price`) remains a pure numeric measure fully compatible with Power BI `SUM()` and `AVERAGE()` aggregations.

## 📁 Repository Structure
* `etl_bolt_drive.py`: Main automated production pipeline utilizing Python (Pandas + SQLAlchemy) for database connection layers, strict RegEx grammar verification, and high-precision Decimal transformations.
* `test_integrity.py`: Automated unit testing suite validating adversarial client inputs (e.g., text corruptions, empty spaces, trailing letters) against the core parsing grammar before staging execution.
* `create_tables.sql` / `database_architecture.sql`: Core structural database layer housing analytical DDL schemas (Raw, Dim, Fact), Slowly Changing Dimension (SCD Type 2) tables, data quality validation functions, database triggers, and business view reporting layouts.
* `.env.example`: Public structural template for required database environmental variables.
* `requirements.txt`: Locked software dependency versions ensuring 100% reproducible environments across external systems.

## 🚀 Quick Start (Clone & Run Standard)

### 1. Install Dependencies
Ensure your environment matches the required configuration:
```powershell
pip install -r requirements.txt
```

### 2. Configure Environment Secrets
Replicate the `.env.example` structure into a local file named `.env` in the root folder and add your database targets (protected locally via `.gitignore`). The configuration is optimized for secure proxied database routing:
```text
DB_USER=your_project_reference_user
DB_PASSWORD=your_secure_password
DB_HOST=://supabase.com
DB_PORT=6543
DB_NAME=postgres
```

### 3. Deploy the SQL Database Schema
Execute the structured code inside `create_tables.sql` (or your layout scripts) in your Supabase SQL editor to establish the raw, dim, and fact layers before running the application.

### 4. Run the Pipeline & Verification Suite
Execute the main ETL pipeline:
```powershell
python etl_bolt_drive.py
```

Execute the automated structural unit test:
```powershell
python test_integrity.py
```

---
*Developed under the UpDataLogic Engineering Framework for verified, reproducible, and honest cloud data pipelines.*
