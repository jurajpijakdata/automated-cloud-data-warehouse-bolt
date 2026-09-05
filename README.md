# 🚗 Automated Cloud Data Warehouse & BI Suite - Mobility Simulation Framework

![Dashboard Preview](dashboard_preview.png)

A self-directed data engineering framework modeling an end-to-end cloud ELT data pipeline and dimensional data warehouse based on a high-scale car-sharing scenario. Built entirely on cloud infrastructure (Supabase/PostgreSQL) via a secure Connection Pooler layer, this project implements a self-healing processing layer, corporate observability logging handlers, decoupled code verification suites, and declarative data schema validation shields.

## 🔗 Live Interactive Dashboard
👉 **[CLICK HERE TO OPEN THE LIVE MANAGEMENT DASHBOARD](https://datastudio.google.com/reporting/489ba77f-5b10-4aea-a723-47137253b3d6)**  
*(Feel free to interact with date ranges and look up specific simulated metrics for cities like Prague, Bratislava, or Frankfurt to watch the analytics recalculate).*

---

## 🏗️ Architecture Design: Enterprise Observability & Self-Healing Layout
To meet the rigorous data quality, error boundaries, and monitoring standards required in production-grade platform deployments, the framework enforces a multi-layered verification layout:
1. **Enterprise Logging Framework (`logging`):** Completely replaced legacy, unmonitored standard stdout text prints with a formal Python logging machine. Events, warning tracks, and subsystem errors are systematically tracked across precise execution states (`INFO`, `WARNING`, `CRITICAL`) to allow direct parsing by automated cloud orchestrators.
2. **First-Class Rejection Metrics & Quarantine:** Malformed textual data corruptions or alphanumeric anomalies are proactively intercepted row-by-row. Instead of masking failures using silent zero conversions that skew averages downstream, corrupt fields are cast to explicit `NULL` maps and actively tracked as a first-class operational quality metric.
3. **Automated Alerting Thresholds (Fail-Fast):** Incorporates an active runtime processing limit constraint. If the data ingestion pipeline encounters a critical row rejection rate greater than **25.0%** of the sample batch payload volume, the entire framework halts execution immediately and throws a hard termination state (`sys.exit(1)`) to trigger scheduler alerts.
4. **Self-Healing Pre-Validation Layer:** Coerces incoming data structure alignments (e.g., automatically casting database sequence IDs to text strings) to eliminate schema drifts and strips raw currency grouping separators, white spaces, or currency text markers before metrics conversion.
5. **Decoupled Unit Testing (`pytest`):** Core transformation math and financial logic are fully isolated inside an independent module (`currency_parser.py`). This removes all environment, database, or connection footprints, executing table-driven parameterized tests to verify that negative refund values stay strictly negative.
6. **Declarative Quality Assurance (`pandera`):** Screens the fully aligned, cleaned, and healed dataframe for structural attributes, duplicate keys, and range constraints before allowing downstream relational loading.
7. **Trunk Ingestion Optimization:** To safely stream data into the production warehouse without fracturing active transactional BI reporting and downstream SQL Views, the loading stage runs an automated `TRUNCATE TABLE` strategy inside atomic connection blocks.

---

## 📊 Project Scope & Simulated Business Scenario
This portfolio project simulates the infrastructure required to solve data fragmentation from mobile application streams and telematics. The architecture is engineered to resolve three core challenges:
1. **Historical Currency Inconsistency:** Processing raw ride records ingested concurrently in multiple currencies (CZK, USD, EUR) to establish a unified financial reporting layer in EUR based on explicit transactional execution dates.
2. **Adversarial Text Ingestion Malfunctions:** Preventing data quality degradation by intercepting corrupted text noise inside numerical price attributes (e.g., stopping nested order context IDs from distorting core metrics).
3. **Missing Telematics Analytics:** Pre-computing precise trip runtime duration metrics from transactional timestamps to analyze asset idling and utilization.

*Note: All transactional data utilized in this project is synthetically generated to model real-world scale and anomalies safely within a test environment.*

---

## 🏗️ 5-Stage Data Warehouse Pipeline Engineering
This repository demonstrates a production-grade approach to resolve data quality and temporal normalization challenges directly inside a PostgreSQL instance while strictly maintaining financial integrity standard constraints:

1. **The Ingestion Layer (`raw_rides`):** Direct append-only storage staging layer simulating transactional inflows from car telematics.
2. **High-Precision Numeric Vectoring:** Binary `float` data types have been entirely deprecated across all monetary pipelines. The ingestion engine enforces strict `decimal.Decimal` inside Python and `NUMERIC(10,2)` inside the cloud database to eliminate fractional drifts (`://30000000000000004.com`).
3. **Explicit Character Parsing (Regex Shield):** Blind string replacement filters have been replaced with a strict, non-destructive Regular Expression parser (`^-?\\d+(?:\\.\\d+)?$`). Malformed alphanumeric payloads (e.g., `"45 (order 5000)"`) are safely rejected and isolated instead of being incorrectly concatenated.
4. **Negative Vector Safety (Refund Shield):** The parsing grammar explicitly validates and preserves leading negative flags (`-`), ensuring financial refunds remain negative data points instead of silently mutating into false revenue spikes.
5. **Slowly Changing Dimensions (SCD Type 2):** The dimensional exchange matrix (`dim_exchange_rates`) incorporates active date horizons (`valid_from`, `valid_to`). Conversions automatically utilize the precise historical exchange rate applicable on the exact calendar date of the ride transaction.
6. **The Metrics Semantic View (`view_clean_reporting`):** Blends normalized transaction records with dimensional registries (`production_cars`, `production_locations`). Textual quality flags (`UNKNOWN`) are completely decoupled into a sibling column (`reporting_quality_flag`), ensuring the core price metric (`reporting_price`) remains a pure numeric measure fully compatible with Power BI and Looker Studio `SUM()` and `AVERAGE()` aggregations.

---

## 📁 Repository Directory Structure

```text
bolt-drive-analytics/
├── currency_parser.py        # Pure Extracted Financial Business Logic Module (Decoupled for Testability)
├── etl_bolt_drive.py         # Main ETL Pipeline Engine & Pandera Inline Schema Shield with Logging
├── test_currency.py          # Parametrized Pytest Suite Simulator & Automated Crash Test Vectors
├── create_tables.sql         # Core Structural Production Database Schemas
├── database_architecture.sql # Permanent Database Views, Triggers, and Analytical View Layouts
├── requirements.txt          # Locked Software Dependency Layout Matrix
└── README.md                 # Enterprise Systems Documentation
```

---

## 🚀 Quick Start (Clone & Run Standard)

### 1. Install Dependencies
Deploy the isolated software version scheme inside your local execution environment:
```powershell
pip install -r requirements.txt
```

### 2. Run Automated Code Testing
Run the comprehensive unit verification suite using the built-in crash-test vectors to ensure calculation stability:
```powershell
pytest test_currency.py -v
```

### 3. Configure Environment Secrets (Optional for Cloud Integration)
Replicate the `.env.example` structure into a local file named `.env` in the root folder and add your database targets (protected locally via `.gitignore`). *If omitted, the framework automatically leverages localized sandbox configurations.*
```text
DB_USER=your_project_reference_user
DB_PASSWORD=your_secure_password
DB_HOST=your_supabase_host_string
DB_PORT=6543
DB_NAME=postgres
```

### 4. Run the Production ETL Pipeline
Launch the automated cleaning, ingestion, and database optimization script:
```powershell
python etl_bolt_drive.py
```

---
*Developed under the UpDataLogic Engineering Framework for verified, reproducible, and honest cloud data pipelines.*
