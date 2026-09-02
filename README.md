# 🚗 Automated Cloud Data Warehouse & BI Suite - Mobility Simulation Framework

![Dashboard Preview](dashboard_preview.png)

A self-directed data engineering framework modeling an end-to-end cloud ELT data pipeline and dimensional data warehouse based on a high-scale car-sharing scenario. Built entirely on cloud infrastructure (Supabase/PostgreSQL) to bypass local dependencies and demonstrate robust database architecture, data quality constraints, and automated currency normalization.

## 🔗 Live Interactive Dashboard
👉 **[CLICK HERE TO OPEN THE LIVE MANAGEMENT DASHBOARD](https://datastudio.google.com/reporting/489ba77f-5b10-4aea-a723-47137253b3d6)**  
*(Feel free to interact with date ranges and look up specific simulated metrics for cities like Prague, Bratislava, or Frankfurt to watch the analytics recalculate).*

## 📊 Project Scope & Simulated Business Scenario
This portfolio project simulates the infrastructure required to solve data fragmentation from mobile application streams and telematics. The architecture is engineered to resolve three core challenges:
1. **Currency Inconsistency:** Processing raw ride records ingested concurrently in multiple currencies (CZK, USD, EUR) to establish a unified financial reporting layer in EUR.
2. **Text Ingestion Malfunctions:** Preventing data quality degradation by sanitizing corrupted text noise inside numerical price attributes (e.g., converting `'500.00a'` to a clean float).
3. **Missing Telematics Analytics:** Pre-computing precise trip runtime duration metrics from transactional timestamps to analyze asset idling and utilization.

*Note: All transactional data utilized in this project is synthetically generated to model real-world scale and anomalies safely within a test environment.*

## 🏗️ Architecture & Engineered Solutions
This repository demonstrates a production-grade 4-stage pipeline approach to resolve data quality and normalization challenges directly inside a PostgreSQL instance:

1. **The Ingestion Layer (`raw_rides`):** Direct append-only storage layer simulating transactional inflows from car telematics.
2. **The Automated Database Layer (`SQL Trigger`):** An automated row-level database trigger that executes validation routines instantly upon every new record insertion.
3. **Data Sanitization & Validation Shield:** Combines regular expression pattern filtering (`REGEXP_REPLACE`) to eliminate string corruptions, automatically converts local historical exchange rates to a pure base currency (`EUR`), and flags unparseable metrics safely as `NULL` without breaking the operational workflow.
4. **The Metrics Semantic View (`view_clean_reporting`):** Blends normalized transaction records with dimensional registries (`production_cars`, `production_locations`) and maps unresolved values as explicit business flags labeled **`UNKNOWN`** for dashboard transparency.

## 📁 Repository Structure
* `etl_bolt_drive.py`: Main automated production pipeline utilizing Python (Pandas + SQLAlchemy) for database connection layers and data transformations.
* `test_currency.py`: Independent unit testing suite validating that negative numbers (refunds/vratky) and currency strings maintain arithmetic integrity.
* `create_tables.sql` / `database_architecture.sql`: Core structural layer housing analytical DDL schemas, data quality validation functions, database triggers, and business view reporting layouts.
* `.env.example`: Public structural template for required database environmental variables.
* `requirements.txt`: Locked software dependency versions ensuring 100% reproducible environments across external systems.

## 🚀 Quick Start (Clone & Run Standard)

### 1. Install Dependencies
Ensure your environment matches the required configuration:
```powershell
pip install -r requirements.txt
```

### 2. Configure Environment Secrets
Replicate the `.env.example` structure into a local file named `.env` in the root folder and add your database targets (protected locally via `.gitignore`):
```text
DB_USER=your_database_user
DB_PASSWORD=your_database_password
DB_HOST=your_database_host
DB_PORT=6543
DB_NAME=postgres
```

### 3. Deploy the SQL Database Schema
Execute the structured code inside `create_tables.sql` (or your layout scripts) in your SQL editor or pgAdmin 4 to establish the raw, dim, and fact layers before running the application.

### 4. Run the Pipeline & Verification Suite
Execute the main ETL pipeline:
```powershell
python etl_bolt_drive.py
```

Execute the automated structural unit test:
```powershell
python test_currency.py
```

---
*Developed under the UpDataLogic Engineering Framework for verified, reproducible, and honest cloud data pipelines.*
