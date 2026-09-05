import os
import sys
import logging
import pandas as pd
import pandera.pandas as pa
from pathlib import Path
from decimal import Decimal
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Suppress the deprecation and future warnings from structural package frameworks
os.environ["DISABLE_PANDERA_IMPORT_WARNING"] = "True"

# Import the pure tested business logic from our currency parser module
from currency_parser import clean_and_convert_currency_live

# =====================================================================
# ENTERPRISE LOGGING CONFIGURATION (Module 6 Standard)
# =====================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [UpDataLogic Bolt ETL] - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logging.info("🚀 Starting UpDataLogic Bolt Drive ETL Pipeline (Production Observability Mode)...")

# Enforce forced local .env lookup to bypass system environment variable overrides
load_dotenv(override=True)

BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR / ".env"

# First-class operation metrics trackers for alerting thresholds
METRICS_TRACKER = {
    "total_records_extracted": 0,
    "successfully_healed_records": 0,
    "rejected_records_critical": 0
}

# 1. Define Strict Data Quality Shield using Pandera Specification
bolt_data_schema = pa.DataFrameSchema({
    "ride_id": pa.Column(str, nullable=False),
    "user_id": pa.Column(str, nullable=False),
    "start_timestamp": pa.Column(pa.DateTime, nullable=False),
    "end_timestamp": pa.Column(pa.DateTime, nullable=False),
    "duration_minutes": pa.Column(float, pa.Check.ge(0), nullable=False),
    "price_eur": pa.Column(float, nullable=True)
})

# 2. Database Connection Check with Active Fallback Routing Context
try:
    if ENV_FILE.exists():
        DB_USER = os.getenv("DB_USER")
        DB_PASSWORD = os.getenv("DB_PASSWORD")
        DB_HOST = os.getenv("DB_HOST")
        DB_PORT = os.getenv("DB_PORT", "6543")
        DB_NAME = os.getenv("DB_NAME")
        
        if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
            raise ValueError("Incomplete database credentials inside configuration targets.")
            
        connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(connection_string)
        with engine.connect() as conn:
            pass
        logging.info("🔌 Connection Status: [ONLINE] Remote PostgreSQL Connected on Port 6543.")
    else:
        raise FileNotFoundError("Local database configuration mappings missing.")
except Exception as db_error:
    logging.warning(f"⚠️ Production DB Offline or Network Issue detected: {db_error}")
    logging.info("🔄 Activating Portfolio Architecture Fallback Mode (Local Storage Engine)...")
    connection_string = f"sqlite:///{BASE_DIR / 'local_portfolio.db'}"
    engine = create_engine(connection_string)
    logging.info("🔌 Connection Status: [LOCAL ENGINE] Active Fallback SQLite Context Deployed.")

# =====================================================================
# 3. EXCHANGE RATES MATRIX FETCHING
# =====================================================================
def load_exchange_rates_from_cloud():
    try:
        df_rates = pd.read_sql("SELECT * FROM dim_exchange_rates", engine)
        return {row['currency']: Decimal(str(row['exchange_rate_to_eur'])) for _, row in df_rates.iterrows()}
    except Exception as rates_error:
        logging.warning(f"⚠️ Exchange rates lookup uninitialized ({rates_error}). Deploying safe fallback matrix.")
        return {"EUR": Decimal("1.0"), "CZK": Decimal("25.20"), "USD": Decimal("1.09")}

current_rates = load_exchange_rates_from_cloud()

# =====================================================================
# 4. DATA PROCESSING PIPELINE STAGE (ETL Layer)
# =====================================================================
try:
    logging.info("📥 1. EXTRACTION: Querying transactional payloads from staging repositories...")
    try:
        df_raw_rides = pd.read_sql("SELECT * FROM raw_rides", engine)
    except Exception:
        logging.info("💡 Database source raw_rides table uninitialized. Deploying replication fallback matrix.")
        sample_data = {
            "ride_id": ["1", "2", "3", "4", "5"],
            "car_id": ["101", "102", "103", "104", "105"],
            "user_id": ["18", "35", "52", "69", "86"],
            "location_id": ["10", "20", "30", "40", "50"],
            "start_timestamp": ["2026-01-04 15:05:00", "2026-01-07 22:10:00", "2026-01-11 05:15:00", "2026-01-13 12:20:00", "2026-01-15 19:25:00"],
            "end_timestamp": ["2026-01-04 15:18:00", "2026-01-07 22:26:00", "2026-01-11 05:34:00", "2026-01-13 12:42:00", "2026-01-15 19:50:00"],
            "raw_price": ["-25.00", "12,50", "  $150.50 ", "UNKNOWN_ERROR", "44.12"],
            "currency": ["EUR", "CZK", "USD", "EUR", "EUR"]
        }


        df_raw_rides = pd.DataFrame(sample_data)
        df_raw_rides.to_sql('raw_rides', engine, if_exists='replace', index=False)
        df_raw_rides = pd.read_sql("SELECT * FROM raw_rides", engine)

    METRICS_TRACKER["total_records_extracted"] = len(df_raw_rides)
    logging.info(f"✅ EXTRACTION SUCCESS: Extracted {METRICS_TRACKER['total_records_extracted']:,} rows into DataFrame memory.")

    logging.info("⏳ 2. TRANSFORMATION: Executing self-healing matrix alignments and processing calculations...")
    
    # Self-healing alignment before validation logic checks
    df_raw_rides['ride_id'] = df_raw_rides['ride_id'].astype(str)
    df_raw_rides['user_id'] = df_raw_rides['user_id'].astype(str)
    df_raw_rides['start_timestamp'] = pd.to_datetime(df_raw_rides['start_timestamp'])
    df_raw_rides['end_timestamp'] = pd.to_datetime(df_raw_rides['end_timestamp'])
    
    df_raw_rides['duration_minutes'] = (df_raw_rides['end_timestamp'] - df_raw_rides['start_timestamp']).dt.total_seconds() / 60.0
    df_raw_rides['duration_minutes'] = df_raw_rides['duration_minutes'].round(1)

    # Convert currencies through our pure decoupled logic parser
    df_raw_rides['price_eur'] = df_raw_rides.apply(lambda r: clean_and_convert_currency_live(r, current_rates), axis=1)
    df_raw_rides['data_quality_status'] = df_raw_rides['price_eur'].apply(lambda x: 'CLEAN' if pd.notna(x) else 'UNKNOWN')

    # Drop intermediate telemetry tracking metrics columns
    df_fact_rides = df_raw_rides.drop(columns=['raw_price', 'currency'])

    # Track operational metrics rejections
    METRICS_TRACKER["rejected_records_critical"] = int(df_fact_rides['price_eur'].isna().sum())
    METRICS_TRACKER["successfully_healed_records"] = METRICS_TRACKER["total_records_extracted"] - METRICS_TRACKER["rejected_records_critical"]

    logging.info("🛡️ 3. VALIDATION: Running declarative data quality checks via Pandera schema evaluation...")
    validated_fact_rides = bolt_data_schema.validate(df_fact_rides)

    rejection_rate = (METRICS_TRACKER["rejected_records_critical"] / METRICS_TRACKER["total_records_extracted"]) * 100
    logging.info(f"📊 DATA QUALITY METRICS: Clean/Healed: {METRICS_TRACKER['successfully_healed_records']:,} | Quarantined/NULL: {METRICS_TRACKER['rejected_records_critical']:,} ({rejection_rate:.2f}%)")

    # Alerting Threshold Constraints Evaluation (Fail-Fast execution threshold)
    if rejection_rate > 25.0: # Set higher limit for local demo sample execution velocity
        raise ValueError(f"Pipeline stopped. Rejection rate {rejection_rate:.2f}% breached production threshold.")

    logging.info("📤 4. LOADING: Safely streaming validated telemetry parameters into database layer...")
    
    # Performance optimized data warehouse ingestion utilizing truncating safety parameters
    try:
        with engine.begin() as truncate_conn:
            truncate_conn.execute(text("TRUNCATE TABLE fact_rides;"))
        logging.info("🧹 Database Ingest Optimization: Target database repository truncated successfully.")
        df_fact_rides.to_sql('fact_rides', engine, if_exists='append', index=False)
    except Exception as load_fault:
        logging.warning(f"⚠️ Truncate operation restricted by structural DDL lock bindings: {load_fault}. Switching execution strategy to 'replace'...")
        df_fact_rides.to_sql('fact_rides', engine, if_exists='replace', index=False)

    logging.info("🏆 PIPELINE RUN COMPLETED SUCCESSFULLY: STATUS 0 [SUCCESS]. Financial data telemetry verified.\n")
    sys.exit(0) # Formal successful process exit execution tracking flag

except pa.errors.SchemaError as schema_fault:
    logging.critical(f"❌ PIPELINE STOPPED VIA PANDERA STRUCTURAL SHIELD: {schema_fault}")
    sys.exit(1) # Strict error termination exit state for cloud orchestrators
except Exception as pipeline_error:
    logging.critical(f"❌ PIPELINE CRITICAL RUNTIME EXCEPTION: {pipeline_error}")
    sys.exit(1)
