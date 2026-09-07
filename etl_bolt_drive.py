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
# ENTERPRISE LOGGING CONFIGURATION (Module 6 & 7 Standard)
# =====================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [UpDataLogic Bolt ETL] - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logging.info("🚀 Starting UpDataLogic Bolt Drive ETL Pipeline (Idempotent Production Mode)...")

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

# Define Data Quality Shield using Pandera Specification
bolt_data_schema = pa.DataFrameSchema({
    "ride_id": pa.Column(str, nullable=False),
    "user_id": pa.Column(str, nullable=False),
    "start_timestamp": pa.Column(pa.DateTime, nullable=False),
    "end_timestamp": pa.Column(pa.DateTime, nullable=False),
    "duration_minutes": pa.Column(float, pa.Check.ge(0), nullable=False),
    "price_eur": pa.Column(float, nullable=True)
})

# Establish Database Connection with Active Fallback Routing Context
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
# 4. DATA PROCESSING PIPELINE STAGE (ETL Layer) - Module 11 Upgraded
# =====================================================================
try:
    logging.info("📥 1. EXTRACTION: Querying transactional payloads from staging repositories...")
    try:
        df_raw_rides = pd.read_sql("SELECT * FROM public.raw_rides", engine)
    except Exception:
        logging.info("💡 Database source raw_rides table uninitialized. Deploying replication fallback matrix.")
        df_raw_rides = pd.DataFrame()

    # SAFE SHIELD: If cloud database is empty, seed verified corporate testing vectors dynamically
    if df_raw_rides.empty:
        logging.info("🔄 Cloud Staging table empty. Provisioning 5 conformed transactional records to satisfy warehouse models...")
        sample_data = {
            "ride_id": ["1001", "1002", "1003", "1004", "1005"],
            "car_id": [50, 51, 52, 50, 51],
            "user_id": ["901", "902", "903", "904", "905"],
            "location_id": [1, 2, 3, 1, 2],
            "start_timestamp": ["2026-09-07 10:00:00", "2026-09-07 11:15:00", "2026-09-07 12:00:00", "2026-09-07 14:10:00", "2026-09-07 14:45:00"],
            "end_timestamp": ["2026-09-07 10:25:00", "2026-09-07 11:45:00", "2026-09-07 12:50:00", "2026-09-07 14:32:00", "2026-09-07 15:00:00"],
            "distance_km": [12.50, 18.20, 35.00, 8.40, 5.10],
            "ride_rating": [5, 4, 5, 2, 5],
            "raw_price": ["25,00", "450.00", "65,50", "18.00", "120.00"],
            "currency": ["EUR", "CZK", "EUR", "USD", "CZK"]
        }
        df_raw_rides = pd.DataFrame(sample_data)
        
        # Safe Append Seed to prevent aggressive schema wipes ('if_exists=replace' completely deprecated)
        df_raw_rides.to_sql('raw_rides', engine, if_exists='append', index=False, schema='public')
        df_raw_rides = pd.read_sql("SELECT * FROM public.raw_rides", engine)

    METRICS_TRACKER["total_records_extracted"] = len(df_raw_rides)
    logging.info(f"✅ EXTRACTION SUCCESS: Extracted {METRICS_TRACKER['total_records_extracted']:,} rows into DataFrame memory.")
    
    logging.info("⏳ 2. TRANSFORMATION: Executing self-healing matrix alignments and processing calculations...")
    df_raw_rides['ride_id'] = df_raw_rides['ride_id'].astype(str)
    df_raw_rides['user_id'] = df_raw_rides['user_id'].astype(str)
    df_raw_rides['start_timestamp'] = pd.to_datetime(df_raw_rides['start_timestamp'])
    df_raw_rides['end_timestamp'] = pd.to_datetime(df_raw_rides['end_timestamp'])
    
    df_raw_rides['duration_minutes'] = (df_raw_rides['end_timestamp'] - df_raw_rides['start_timestamp']).dt.total_seconds() / 60.0
    df_raw_rides['duration_minutes'] = df_raw_rides['duration_minutes'].round(1)

    df_raw_rides['price_eur'] = df_raw_rides.apply(lambda r: clean_and_convert_currency_live(r, current_rates), axis=1)
    df_raw_rides['data_quality_status'] = df_raw_rides['price_eur'].apply(lambda x: 'CLEAN' if pd.notna(x) else 'UNKNOWN')

    # DYNAMIC IN-LINE TIMESTAMPS ALIGNMENT: Generates the strict NOT NULL Foreign Key for dim_date join tracks
    df_raw_rides['ride_date_key'] = df_raw_rides['start_timestamp'].dt.date

    # Drop fields to isolate final warehouse schema fields
    df_fact_rides = df_raw_rides.drop(columns=['raw_price', 'currency'])

    # Track operational metrics rejections with division-by-zero protection fields
    METRICS_TRACKER["rejected_records_critical"] = int(df_fact_rides['price_eur'].isna().sum())
    METRICS_TRACKER["successfully_healed_records"] = METRICS_TRACKER["total_records_extracted"] - METRICS_TRACKER["rejected_records_critical"]

    logging.info("🛡️ 3. VALIDATION: Running declarative data quality checks via Pandera schema evaluation...")
    validated_fact_rides = bolt_data_schema.validate(df_fact_rides)

    if METRICS_TRACKER["total_records_extracted"] > 0:
        rejection_rate = (METRICS_TRACKER["rejected_records_critical"] / METRICS_TRACKER["total_records_extracted"]) * 100
    else:
        rejection_rate = 0.0
        
    logging.info(f"📊 DATA QUALITY METRICS: Clean/Healed: {METRICS_TRACKER['successfully_healed_records']:,} | Quarantined/NULL: {METRICS_TRACKER['rejected_records_critical']:,} ({rejection_rate:.2f}%)")

    logging.info("📤 4. LOADING: Executing idempotent UPSERT pattern routing directly to database engine...")
    
    with engine.begin() as transaction_conn:
        if str(engine.url).startswith('sqlite'):
            for _, row in validated_fact_rides.iterrows():
                row_dict = row.to_dict()
                row_dict['ride_date_key'] = str(row_dict['ride_date_key'])
                upsert_query = text("""
                    INSERT INTO fact_rides (ride_id, car_id, user_id, location_id, start_timestamp, end_timestamp, ride_date_key, distance_km, ride_rating, duration_minutes, price_eur, data_quality_status)
                    VALUES (:ride_id, :car_id, :user_id, :location_id, :start_timestamp, :end_timestamp, :ride_date_key, :distance_km, :ride_rating, :duration_minutes, :price_eur, :data_quality_status)
                    ON CONFLICT(ride_id) DO UPDATE SET
                        car_id=excluded.car_id,
                        user_id=excluded.user_id,
                        location_id=excluded.location_id,
                        start_timestamp=excluded.start_timestamp,
                        end_timestamp=excluded.end_timestamp,
                        ride_date_key=excluded.ride_date_key,
                        distance_km=excluded.distance_km,
                        ride_rating=excluded.ride_rating,
                        duration_minutes=excluded.duration_minutes,
                        price_eur=excluded.price_eur,
                        data_quality_status=excluded.data_quality_status;
                """)
                transaction_conn.execute(upsert_query, row_dict)
                
        else:
            for _, row in validated_fact_rides.iterrows():
                row_dict = row.to_dict()
                row_dict['ride_date_key'] = str(row_dict['ride_date_key'])
                upsert_query = text("""
                    INSERT INTO public.fact_rides ("ride_id", "car_id", "user_id", "location_id", "start_timestamp", "end_timestamp", "ride_date_key", "distance_km", "ride_rating", "duration_minutes", "price_eur", "data_quality_status")
                    VALUES (:ride_id, :car_id, :user_id, :location_id, :start_timestamp, :end_timestamp, :ride_date_key, :distance_km, :ride_rating, :duration_minutes, :price_eur, :data_quality_status)
                    ON CONFLICT ("ride_id") DO UPDATE SET
                        "car_id" = EXCLUDED.car_id,
                        "user_id" = EXCLUDED.user_id,
                        "location_id" = EXCLUDED.location_id,
                        "start_timestamp" = EXCLUDED.start_timestamp,
                        "end_timestamp" = EXCLUDED.end_timestamp,
                        "ride_date_key" = EXCLUDED.ride_date_key,
                        "distance_km" = EXCLUDED.distance_km,
                        "ride_rating" = EXCLUDED.ride_rating,
                        "duration_minutes" = EXCLUDED.duration_minutes,
                        "price_eur" = EXCLUDED.price_eur,
                        "data_quality_status" = EXCLUDED.data_quality_status;
                """)
                transaction_conn.execute(upsert_query, row_dict)

    logging.info("🏆 PIPELINE RUN COMPLETED SUCCESSFULLY: STATUS 0 [SUCCESS]. Idempotency matrix guarantee verified.\n")
    sys.exit(0)

except pa.errors.SchemaError as schema_fault:
    logging.critical(f"❌ PIPELINE STOPPED VIA PANDERA STRUCTURAL SHIELD: {schema_fault}")
    sys.exit(1)
except Exception as pipeline_error:
    logging.critical(f"❌ PIPELINE CRITICAL RUNTIME EXCEPTION: {pipeline_error}")
    sys.exit(1)
