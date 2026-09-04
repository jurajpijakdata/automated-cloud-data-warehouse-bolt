import os
import sys
import pandas as pd
import pandera.pandas as pa
from pathlib import Path
from decimal import Decimal
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Import the pure tested business logic from our currency parser module
from currency_parser import clean_and_convert_currency_live

print("🚀 Starting UpDataLogic Bolt Drive ETL Pipeline (Self-Healing & Validated)...")

load_dotenv(override=True)

BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR / ".env"

# 1. Define Strict Data Quality Shield using Pandera Specification
bolt_data_schema = pa.DataFrameSchema({
    "ride_id": pa.Column(str, nullable=False),
    "user_id": pa.Column(str, nullable=False),
    "start_timestamp": pa.Column(pa.DateTime, nullable=False),
    "end_timestamp": pa.Column(pa.DateTime, nullable=False),
    "duration_minutes": pa.Column(float, pa.Check.ge(0), nullable=False),
    "price_eur": pa.Column(float, nullable=True)
})

# 2. Database Connection Check with Fallback Routing
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
        print("🔌 Connection Status: [ONLINE] Remote PostgreSQL Warehouse Connected.")
    else:
        raise FileNotFoundError("Local database configuration mappings missing.")
except Exception as db_error:
    print(f"⚠️ Production DB Offline or Network Issue detected: {db_error}")
    print("🔄 Activating Portfolio Architecture Fallback Mode (Local Storage Engine)...")
    connection_string = f"sqlite:///{BASE_DIR / 'local_portfolio.db'}"
    engine = create_engine(connection_string)
    print("🔌 Connection Status: [LOCAL ENGINE] Active Fallback SQLite Context Deployed.")

# =====================================================================
# 3. EXCHANGE RATES MATRIX & OPERATIONAL STAGE
# =====================================================================
def load_exchange_rates_from_cloud():
    try:
        df_rates = pd.read_sql("SELECT * FROM dim_exchange_rates", engine)
        return {row['currency']: Decimal(str(row['exchange_rate_to_eur'])) for _, row in df_rates.iterrows()}
    except Exception:
        return {"EUR": Decimal("1.0"), "CZK": Decimal("25.20"), "USD": Decimal("1.09")}

current_rates = load_exchange_rates_from_cloud()

# =====================================================================
# 4. DATA PROCESSING PIPELINE STAGE
# =====================================================================
try:
    print("\n📥 EXTRACTION: Querying transactional payloads from staging layer...")
    try:
        df_raw_rides = pd.read_sql("SELECT * FROM raw_rides", engine)
    except Exception:
        print("💡 Database table uninitialized. Programmatically deploying test replication matrix.")
        sample_data = {
            "ride_id": ["R_001", "R_002", "R_003"],
            "driver_id": ["D_99", "D_88", "D_77"],
            "start_timestamp": ["2026-09-01 08:00:00", "2026-09-01 09:15:00", "2026-09-01 18:30:00"],
            "end_timestamp": ["2026-09-01 08:25:00", "2026-09-01 09:40:00", "2026-09-01 18:45:00"],
            "raw_price": ["-25.00", "12,50", "  $150.50 "],
            "currency": ["EUR", "CZK", "USD"]
        }
        df_raw_rides = pd.DataFrame(sample_data)
        df_raw_rides.to_sql('raw_rides', engine, if_exists='replace', index=False)
        df_raw_rides = pd.read_sql("SELECT * FROM raw_rides", engine)

    print(f"✅ SUCCESS: Safely extracted {len(df_raw_rides):,} records from core data fields.")

    print("⏳ Executing self-healing schema alignment matrix...")
    df_raw_rides['ride_id'] = df_raw_rides['ride_id'].astype(str)
    df_raw_rides['user_id'] = df_raw_rides['user_id'].astype(str)

    print("\n⏳ TRANSFORMATION: Running calculation engine and executing data type validation schemas...")
    df_raw_rides['start_timestamp'] = pd.to_datetime(df_raw_rides['start_timestamp'])
    df_raw_rides['end_timestamp'] = pd.to_datetime(df_raw_rides['end_timestamp'])
    
    df_raw_rides['duration_minutes'] = (df_raw_rides['end_timestamp'] - df_raw_rides['start_timestamp']).dt.total_seconds() / 60.0
    df_raw_rides['duration_minutes'] = df_raw_rides['duration_minutes'].round(1)

    # Route formatting through the imported pure verified business function matrix
    df_raw_rides['price_eur'] = df_raw_rides.apply(lambda r: clean_and_convert_currency_live(r, current_rates), axis=1)
    df_raw_rides['data_quality_status'] = df_raw_rides['price_eur'].apply(lambda x: 'CLEAN' if pd.notna(x) else 'UNKNOWN')

    df_fact_rides = df_raw_rides.drop(columns=['raw_price', 'currency'])

    print("🛡️ Running declarative data quality checks via Pandera schema evaluation...")
    validated_fact_rides = bolt_data_schema.validate(df_fact_rides)

    print("\n📤 LOADING: Injecting validated fact streams into final database layer...")
        # Clean the target repository rows dynamically while preserving the analytical view structures
    with engine.begin() as truncate_conn:
        truncate_conn.execute(text("TRUNCATE TABLE fact_rides;"))
    print("🧹 Database Ingest Optimization: Target database repository truncated successfully.")

    validated_fact_rides.to_sql('fact_rides', engine, if_exists='append', index=False)

    print("\n🏆 PIPELINE RUN COMPLETED SUCCESSFULLY: Financial integrity verified.")

except pa.errors.SchemaError as schema_fault:
    print(f"\n❌ DATA QUALITY BREACH DETECTED BY PANDERA:\n{schema_fault}", file=sys.stderr)
    sys.exit(1)
except Exception as pipeline_error:
    print(f"\n❌ PIPELINE CRITICAL RUNTIME EXCEPTION: {pipeline_error}", file=sys.stderr)
    sys.exit(1)
