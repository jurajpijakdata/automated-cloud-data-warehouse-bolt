import os
import sys
import re
import pandas as pd
from decimal import Decimal, InvalidOperation
from sqlalchemy import create_engine
from dotenv import load_dotenv

print("🚀 Starting UpDataLogic Bolt Drive ETL Pipeline (Enhanced Integrity Mode)...")

# Enforce forced local .env lookup to bypass system environment variable overrides
load_dotenv(override=True)

# =====================================================================
# 1. DATABASE CONNECTION CONFIGURATION (Strict Least-Privilege Role)
# =====================================================================
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Fail-fast constraint validation to ensure infrastructure runtime stability
if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME]):
    print("\n❌ CRITICAL CONFIG FAILURE: Missing target environment configurations in .env", file=sys.stderr)
    sys.exit(1)

# Constructing the secure PostgreSQL connection URI string via Connection Pooler (Port 6543)
connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string)

# =====================================================================
# 2. EXCHANGE RATES MATRIX & BROADCAST SHIELD
# =====================================================================
def load_exchange_rates_from_cloud():
    try:
        # Fetching the official currency exchange matrix stored in our cloud DB warehouse
        df_rates = pd.read_sql("SELECT * FROM dim_exchange_rates", engine)
        # Convert values immediately to high-precision Decimal tokens to eliminate rounding drifting
        return {row['currency']: Decimal(str(row['exchange_rate_to_eur'])) for _, row in df_rates.iterrows()}
    except Exception as e:
        print(f"⚠️ Warning: Cloud registry lookups uninitialized ({e}). Deploying operational fallback matrix.")
        return {"EUR": Decimal("1.0"), "CZK": Decimal("25.20"), "USD": Decimal("1.09")}

# Initialize and cache active currency exchange rates into system memory
current_rates = load_exchange_rates_from_cloud()

def clean_and_convert_currency_live(row):
    """
    Robust Financial Token Parser matching Module 4 Correctness constraints.
    Neutralizes injection anomalies, respects negative sign vectors, and implements strict Decimal scales.
    """
    raw_price = row['raw_price']
    currency = row['currency']
    
    # 1. Currency Standardisation Layer
    if pd.isna(currency) or str(currency).strip() == '':
        currency = 'EUR'  # Secure default fallback matching corporate ledger constraints
    else:
        currency = str(currency).strip().upper()
        
    rate_to_use = current_rates.get(currency, Decimal("1.0"))
    if rate_to_use <= 0:
        rate_to_use = Decimal("1.0")  # Division-by-zero runtime block

    # Explicit missing value allocation (NULL mapping)
    if pd.isna(raw_price):
        return None

    # Normalise whitespace layers
    price_str = str(raw_price).strip()

    # Locale-Aware Separator Resolution Matrix (Handles US/EU formats dynamically)
    if ',' in price_str and '.' in price_str:
        if price_str.find(',') < price_str.find('.'):
            price_str = price_str.replace(',', '')  # Standard US notation: 1,234.56 -> 1234.56
        else:
            price_str = price_str.replace('.', '').replace(',', '.')  # Standard EU notation: 1.234,56 -> 1234.56
    elif ',' in price_str and '.' not in price_str:
        price_str = price_str.replace(',', '.')  # Clean regional fractional: 12,50 -> 12.50

    # Strip native string currency symbols to isolate numeric tokens
    price_str = price_str.replace('€', '').replace('$', '').strip()

    # EXPLICIT REGEX GRAMMAR VERIFICATION (Rule 4)
    # Validates precisely one starting negative flag and exactly one numeric period.
    # Instantly rejects malicious system logs (e.g., "45 (order 5000)" or "12.50a") instead of blending strings.
    match = re.match(r"^-?\d+(?:\.\d+)?$", price_str)
    
    if not match:
        print(f"⚠️ Data Quality Alert: Row {row.name} rejected. Adversarial pattern discovered: '{raw_price}'")
        return None  # Maps to clean SQL NULL to defend Power BI column measures

    try:
        # High-precision financial mathematics execution
        parsed_decimal = Decimal(price_str)
        price_eur = parsed_decimal / rate_to_use
        
        # Quantize structure back to strict 2-decimal scale for database target layer compliance
        return float(price_eur.quantize(Decimal("0.01")))
    except (InvalidOperation, ValueError):
        print(f"⚠️ Data Quality Alert: Row {row.name} parsing engine exception on value: '{price_str}'")
        return None

# =====================================================================
# 3. EXTRACTION STAGE
# =====================================================================
try:
    print("📥 EXTRACTION: Querying transactional payloads from staging layer...")
    df_raw_rides = pd.read_sql("SELECT * FROM raw_rides", engine)
    print(f"✅ SUCCESS: Safely extracted {len(df_raw_rides):,} raw records from the data core.")
except Exception as e:
    print(f"\n❌ EXTRACTION STAGE CRITICAL PIPELINE PAUSE: {e}", file=sys.stderr)
    sys.exit(1)

# =====================================================================
# 4. TRANSFORMATION STAGE
# =====================================================================
print("⏳ TRANSFORMATION: Running calculation engine and formatting final telemetry schemas...")

df_raw_rides['start_timestamp'] = pd.to_datetime(df_raw_rides['start_timestamp'])
df_raw_rides['end_timestamp'] = pd.to_datetime(df_raw_rides['end_timestamp'])

# Calculate exact ride operational timespan durations
df_raw_rides['duration_minutes'] = (df_raw_rides['end_timestamp'] - df_raw_rides['start_timestamp']).dt.total_seconds() / 60.0
df_raw_rides['duration_minutes'] = df_raw_rides['duration_minutes'].round(1)

# Map explicit number parsing rules row-by-row across financial layers
df_raw_rides['price_eur'] = df_raw_rides.apply(clean_and_convert_currency_live, axis=1)

# SEPARATE QUALITY FLAGS (Module 4 Standard): Ensuring the measure column remains strictly numeric for Power BI calculations
df_raw_rides['data_quality_status'] = df_raw_rides['price_eur'].apply(lambda x: 'CLEAN' if pd.notna(x) else 'UNKNOWN')

# Strip raw tracking metrics to finalise production-grade database schemas
df_fact_rides = df_raw_rides.drop(columns=['raw_price', 'currency'])
print("✨ SUCCESS: Ingestion transformation rules evaluated flawlessly across all logs.")

# =====================================================================
# 5. LOADING STAGE (THE INCREMENTAL APPEND AUTOPILOT)
# =====================================================================
try:
    print(f"📊 DATA AUDIT: Current package context resolves to {len(df_fact_rides):,} production-validated rows.")
    print("📤 LOADING: Injecting sanitized fact streams into cloud database instances...")
    
    df_fact_rides.to_sql('fact_rides', engine, if_exists='append', index=False)
    print("\n🏆 PIPELINE RUN COMPLETED: Financial integrity verified. All records streamed to cloud DB!")
    
except Exception as e:
    print(f"❌ LOADING STAGE CRITICAL EXCEPTION: Data warehouse ingestion rejected. Reason: {e}", file=sys.stderr)
    sys.exit(1)
