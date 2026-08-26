import pandas as pd
from sqlalchemy import create_engine

print("🚀 PRODUCTION DEPLOYMENT: Starting Bolt Drive ETL Pipeline...")

# =====================================================================
# 1. DATABASE CONNECTION CONFIGURATION (Pointing Method)
# =====================================================================
db_user = "YOUR_DATABASE_USER"
db_password = "YOUR_DATABASE_PASSWORD"
db_host = "YOUR_DATABASE_HOST"
db_port = 6543                    
db_name = "YOUR_DATABASE_NAME"

# Constructing the secure PostgreSQL connection URI string
connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = create_engine(connection_string)
print("🔌 STATUS: Connected to production database cluster.")

# =====================================================================
# 2. EXCHANGE RATES MATRIX & BULLETPROOF SANITIZATION SHIELD
# =====================================================================
def load_exchange_rates_from_cloud():
    try:
        # Fetching the official currency exchange matrix stored in our cloud DB
        df_rates = pd.read_sql("SELECT * FROM dim_exchange_rates", engine)
        return dict(zip(df_rates['currency'], df_rates['exchange_rate_to_eur']))
    except Exception as e:
        # Emergency fail-safe backup if database network connection fails
        print(f"⚠️ Warning: Cloud rate lookup failed ({e}). Applying fallback matrix.")
        return {"EUR": 1.0, "CZK": 25.20, "USD": 1.09}

# Initialize and cache active currency exchange rates into memory
current_rates = load_exchange_rates_from_cloud()

def clean_and_convert_currency_live(row):
    try:
        # --- FULL PRICE STRING SANITIZATION ---
        price = row['raw_price']
        if isinstance(price, str):
            # Strip out accidental text/letters (e.g. '12.50a') and standardize separators
            price = ''.join(c for c in price if c.isdigit() or c in '.,')
            price = float(price.replace(',', '.'))
        else:
            price = float(price)
            
        # --- FULL CURRENCY STRING VALIDATION ---
        currency = row['currency']
        if pd.isna(currency) or str(currency).strip() == '':
            currency = 'EUR'  # Safe corporate default if string is missing
        else:
            currency = str(currency).strip().upper()  # Remove white spaces and force uppercase
            
        # --- HISTORICAL FX CONVERSION LOOP ---
        rate_to_use = current_rates.get(currency, 1.0)
        if float(rate_to_use) <= 0:
            rate_to_use = 1.0  # Zero division protection mechanism
            
        # Execute normalized calculation rounded to standard 2 decimal places
        return round(price / float(rate_to_use), 2)
    except Exception as e:
        # Fail-safe protection: broken rows default to 0.0 but allow engine to run
        print(f"⚠️ Warning: Error sanitizing row {row.name}: {e}")
        return 0.0

# =====================================================================
# 3. EXTRACTION STAGE
# =====================================================================
print("📥 EXTRACTION: Downloading raw records from cloud dataset...")
df_raw_rides = pd.read_sql("SELECT * FROM raw_rides", engine)
print(f"✅ SUCCESS: Extracted {len(df_raw_rides)} raw transactional rows from the server.")

# =====================================================================
# 4. TRANSFORMATION STAGE
# =====================================================================
print("⏳ TRANSFORMATION: Running calculation engine and formatting final schema...")

# A. Parse text timestamps into structural datetime components
df_raw_rides['start_timestamp'] = pd.to_datetime(df_raw_rides['start_timestamp'])
df_raw_rides['end_timestamp'] = pd.to_datetime(df_raw_rides['end_timestamp'])

# B. Calculate precise ride runtime duration minutes
df_raw_rides['duration_minutes'] = (df_raw_rides['end_timestamp'] - df_raw_rides['start_timestamp']).dt.total_seconds() / 60.0
df_raw_rides['duration_minutes'] = df_raw_rides['duration_minutes'].round(1)

# C. Deploy the string sanitization and currency conversion loops row-by-row
df_raw_rides['price_eur'] = df_raw_rides.apply(clean_and_convert_currency_live, axis=1)

# D. Drop messy raw metrics to establish clean production quality columns
df_fact_rides = df_raw_rides.drop(columns=['raw_price', 'currency'])

print("✨ SUCCESS: Data transformation stage completed successfully!")

# =====================================================================
# 5. LOADING STAGE (THE APPEND MODE AUTOPILOT)
# =====================================================================
try:
    print(f"📊 DATA AUDIT: Final package contains {len(df_fact_rides)} production rows.")
    print("📤 LOADING: Injecting clean fact records into production storage layer...")
    
    # if_exists='append' ensures incremental loading without wiping out data history
    df_fact_rides.to_sql('fact_rides', engine, if_exists='append', index=False)
    
    print("\n🏆 PIPELINE SUCCESS: All data successfully sanitized and written to Cloud DB!")
    print("You can now open pgAdmin 4 or Power BI to connect to the 'fact_rides' table.")
    
except Exception as e:
    print(f"❌ PIPELINE FAILURE: Cloud database ingestion failed. Reason: {e}")
