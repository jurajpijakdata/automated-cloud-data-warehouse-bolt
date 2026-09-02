import os
import sys
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

print("🚀 Starting UpDataLogic Bolt Drive ETL Pipeline...")

# Vynútené načítanie lokálneho .env súboru s prepísaním systémovej cache Windowsu
load_dotenv(override=True)

# =====================================================================
# 1. DATABASE CONNECTION CONFIGURATION (Strict Security Model)
# =====================================================================
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Ak v .env niečo chýba, pipeline z bezpečnostných dôvodov hlučne zastavíme
if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME]):
    print("\n❌ CRITICAL CONFIG FAILURE: Missing database environment variables in .env", file=sys.stderr)
    sys.exit(1)

# Dynamické vyskladanie pripojenia pre bezpečný Connection Pooler
connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string)

# =====================================================================
# 2. EXCHANGE RATES MATRIX & BROADCAST SHIELD
# =====================================================================
def load_exchange_rates_from_cloud():
    try:
        # Načítanie oficiálnej matice menových kurzov z databázy
        df_rates = pd.read_sql("SELECT * FROM dim_exchange_rates", engine)
        return dict(zip(df_rates['currency'], df_rates['exchange_rate_to_eur']))
    except Exception as e:
        print(f"⚠️ Warning: Cloud structure verification status ({e}). Applying operational fallback matrix.")
        return {"EUR": 1.0, "CZK": 25.20, "USD": 1.09}

# Uloženie aktívnych kurzov do dočasnej pamäte cache
current_rates = load_exchange_rates_from_cloud()

def clean_and_convert_currency_live(row):
    try:
        # Sanitizácia textových anomálií v cenách (napr. '500.00a')
        price = row['raw_price']
        if isinstance(price, str):
            price = ''.join(c for c in price if c.isdigit() or c in '.,-')
            price = float(price.replace(',', '.'))
        else:
            price = float(price)
            
        currency = row['currency']
        if pd.isna(currency) or str(currency).strip() == '':
            currency = 'EUR'
        else:
            currency = str(currency).strip().upper()
            
        rate_to_use = current_rates.get(currency, 1.0)
        if float(rate_to_use) <= 0:
            rate_to_use = 1.0
            
        return round(price / float(rate_to_use), 2)
    except Exception as e:
        print(f"⚠️ Warning: Error sanitizing row {row.name}: {e}")
        return 0.0

# =====================================================================
# 3. EXTRACTION STAGE
# =====================================================================
try:
    print("📥 EXTRACTION: Downloading raw records from cloud dataset...")
    df_raw_rides = pd.read_sql("SELECT * FROM raw_rides", engine)
    print(f"✅ SUCCESS: Extracted {len(df_raw_rides):,} raw rows from the server.")
except Exception as e:
    print(f"\n❌ EXTRACTION STAGE FAILED: {e}", file=sys.stderr)
    sys.exit(1)

# =====================================================================
# 4. TRANSFORMATION STAGE
# =====================================================================
print("⏳ TRANSFORMATION: Running calculation engine and formatting final schema...")

df_raw_rides['start_timestamp'] = pd.to_datetime(df_raw_rides['start_timestamp'])
df_raw_rides['end_timestamp'] = pd.to_datetime(df_raw_rides['end_timestamp'])

# Výpočet dĺžky jazdy v minútach
df_raw_rides['duration_minutes'] = (df_raw_rides['end_timestamp'] - df_raw_rides['start_timestamp']).dt.total_seconds() / 60.0
df_raw_rides['duration_minutes'] = df_raw_rides['duration_minutes'].round(1)

# Aplikácia row-by-row čistenia a konverzie mien
df_raw_rides['price_eur'] = df_raw_rides.apply(clean_and_convert_currency_live, axis=1)

# Odstránenie starých nepotrebných stĺpcov pre finálnu čistú fact tabuľku
df_fact_rides = df_raw_rides.drop(columns=['raw_price', 'currency'])
print("✨ SUCCESS: Data transformation stage completed successfully!")

# =====================================================================
# 5. LOADING STAGE (THE APPEND MODE AUTOPILOT)
# =====================================================================
try:
    print(f"📊 DATA AUDIT: Final package contains {len(df_fact_rides):,} production rows.")
    print("📤 LOADING: Injecting clean fact records into production storage layer...")
    
    df_fact_rides.to_sql('fact_rides', engine, if_exists='append', index=False)
    print("\n🏆 PIPELINE SUCCESS: All data successfully sanitized and written to Cloud DB!")
    
except Exception as e:
    print(f"❌ LOADING STAGE FAILED: Cloud database ingestion failed. Reason: {e}", file=sys.stderr)
    sys.exit(1)
