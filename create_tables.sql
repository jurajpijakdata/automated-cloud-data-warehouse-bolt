-- =====================================================================
-- UPDATALOGIC DATA WAREHOUSE: STAGING & CONFIGURATION SCHEMAS
-- =====================================================================

-- 1. Create Raw Rides Ingestion Staging Table
CREATE TABLE IF NOT EXISTS public.raw_rides (
    ride_id VARCHAR(50) PRIMARY KEY,
    car_id INT,
    user_id INT,
    location_id INT,
    start_timestamp TIMESTAMP,
    end_timestamp TIMESTAMP,
    distance_km DECIMAL(6,2),
    ride_rating INT,
    raw_price VARCHAR(50), -- Staging layer keeps raw payload text intact
    currency VARCHAR(10)
);

-- 2. Create Exchange Rates Dimension Table (SCD Type 2 - Historical Track)
-- Matches Module 4 criteria for effective-dated historical FX conversions.
CREATE TABLE IF NOT EXISTS public.dim_exchange_rates (
    rate_id SERIAL PRIMARY KEY,
    currency VARCHAR(10) NOT NULL,
    exchange_rate_to_eur NUMERIC(10, 4) NOT NULL,
    valid_from DATE NOT NULL, -- Date when this rate became active
    valid_to DATE -- NULL or max date means currently active rate
);

-- Create an index to maximize high-speed temporal join lookups
CREATE INDEX IF NOT EXISTS idx_exchange_rates_temporal 
ON public.dim_exchange_rates (currency, valid_from, valid_to);
