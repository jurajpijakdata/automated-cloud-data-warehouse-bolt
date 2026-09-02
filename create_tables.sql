-- 1. Create Raw Rides Ingestion Table
CREATE TABLE IF NOT EXISTS public.raw_rides (
    ride_id VARCHAR(50) PRIMARY KEY,
    start_timestamp VARCHAR(30),
    end_timestamp VARCHAR(30),
    raw_price VARCHAR(20),
    currency VARCHAR(10)
);

-- 2. Create Exchange Rates Dimension Table
CREATE TABLE IF NOT EXISTS public.dim_exchange_rates (
    currency VARCHAR(10) PRIMARY KEY,
    exchange_rate_to_eur NUMERIC(10, 4)
);

-- 3. Create Cleaned Fact Table For Power BI Connection
CREATE TABLE IF NOT EXISTS public.fact_rides (
    ride_id VARCHAR(50) PRIMARY KEY,
    start_timestamp TIMESTAMP,
    end_timestamp TIMESTAMP,
    duration_minutes NUMERIC(10, 1),
    price_eur NUMERIC(10, 2)
);
