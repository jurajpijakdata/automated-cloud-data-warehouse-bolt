-- =====================================================================
-- AUTOMATED CLOUD DATA WAREHOUSE (BOLT DRIVE) - KIMBALL STAR SCHEMA
-- Module 10: Advanced Dimensional Modeling, Constraints & Indexing
-- =====================================================================

-- 1. STAGING LAYER (Immutable Raw Ingestion Inflows)
CREATE TABLE IF NOT EXISTS public.raw_rides (
    ride_id TEXT,
    car_id TEXT,
    user_id TEXT,
    location_id TEXT,
    start_timestamp TEXT,
    end_timestamp TEXT,
    raw_price TEXT,
    currency TEXT
);

-- 2. DIMENSION: HIGH-PRECISION EXCHANGE RATES REFERENCE
CREATE TABLE IF NOT EXISTS public.dim_exchange_rates (
    currency TEXT PRIMARY KEY,
    exchange_rate_to_eur NUMERIC(10, 4) NOT NULL CHECK (exchange_rate_to_eur > 0),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 3. DIMENSION: COFORMED LOGISTICAL LOCATIONS (SCD Type 1 Overwrite)
CREATE TABLE IF NOT EXISTS public.dim_locations (
    location_id TEXT PRIMARY KEY,
    city TEXT NOT NULL,
    country TEXT NOT NULL,
    zone_type TEXT CHECK (zone_type IN ('Urban', 'Suburban', 'Airport', 'Business')),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 4. DIMENSION: HIGH-SCALE FLEET ASSETS (Strict Kimball SCD Type 2 Tracking)
CREATE TABLE IF NOT EXISTS public.dim_cars (
    car_surrogate_key SERIAL PRIMARY KEY, -- Surrogate Key tracking dimensional variations
    car_id TEXT NOT NULL,                  -- Natural business key identifier
    model TEXT NOT NULL,
    fuel_type TEXT NOT NULL CHECK (fuel_type IN ('Petrol', 'Diesel', 'Electric', 'Hybrid')),
    transmission TEXT CHECK (transmission IN ('Manual', 'Automatic')),
    
    -- Kimball SCD Type 2 Temporal Validity Tracks
    valid_from TIMESTAMP WITH TIME ZONE NOT NULL,
    valid_to TIMESTAMP WITH TIME ZONE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    
    CONSTRAINT unique_active_car_version UNIQUE (car_id, is_current)
);

-- 5. FACT TABLE: CORE MOBILITY MEASUREMENTS & TELEMETRY TRANSACTION SYSTEM
CREATE TABLE IF NOT EXISTS public.fact_rides (
    ride_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL, -- Natural key referencing downstream CRM dimensions
    location_id TEXT NOT NULL,
    car_id TEXT NOT NULL,
    
    start_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    end_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    duration_minutes NUMERIC(10, 1) NOT NULL,
    distance_km NUMERIC(10, 2),
    ride_rating INTEGER,
    price_eur NUMERIC(10, 2),
    data_quality_status TEXT NOT NULL DEFAULT 'CLEAN',
    
    -- STRUCTURAL DATABASE DATA QUALITY SHIELDS (Defensive Constraints)
    CONSTRAINT fk_fact_rides_location FOREIGN KEY (location_id) REFERENCES public.dim_locations(location_id),
    CONSTRAINT check_ride_time_chronology CHECK (end_timestamp >= start_timestamp),
    CONSTRAINT check_ride_rating_boundary CHECK (ride_rating BETWEEN 1 AND 5)
);

-- =====================================================================
-- 6. PERFORMANCE OPTIMIZATION LAYER (Analytical Filter Indexing Matrix)
-- =====================================================================
-- Eliminate sequential table scans across major dashboard slicer vectors
CREATE INDEX IF NOT EXISTS idx_fact_rides_start_date ON public.fact_rides (start_timestamp);
CREATE INDEX IF NOT EXISTS idx_fact_rides_location ON public.fact_rides (location_id);
CREATE INDEX IF NOT EXISTS idx_fact_rides_car ON public.fact_rides (car_id);
CREATE INDEX IF NOT EXISTS idx_dim_cars_scd_lookup ON public.dim_cars (car_id, is_current);
