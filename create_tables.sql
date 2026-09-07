-- =====================================================================
-- AUTOMATED CLOUD DATA WAREHOUSE (BOLT DRIVE) - KIMBALL STAR SCHEMA
-- Module 10: Advanced Dimensional Modeling, Constraints & Indexing
-- =====================================================================

-- 1. STAGING LAYER (Immutable Raw Ingestion Inflows Matrix)
CREATE TABLE IF NOT EXISTS public.raw_rides (
    ride_id VARCHAR(50), -- Removed primary key constraint to allow staging re-runs safely
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

-- 2. CONFIGURATION LAYER (SCD Type 2 Exchange Rates Dimension Matrix)
CREATE TABLE IF NOT EXISTS public.dim_exchange_rates (
    rate_id SERIAL PRIMARY KEY,
    currency VARCHAR(10) NOT NULL,
    -- DEFENSIVE SHIELD: Blocks negative or zero rates from crashing calculation divisions
    exchange_rate_to_eur NUMERIC(10, 4) NOT NULL CHECK (exchange_rate_to_eur > 0),
    valid_from DATE NOT NULL, 
    valid_to DATE 
);

-- High-speed temporal index lookup optimization vector
CREATE INDEX IF NOT EXISTS idx_exchange_rates_temporal 
ON public.dim_exchange_rates (currency, valid_from, valid_to);

-- =====================================================================
-- 3. PRODUCTION WAREHOUSE TARGET TABLES (Strict Kimball Dimensions)
-- =====================================================================

-- Coformed Logistical Locations (SCD Type 1 Overwrite Table)
CREATE TABLE IF NOT EXISTS public.production_locations (
    location_id INT PRIMARY KEY,
    city_name VARCHAR(50) NOT NULL,
    country VARCHAR(50) NOT NULL,
    local_manager VARCHAR(50)
);

-- High-Scale Fleet Assets (Upgraded to Strict Kimball SCD Type 2 Tracking Matrix)
CREATE TABLE IF NOT EXISTS public.production_cars (
    car_surrogate_key SERIAL PRIMARY KEY, -- Surrogate Key tracking dimensional variations
    car_id INT NOT NULL,                  -- Natural business key identifier
    brand VARCHAR(50) NOT NULL,
    model_name VARCHAR(50) NOT NULL,
    fuel_type VARCHAR(20) NOT NULL CHECK (fuel_type IN ('Petrol', 'Diesel', 'Electric', 'Hybrid')),
    battery_capacity_kwh INT,
    purchase_date DATE,
    license_plate VARCHAR(20),
    
    -- Kimball SCD Type 2 Temporal Validity Horizons
    valid_from TIMESTAMP WITH TIME ZONE NOT NULL,
    valid_to TIMESTAMP WITH TIME ZONE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    
    CONSTRAINT unique_active_car_version UNIQUE (car_id, is_current)
);

-- Production-grade Fact Table heavily protected with Database Constraints
CREATE TABLE IF NOT EXISTS public.fact_rides (
    ride_id INT PRIMARY KEY,
    car_id INT NOT NULL,
    user_id INT NOT NULL, -- Natural key referencing downstream CRM dimensions
    location_id INT NOT NULL,
    start_timestamp TIMESTAMP NOT NULL,
    end_timestamp TIMESTAMP NOT NULL,
    distance_km DECIMAL(6,2),
    ride_rating INT,
    duration_minutes DECIMAL(10,1) NOT NULL,
    price_eur DECIMAL(10,2), 
    data_quality_status VARCHAR(20) DEFAULT 'CLEAN',
    
    -- STRUCTURAL DATABASE DATA QUALITY SHIELDS (Defensive Constraints Matrix)
    CONSTRAINT fk_fact_rides_location FOREIGN KEY (location_id) REFERENCES public.production_locations(location_id),
    CONSTRAINT check_ride_time_chronology CHECK (end_timestamp >= start_timestamp),
    CONSTRAINT check_ride_rating_boundary CHECK (ride_rating BETWEEN 1 AND 5)
);

-- =====================================================================
-- 4. ADVANCED FINANCIAL REGEX SANITIZATION ENGINE (The Database Brain)
-- =====================================================================
CREATE OR REPLACE FUNCTION public.sync_raw_to_fact_function()
RETURNS TRIGGER AS $$
DECLARE
    active_rate DECIMAL(10,4);
    calculated_minutes DECIMAL(10,1);
    calculated_price_eur DECIMAL(10,2);
    price_working_text VARCHAR(50);
    final_raw_price DECIMAL(10,2);
    clean_currency VARCHAR(3);
    ride_date DATE;
    quality_status VARCHAR(20) := 'CLEAN';
BEGIN
    -- Extract the operational date vector for temporal currency conversions
    ride_date := NEW.start_timestamp::DATE;

    -- Clean baseline whitespaces and normalize locale decimal separators
    price_working_text := TRIM(NEW.raw_price);
    price_working_text := REPLACE(price_working_text, '€', '');
    price_working_text := REPLACE(price_working_text, '$', '');
    price_working_text := REPLACE(price_working_text, ',', '.');

    -- EXPLICIT REGEX GRAMMAR VERIFICATION (Module 4 Standard)
    IF price_working_text !~ '^-?[0-9]+(?:\.[0-9]+)?$' OR price_working_text IS NULL OR price_working_text = '' THEN
        quality_status := 'UNKNOWN';
        final_raw_price := NULL;
    ELSE
        final_raw_price := price_working_text::DECIMAL(10,2);
    END IF;

    -- Standardize currency strings
    clean_currency := UPPER(TRIM(COALESCE(NEW.currency, 'EUR')));
    IF clean_currency = '' THEN clean_currency := 'EUR'; END IF;

    -- Automated operational timespan calculation
    calculated_minutes := ROUND((EXTRACT(EPOCH FROM (NEW.end_timestamp - NEW.start_timestamp)) / 60.0)::NUMERIC, 1);

    -- 3. TEMPORAL EFFECTIVE-DATED FX LOOKUP (SCD Type 2 Architecture Matrix)
    IF final_raw_price IS NULL THEN
        calculated_price_eur := NULL;
    ELSE
        SELECT exchange_rate_to_eur INTO active_rate 
        FROM public.dim_exchange_rates 
        WHERE currency = clean_currency 
          AND ride_date >= valid_from 
          AND (valid_to IS NULL OR ride_date <= valid_to)
        LIMIT 1;

        -- Safe default execution safety valve
        IF active_rate IS NULL OR active_rate <= 0 THEN active_rate := 1.0; END IF;
        calculated_price_eur := ROUND((final_raw_price / active_rate), 2);
    END IF;

    -- Defensive rating alignment constraint to protect fact insertions from failing
    IF NEW.ride_rating < 1 OR NEW.ride_rating > 5 THEN
        NEW.ride_rating := NULL;
    END IF;

    -- Secure transaction insertion into production layer using an idempotent UPSERT pattern
    INSERT INTO public.fact_rides (
        ride_id, car_id, user_id, location_id, start_timestamp, end_timestamp, 
        distance_km, ride_rating, duration_minutes, price_eur, data_quality_status
    )
    VALUES (
        NEW.ride_id::INT, NEW.car_id, NEW.user_id, NEW.location_id, NEW.start_timestamp, NEW.end_timestamp, 
        NEW.distance_km, NEW.ride_rating, calculated_minutes, calculated_price_eur, quality_status
    )
    ON CONFLICT (ride_id) DO UPDATE SET
        car_id = EXCLUDED.car_id,
        user_id = EXCLUDED.user_id,
        location_id = EXCLUDED.location_id,
        start_timestamp = EXCLUDED.start_timestamp,
        end_timestamp = EXCLUDED.end_timestamp,
        distance_km = EXCLUDED.distance_km,
        ride_rating = EXCLUDED.ride_rating,
        duration_minutes = EXCLUDED.duration_minutes,
        price_eur = EXCLUDED.price_eur,
        data_quality_status = EXCLUDED.data_quality_status;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Deploy safe database transactional hooks
DROP TRIGGER IF EXISTS trg_sync_raw_to_fact ON public.raw_rides;
CREATE TRIGGER trg_sync_raw_to_fact
AFTER INSERT ON public.raw_rides
FOR EACH ROW
EXECUTE FUNCTION public.sync_raw_to_fact_function();

-- =====================================================================
-- 5. PERFORMANCE OPTIMIZATION LAYER (Analytical Filter Indexing Matrix)
-- =====================================================================
-- Eliminate sequential scans across major dashboard slicer filtering tracks
CREATE INDEX IF NOT EXISTS idx_fact_rides_start_date ON public.fact_rides (start_timestamp);
CREATE INDEX IF NOT EXISTS idx_fact_rides_location ON public.fact_rides (location_id);
CREATE INDEX IF NOT EXISTS idx_fact_rides_car ON public.fact_rides (car_id);
CREATE INDEX IF NOT EXISTS idx_production_cars_scd ON public.production_cars (car_id, is_current);

-- =====================================================================
-- 6. REPORTING VIEWS FOR POWER BI COMPATIBILITY
-- =====================================================================
CREATE OR REPLACE VIEW public.view_fleet_performance AS
SELECT 
    r.ride_id, r.start_timestamp, r.distance_km, r.duration_minutes, r.price_eur, r.ride_rating, r.data_quality_status,
    c.brand, c.model_name, c.fuel_type, c.license_plate,
    l.city_name, l.country
FROM public.fact_rides r
LEFT JOIN public.production_locations l ON r.location_id = l.location_id
LEFT JOIN public.production_cars c ON r.car_id = c.car_id 
    AND r.start_timestamp >= c.valid_from 
    AND (r.start_timestamp <= c.valid_to OR c.valid_to IS NULL);

-- Fixed View Layer: Keeps metrics strictly numerical for calculations
CREATE OR REPLACE VIEW public.view_clean_reporting AS
SELECT 
    ride_id, start_timestamp, distance_km, duration_minutes, brand, model_name, city_name, ride_rating,
    price_eur AS reporting_price,
    data_quality_status AS reporting_quality_flag
FROM public.view_fleet_performance;
