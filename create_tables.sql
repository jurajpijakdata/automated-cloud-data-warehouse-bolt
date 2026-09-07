-- =====================================================================
-- AUTOMATED CLOUD DATA WAREHOUSE (BOLT DRIVE) - KIMBALL STAR SCHEMA
-- Module 10 & 11: Advanced Dimensional Modeling, Constraints & Time Intelligence
-- =====================================================================

-- 1. STAGING LAYER (Immutable Raw Ingestion Inflows Matrix)
CREATE TABLE IF NOT EXISTS public.raw_rides (
    ride_id VARCHAR(50),
    car_id INT,
    user_id INT,
    location_id INT,
    start_timestamp TIMESTAMP,
    end_timestamp TIMESTAMP,
    distance_km DECIMAL(6,2),
    ride_rating INT,
    raw_price VARCHAR(50),
    currency VARCHAR(10)
);

-- 2. CONFIGURATION LAYER (SCD Type 2 Exchange Rates Dimension Matrix)
CREATE TABLE IF NOT EXISTS public.dim_exchange_rates (
    rate_id SERIAL PRIMARY KEY,
    currency VARCHAR(10) NOT NULL,
    exchange_rate_to_eur NUMERIC(10, 4) NOT NULL CHECK (exchange_rate_to_eur > 0),
    valid_from DATE NOT NULL, 
    valid_to DATE 
);

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

-- High-Scale Fleet Assets (Strict Kimball SCD Type 2 Tracking Matrix)
CREATE TABLE IF NOT EXISTS public.production_cars (
    car_surrogate_key SERIAL PRIMARY KEY,
    car_id INT NOT NULL,                  
    brand VARCHAR(50) NOT NULL,
    model_name VARCHAR(50) NOT NULL,
    fuel_type VARCHAR(20) NOT NULL CHECK (fuel_type IN ('Petrol', 'Diesel', 'Electric', 'Hybrid')),
    battery_capacity_kwh INT,
    purchase_date DATE,
    license_plate VARCHAR(20),
    
    valid_from TIMESTAMP WITH TIME ZONE NOT NULL,
    valid_to TIMESTAMP WITH TIME ZONE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    
    CONSTRAINT unique_active_car_version UNIQUE (car_id, is_current)
);

-- CONFORMED TIME DIMENSION: Solves missing calendar layers for advanced DAX Time Intelligence
CREATE TABLE IF NOT EXISTS public.dim_date (
    date_key DATE PRIMARY KEY,
    year_attribute INT NOT NULL,
    month_attribute INT NOT NULL,
    month_name_attribute VARCHAR(20) NOT NULL,
    quarter_attribute INT NOT NULL,
    week_of_year_attribute INT NOT NULL,
    day_of_week_attribute INT NOT NULL,
    is_weekend_attribute BOOLEAN NOT NULL
);

-- Production-grade Fact Table heavily protected with Database Constraints
CREATE TABLE IF NOT EXISTS public.fact_rides (
    ride_id INT PRIMARY KEY,
    car_id INT NOT NULL,
    user_id INT NOT NULL, 
    location_id INT NOT NULL,
    start_timestamp TIMESTAMP NOT NULL,
    end_timestamp TIMESTAMP NOT NULL,
    ride_date_key DATE NOT NULL, -- Conformed Time Intelligence Join Vector
    distance_km DECIMAL(6,2),
    ride_rating INT,
    duration_minutes DECIMAL(10,1) NOT NULL,
    price_eur DECIMAL(10,2), 
    data_quality_status VARCHAR(20) DEFAULT 'CLEAN',
    
    -- STRUCTURAL DATABASE DATA QUALITY SHIELDS (Defensive Constraints Matrix)
    CONSTRAINT fk_fact_rides_location FOREIGN KEY (location_id) REFERENCES public.production_locations(location_id),
    CONSTRAINT fk_fact_rides_date FOREIGN KEY (ride_date_key) REFERENCES public.dim_date(date_key),
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
    ride_date := NEW.start_timestamp::DATE;

    price_working_text := TRIM(NEW.raw_price);
    price_working_text := REPLACE(price_working_text, '€', '');
    price_working_text := REPLACE(price_working_text, '$', '');
    price_working_text := REPLACE(price_working_text, ',', '.');

    IF price_working_text !~ '^-?[0-9]+(?:\.[0-9]+)?$' OR price_working_text IS NULL OR price_working_text = '' THEN
        quality_status := 'UNKNOWN';
        final_raw_price := NULL;
    ELSE
        final_raw_price := price_working_text::DECIMAL(10,2);
    END IF;

    clean_currency := UPPER(TRIM(COALESCE(NEW.currency, 'EUR')));
    IF clean_currency = '' THEN clean_currency := 'EUR'; END IF;

    calculated_minutes := ROUND((EXTRACT(EPOCH FROM (NEW.end_timestamp - NEW.start_timestamp)) / 60.0)::NUMERIC, 1);

    IF final_raw_price IS NULL THEN
        calculated_price_eur := NULL;
    ELSE
        -- FIXED FX LOOKUP VECTOR: Now safely queries external dim_exchange_rates table attributes correctly
        SELECT er.exchange_rate_to_eur INTO active_rate 
        FROM public.dim_exchange_rates er
        WHERE er.currency = clean_currency 
          AND ride_date >= er.valid_from 
          AND (er.valid_to IS NULL OR ride_date <= er.valid_to)
        LIMIT 1;

        IF active_rate IS NULL OR active_rate <= 0 THEN active_rate := 1.0; END IF;
        calculated_price_eur := ROUND((final_raw_price / active_rate), 2);
    END IF;

    IF NEW.ride_rating < 1 OR NEW.ride_rating > 5 THEN
        NEW.ride_rating := NULL;
    END IF;

    -- SAMOOPRAVA KALENDÁRA: Dynamicky vložíme chýbajúci dátum do dim_date pred zápisom do faktov
    INSERT INTO public.dim_date (date_key, year_attribute, month_attribute, month_name_attribute, quarter_attribute, week_of_year_attribute, day_of_week_attribute, is_weekend_attribute)
    VALUES (
        ride_date,
        EXTRACT(YEAR FROM ride_date)::INT,
        EXTRACT(MONTH FROM ride_date)::INT,
        TO_CHAR(ride_date, 'Month'),
        EXTRACT(QUARTER FROM ride_date)::INT,
        EXTRACT(WEEK FROM ride_date)::INT,
        EXTRACT(ISODOW FROM ride_date)::INT,
        CASE WHEN EXTRACT(ISODOW FROM ride_date) IN (6, 7) THEN TRUE ELSE FALSE END
    ) ON CONFLICT (date_key) DO NOTHING;

    INSERT INTO public.fact_rides (
        ride_id, car_id, user_id, location_id, start_timestamp, end_timestamp, ride_date_key,
        distance_km, ride_rating, duration_minutes, price_eur, data_quality_status
    )
    VALUES (
        NEW.ride_id::INT, NEW.car_id, NEW.user_id, NEW.location_id, NEW.start_timestamp, NEW.end_timestamp, ride_date,
        NEW.distance_km, NEW.ride_rating, calculated_minutes, calculated_price_eur, quality_status
    )
    ON CONFLICT (ride_id) DO UPDATE SET
        car_id = EXCLUDED.car_id,
        user_id = EXCLUDED.user_id,
        location_id = EXCLUDED.location_id,
        start_timestamp = EXCLUDED.start_timestamp,
        end_timestamp = EXCLUDED.end_timestamp,
        ride_date_key = EXCLUDED.ride_date_key,
        distance_km = EXCLUDED.distance_km,
        ride_rating = EXCLUDED.ride_rating,
        duration_minutes = EXCLUDED.duration_minutes,
        price_eur = EXCLUDED.price_eur,
        data_quality_status = EXCLUDED.data_quality_status;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_sync_raw_to_fact ON public.raw_rides;
CREATE TRIGGER trg_sync_raw_to_fact
AFTER INSERT ON public.raw_rides
FOR EACH ROW
EXECUTE FUNCTION public.sync_raw_to_fact_function();

-- =====================================================================
-- 5. PERFORMANCE OPTIMIZATION LAYER (Analytical Filter Indexing Matrix)
-- =====================================================================
CREATE INDEX IF NOT EXISTS idx_fact_rides_start_date ON public.fact_rides (start_timestamp);
CREATE INDEX IF NOT EXISTS idx_fact_rides_date_key ON public.fact_rides (ride_date_key);
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
    l.city_name, l.country,
    d.year_attribute, d.month_name_attribute, d.is_weekend_attribute
FROM public.fact_rides r
INNER JOIN public.dim_date d ON r.ride_date_key = d.date_key
LEFT JOIN public.production_locations l ON r.location_id = l.location_id
LEFT JOIN public.production_cars c ON r.car_id = c.car_id 
    AND r.start_timestamp >= c.valid_from 
    AND (r.start_timestamp <= c.valid_to OR c.valid_to IS NULL);

CREATE OR REPLACE VIEW public.view_clean_reporting AS
SELECT 
    ride_id, start_timestamp, distance_km, duration_minutes, brand, model_name, city_name, ride_rating,
    year_attribute, month_name_attribute, is_weekend_attribute,
    price_eur AS reporting_price,
    data_quality_status AS reporting_quality_flag
FROM public.view_fleet_performance;
