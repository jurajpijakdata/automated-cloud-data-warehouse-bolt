-- =====================================================================
-- 1. PRODUCTION WAREHOUSE TARGET TABLES (The Schema Layers)
-- =====================================================================

-- Production-grade Fact Table optimized for Power BI & Looker Studio calculations
CREATE TABLE IF NOT EXISTS public.fact_rides (
    ride_id INT PRIMARY KEY,
    car_id INT,
    user_id INT,
    location_id INT,
    start_timestamp TIMESTAMP,
    end_timestamp TIMESTAMP,
    distance_km DECIMAL(6,2),
    ride_rating INT,
    duration_minutes DECIMAL(10,1),
    price_eur DECIMAL(10,2), -- Kept purely numerical for Power BI SUM() operations
    data_quality_status VARCHAR(20) DEFAULT 'CLEAN' -- Sibling column for telemetry flags
);

CREATE TABLE IF NOT EXISTS public.production_cars (
    car_id INT PRIMARY KEY,
    brand VARCHAR(50),
    model_name VARCHAR(50),
    fuel_type VARCHAR(20),
    battery_capacity_kwh INT,
    purchase_date DATE,
    license_plate VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS public.production_locations (
    location_id INT PRIMARY KEY,
    city_name VARCHAR(50),
    country VARCHAR(50),
    local_manager VARCHAR(50)
);

-- =====================================================================
-- 2. ADVANCED FINANCIAL REGEX SANITIZATION ENGINE (The Database Brain)
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
    -- Validates exactly one optional leading negative sign and a correct decimal structure.
    -- Instantly blocks adversarial strings like "45 (order 5000)" or "12.50a" from concatenation.
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

    -- 3. TEMPORAL EFFECTIVE-DATED FX LOOKUP (SCD Type 2 Architecture)
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

    -- Secure transaction insertion into production layer
    INSERT INTO public.fact_rides (
        ride_id, car_id, user_id, location_id, start_timestamp, end_timestamp, 
        distance_km, ride_rating, duration_minutes, price_eur, data_quality_status
    )
    VALUES (
        NEW.ride_id, NEW.car_id, NEW.user_id, NEW.location_id, NEW.start_timestamp, NEW.end_timestamp, 
        NEW.distance_km, NEW.ride_rating, calculated_minutes, calculated_price_eur, quality_status
    );

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
-- 3. REPORTING VIEWS FOR POWER BI COMPATIBILITY
-- =====================================================================
CREATE OR REPLACE VIEW public.view_fleet_performance AS
SELECT 
    r.ride_id, r.start_timestamp, r.distance_km, r.duration_minutes, r.price_eur, r.ride_rating, r.data_quality_status,
    c.brand, c.model_name, c.fuel_type, c.license_plate,
    l.city_name, l.country, l.local_manager
FROM public.fact_rides r
LEFT JOIN public.production_cars c ON r.car_id = c.car_id
LEFT JOIN public.production_locations l ON r.location_id = l.location_id;

-- Fixed View Layer: Keeps metrics strictly decimal so Power BI can execute SUM() operations flawlessly
CREATE OR REPLACE VIEW public.view_clean_reporting AS
SELECT 
    ride_id, start_timestamp, distance_km, duration_minutes, brand, model_name, city_name, ride_rating,
    price_eur AS reporting_price, -- Pure DECIMAL measure column
    data_quality_status AS reporting_quality_flag -- Separated text attribute column
FROM public.view_fleet_performance;
