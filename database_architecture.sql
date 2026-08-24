-- =====================================================================
-- 1. PRODUCTION WAREHOUSE TARGET TABLES (The Schema)
-- =====================================================================

-- Clean target fact table for Power BI & Looker Studio optimization
CREATE TABLE IF NOT EXISTS fact_rides (
    ride_id INT PRIMARY KEY,
    car_id INT,
    user_id INT,
    location_id INT,
    start_timestamp TIMESTAMP,
    end_timestamp TIMESTAMP,
    distance_km DECIMAL(6,2),
    ride_rating INT,
    duration_minutes DECIMAL(10,1),
    price_eur DECIMAL(10,2)
);

-- Clean production registries for Slowly Changing Dimensions (SCD Upsert)
CREATE TABLE IF NOT EXISTS production_cars (
    car_id INT PRIMARY KEY,
    brand VARCHAR(50),
    model_name VARCHAR(50),
    fuel_type VARCHAR(20),
    battery_capacity_kwh INT,
    purchase_date DATE,
    license_plate VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS production_locations (
    location_id INT PRIMARY KEY,
    city_name VARCHAR(50),
    country VARCHAR(50),
    local_manager VARCHAR(50)
);

-- =====================================================================
-- 2. AUTOMATED SANITIZATION & FX CALCULATOR (The Database Brain)
-- =====================================================================
CREATE OR REPLACE FUNCTION sync_raw_to_fact_function()
RETURNS TRIGGER AS $$
DECLARE
    active_rate DECIMAL(10,4);
    calculated_minutes DECIMAL(10,1);
    calculated_price_eur DECIMAL(10,2);
    sanitized_price_text VARCHAR(50);
    final_raw_price DECIMAL(10,2);
    clean_currency VARCHAR(3);
    is_price_broken BOOLEAN := FALSE;
BEGIN
    -- Bulletproof Price Sanitization (Strips alphabet characters, handles decimals)
    sanitized_price_text := REGEXP_REPLACE(REPLACE(NEW.raw_price::TEXT, ',', '.'), '[^0-9.]', '', 'g');
    
    IF sanitized_price_text = '' OR sanitized_price_text IS NULL THEN
        is_price_broken := TRUE;
    ELSE
        final_raw_price := sanitized_price_text::DECIMAL(10,2);
    END IF;

    -- Currency normalization fallback
    clean_currency := UPPER(TRIM(COALESCE(NEW.currency, 'EUR')));
    IF clean_currency = '' THEN clean_currency := 'EUR'; END IF;

    -- Automated minutes runtime calculation
    calculated_minutes := ROUND((EXTRACT(EPOCH FROM (NEW.end_timestamp - NEW.start_timestamp)) / 60.0)::NUMERIC, 1);

    -- Historical FX conversion lookup matrix
    IF is_price_broken = TRUE THEN
        calculated_price_eur := NULL; -- Flags data quality errors as UNKNOWN in frontend
    ELSE
        SELECT exchange_rate_to_eur INTO active_rate FROM dim_exchange_rates WHERE currency = clean_currency;
        IF active_rate IS NULL OR active_rate <= 0 THEN active_rate := 1.0; END IF;
        calculated_price_eur := ROUND((final_raw_price / active_rate), 2);
    END IF;

    -- Append mode injection into business logic layer
    INSERT INTO fact_rides (ride_id, car_id, user_id, location_id, start_timestamp, end_timestamp, distance_km, ride_rating, duration_minutes, price_eur)
    VALUES (NEW.ride_id, NEW.car_id, NEW.user_id, NEW.location_id, NEW.start_timestamp, NEW.end_timestamp, NEW.distance_km, NEW.ride_rating, calculated_minutes, calculated_price_eur);

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Deploy database webhook trigger
DROP TRIGGER IF EXISTS trg_sync_raw_to_fact ON raw_rides;
CREATE TRIGGER trg_sync_raw_to_fact
AFTER INSERT ON raw_rides
FOR EACH ROW
EXECUTE FUNCTION sync_raw_to_fact_function();

-- =====================================================================
-- 3. REPORTING VIEWS FOR METRIC SHIELDING
-- =====================================================================
CREATE OR REPLACE VIEW view_fleet_performance AS
SELECT 
    r.ride_id, r.start_timestamp, r.distance_km, r.duration_minutes, r.price_eur, r.ride_rating,
    c.brand, c.model_name, c.fuel_type, c.license_plate,
    l.city_name, l.country, l.local_manager
FROM fact_rides r
LEFT JOIN production_cars c ON r.car_id = c.car_id
LEFT JOIN production_locations l ON r.location_id = l.location_id;

CREATE OR REPLACE VIEW view_clean_reporting AS
SELECT 
    ride_id, start_timestamp, distance_km, duration_minutes, brand, model_name, city_name, ride_rating,
    CASE WHEN price_eur IS NULL THEN 'UNKNOWN' ELSE price_eur::TEXT END AS reporting_price
FROM view_fleet_performance;
