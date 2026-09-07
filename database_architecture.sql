-- =====================================================================
-- AUTOMATED CLOUD DATA WAREHOUSE - ADVANCED ANALYTICS LAYER
-- Module 10: Common Table Expressions (CTE) & Window Functions
-- =====================================================================

CREATE OR REPLACE VIEW public.v_bi_enterprise_reporting_marts AS
WITH cte_normalized_rides AS (
    -- Stage 1: Standardize raw elements and join descriptive dimensions
    SELECT 
        f.ride_id,
        f.user_id,
        f.start_timestamp,
        f.duration_minutes,
        f.price_eur,
        f.data_quality_status,
        l.city_name,
        l.country,
        c.brand AS car_brand,
        c.model_name AS car_model_name,
        c.fuel_type AS car_fuel_type
    FROM public.fact_rides f
    INNER JOIN public.production_locations l ON f.location_id = l.location_id
    -- Dynamic join to capture the exact SCD Type 2 dimension record valid at transaction time
    LEFT JOIN public.production_cars c ON f.car_id = c.car_id 
        AND f.start_timestamp >= c.valid_from 
        AND (f.start_timestamp <= c.valid_to OR c.valid_to IS NULL)
    WHERE f.data_quality_status = 'CLEAN'
),
cte_financial_telemetry AS (
    -- Stage 2: Execute cumulative window analytics across core dimensions
    SELECT 
        ride_id,
        user_id,
        city_name,
        country,
        start_timestamp,
        price_eur,
        car_brand,
        car_model_name,
        car_fuel_type,
        
        -- WINDOW FUNCTION 1: Calculates running revenue total partitioned by city over time
        SUM(price_eur) OVER (
            PARTITION BY city_name 
            ORDER BY start_timestamp 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_total_revenue_city,
        
        -- WINDOW FUNCTION 2: Deterministically ranks users based on transaction velocity per city
        ROW_NUMBER() OVER (
            PARTITION BY city_name 
            ORDER BY price_eur DESC, start_timestamp ASC
        ) AS user_spend_rank_in_city
    FROM cte_normalized_rides
)
-- Final Marts Output Deployment Layer for Power BI & Data Studio ingestion
SELECT 
    ride_id,
    user_id,
    city_name,
    country,
    start_timestamp,
    price_eur,
    car_brand,
    car_model_name,
    car_fuel_type,
    ROUND(running_total_revenue_city, 2) AS running_total_revenue_city,
    user_spend_rank_in_city,
    CASE 
        WHEN user_spend_rank_in_city <= 3 THEN 'Top Tier VIP'
        WHEN user_spend_rank_in_city <= 10 THEN 'High Value Active'
        ELSE 'Standard Retail'
    END AS user_segment_tier
FROM cte_financial_telemetry;
