CREATE OR REPLACE VIEW `nsw_trains_analytics_gold.vw_train_performance_metrics` AS

WITH base_silver_data AS (
    SELECT * FROM `nsw_trains_analytics_gold.vw_live_train_positions`
)

SELECT 
    *, 
    -- 1. SIMULATE DELAY 
    CASE 
        WHEN RAND() < 0.7 THEN CAST(FLOOR(2 * RAND()) AS INT64) 
        ELSE CAST(FLOOR(2 + (13 * RAND())) AS INT64)
    END as delay_minutes,
    
    -- 2. PEAK VS OFF-PEAK FLAG
    CASE 
        WHEN EXTRACT(DAYOFWEEK FROM last_updated) BETWEEN 2 AND 6 
             AND EXTRACT(HOUR FROM last_updated) IN (7, 8, 9, 16, 17, 18)
        THEN 'PEAK'
        ELSE 'OFF-PEAK'
    END as time_period,

    -- 3. GEO-CLUSTERS 
    CONCAT(CAST(ROUND(latitude, 2) AS STRING), ',', CAST(ROUND(longitude, 2) AS STRING)) as geo_cluster
FROM base_silver_data;