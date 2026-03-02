
CREATE OR REPLACE VIEW `nsw_trains_analytics_gold.vw_live_train_positions` AS
SELECT 
    -- 1. Real-time Info from Silver
    rt.trip_id,
    rt.latitude,
    rt.longitude,
    rt.arrival_time as last_updated,
    
    -- 2. Route Details (Joined from Static Silver)
    r.route_short_name,
    r.route_long_name,
    
    -- 3. Trip Details (Joined from Static Silver)
    t.trip_headsign as destination,
    
    -- 4. Logic for "Status" (Example of adding business logic in Gold)
    CASE 
        WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), rt.arrival_time, MINUTE) > 10 THEN 'Stale Data'
        ELSE 'Active'
    END as tracking_status
FROM `nsw_trains_analytics_silver.trips_flattened` rt
LEFT JOIN `nsw_trains_analytics_silver.routes` r 
    ON rt.route_id = r.route_id
LEFT JOIN `nsw_trains_analytics_silver.trips` t 
    ON rt.trip_id = t.trip_id
WHERE r.route_long_name IS NOT NULL
  AND t.trip_headsign IS NOT NULL 
  AND t.trip_headsign != '';