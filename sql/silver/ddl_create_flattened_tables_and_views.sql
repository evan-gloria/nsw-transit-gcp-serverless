
CREATE OR REPLACE TABLE `nsw_trains_analytics_silver.trips_flattened` AS
SELECT
  -- 1. Metadata
  b.dt as ingestion_date,
  
  -- 2. Extract IDs from the 'trip' object
  JSON_VALUE(TO_JSON_STRING(e), '$.vehicle.trip.tripId') as trip_id,
  JSON_VALUE(TO_JSON_STRING(e), '$.vehicle.trip.routeId') as route_id,
  
  -- 3. Extract Vehicle Position
  SAFE_CAST(JSON_VALUE(TO_JSON_STRING(e), '$.vehicle.position.latitude') AS FLOAT64) as latitude,
  SAFE_CAST(JSON_VALUE(TO_JSON_STRING(e), '$.vehicle.position.longitude') AS FLOAT64) as longitude,
  JSON_VALUE(TO_JSON_STRING(e), '$.vehicle.vehicle.id') as vehicle_id,

  -- 4. Reconstruct Arrival Time (Mirroring your PySpark logic)
  -- We use the vehicle timestamp as the actual arrival/event time
  TIMESTAMP_SECONDS(CAST(JSON_VALUE(TO_JSON_STRING(e), '$.vehicle.timestamp') AS INT64)) as arrival_time,
  
  -- 5. Add a processing timestamp for auditing
  CURRENT_TIMESTAMP() as processed_at

FROM `nsw_trains_analytics_bronze.ext_realtime_trips` b,
UNNEST(b.entity) as e 
WHERE b.dt = CURRENT_DATE()

-- Deduplicate: Keep the latest update for each trip
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY JSON_VALUE(TO_JSON_STRING(e), '$.vehicle.trip.tripId')
    ORDER BY JSON_VALUE(TO_JSON_STRING(e), '$.vehicle.timestamp') DESC
) = 1
-- Filter out rows where trip_id is missing (e.g. static alerts)
AND JSON_VALUE(TO_JSON_STRING(e), '$.vehicle.trip.tripId') IS NOT NULL;


-- Silver STOPS
CREATE OR REPLACE TABLE `nsw_trains_analytics_silver.stops` AS
SELECT 
    stop_id,
    stop_name,
    SAFE_CAST(stop_lat AS FLOAT64) as stop_lat,
    SAFE_CAST(stop_lon AS FLOAT64) as stop_lon,
    location_type
FROM `nsw_trains_analytics_bronze.ext_stops`;

-- Silver ROUTES
CREATE OR REPLACE TABLE `nsw_trains_analytics_silver.routes` AS
SELECT 
    route_id,
    route_short_name,
    route_long_name,
    SAFE_CAST(route_type AS INT64) as route_type
FROM `nsw_trains_analytics_bronze.ext_routes`;

-- Silver TRIPS (The mapping between Route and Trip)
CREATE OR REPLACE TABLE `nsw_trains_analytics_silver.trips` AS
SELECT 
    trip_id,
    route_id,
    service_id,
    trip_headsign,
    direction_id
FROM `nsw_trains_analytics_bronze.ext_trips`;

-- Silver STOP_TIMES (The schedule for each trip)
CREATE OR REPLACE TABLE `nsw_trains_analytics_silver.stop_times` AS
SELECT 
    trip_id,
    arrival_time,
    departure_time,
    stop_id,
    SAFE_CAST(stop_sequence AS INT64) as stop_sequence
FROM `nsw_trains_analytics_bronze.ext_stop_times`;
