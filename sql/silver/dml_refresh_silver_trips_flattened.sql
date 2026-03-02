-- BigQuery Data Refresh Job that runs every hour
BEGIN
  -- 1. Clear out the data for the current day to avoid duplicates
  DELETE FROM `nsw_trains_analytics_silver.trips_flattened`
  WHERE ingestion_date = CURRENT_DATE();

  -- 2. Insert the fresh, deduplicated batch from Bronze
  INSERT INTO `nsw_trains_analytics_silver.trips_flattened` (
    ingestion_date,
    trip_id,
    route_id,
    latitude,
    longitude,
    vehicle_id,
    arrival_time,
    processed_at
  )
  SELECT
    b.dt as ingestion_date,
    JSON_VALUE(TO_JSON_STRING(e), '$.vehicle.trip.tripId') as trip_id,
    JSON_VALUE(TO_JSON_STRING(e), '$.vehicle.trip.routeId') as route_id,
    SAFE_CAST(JSON_VALUE(TO_JSON_STRING(e), '$.vehicle.position.latitude') AS FLOAT64) as latitude,
    SAFE_CAST(JSON_VALUE(TO_JSON_STRING(e), '$.vehicle.position.longitude') AS FLOAT64) as longitude,
    JSON_VALUE(TO_JSON_STRING(e), '$.vehicle.vehicle.id') as vehicle_id,
    TIMESTAMP_SECONDS(CAST(JSON_VALUE(TO_JSON_STRING(e), '$.vehicle.timestamp') AS INT64)) as arrival_time,
    CURRENT_TIMESTAMP() as processed_at
  FROM `nsw_trains_analytics_bronze.ext_realtime_trips` b,
  UNNEST(b.entity) as e 
  WHERE b.dt = CURRENT_DATE()
  -- Deduplicate within the source to ensure we only take the absolute latest update
  QUALIFY ROW_NUMBER() OVER (
      PARTITION BY JSON_VALUE(TO_JSON_STRING(e), '$.vehicle.trip.tripId')
      ORDER BY JSON_VALUE(TO_JSON_STRING(e), '$.vehicle.timestamp') DESC
  ) = 1
  AND JSON_VALUE(TO_JSON_STRING(e), '$.vehicle.trip.tripId') IS NOT NULL;
END;