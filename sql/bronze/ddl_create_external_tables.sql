-- 1. Create the External Table
-- This points to your GCS bucket but lets you query it like a table.
CREATE OR REPLACE EXTERNAL TABLE `nsw_trains_analytics_bronze.ext_realtime_trips`
WITH PARTITION COLUMNS (
    dt DATE -- This tells BQ: "The folder dt=YYYY-MM-DD is actually a date column"
)
OPTIONS (
    format = 'JSON',
    -- POINT THIS TO YOUR BUCKET NAME:
    uris = ['gs://nsw-trains-analytics-bronze/realtime_trip_updates/nsw_trains/*'],
    
    -- This prefix tells BQ where the partitioning starts:
    hive_partition_uri_prefix = 'gs://nsw-trains-analytics-bronze/realtime_trip_updates/nsw_trains',
    
    -- Optimize by requiring users to filter by date (Good practice):
    require_hive_partition_filter = false 
);

-- 2. External Table for STOPS
CREATE OR REPLACE EXTERNAL TABLE `nsw_trains_analytics_bronze.ext_stops`
OPTIONS (
    format = 'CSV',
    uris = ['gs://nsw-trains-analytics-bronze/static_files/static_gtfs/stops.txt'],
    skip_leading_rows = 1,
    field_delimiter = ','
);

-- 3. External Table for ROUTES
CREATE OR REPLACE EXTERNAL TABLE `nsw_trains_analytics_bronze.ext_routes`
OPTIONS (
    format = 'CSV',
    uris = ['gs://nsw-trains-analytics-bronze/static_files/static_gtfs/routes.txt'],
    skip_leading_rows = 1,
    field_delimiter = ','
);


-- 4. External Table for TRIPS
CREATE OR REPLACE EXTERNAL TABLE `nsw_trains_analytics_bronze.ext_trips`
OPTIONS (
    format = 'CSV',
    uris = ['gs://nsw-trains-analytics-bronze/static_files/static_gtfs/trips.txt'],
    skip_leading_rows = 1,
    field_delimiter = ','
);

-- 5. External Table for STOP TIMES
CREATE OR REPLACE EXTERNAL TABLE `nsw_trains_analytics_bronze.ext_stop_times` (
    trip_id STRING,
    arrival_time STRING,
    departure_time STRING,
    stop_id STRING, -- Manually defining this as STRING fixes the error
    stop_sequence STRING,
    stop_headsign STRING,
    pickup_type STRING,
    drop_off_type STRING,
    shape_dist_traveled STRING
)
OPTIONS (
    format = 'CSV',
    uris = ['gs://nsw-trains-analytics-bronze/static_files/static_gtfs/stop_times.txt'],
    skip_leading_rows = 1,
    field_delimiter = ','
);