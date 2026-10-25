-- Athena Validation Suite: NYC TLC Mobility Platform

-- 1. Verify Raw Layer Ingestion Count (~90-120M records across 6 months)
SELECT COUNT(*) AS total_raw_records FROM nyctlc_raw.fhvhv_tripdata;

-- 2. Verify Curated Layer Count (~35M Lyft trips after quality filter)
SELECT COUNT(*) AS total_curated_records FROM nyctlc_curated.fhvhv_trips_curated;

-- 3. Curated Trips Grouped by Borough
SELECT 
    pickup_borough, 
    COUNT(*) AS total_trips,
    ROUND(AVG(base_passenger_fare), 2) AS avg_fare,
    ROUND(AVG(trip_duration_minutes), 2) AS avg_duration_min
FROM 
    nyctlc_curated.fhvhv_trips_curated 
GROUP BY 
    pickup_borough 
ORDER BY 
    total_trips DESC;

-- 4. Hourly Peak Demand Analysis
SELECT 
    pickup_hour,
    is_weekend,
    total_trips,
    avg_fare,
    avg_tip_percentage
FROM 
    nyctlc_aggregated.hourly_pattern_summary
ORDER BY 
    total_trips DESC
LIMIT 10;

-- 5. Top 5 Busiest Commute Routes
SELECT 
    pickup_zone, 
    dropoff_zone, 
    total_trips, 
    avg_fare 
FROM 
    nyctlc_aggregated.route_summary 
ORDER BY 
    total_trips DESC 
LIMIT 5;

-- 6. Check DynamoDB Audit Logs
SELECT 
    job_name,
    layer,
    status,
    records_written,
    duration_seconds,
    run_timestamp
FROM 
    "pipeline-job-audit"
ORDER BY 
    run_timestamp DESC;
