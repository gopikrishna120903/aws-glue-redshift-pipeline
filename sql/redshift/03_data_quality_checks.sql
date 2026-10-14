-- Post-load checks. Each query must return 0 rows; Step Functions fails the run otherwise.

-- 1. No duplicate trips after MERGE
SELECT trip_id, COUNT(*) FROM analytics.fact_trips GROUP BY trip_id HAVING COUNT(*) > 1;

-- 2. Aggregate table agrees with the fact table
SELECT m.pickup_date, m.pickup_zone_id, m.product, m.trips, f.trips AS fact_trips
FROM analytics.daily_zone_metrics m
JOIN (SELECT pickup_date, pickup_zone_id, product, COUNT(*) AS trips
      FROM analytics.fact_trips GROUP BY 1, 2, 3) f
  USING (pickup_date, pickup_zone_id, product)
WHERE m.trips <> f.trips;

-- 3. No impossible values slipped through
SELECT trip_id FROM analytics.fact_trips
WHERE dropoff_ts <= pickup_ts OR fare_amount < 0 OR distance_miles <= 0;
