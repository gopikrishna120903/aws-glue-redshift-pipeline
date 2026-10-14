-- Reporting views for BI tools (QuickSight, Tableau, Power BI).

-- Hourly demand curve by zone, for driver supply planning.
CREATE OR REPLACE VIEW analytics.v_hourly_demand AS
SELECT pickup_zone,
       pickup_hour,
       COUNT(*)                                   AS trips,
       ROUND(AVG(surge_multiplier), 3)            AS avg_surge,
       ROUND(AVG(duration_minutes), 1)            AS avg_duration_minutes
FROM analytics.fact_trips
WHERE NOT is_speed_outlier
GROUP BY pickup_zone, pickup_hour;

-- Driver daily earnings with a rank inside each day.
CREATE OR REPLACE VIEW analytics.v_driver_daily_earnings AS
SELECT pickup_date,
       driver_id,
       COUNT(*)                                   AS trips,
       SUM(total_amount)                          AS gross_earnings,
       SUM(tip_amount)                            AS tips,
       RANK() OVER (PARTITION BY pickup_date ORDER BY SUM(total_amount) DESC) AS earnings_rank
FROM analytics.fact_trips
GROUP BY pickup_date, driver_id;

-- Day-over-day revenue change per zone.
CREATE OR REPLACE VIEW analytics.v_zone_revenue_trend AS
SELECT pickup_date,
       pickup_zone,
       SUM(fare_revenue)                          AS fare_revenue,
       SUM(fare_revenue) - LAG(SUM(fare_revenue)) OVER (PARTITION BY pickup_zone ORDER BY pickup_date) AS change_vs_prior_day
FROM analytics.daily_zone_metrics
GROUP BY pickup_date, pickup_zone;
