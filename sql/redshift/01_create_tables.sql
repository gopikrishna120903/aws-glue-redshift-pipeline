-- Amazon Redshift (Serverless) schema for the ride-share pipeline.
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Trip fact. Distributed on driver_id (most joins and per-driver metrics),
-- sorted on pickup_date so date-range queries skip blocks.
CREATE TABLE IF NOT EXISTS analytics.fact_trips (
    trip_id            VARCHAR(32)   NOT NULL,
    driver_id          VARCHAR(16)   NOT NULL,
    rider_id           VARCHAR(16)   NOT NULL,
    product            VARCHAR(16),
    pickup_ts          TIMESTAMP     NOT NULL,
    dropoff_ts         TIMESTAMP     NOT NULL,
    pickup_date        DATE          NOT NULL,
    pickup_hour        SMALLINT,
    pickup_zone_id     INTEGER,
    pickup_zone        VARCHAR(64),
    pickup_borough     VARCHAR(32),
    dropoff_zone_id    INTEGER,
    dropoff_zone       VARCHAR(64),
    distance_miles     DOUBLE PRECISION,
    duration_minutes   DOUBLE PRECISION,
    avg_speed_mph      DOUBLE PRECISION,
    fare_amount        DECIMAL(10,2),
    tip_amount         DECIMAL(10,2),
    total_amount       DECIMAL(10,2),
    tip_pct            DOUBLE PRECISION,
    surge_multiplier   DOUBLE PRECISION,
    is_surge           BOOLEAN,
    is_airport_trip    BOOLEAN,
    is_speed_outlier   BOOLEAN,
    payment_type       VARCHAR(16),
    event_ts           TIMESTAMP,
    source_file        VARCHAR(512),
    loaded_at          TIMESTAMP DEFAULT GETDATE(),
    PRIMARY KEY (trip_id)          -- informational in Redshift; MERGE enforces uniqueness
)
DISTKEY (driver_id)
SORTKEY (pickup_date, pickup_zone_id);

-- Staging table: same columns, no defaults, truncated after every load.
CREATE TABLE IF NOT EXISTS staging.trips (LIKE analytics.fact_trips);

-- Small daily aggregate: replicated to every node (DISTSTYLE ALL) for fast dashboard joins.
CREATE TABLE IF NOT EXISTS analytics.daily_zone_metrics (
    pickup_date           DATE         NOT NULL,
    pickup_zone_id        INTEGER      NOT NULL,
    pickup_zone           VARCHAR(64),
    product               VARCHAR(16)  NOT NULL,
    trips                 BIGINT,
    active_drivers        BIGINT,
    total_miles           DOUBLE PRECISION,
    fare_revenue          DECIMAL(14,2),
    tips                  DECIMAL(14,2),
    avg_duration_minutes  DOUBLE PRECISION,
    surge_share           DOUBLE PRECISION,
    PRIMARY KEY (pickup_date, pickup_zone_id, product)
)
DISTSTYLE ALL
SORTKEY (pickup_date);

CREATE TABLE IF NOT EXISTS staging.daily_zone_metrics (LIKE analytics.daily_zone_metrics);

-- Zone lookup, also small and replicated.
CREATE TABLE IF NOT EXISTS analytics.dim_zone (
    zone_id    INTEGER PRIMARY KEY,
    zone_name  VARCHAR(64),
    borough    VARCHAR(32)
)
DISTSTYLE ALL;
