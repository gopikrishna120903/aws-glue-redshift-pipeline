from datetime import datetime
from decimal import Decimal

from rides_pipeline.transforms import RAW_SCHEMA, daily_zone_metrics, deduplicate, enrich, parse, validate


def trip(**overrides):
    base = {
        "trip_id": "T1", "driver_id": "D0001", "rider_id": "R00001", "product": " Standard ",
        "pickup_ts": "2025-06-01T08:00:00Z", "dropoff_ts": "2025-06-01T08:20:00Z",
        "pickup_zone_id": "1", "dropoff_zone_id": "4", "distance_miles": "10.0", "fare_amount": "25.00",
        "surge_multiplier": 1.5, "tip_amount": "5.00", "payment_type": "CARD", "event_ts": "2025-06-01T08:21:00Z",
    }
    base.update(overrides)
    return base


def frame(spark, rows):
    return parse(spark.createDataFrame([tuple(r[f.name] for f in RAW_SCHEMA) for r in rows], RAW_SCHEMA))


def test_parse_casts_types_and_cleans_strings(spark):
    row = frame(spark, [trip()]).first()
    assert row.pickup_ts == datetime(2025, 6, 1, 8, 0, 0)
    assert row.fare_amount == Decimal("25.00")
    assert row.pickup_zone_id == 1
    assert row.product == "standard"
    assert row.payment_type == "card"


def test_deduplicate_keeps_latest_event(spark):
    df = frame(spark, [trip(tip_amount="1.00", event_ts="2025-06-01T08:21:00Z"),
                       trip(tip_amount="9.00", event_ts="2025-06-01T09:00:00Z")])
    out = deduplicate(df).collect()
    assert len(out) == 1 and out[0].tip_amount == Decimal("9.00")


def test_validate_flags_every_broken_rule(spark, zones):
    df = frame(spark, [
        trip(trip_id="OK"),
        trip(trip_id="NOPICKUP", pickup_ts=None),
        trip(trip_id="BACKWARDS", pickup_ts="2025-06-01T09:00:00Z", dropoff_ts="2025-06-01T08:00:00Z"),
        trip(trip_id="NEGATIVE", fare_amount="-5.00"),
        trip(trip_id="FAR", distance_miles="950"),
        trip(trip_id="NOWHERE", pickup_zone_id="99"),
        trip(trip_id="MULTI", fare_amount="-1.00", pickup_zone_id="99"),
    ])
    valid, rejected = validate(df, zones)
    assert [r.trip_id for r in valid.collect()] == ["OK"]
    reasons = {r.trip_id: set(r.reject_reasons) for r in rejected.collect()}
    assert reasons["NOPICKUP"] == {"missing_timestamps"}
    assert reasons["BACKWARDS"] == {"dropoff_before_pickup"}
    assert reasons["NEGATIVE"] == {"negative_amount"}
    assert reasons["FAR"] == {"invalid_distance"}
    assert reasons["NOWHERE"] == {"unknown_zone"}
    assert reasons["MULTI"] == {"negative_amount", "unknown_zone"}


def test_enrich_derives_metrics(spark, zones):
    valid, _ = validate(frame(spark, [trip()]), zones)
    r = enrich(valid, zones).first()
    assert r.duration_minutes == 20.0
    assert r.avg_speed_mph == 30.0
    assert r.total_amount == Decimal("30.00")
    assert r.tip_pct == 0.2
    assert r.is_airport_trip is True and r.is_surge is True and r.is_speed_outlier is False
    assert r.pickup_zone == "Downtown" and r.dropoff_zone == "Airport"
    assert str(r.pickup_date) == "2025-06-01" and r.pickup_hour == 8


def test_speed_outliers_are_flagged_not_dropped(spark, zones):
    valid, _ = validate(frame(spark, [trip(distance_miles="100", dropoff_ts="2025-06-01T08:30:00Z")]), zones)
    r = enrich(valid, zones).first()
    assert r.avg_speed_mph == 200.0 and r.is_speed_outlier is True


def test_daily_zone_metrics_aggregates(spark, zones):
    df = frame(spark, [trip(trip_id="A", driver_id="D1"), trip(trip_id="B", driver_id="D1"),
                       trip(trip_id="C", driver_id="D2", surge_multiplier=1.0)])
    valid, _ = validate(df, zones)
    m = daily_zone_metrics(enrich(valid, zones)).first()
    assert m.trips == 3 and m.active_drivers == 2
    assert m.fare_revenue == Decimal("75.00") and m.tips == Decimal("15.00")
    assert abs(m.surge_share - 2 / 3) < 1e-3
