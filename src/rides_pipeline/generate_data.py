"""Generate deterministic ride-share trip events as JSON Lines files.

Files are laid out the way an ingestion service (Kinesis Firehose, an API
exporter) would land them in S3: landing/trips/dt=YYYY-MM-DD/hour=HH/part-N.json

The data includes the problems a real feed has:
  * the same trip sent twice (an updated version with a later event time)
  * malformed records: missing pickup time, drop-off before pickup,
    negative fares, impossible distances, unknown zone IDs
  * fields as strings, as a JSON producer would send them
"""
from __future__ import annotations

import json
import random
from datetime import datetime, timedelta
from pathlib import Path

SEED = 11
ZONES = {  # zone_id -> (zone name, borough)
    1: ("Downtown", "Central"), 2: ("Midtown", "Central"), 3: ("Uptown", "North"), 4: ("Airport", "East"),
    5: ("University", "West"), 6: ("Harbor", "South"), 7: ("Old Town", "Central"), 8: ("Tech Park", "North"),
    9: ("Stadium", "East"), 10: ("Riverside", "West"), 11: ("Medical Center", "South"), 12: ("Suburbs", "North"),
}
PAYMENT_TYPES = ["card", "card", "card", "wallet", "cash"]
PRODUCTS = [("standard", 1.0), ("xl", 1.5), ("premium", 2.1), ("shared", 0.7)]


def _trip(rng: random.Random, trip_no: int, day: datetime) -> dict:
    hour = rng.choices(range(24), [1, 1, 1, 1, 1, 2, 4, 7, 9, 6, 5, 5, 6, 5, 5, 6, 8, 9, 8, 6, 5, 4, 3, 2])[0]
    pickup = day + timedelta(hours=hour, minutes=rng.randint(0, 59), seconds=rng.randint(0, 59))
    pu, do = rng.sample(list(ZONES), 2)
    miles = round(rng.lognormvariate(1.1, 0.6), 2)
    if 4 in (pu, do):
        miles = round(miles + rng.uniform(8, 15), 2)  # airport trips are long
    speed = rng.uniform(9, 28)
    duration_s = int(miles / speed * 3600) + rng.randint(60, 300)
    product, mult = rng.choices(PRODUCTS, [0.7, 0.12, 0.08, 0.1])[0]
    surge = rng.choices([1.0, 1.2, 1.5, 2.0], [0.8, 0.1, 0.07, 0.03])[0] if hour in (7, 8, 17, 18, 19) else 1.0
    fare = round((2.5 + miles * 1.75 + duration_s / 60 * 0.35) * mult * surge, 2)
    tip = round(fare * rng.choice([0, 0, 0.1, 0.15, 0.2]), 2) if rng.random() < 0.7 else 0.0
    return {
        "trip_id": f"T{day:%Y%m%d}-{trip_no:06d}",
        "driver_id": f"D{rng.randint(1, 400):04d}",
        "rider_id": f"R{rng.randint(1, 5000):05d}",
        "product": product,
        "pickup_ts": pickup.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dropoff_ts": (pickup + timedelta(seconds=duration_s)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "pickup_zone_id": str(pu),
        "dropoff_zone_id": str(do),
        "distance_miles": str(miles),
        "fare_amount": str(fare),
        "surge_multiplier": surge,
        "tip_amount": str(tip),
        "payment_type": rng.choice(PAYMENT_TYPES),
        "event_ts": (pickup + timedelta(seconds=duration_s + rng.randint(5, 120))).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def _corrupt(rng: random.Random, t: dict) -> dict:
    kind = rng.choice(["no_pickup", "time_travel", "negative_fare", "huge_distance", "bad_zone"])
    t = dict(t)
    if kind == "no_pickup":
        t["pickup_ts"] = None
    elif kind == "time_travel":
        t["dropoff_ts"], t["pickup_ts"] = t["pickup_ts"], t["dropoff_ts"]
    elif kind == "negative_fare":
        t["fare_amount"] = str(-abs(float(t["fare_amount"])))
    elif kind == "huge_distance":
        t["distance_miles"] = "950.0"
    else:
        t["pickup_zone_id"] = "99"
    return t


def generate(out_dir: Path, days: int = 3, trips_per_day: int = 4000, start: datetime = datetime(2025, 6, 1)) -> dict:
    """Write JSON Lines files under out_dir/dt=.../hour=.../ and return counts."""
    rng = random.Random(SEED)
    stats = {"files": 0, "records": 0, "unique_trips": 0, "duplicates": 0, "corrupt": 0}
    for d in range(days):
        day = start + timedelta(days=d)
        by_hour: dict[int, list[dict]] = {}
        for n in range(1, trips_per_day + 1):
            t = _trip(rng, n, day)
            if rng.random() < 0.02:
                t = _corrupt(rng, t)
                stats["corrupt"] += 1
            hour = int(t["event_ts"][11:13]) if t["event_ts"] else 0
            by_hour.setdefault(hour, []).append(t)
            stats["unique_trips"] += 1
            if rng.random() < 0.03:  # re-sent with a corrected tip and a later event time
                dup = dict(t)
                dup["tip_amount"] = str(round(float(t["fare_amount"]) * 0.2, 2)) if t["fare_amount"] else "0"
                dup["event_ts"] = (datetime.strptime(t["event_ts"], "%Y-%m-%dT%H:%M:%SZ") + timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
                by_hour.setdefault(min(hour + 1, 23), []).append(dup)
                stats["duplicates"] += 1
        for hour, rows in sorted(by_hour.items()):
            folder = out_dir / f"dt={day:%Y-%m-%d}" / f"hour={hour:02d}"
            folder.mkdir(parents=True, exist_ok=True)
            with (folder / "part-0000.json").open("w") as f:
                for r in rows:
                    f.write(json.dumps(r) + "\n")
            stats["files"] += 1
            stats["records"] += len(rows)
    return stats


def write_zone_lookup(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        f.write("zone_id,zone_name,borough\n")
        for zid, (name, borough) in ZONES.items():
            f.write(f"{zid},{name},{borough}\n")


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[2] / "data"
    print(generate(root / "landing" / "trips"))
    write_zone_lookup(root / "reference" / "zones.csv")
