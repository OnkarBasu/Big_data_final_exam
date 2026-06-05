from dataclasses import dataclass
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from src.config import (
    COLLISION_RADIUS_NM, TIME_BUCKET_SECONDS, TIME_BUCKET_SLACK,
)
from src.preprocess import haversine_nm_expr_pair


@dataclass
class CollisionResult:
    mmsi_a:     int
    mmsi_b:     int
    event_time: datetime
    event_lat:  float
    event_lon:  float
    distance_nm: float


def find_collision(df: DataFrame) -> CollisionResult:
    df = df.withColumn(
        "time_bucket",
        (F.unix_timestamp("timestamp") / TIME_BUCKET_SECONDS).cast("long"),
    )

    thresholds = [COLLISION_RADIUS_NM, 0.2, 0.5]
    for radius in thresholds:
        print(f"[detect] Trying collision radius = {radius} nm")
        result = _run_detection(df, radius)
        if result is not None:
            return result

    raise RuntimeError("No collision candidates found even at 0.5 nm — check data coverage.")


def _pair_select(a, b):
    """Select and rename columns from both aliases into flat named columns."""
    return a.join(b, (F.col("a.time_bucket") == F.col("b.time_bucket")) & (F.col("a.mmsi") < F.col("b.mmsi"))) \
            .select(
                F.col("a.mmsi").alias("mmsi_a"),
                F.col("b.mmsi").alias("mmsi_b"),
                F.col("a.timestamp").alias("ts_a"),
                F.col("a.lat").alias("lat_a"),
                F.col("a.lon").alias("lon_a"),
                F.col("b.lat").alias("lat_b"),
                F.col("b.lon").alias("lon_b"),
            )


def _run_detection(df: DataFrame, radius: float):
    # Shift bucket on one side to cover ±1 slack with equi-joins (hash join)
    a  = df.alias("a")
    b0 = df.alias("b")
    b1 = df.withColumn("time_bucket", F.col("time_bucket") + TIME_BUCKET_SLACK).alias("b")
    b2 = df.withColumn("time_bucket", F.col("time_bucket") - TIME_BUCKET_SLACK).alias("b")

    pairs = _pair_select(a, b0) \
        .union(_pair_select(a, b1)) \
        .union(_pair_select(a, b2))

    pairs = pairs.withColumn(
        "distance_nm",
        haversine_nm_expr_pair("lat_a", "lon_a", "lat_b", "lon_b"),
    ).filter(F.col("distance_nm") <= radius)

    rows = pairs.orderBy("distance_nm").limit(1).collect()
    print(f"[detect] Radius {radius} nm — {'found candidate' if rows else 'no candidates'}")
    if not rows:
        return None

    row = rows[0]
    return CollisionResult(
        mmsi_a=row["mmsi_a"],
        mmsi_b=row["mmsi_b"],
        event_time=row["ts_a"],
        event_lat=(row["lat_a"] + row["lat_b"]) / 2,
        event_lon=(row["lon_a"] + row["lon_b"]) / 2,
        distance_nm=row["distance_nm"],
    )
