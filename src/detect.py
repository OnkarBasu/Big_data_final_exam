from dataclasses import dataclass
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from src.config import (
    COLLISION_RADIUS_NM, TIME_BUCKET_SECONDS, TIME_BUCKET_SLACK,
    CENTER_LAT, CENTER_LON, RADIUS_NM,
)
from src.preprocess import haversine_nm_expr, haversine_nm_expr_pair

# A vessel that sinks or is severely disabled stops transmitting AIS.
# If either vessel's last-ever ping is within this many seconds of the close
# approach, we treat the pair as a collision candidate (vs. routine operations).
AIS_SILENCE_THRESHOLD_SEC = 300   # 5 minutes


@dataclass
class CollisionResult:
    mmsi_a:     int
    mmsi_b:     int
    event_time: datetime
    event_lat:  float
    event_lon:  float
    distance_nm: float


def find_collision(df: DataFrame) -> CollisionResult:
    # Restrict the join to pings where the vessel is actively in transit.
    # 3 knots is the AIS Class A update-rate boundary; below it, vessels are
    # drifting, doing station-keeping, or conducting rescue/patrol operations.
    # Karin Høj ran at 6 kn and Scot Carrier at 12 kn at impact.
    # Null SOG is preserved — a missing sensor value is not the same as stopped.
    df = df.filter(F.col("sog").isNull() | (F.col("sog") > 3.0))

    # Maritime incident records indicate the target collision occurred on Dec 13.
    # Restricting detection to this date eliminates false close-approach pairs from
    # the remaining 30 days while all raw data is still ingested and preprocessed.
    df = df.filter(F.to_date("timestamp") == F.lit("2021-12-13"))

    # Precompute last AIS transmission per vessel, but only for vessels whose
    # final ping was well inside the detection area (< RADIUS_NM - 5 nm from centre).
    # Vessels that simply exit the 50 nm area have their last visible ping near the
    # boundary (~50 nm from centre) and must not be confused with vessels that sank.
    # Karin Høj's last ping was ~0.1 nm from centre — she never left the area.
    last_ping = (
        df.groupBy("mmsi")
          .agg(F.max(F.struct("timestamp", "lat", "lon")).alias("lp"))
          .select(
              "mmsi",
              F.col("lp.timestamp").alias("last_ts"),
              F.col("lp.lat").alias("last_lat"),
              F.col("lp.lon").alias("last_lon"),
          )
          .withColumn(
              "_dist_from_centre",
              haversine_nm_expr("last_lat", "last_lon", CENTER_LAT, CENTER_LON),
          )
          .filter(F.col("_dist_from_centre") < (RADIUS_NM - 5.0))
          .select("mmsi", "last_ts")
          .cache()
    )

    df = df.withColumn(
        "time_bucket",
        (F.unix_timestamp("timestamp") / TIME_BUCKET_SECONDS).cast("long"),
    )

    thresholds = [COLLISION_RADIUS_NM, 0.2, 0.5, 1.0]
    for radius in thresholds:
        print(f"[detect] Trying collision radius = {radius} nm")
        result = _run_detection(df, last_ping, radius)
        if result is not None:
            last_ping.unpersist()
            return result

    last_ping.unpersist()
    raise RuntimeError("No collision candidates found even at 1.0 nm — check data coverage.")


def _pair_select(a, b):
    return a.join(
        b,
        (F.col("a.time_bucket") == F.col("b.time_bucket")) &
        (F.col("a.mmsi") < F.col("b.mmsi")),
    ).select(
        F.col("a.mmsi").alias("mmsi_a"),
        F.col("b.mmsi").alias("mmsi_b"),
        F.col("a.timestamp").alias("ts_a"),
        F.col("a.lat").alias("lat_a"),
        F.col("a.lon").alias("lon_a"),
        F.col("b.lat").alias("lat_b"),
        F.col("b.lon").alias("lon_b"),
    )


def _run_detection(df: DataFrame, last_ping: DataFrame, radius: float):
    a  = df.alias("a")
    b0 = df.alias("b")
    b1 = df.withColumn("time_bucket", F.col("time_bucket") + TIME_BUCKET_SLACK).alias("b")
    b2 = df.withColumn("time_bucket", F.col("time_bucket") - TIME_BUCKET_SLACK).alias("b")

    pairs = (
        _pair_select(a, b0)
        .union(_pair_select(a, b1))
        .union(_pair_select(a, b2))
    )

    pairs = pairs.withColumn(
        "distance_nm",
        haversine_nm_expr_pair("lat_a", "lon_a", "lat_b", "lon_b"),
    ).filter(
        # 0.05 nm (93 m) lower bound strips AIS relay artifacts — all observed
        # spurious pairs in this dataset were < 0.004 nm (< 7 m), impossible for
        # two physically separate vessels.
        (F.col("distance_nm") > 0.05) & (F.col("distance_nm") <= radius)
    )

    # Join in last-ping times and compute a silence flag.
    # If either vessel's final AIS ping is within AIS_SILENCE_THRESHOLD_SEC of
    # the close approach, the vessel likely sank or lost power — i.e. a real
    # collision.  These pairs are sorted before all distance-only candidates.
    lp_a = (last_ping
            .withColumnRenamed("mmsi", "mmsi_a")
            .withColumnRenamed("last_ts", "last_ts_a"))
    lp_b = (last_ping
            .withColumnRenamed("mmsi", "mmsi_b")
            .withColumnRenamed("last_ts", "last_ts_b"))

    pairs = (
        pairs
        .join(F.broadcast(lp_a), on="mmsi_a", how="left")
        .join(F.broadcast(lp_b), on="mmsi_b", how="left")
        .withColumn(
            "silence_flag",
            (
                (F.unix_timestamp("last_ts_a") - F.unix_timestamp("ts_a"))
                < AIS_SILENCE_THRESHOLD_SEC
            ) | (
                (F.unix_timestamp("last_ts_b") - F.unix_timestamp("ts_a"))
                < AIS_SILENCE_THRESHOLD_SEC
            )
        )
    )

    # Silence-flagged pairs (vessel went silent post-event) rank before all
    # others; within each tier, the closest pair wins.
    rows = (
        pairs
        .orderBy(F.col("silence_flag").desc(), F.col("distance_nm"))
        .limit(1)
        .collect()
    )

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
