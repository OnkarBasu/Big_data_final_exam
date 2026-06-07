import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.types import LongType, DoubleType
from src.config import (
    AIS_COLUMNS, CENTER_LAT, CENTER_LON, RADIUS_NM,
    MAX_SPEED_KNOTS, MIN_MOVING_SOG_KNOTS, STATIONARY_NAV_CODES,
)


def haversine_nm_expr(lat_col: str, lon_col: str, lat2: float, lon2: float):
    R = 3440.065
    dlat = F.radians(F.col(lat_col) - F.lit(lat2))
    dlon = F.radians(F.col(lon_col) - F.lit(lon2))
    a = (
        F.sin(dlat / 2) ** 2
        + F.cos(F.radians(F.col(lat_col)))
        * F.cos(F.lit(lat2 * 3.14159265358979 / 180))
        * F.sin(dlon / 2) ** 2
    )
    return F.lit(2 * R) * F.asin(F.sqrt(a))


def haversine_nm_expr_pair(lat1: str, lon1: str, lat2: str, lon2: str):
    R = 3440.065
    dlat = F.radians(F.col(lat1) - F.col(lat2))
    dlon = F.radians(F.col(lon1) - F.col(lon2))
    a = (
        F.sin(dlat / 2) ** 2
        + F.cos(F.radians(F.col(lat1)))
        * F.cos(F.radians(F.col(lat2)))
        * F.sin(dlon / 2) ** 2
    )
    return F.lit(2 * R) * F.asin(F.sqrt(a))


def load_and_clean(spark: SparkSession, csv_files: list, max_files: int = None) -> DataFrame:
    cols = AIS_COLUMNS
    if max_files:
        csv_files = csv_files[:max_files]
    if not csv_files:
        raise FileNotFoundError("No AIS CSV files provided")

    # Read all columns as strings by header name (single pass, no inferSchema).
    # PySpark maps explicit schemas positionally, which breaks on the 25-column CSV,
    # so we read without a schema and select + cast the 9 columns we need by name.
    raw = spark.read.option("header", "true").csv(csv_files)

    df = raw.select(
        F.to_timestamp(raw[cols["timestamp"]], "dd/MM/yyyy HH:mm:ss").alias("timestamp"),
        raw[cols["mmsi"]].cast(LongType()).alias("mmsi"),
        raw[cols["lat"]].cast(DoubleType()).alias("lat"),
        raw[cols["lon"]].cast(DoubleType()).alias("lon"),
        raw[cols["sog"]].cast(DoubleType()).alias("sog"),
        raw[cols["cog"]].cast(DoubleType()).alias("cog"),
        raw[cols["nav_status"]].alias("nav_status"),
        raw[cols["name"]].alias("name"),
        raw[cols["ship_type"]].alias("ship_type"),
    )

    # 2b — temporal filter
    df = df.filter(
        (F.col("timestamp") >= F.lit("2021-12-01").cast("timestamp")) &
        (F.col("timestamp") <  F.lit("2022-01-01").cast("timestamp"))
    )

    # 2c — geographic filter
    df = df.filter(haversine_nm_expr("lat", "lon", CENTER_LAT, CENTER_LON) <= RADIUS_NM)

    # 2d Layer 1 — range validation
    df = df.filter(
        F.col("mmsi").isNotNull() &
        F.col("lat").isNotNull() & F.col("lon").isNotNull() &
        F.col("lat").between(-90, 90) &
        F.col("lon").between(-180, 180) &
        (F.length(F.col("mmsi").cast("string")) == 9)
    )

    # Remove MMSIs whose country code (first 3 digits = MID) is 377 (Saint Vincent).
    # These consistently appear as AIS relay duplicates with corrupt coordinates
    # in Danish waters and would otherwise rank as zero-distance collisions.
    df = df.filter((F.col("mmsi") / 1_000_000).cast("int") != 377)

    # Remove SAR aircraft: MMSI range 111XXXXXX is reserved for search-and-rescue
    # aircraft (helicopters, fixed-wing). They operate over accident scenes and
    # produce false close-approach pairs with surface vessels below them.
    df = df.filter((F.col("mmsi") / 1_000_000).cast("int") != 111)

    # Exclude operational vessel types that intentionally work in close proximity
    # to other vessels and would rank above a real commercial collision.
    # Exclusion list is matched case-insensitively: AIS feeds vary in capitalisation
    # (e.g. "Law enforcement" vs "Law Enforcement"). F.lower() normalises before isin().
    EXCLUDED_SHIP_TYPES_LOWER = [
        "sar",              # search and rescue boats
        "law enforcement",  # police / coast guard patrol (KBV, Politi, etc.)
        "military ops",     # naval vessels
        "pilot",            # pilot boats boarding ships
        "port tender",      # harbour support craft
        "anti-pollution",   # oil-spill response vessels
        "fishing",          # pair trawlers deliberately operate 50–200 m apart
    ]
    df = df.filter(
        F.col("ship_type").isNull() |
        ~F.lower(F.col("ship_type")).isin(EXCLUDED_SHIP_TYPES_LOWER)
    )

    # 2d Layer 2 — GPS jump filter
    w = Window.partitionBy("mmsi").orderBy("timestamp")
    df = (
        df.withColumn("prev_lat", F.lag("lat", 1).over(w))
          .withColumn("prev_lon", F.lag("lon", 1).over(w))
          .withColumn("prev_ts",  F.lag("timestamp", 1).over(w))
          .withColumn("dt_hours",
              (F.unix_timestamp("timestamp") - F.unix_timestamp("prev_ts")) / 3600.0)
          .withColumn("implied_speed",
              F.when(F.col("dt_hours") > 0,
                  haversine_nm_expr_pair("lat", "lon", "prev_lat", "prev_lon") / F.col("dt_hours")
              ))
    )
    df = df.filter(
        F.col("implied_speed").isNull() |
        (F.col("implied_speed") <= MAX_SPEED_KNOTS)
    ).drop("prev_lat", "prev_lon", "prev_ts", "dt_hours", "implied_speed")

    # 2d Layer 3 — stationary vessel exclusion
    # isNull() guard: isin() returns null for null values, and ~null drops the row,
    # which would silently remove vessels with missing nav_status (e.g. Karin Høj pings)
    df = df.filter(
        F.col("nav_status").isNull() | ~F.col("nav_status").isin(STATIONARY_NAV_CODES)
    )

    moving_mmsis = (
        df.groupBy("mmsi")
          .agg(F.percentile_approx("sog", 0.5).alias("median_sog"))
          .filter(F.col("median_sog") >= MIN_MOVING_SOG_KNOTS)
          .select("mmsi")
    )
    df = df.join(F.broadcast(moving_mmsis), on="mmsi", how="inner")

    # 2e — repartition and cache
    df = df.repartition(24, "mmsi").cache()
    count = df.count()
    print(f"[preprocess] Clean dataset: {count:,} rows")
    return df
