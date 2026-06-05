from typing import Dict, List
import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def resolve_names(df: DataFrame, mmsis: List[int]) -> Dict[int, str]:
    try:
        names_df = (
            df.filter(F.col("mmsi").isin(mmsis))
              .filter(F.col("name").isNotNull() & (F.col("name") != ""))
              .groupBy("mmsi")
              .agg(F.mode("name").alias("vessel_name"))
        )
    except Exception:
        names_df = (
            df.filter(F.col("mmsi").isin(mmsis))
              .filter(F.col("name").isNotNull() & (F.col("name") != ""))
              .groupBy("mmsi")
              .agg(F.first("name", ignorenulls=True).alias("vessel_name"))
        )

    rows = names_df.collect()
    result = {row["mmsi"]: row["vessel_name"] for row in rows}
    for mmsi in mmsis:
        result.setdefault(mmsi, f"UNKNOWN ({mmsi})")
    return result
