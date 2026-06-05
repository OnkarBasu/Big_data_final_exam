from pyspark.sql import SparkSession
from src import ingest, preprocess, detect, enrich, visualize
from src.config import DATA_DIR, OUTPUT_DIR


def main():
    spark = (
        SparkSession.builder
        .appName("VesselCollisionDetection")
        .master("local[*]")
        .config("spark.driver.memory",                      "6g")
        .config("spark.sql.shuffle.partitions",             "24")
        .config("spark.sql.files.maxPartitionBytes",        "268435456")
        .config("spark.sql.ansi.enabled",                   "false")
        .config("spark.sql.adaptive.enabled",               "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=== Stage 1: Ingest ===")
    csv_files = ingest.load_december_2021(DATA_DIR)

    print("=== Stage 2: Preprocess ===")
    df = preprocess.load_and_clean(spark, csv_files)

    print("=== Stage 3: Detect ===")
    result = detect.find_collision(df)

    print("=== Stage 4: Enrich ===")
    names = enrich.resolve_names(df, [result.mmsi_a, result.mmsi_b])

    print("\n" + "=" * 60)
    print("COLLISION DETECTED")
    print(f"  Vessel A: MMSI {result.mmsi_a} — {names[result.mmsi_a]}")
    print(f"  Vessel B: MMSI {result.mmsi_b} — {names[result.mmsi_b]}")
    print(f"  Time:     {result.event_time}")
    print(f"  Location: {result.event_lat:.6f}°N, {result.event_lon:.6f}°E")
    print(f"  Distance: {result.distance_nm:.4f} nm ({result.distance_nm * 1852:.1f} m)")
    print("=" * 60)

    print("=== Stage 5: Visualize ===")
    visualize.plot_trajectories(df, result, names, OUTPUT_DIR)
    print(f"Map saved to {OUTPUT_DIR}/collision_map.html")
    print(f"PNG saved to {OUTPUT_DIR}/collision_map.png")

    spark.stop()


if __name__ == "__main__":
    main()
