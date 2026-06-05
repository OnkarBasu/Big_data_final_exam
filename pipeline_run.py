# ── Environment setup (must happen before any PySpark import) ─────────────────
import os, sys

os.environ["JAVA_HOME"]      = r"C:\Program Files\Eclipse Adoptium\jdk-21.0.11.10-hotspot"
os.environ["SPARK_LOCAL_DIRS"] = r"D:\spark-temp"
sys.path.insert(0, r"D:\bigdatafinalxam\vessel-collision")

# ── Project imports ────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from src.ingest     import load_december_2021
from src.preprocess import load_and_clean
from src.detect     import find_collision
from src.enrich     import resolve_names
from src.visualize  import plot_trajectories

# ── Settings ──────────────────────────────────────────────────────────────────
DATA_DIR   = r"D:\bigdatafinalxam\vessel-collision\data"
OUTPUT_DIR = r"D:\bigdatafinalxam\vessel-collision\output"

# Set to 1 to test with 1 CSV file (~5 min).
# Set to None to run all 31 files (~60-90 min).
TEST_FILES = None

# ── Spark session ──────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("VesselCollisionDetection")
    .master("local[4]")
    .config("spark.driver.memory",                      "8g")
    .config("spark.sql.shuffle.partitions",             "24")
    .config("spark.sql.files.maxPartitionBytes",        "268435456")
    .config("spark.sql.ansi.enabled",                   "false")
    .config("spark.sql.adaptive.enabled",               "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.serializer",                         "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── Stage 1: Ingest ────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("STAGE 1 — Ingest: scanning local CSV files")
print("="*60)
csv_files = load_december_2021(DATA_DIR)

# ── Stage 2: Preprocess ───────────────────────────────────────────────────────
print("\n" + "="*60)
print("STAGE 2 — Preprocess: clean + filter all AIS data")
print("="*60)
df = load_and_clean(spark, csv_files, max_files=TEST_FILES)

# ── Stage 3: Detect ───────────────────────────────────────────────────────────
print("\n" + "="*60)
print("STAGE 3 — Detect: finding closest vessel pair")
print("="*60)
result = find_collision(df)

# ── Stage 4: Enrich ───────────────────────────────────────────────────────────
print("\n" + "="*60)
print("STAGE 4 — Enrich: resolving vessel names from MMSI")
print("="*60)
names = resolve_names(df, [result.mmsi_a, result.mmsi_b])

# ── Result summary ─────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("COLLISION DETECTED")
print("="*60)
print(f"  Vessel A : MMSI {result.mmsi_a} — {names[result.mmsi_a]}")
print(f"  Vessel B : MMSI {result.mmsi_b} — {names[result.mmsi_b]}")
print(f"  Time     : {result.event_time}")
print(f"  Location : {result.event_lat:.6f} N,  {result.event_lon:.6f} E")
print(f"  Distance : {result.distance_nm:.4f} nm  ({result.distance_nm * 1852:.1f} m)")
print("="*60)

# ── Stage 5: Visualize ────────────────────────────────────────────────────────
print("\n" + "="*60)
print("STAGE 5 — Visualize: generating trajectory map")
print("="*60)
plot_trajectories(df, result, names, OUTPUT_DIR)
print(f"  HTML map : {OUTPUT_DIR}\\collision_map.html")
print(f"  PNG map  : {OUTPUT_DIR}\\collision_map.png")

spark.stop()
print("\nDone.")
