# Vessel Collision Detection — Big Data Final Exam

A production-grade PySpark pipeline that processes **57 GB of Danish AIS maritime tracking data** across **31 days in December 2021** to identify two vessels that collided within a 50 nautical mile radius of Bornholm Island in the Baltic Sea.

---

## The Collision

After processing over 15 million clean AIS pings, the pipeline identified the following event:

| Field | Value |
|---|---|
| **Vessel A (MMSI)** | 377084488 |
| **Vessel B** | ALVA (MMSI 377085000) |
| **Date & Time** | 15 December 2021, 06:43:12 UTC |
| **Latitude** | 55.000873° N |
| **Longitude** | 13.295165° E |
| **Separation** | 0.0000 nm — direct collision |

Both vessels were actively underway. The Haversine-computed distance was effectively zero, meaning the two ships occupied the same GPS coordinates at the same timestamp.

---

## Project Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    VESSEL COLLISION PIPELINE                     │
└─────────────────────────────────────────────────────────────────┘

  ┌──────────┐    31 CSV paths    ┌────────────────┐
  │ ingest   │──────────────────► │  preprocess    │
  │          │                   │                │
  │ Scans    │                   │ • Read 57 GB   │
  │ data/    │                   │ • Geo filter   │
  │ folder   │                   │ • Noise removal│
  │          │                   │ • Cache df     │
  └──────────┘                   └───────┬────────┘
                                         │
                                  clean df (cached)
                                         │
                   ┌─────────────────────┼─────────────────────┐
                   │                     │                     │
                   ▼                     ▼                     ▼
            ┌────────────┐       ┌────────────┐       ┌─────────────┐
            │  detect    │       │  enrich    │       │  visualize  │
            │            │       │            │       │             │
            │ Time-bucket│       │ MMSI →     │       │ Folium HTML │
            │ self-join  │       │ vessel name│       │ Matplotlib  │
            │ Haversine  │       │ lookup     │       │ PNG chart   │
            └─────┬──────┘       └─────┬──────┘       └──────┬──────┘
                  │                    │                      │
                  └────────────────────┴──────────────────────┘
                                       │
                                       ▼
                           ┌───────────────────────┐
                           │   COLLISION RESULT    │
                           │  MMSI A, MMSI B,      │
                           │  Time, Location,      │
                           │  Distance             │
                           └───────────────────────┘
```

---

## Technology Stack

| Component | Technology | Version |
|---|---|---|
| Processing Engine | Apache PySpark | 4.0.0 |
| Language | Python | 3.11 |
| Runtime | Java (OpenJDK) | 21 |
| Containerisation | Docker | — |
| Interactive Maps | Folium | 0.20.0 |
| Static Charts | Matplotlib | 3.10.0 |
| Data Manipulation | Pandas | 2.2.2 |
| Orchestration | Docker Compose | 3.9 |

---

## Repository Structure

```
vessel-collision/
├── pipeline_run.py        ← Local Windows runner (sets JAVA_HOME, TEST_FILES flag)
├── Dockerfile             ← Container build (Java 21 + Python 3.11)
├── docker-compose.yml     ← Volume mounts + memory config
├── requirements.txt       ← Pinned Python dependencies
├── CLAUDE.md              ← Build specification
├── data/                  ← AIS CSV files (not in git — 57 GB)
│   └── aisdk-2021-12-*.csv
├── output/                ← Generated maps (not in git)
│   ├── collision_map.html
│   └── collision_map.png
└── src/
    ├── config.py          ← All constants (thresholds, paths, coordinates)
    ├── ingest.py          ← File discovery
    ├── preprocess.py      ← Schema, filters, noise removal, cache
    ├── detect.py          ← Time-bucket collision detection
    ├── enrich.py          ← MMSI → vessel name resolution
    ├── visualize.py       ← Folium HTML + Matplotlib PNG
    └── main.py            ← Docker entrypoint
```

---

## Dataset

| Property | Detail |
|---|---|
| Source | Danish Maritime Authority — aisdata.ais.dk |
| Period | December 2021 (all 31 days) |
| Format | CSV, one file per day |
| Raw size | ~57 GB (31 files × ~1.8 GB each) |
| Columns | 25 per row (timestamp, MMSI, lat, lon, SOG, COG, nav status, name, ship type…) |
| Geographic scope | Global AIS coverage, filtered to 50 nm radius of Bornholm |

---

## Source Code — Stage by Stage

### `src/config.py` — All Constants

Every threshold, coordinate, and path lives here. No magic numbers anywhere else in the codebase.

```python
# Geographic filter — centre of Bornholm Island
CENTER_LAT = 55.225000
CENTER_LON = 14.245000
RADIUS_NM  = 50.0

# Temporal filter
START_DATE = "2021-12-01"
END_DATE   = "2021-12-31"

# Noise removal
MAX_SPEED_KNOTS      = 50.0        # GPS jump threshold
MIN_MOVING_SOG_KNOTS = 0.5         # median SOG filter
STATIONARY_NAV_CODES = ["At anchor", "Moored"]

# Collision detection
COLLISION_RADIUS_NM  = 0.1         # first attempt, relaxes to 0.2 then 0.5
TIME_BUCKET_SECONDS  = 60          # 1-minute buckets
TIME_BUCKET_SLACK    = 1           # ±1 bucket overlap

# Paths — read from Docker environment variables at runtime
DATA_DIR   = os.getenv("DATA_DIR",   "/data")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/output")

# Danish AIS column names (exact header strings in the CSV)
AIS_COLUMNS = {
    "timestamp":  "# Timestamp",
    "mmsi":       "MMSI",
    "lat":        "Latitude",
    "lon":        "Longitude",
    "sog":        "SOG",
    "cog":        "COG",
    "nav_status": "Navigational status",
    "name":       "Name",
    "ship_type":  "Ship type",
}
```

---

### `src/ingest.py` — File Discovery

Scans the local `data/` directory for the 31 expected daily CSV files. Returns a list of absolute paths. No data is read at this point.

```python
def load_december_2021(data_dir: str = DATA_DIR) -> list[str]:
    paths = []
    start = date(2021, 12, 1)
    for i in range(31):
        day = start + timedelta(days=i)
        csv_name = f"aisdk-{day.year}-{day.month:02d}-{day.day:02d}.csv"
        csv_path = os.path.join(data_dir, csv_name)
        if os.path.exists(csv_path):
            size_mb = os.path.getsize(csv_path) / (1024 ** 2)
            print(f"[ingest] Found {csv_name} ({size_mb:.0f} MB)")
            paths.append(csv_path)
        else:
            print(f"[ingest] WARNING: {csv_name} not found in {data_dir}")
    print(f"[ingest] {len(paths)}/31 files available")
    return paths
```

**Why:** Checking file existence before starting Spark is cheap. It surfaces missing files immediately rather than letting Spark fail silently mid-run.

---

### `src/preprocess.py` — Reading, Cleaning, Caching

This is the heaviest stage. It reads all 57 GB, applies five layers of cleaning, and caches the result.

#### Haversine as a native Spark SQL expression

The distance formula runs entirely on the JVM — no Python UDF serialisation overhead.

```python
def haversine_nm_expr(lat_col: str, lon_col: str, lat2: float, lon2: float):
    """Distance from a DataFrame column to a fixed point, in nautical miles."""
    R = 3440.065  # Earth radius in nautical miles
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
    """Distance between two DataFrame columns — used in detect.py."""
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
```

#### Reading without an explicit schema

The CSV has 25 columns. If an explicit `StructType` is provided, Spark maps it **positionally**, not by name — causing column misalignment. The fix: read without a schema, then select the 9 required columns by header name and cast them.

```python
raw = spark.read.option("header", "true").csv(csv_files)

df = raw.select(
    F.to_timestamp(raw[cols["timestamp"]], "dd/MM/yyyy HH:mm:ss").alias("timestamp"),
    raw[cols["mmsi"]].cast(LongType()).alias("mmsi"),
    raw[cols["lat"]].cast(DoubleType()).alias("lat"),
    raw[cols["lon"]].cast(DoubleType()).alias("lon"),
    raw[cols["sog"]].cast(DoubleType()).alias("sog"),
    raw[cols["cog"]].cast(DoubleType()).alias("cog"),
    raw[cols["nav_status"]].alias("nav_status"),   # kept as string — values like "Under way"
    raw[cols["name"]].alias("name"),
    raw[cols["ship_type"]].alias("ship_type"),      # kept as string — values like "Cargo"
)
```

**Why `spark.sql.ansi.enabled=false`:** The SOG column sometimes contains the string `'GPS'` (a sensor artefact). With ANSI on, casting `'GPS'` to `DoubleType` raises `CAST_INVALID_INPUT`. With ANSI off, it silently becomes `null`, which is then dropped by the null filter.

#### Five cleaning layers

```python
# 1 — Temporal filter: drop rows outside December 2021
df = df.filter(
    (F.col("timestamp") >= F.lit("2021-12-01").cast("timestamp")) &
    (F.col("timestamp") <  F.lit("2022-01-01").cast("timestamp"))
)

# 2 — Geographic filter: keep only pings within 50 nm of Bornholm
df = df.filter(haversine_nm_expr("lat", "lon", CENTER_LAT, CENTER_LON) <= RADIUS_NM)

# 3 — Range validation: drop nulls, invalid lat/lon, non-9-digit MMSI
df = df.filter(
    F.col("mmsi").isNotNull() &
    F.col("lat").isNotNull() & F.col("lon").isNotNull() &
    F.col("lat").between(-90, 90) &
    F.col("lon").between(-180, 180) &
    (F.length(F.col("mmsi").cast("string")) == 9)
)

# 4 — GPS jump filter: drop pings implying speed > 50 knots
w = Window.partitionBy("mmsi").orderBy("timestamp")
df = (
    df.withColumn("prev_lat", F.lag("lat", 1).over(w))
      .withColumn("prev_lon", F.lag("lon", 1).over(w))
      .withColumn("prev_ts",  F.lag("timestamp", 1).over(w))
      .withColumn("dt_hours",
          (F.unix_timestamp("timestamp") - F.unix_timestamp("prev_ts")) / 3600.0)
      .withColumn("implied_speed",
          # F.when guard prevents divide-by-zero for same-timestamp pings
          F.when(F.col("dt_hours") > 0,
              haversine_nm_expr_pair("lat", "lon", "prev_lat", "prev_lon") / F.col("dt_hours")
          ))
)
df = df.filter(
    F.col("implied_speed").isNull() | (F.col("implied_speed") <= MAX_SPEED_KNOTS)
).drop("prev_lat", "prev_lon", "prev_ts", "dt_hours", "implied_speed")

# 5 — Stationary vessel exclusion
df = df.filter(~F.col("nav_status").isin(STATIONARY_NAV_CODES))

moving_mmsis = (
    df.groupBy("mmsi")
      .agg(F.percentile_approx("sog", 0.5).alias("median_sog"))
      .filter(F.col("median_sog") >= MIN_MOVING_SOG_KNOTS)
      .select("mmsi")
)
# broadcast() — moving_mmsis is tiny; avoids a shuffle join
df = df.join(F.broadcast(moving_mmsis), on="mmsi", how="inner")

# Repartition by MMSI hash and cache — all downstream stages read from RAM
df = df.repartition(24, "mmsi").cache()
count = df.count()
print(f"[preprocess] Clean dataset: {count:,} rows")
```

---

### `src/detect.py` — Collision Detection

#### Why time buckets?

A naïve O(n²) cross-join between all pings is infeasible at 15 million rows. Instead, each ping is assigned a one-minute time bucket. Two vessels can only collide if their pings fall in the same (or adjacent) bucket.

#### Why three equi-joins instead of a `BETWEEN` join?

A non-equi `BETWEEN` join forces Spark into a **cross-join** (no hash join possible), which is prohibitively slow. The equivalent result is achieved with three fast hash joins — bucket, bucket+1, bucket−1 — unioned together.

```python
@dataclass
class CollisionResult:
    mmsi_a:      int
    mmsi_b:      int
    event_time:  datetime
    event_lat:   float
    event_lon:   float
    distance_nm: float


def find_collision(df: DataFrame) -> CollisionResult:
    df = df.withColumn(
        "time_bucket",
        (F.unix_timestamp("timestamp") / TIME_BUCKET_SECONDS).cast("long"),
    )
    # Try progressively larger radii if no pair found at the tight threshold
    for radius in [COLLISION_RADIUS_NM, 0.2, 0.5]:
        print(f"[detect] Trying collision radius = {radius} nm")
        result = _run_detection(df, radius)
        if result is not None:
            return result
    raise RuntimeError("No collision candidates found even at 0.5 nm.")


def _pair_select(a, b):
    """Join two aliases on equal time_bucket + MMSI ordering, flatten columns."""
    return (
        a.join(b,
            (F.col("a.time_bucket") == F.col("b.time_bucket")) &
            (F.col("a.mmsi") < F.col("b.mmsi"))
        )
        .select(
            F.col("a.mmsi").alias("mmsi_a"),
            F.col("b.mmsi").alias("mmsi_b"),
            F.col("a.timestamp").alias("ts_a"),
            F.col("a.lat").alias("lat_a"), F.col("a.lon").alias("lon_a"),
            F.col("b.lat").alias("lat_b"), F.col("b.lon").alias("lon_b"),
        )
    )


def _run_detection(df: DataFrame, radius: float):
    a  = df.alias("a")
    b0 = df.alias("b")                                                  # exact bucket
    b1 = df.withColumn("time_bucket", F.col("time_bucket") + TIME_BUCKET_SLACK).alias("b")  # bucket+1
    b2 = df.withColumn("time_bucket", F.col("time_bucket") - TIME_BUCKET_SLACK).alias("b")  # bucket-1

    pairs = _pair_select(a, b0).union(_pair_select(a, b1)).union(_pair_select(a, b2))

    pairs = pairs.withColumn(
        "distance_nm",
        haversine_nm_expr_pair("lat_a", "lon_a", "lat_b", "lon_b"),
    ).filter(F.col("distance_nm") <= radius)

    # Single collect() — no pre-count scan; limit(1) pushes down to Spark planner
    rows = pairs.orderBy("distance_nm").limit(1).collect()
    if not rows:
        return None

    row = rows[0]
    return CollisionResult(
        mmsi_a=row["mmsi_a"], mmsi_b=row["mmsi_b"],
        event_time=row["ts_a"],
        event_lat=(row["lat_a"] + row["lat_b"]) / 2,
        event_lon=(row["lon_a"] + row["lon_b"]) / 2,
        distance_nm=row["distance_nm"],
    )
```

---

### `src/enrich.py` — Vessel Name Resolution

Resolves MMSI numbers to human-readable names by scanning the cached DataFrame for the most frequently occurring non-null name value for each MMSI.

```python
def resolve_names(df: DataFrame, mmsis: List[int]) -> Dict[int, str]:
    try:
        names_df = (
            df.filter(F.col("mmsi").isin(mmsis))
              .filter(F.col("name").isNotNull() & (F.col("name") != ""))
              .groupBy("mmsi")
              .agg(F.mode("name").alias("vessel_name"))   # most frequent name wins
        )
    except Exception:
        # F.mode() not available in older Spark builds — fall back to first non-null
        names_df = (
            df.filter(F.col("mmsi").isin(mmsis))
              .filter(F.col("name").isNotNull() & (F.col("name") != ""))
              .groupBy("mmsi")
              .agg(F.first("name", ignorenulls=True).alias("vessel_name"))
        )

    rows = names_df.collect()
    result = {row["mmsi"]: row["vessel_name"] for row in rows}
    for mmsi in mmsis:
        result.setdefault(mmsi, f"UNKNOWN ({mmsi})")   # guard if no name pings found
    return result
```

---

### `src/visualize.py` — Output Maps

Filters the cached DataFrame to a ±10 minute window around the collision, collects only that small slice to the driver, and generates two output files.

```python
def plot_trajectories(df, result, vessel_names, output_dir=OUTPUT_DIR):
    window_start = result.event_time - timedelta(minutes=TRAJECTORY_WINDOW_MIN)
    window_end   = result.event_time + timedelta(minutes=TRAJECTORY_WINDOW_MIN)

    traj_pd = (
        df.filter(
            F.col("mmsi").isin([result.mmsi_a, result.mmsi_b]) &
            (F.col("timestamp") >= window_start) &
            (F.col("timestamp") <= window_end)
        )
        .orderBy("mmsi", "timestamp")
        .toPandas()   # collect only ~20 minutes of two vessels — tiny slice
    )

    _save_folium(traj_pd, result, vessel_names, output_dir)      # interactive HTML
    _save_matplotlib(traj_pd, result, vessel_names, output_dir)  # static PNG
```

**Folium map:** draws coloured polyline trajectories with start/end markers and a collision point popup.

**Matplotlib chart:** two panels — left is the geographic trajectory (lon vs lat), right is the speed profile (SOG over time) for both vessels, with a vertical line at the collision timestamp.

---

### `src/main.py` — Docker Entrypoint

The Docker `CMD` runs this module. It builds the SparkSession with all performance configs and calls each stage in order.

```python
def main():
    spark = (
        SparkSession.builder
        .appName("VesselCollisionDetection")
        .master("local[*]")                                         # use all available cores
        .config("spark.driver.memory",                      "6g")
        .config("spark.sql.shuffle.partitions",             "24")
        .config("spark.sql.files.maxPartitionBytes",        "268435456")  # 256 MB partitions
        .config("spark.sql.ansi.enabled",                   "false")      # tolerate dirty AIS data
        .config("spark.sql.adaptive.enabled",               "true")       # AQE auto-coalescing
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

    print("=" * 60)
    print("COLLISION DETECTED")
    print(f"  Vessel A : MMSI {result.mmsi_a} — {names[result.mmsi_a]}")
    print(f"  Vessel B : MMSI {result.mmsi_b} — {names[result.mmsi_b]}")
    print(f"  Time     : {result.event_time}")
    print(f"  Location : {result.event_lat:.6f} N, {result.event_lon:.6f} E")
    print(f"  Distance : {result.distance_nm:.4f} nm ({result.distance_nm * 1852:.1f} m)")
    print("=" * 60)

    print("=== Stage 5: Visualize ===")
    visualize.plot_trajectories(df, result, names, OUTPUT_DIR)

    spark.stop()
```

---

## Data Quality Challenges

| Issue | Example | Fix |
|---|---|---|
| Text in numeric fields | `'GPS'` in SOG column | ANSI mode off — bad casts return null |
| Text navigational status | `"Under way using engine"` instead of integer | Kept as string, filter uses text values |
| Text ship type | `"Passenger"`, `"Cargo"` instead of integer | Kept as string |
| Schema positional mismatch | Explicit StructType maps by position, not name | Read without schema, select by column name |
| GPS jumps | Ping implying 200-knot speed | Haversine speed check per consecutive ping pair |
| Divide-by-zero | Two pings with the same timestamp | `F.when(dt_hours > 0, ...)` guard |
| Null coordinates | Missing lat/lon | Dropped by range validation layer |
| Invalid MMSI | Non-9-digit identifiers | `F.length(mmsi.cast("string")) == 9` filter |

---

## Performance Optimisations

| Optimisation | Impact |
|---|---|
| Read without schema, select by column name | Avoids positional mismatch on 25-column CSV |
| Native Spark SQL Haversine (no Python UDF) | JVM execution — no Python serialisation |
| Geographic filter before all joins | Drops ~90% of data in Stage 2 |
| Three equi-joins unioned instead of non-equi `BETWEEN` | Enables hash join; eliminates cross-join |
| `broadcast()` on moving-MMSI allowlist | Avoids shuffle join for small DataFrame |
| `.cache()` after cleaning | Detect, enrich, visualize all read from RAM |
| `repartition(24, "mmsi")` | Collocates vessel pings; matches CPU core count |
| Adaptive Query Execution | Spark auto-coalesces partitions at runtime |
| `SPARK_LOCAL_DIRS` → external drive | Prevents C: drive filling during shuffle |
| Single `.collect()` in detect, no pre-count | Eliminates one full scan of the join result |

---

## Running with Docker (Recommended)

Docker is the easiest way to run this project. The image is publicly available on Docker Hub and bundles Java 21, Python 3.11, PySpark, and all dependencies. You only need Docker Desktop.

| Resource | Link |
|---|---|
| **Docker Hub** | [`onkar45612/vessel-collision:latest`](https://hub.docker.com/r/onkar45612/vessel-collision) |
| **GitHub** | [https://github.com/OnkarBasu/Big_data_final_exam](https://github.com/OnkarBasu/Big_data_final_exam) |

---

### What you need on your machine

**1. Docker Desktop**
Download from [docker.com](https://www.docker.com/products/docker-desktop). Ensure it is running before you proceed.

In Docker Desktop → Settings → Resources, allocate **at least 8 GB RAM** to Docker.

**2. The AIS CSV data files (57 GB)**
The dataset is not in the image. Download all 31 daily files for December 2021 from [aisdata.ais.dk](http://aisdata.ais.dk) and place them in a local `data/` folder:

```
your-folder/
├── data/
│   ├── aisdk-2021-12-01.csv
│   ├── aisdk-2021-12-02.csv
│   │   ... (all 31 files)
│   └── aisdk-2021-12-31.csv
└── docker-compose.yml          ← create this (see below)
```

**3. A `docker-compose.yml` file**
Create a file called `docker-compose.yml` in the same directory as your `data/` folder with this exact content:

```yaml
version: "3.9"
services:
  vessel-collision:
    image: onkar45612/vessel-collision:latest
    volumes:
      - ./data:/data
      - ./output:/output
    environment:
      - DATA_DIR=/data
      - OUTPUT_DIR=/output
      - SPARK_LOCAL_DIRS=/tmp/spark
    mem_limit: 8g
    shm_size: 2g
```

> **Important:** Use `image:` (not `build:`). This pulls the pre-built image from Docker Hub. Do not run the container from Docker Desktop's UI — it will not apply the volume mounts. Always use the terminal command below.

**4. Disk space**
At least 100 GB free (57 GB data + Spark temp files during processing).

---

### Running the pipeline

Open a terminal in the folder containing `docker-compose.yml` and run:

```bash
docker compose up
```

On the first run Docker pulls the image automatically (~2 GB download). The pipeline then runs all five stages. **Do not close the terminal while it is running.**

Expected terminal output:

```
=== Stage 1: Ingest ===
[ingest] Found aisdk-2021-12-01.csv (1698 MB)
...
[ingest] 31/31 files available

=== Stage 2: Preprocess ===
[preprocess] Clean dataset: XX,XXX,XXX rows

=== Stage 3: Detect ===
[detect] Trying collision radius = 0.1 nm
[detect] Radius 0.1 nm — found candidate

=== Stage 4: Enrich ===

============================================================
COLLISION DETECTED
  Vessel A : MMSI 377084488 — UNKNOWN
  Vessel B : MMSI 377085000 — ALVA
  Time     : 2021-12-15 06:43:12
  Location : 55.000873 N, 13.295165 E
  Distance : 0.0000 nm (0.0 m)
============================================================

=== Stage 5: Visualize ===
[visualize] Saved /output/collision_map.html
[visualize] Saved /output/collision_map.png
```

When finished, an `output/` folder appears next to your `docker-compose.yml` containing:
- `collision_map.html` — open in any browser for the interactive trajectory map
- `collision_map.png` — static image for reports

**Expected runtime:** 2–4 hours depending on machine speed and disk I/O.

---

### Building the image yourself

```bash
git clone https://github.com/OnkarBasu/Big_data_final_exam.git
cd Big_data_final_exam
docker compose build
docker compose up
```

> `docker-compose.yml` in the repo uses `build: .` (for local builds). The file shown above uses `image: onkar45612/vessel-collision:latest` (for pulling from Docker Hub). Choose accordingly.

---

## Running Locally on Windows

Requires Java 21 (Temurin) and Python 3.13.

```powershell
pip install -r requirements.txt

$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-21.0.11.10-hotspot"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"

cd vessel-collision
python pipeline_run.py
```

To test with a single file before running all 31, set `TEST_FILES = 1` at the top of `pipeline_run.py`.

| Mode | Files | Expected Time |
|---|---|---|
| Test | 1 CSV (1.7 GB) | 10–15 minutes |
| Full | 31 CSVs (57 GB) | 60–120 minutes |

---

## Requirements

- Docker Desktop (for containerised run) or Java 21 + Python 3.13 (for local run)
- 8 GB RAM minimum (16 GB recommended)
- 100 GB free disk space (57 GB data + Spark temp)
- AIS CSV files for December 2021 from aisdata.ais.dk
