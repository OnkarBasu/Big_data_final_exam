# CLAUDE.md — Vessel Collision Detection (Big Data Exam)

> This file is the single source of truth for Claude Code when building this project.
> Read it fully before writing any code. Follow every instruction precisely.

---

## Project Goal

Detect two vessels that collided (or reached closest physical proximity) in Danish AIS data
for **December 2021**, within a **50 nautical mile radius** of `(55.225000°N, 14.245000°E)`.
Visualize both vessels' trajectories ±10 minutes around the collision time.

**Hard constraints from the exam spec:**
- Language: Python 3.x only
- Framework: PySpark (no pure-Pandas solutions accepted)
- Environment: Fully containerized in Docker
- Version control: Git repository

---

## Repo Structure

Build exactly this layout — do not deviate:

```
vessel-collision/
├── CLAUDE.md                  ← this file
├── Dockerfile
├── docker-compose.yml
├── .dockerignore
├── .gitignore
├── README.md                  ← build + run instructions
├── report.md                  ← methodology writeup (exam deliverable)
├── requirements.txt
├── data/                      ← mounted Docker volume; raw AIS CSVs land here
│   └── .gitkeep
├── output/                    ← mounted Docker volume; map + results saved here
│   └── .gitkeep
└── src/
    ├── main.py                ← entrypoint; orchestrates all stages
    ├── config.py              ← all constants in one place (thresholds, paths, coords)
    ├── ingest.py              ← download AIS data from aisdata.ais.dk
    ├── preprocess.py          ← schema enforcement, temporal/geo filter, noise removal
    ├── detect.py              ← collision detection algorithm
    ├── enrich.py              ← vessel name lookup from MMSI
    └── visualize.py           ← trajectory map generation
```

---

## config.py — Single Source of Truth for All Constants

```python
# src/config.py
import os

# --- Geographic filter ---
CENTER_LAT = 55.225000
CENTER_LON = 14.245000
RADIUS_NM  = 50.0          # nautical miles

# --- Temporal filter ---
START_DATE = "2021-12-01"
END_DATE   = "2021-12-31"

# --- Noise removal thresholds ---
MAX_SPEED_KNOTS      = 50.0   # GPS jump threshold — pings implying faster speed are dropped
MIN_MOVING_SOG_KNOTS = 0.5    # vessels below this median SOG are considered stationary
STATIONARY_NAV_CODES = [1, 5] # 1=at anchor, 5=moored (per ITU AIS spec)

# --- Collision detection ---
COLLISION_RADIUS_NM  = 0.1    # ~185 metres — vessels within this = collision candidate
TIME_BUCKET_SECONDS  = 60     # round timestamps to 1-minute buckets for join
TIME_BUCKET_SLACK    = 1      # ±1 bucket to catch cross-minute ping offsets
TRAJECTORY_WINDOW_MIN = 10    # minutes before/after collision to extract

# --- Paths ---
DATA_DIR   = os.getenv("DATA_DIR",   "/data")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/output")
AIS_URL_TEMPLATE = "http://aisdata.ais.dk/download/aisdk-{year}-{month:02d}-{day:02d}.zip"

# --- AIS CSV column names (Danish AIS schema) ---
AIS_COLUMNS = {
    "timestamp": "# Timestamp",
    "mmsi":      "MMSI",
    "lat":       "Latitude",
    "lon":       "Longitude",
    "sog":       "SOG",           # Speed Over Ground (knots)
    "cog":       "COG",           # Course Over Ground
    "nav_status":"Navigational status",
    "name":      "Name",
    "ship_type": "Ship type",
}
```

---

## Stage-by-Stage Implementation Guide

### Stage 1 — ingest.py

**Purpose:** Download and cache AIS data files for December 2021.

**Logic:**
1. Loop over all 31 days in December 2021
2. For each day, construct the URL using `AIS_URL_TEMPLATE`
3. Download the `.zip` to `DATA_DIR` if not already present (check before downloading — idempotent)
4. Unzip the CSV inside `DATA_DIR`
5. Log each file's row count as a sanity check

**Key detail:** The Danish AIS files are one CSV per day. December 2021 = 31 files.
File naming pattern: `aisdk-2021-12-01.csv`, `aisdk-2021-12-02.csv`, etc.

**Do not re-download** if the `.csv` file already exists — support offline reruns.

---

### Stage 2 — preprocess.py

**Purpose:** Load raw CSVs into Spark, enforce schema, filter, and clean.

#### 2a — Schema

Define an explicit PySpark `StructType` schema. Never use `inferSchema=True` (too slow for large CSVs).

```python
from pyspark.sql.types import *

AIS_SCHEMA = StructType([
    StructField("timestamp",  TimestampType(), True),
    StructField("mmsi",       LongType(),      True),
    StructField("lat",        DoubleType(),    True),
    StructField("lon",        DoubleType(),    True),
    StructField("sog",        DoubleType(),    True),
    StructField("cog",        DoubleType(),    True),
    StructField("nav_status", IntegerType(),   True),
    StructField("name",       StringType(),    True),
    StructField("ship_type",  IntegerType(),   True),
])
```

Rename columns from Danish AIS headers to the clean names above on load.

#### 2b — Temporal filter

```python
df = df.filter(
    (F.col("timestamp") >= F.lit("2021-12-01").cast("timestamp")) &
    (F.col("timestamp") <  F.lit("2022-01-01").cast("timestamp"))
)
```

#### 2c — Geographic filter (Haversine)

Implement Haversine as a **PySpark SQL expression** (not a UDF) using built-in `sin`, `cos`, `asin`, `sqrt`, `radians` functions. This runs natively on the JVM — far faster than a Python UDF.

```python
def haversine_nm_expr(lat1, lon1, lat2_lit, lon2_lit):
    """Returns a Column expression computing Haversine distance in nautical miles."""
    R = 3440.065  # Earth radius in nautical miles
    dlat = F.radians(F.col(lat1) - F.lit(lat2_lit))
    dlon = F.radians(F.col(lon1) - F.lit(lon2_lit))
    a = (F.sin(dlat / 2) ** 2 +
         F.cos(F.radians(F.col(lat1))) *
         F.cos(F.lit(lat2_lit * 3.14159265 / 180)) *
         F.sin(dlon / 2) ** 2)
    return F.lit(2 * R) * F.asin(F.sqrt(a))
```

Filter: `haversine_nm_expr(...) <= RADIUS_NM`

**Optimization:** Broadcast the center coordinate (it's a scalar — implicit in the expression approach above, no shuffle needed).

#### 2d — Data quality / noise removal (three layers)

**Layer 1 — Range validation (drop obviously bad rows):**
```python
df = df.filter(
    F.col("mmsi").isNotNull() &
    F.col("lat").isNotNull() & F.col("lon").isNotNull() &
    (F.col("lat").between(-90, 90)) &
    (F.col("lon").between(-180, 180)) &
    (F.length(F.col("mmsi").cast("string")) == 9)
)
```

**Layer 2 — GPS jump filter (instantaneous speed check):**

Use a PySpark Window function. For each MMSI ordered by timestamp, compute the implied speed between consecutive pings. Drop pings where implied speed > `MAX_SPEED_KNOTS`.

```python
from pyspark.sql import Window
import pyspark.sql.functions as F

w = Window.partitionBy("mmsi").orderBy("timestamp")

df = df.withColumn("prev_lat",  F.lag("lat",  1).over(w))
df = df.withColumn("prev_lon",  F.lag("lon",  1).over(w))
df = df.withColumn("prev_ts",   F.lag("timestamp", 1).over(w))
df = df.withColumn("dt_hours",
    (F.unix_timestamp("timestamp") - F.unix_timestamp("prev_ts")) / 3600.0
)
df = df.withColumn("implied_speed_knots",
    haversine_nm_expr_pair() / F.col("dt_hours")   # implement pair version
)
df = df.filter(
    F.col("implied_speed_knots").isNull() |   # keep first ping per MMSI
    (F.col("implied_speed_knots") <= MAX_SPEED_KNOTS)
).drop("prev_lat", "prev_lon", "prev_ts", "dt_hours", "implied_speed_knots")
```

**Layer 3 — Stationary vessel exclusion:**

```python
# Exclude by navigational status code
df = df.filter(~F.col("nav_status").isin(STATIONARY_NAV_CODES))

# Exclude by median SOG per MMSI
from pyspark.sql.functions import percentile_approx
moving_mmsis = (
    df.groupBy("mmsi")
      .agg(percentile_approx("sog", 0.5).alias("median_sog"))
      .filter(F.col("median_sog") >= MIN_MOVING_SOG_KNOTS)
      .select("mmsi")
)
df = df.join(F.broadcast(moving_mmsis), on="mmsi", how="inner")
```

Note the `F.broadcast()` on the small allowlist — avoids a shuffle join.

#### 2e — Repartition and cache

After cleaning, repartition by a hash of MMSI for the collision join:

```python
df = df.repartition(200, "mmsi").cache()
df.count()  # materialize the cache
```

---

### Stage 3 — detect.py

**Purpose:** Identify the vessel pair with minimum separation — the collision.

This is the most important stage for grading. The key insight: **avoid a full Cartesian product**.

#### Algorithm: Time-bucket self-join

```python
# Step 1: Add time bucket column
df = df.withColumn(
    "time_bucket",
    (F.unix_timestamp("timestamp") / TIME_BUCKET_SECONDS).cast("long")
)

# Step 2: Create alias copies for self-join
a = df.alias("a")
b = df.alias("b")

# Step 3: Join on time bucket (and adjacent buckets to handle ping-offset)
joined = a.join(b,
    (F.col("a.time_bucket").between(
        F.col("b.time_bucket") - TIME_BUCKET_SLACK,
        F.col("b.time_bucket") + TIME_BUCKET_SLACK
    )) &
    (F.col("a.mmsi") < F.col("b.mmsi"))   # dedup: only A < B pairs
)

# Step 4: Compute actual Haversine distance between the pair
joined = joined.withColumn(
    "distance_nm",
    haversine_nm_expr_pair(
        "a.lat", "a.lon", "b.lat", "b.lon"
    )
)

# Step 5: Filter to collision candidates and find minimum
candidates = joined.filter(F.col("distance_nm") <= COLLISION_RADIUS_NM)
collision = candidates.orderBy("distance_nm").limit(1)
```

**Why this works:** Rather than comparing every ping against every other ping (O(n²)),
we only compare pings that occur within the same 1-minute window. The bucket join reduces
comparisons by a factor of ~1/T where T is the number of unique time buckets — typically
2–3 orders of magnitude fewer pairs.

**If `candidates` is empty:** Relax `COLLISION_RADIUS_NM` to 0.2 nm, then 0.5 nm.
Log each threshold tried. The collision event in the data may have poor ping timing coverage.

#### Result extraction

```python
result = collision.collect()[0]
mmsi_a     = result["a.mmsi"]
mmsi_b     = result["b.mmsi"]
event_time = result["a.timestamp"]   # or average of a and b timestamps
event_lat  = (result["a.lat"] + result["b.lat"]) / 2
event_lon  = (result["a.lon"] + result["b.lon"]) / 2
distance   = result["distance_nm"]
```

---

### Stage 4 — enrich.py

**Purpose:** Resolve MMSI numbers to vessel names.

The AIS data itself contains a `Name` field — it is populated in some pings and null in others.
Use the most common non-null name per MMSI:

```python
from pyspark.sql.functions import mode

names = (
    df.filter(F.col("name").isNotNull() & (F.col("name") != ""))
      .groupBy("mmsi")
      .agg(F.mode("name").alias("vessel_name"))
)
```

If `mode()` is unavailable in your Spark version (< 3.4), use:
```python
.agg(F.first("name", ignorenulls=True).alias("vessel_name"))
```

---

### Stage 5 — visualize.py

**Purpose:** Generate the trajectory map as a deliverable.

Use **Folium** for an interactive HTML map (better presentation than static PNG).
Also save a static PNG via **Matplotlib** as fallback (some graders may not open HTML).

#### Trajectory extraction

```python
window_start = event_time - timedelta(minutes=TRAJECTORY_WINDOW_MIN)
window_end   = event_time + timedelta(minutes=TRAJECTORY_WINDOW_MIN)

traj = df.filter(
    F.col("mmsi").isin([mmsi_a, mmsi_b]) &
    (F.col("timestamp") >= window_start) &
    (F.col("timestamp") <= window_end)
).orderBy("mmsi", "timestamp")

traj_pd = traj.toPandas()   # safe — only ~20 min of 2 vessels' pings
```

#### Folium map

```python
import folium

m = folium.Map(location=[event_lat, event_lon], zoom_start=12, tiles="OpenStreetMap")

colors = {mmsi_a: "blue", mmsi_b: "red"}
for mmsi, group in traj_pd.groupby("mmsi"):
    coords = list(zip(group["lat"], group["lon"]))
    folium.PolyLine(
        coords,
        color=colors[mmsi],
        weight=3,
        tooltip=f"MMSI {mmsi} — {vessel_names[mmsi]}"
    ).add_to(m)
    # Mark start and end
    folium.CircleMarker(coords[0],  radius=5, color=colors[mmsi], fill=True, tooltip="Start").add_to(m)
    folium.CircleMarker(coords[-1], radius=5, color=colors[mmsi], fill=True, tooltip="End").add_to(m)

# Collision marker
folium.Marker(
    [event_lat, event_lon],
    popup=f"Collision at {event_time}<br>Distance: {distance:.4f} nm",
    icon=folium.Icon(color="black", icon="warning-sign", prefix="glyphicon")
).add_to(m)

m.save(f"{OUTPUT_DIR}/collision_map.html")
```

Also save a PNG using Matplotlib with two subplots: (1) map view with trajectories, (2) distance-over-time chart showing the approach and departure.

---

### Stage 6 — main.py

```python
from pyspark.sql import SparkSession
from src import ingest, preprocess, detect, enrich, visualize
from src.config import *

def main():
    spark = (
        SparkSession.builder
        .appName("VesselCollisionDetection")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=== Stage 1: Ingesting AIS data ===")
    ingest.download_december_2021(DATA_DIR)

    print("=== Stage 2: Preprocessing ===")
    df = preprocess.load_and_clean(spark, DATA_DIR)

    print("=== Stage 3: Detecting collision ===")
    result = detect.find_collision(df)

    print("=== Stage 4: Enriching with vessel names ===")
    names = enrich.resolve_names(df, [result.mmsi_a, result.mmsi_b])

    print("\n" + "="*60)
    print(f"COLLISION DETECTED")
    print(f"  Vessel A: MMSI {result.mmsi_a} — {names[result.mmsi_a]}")
    print(f"  Vessel B: MMSI {result.mmsi_b} — {names[result.mmsi_b]}")
    print(f"  Time:     {result.event_time}")
    print(f"  Location: {result.event_lat:.6f}°N, {result.event_lon:.6f}°E")
    print(f"  Distance: {result.distance_nm:.4f} nm ({result.distance_nm * 1852:.1f} m)")
    print("="*60 + "\n")

    print("=== Stage 5: Generating visualization ===")
    visualize.plot_trajectories(df, result, names)
    print(f"Map saved to {OUTPUT_DIR}/collision_map.html")
    print(f"PNG saved to {OUTPUT_DIR}/collision_map.png")

    spark.stop()

if __name__ == "__main__":
    main()
```

---

## Docker

### Dockerfile

```dockerfile
FROM python:3.11-slim

# Install Java (required by PySpark)
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    wget \
    unzip \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

VOLUME ["/data", "/output"]

CMD ["python", "-m", "src.main"]
```

### docker-compose.yml

```yaml
version: "3.9"
services:
  vessel-collision:
    build: .
    volumes:
      - ./data:/data
      - ./output:/output
    environment:
      - DATA_DIR=/data
      - OUTPUT_DIR=/output
    mem_limit: 8g
```

### requirements.txt

```
pyspark==3.5.1
pandas==2.2.2
folium==0.17.0
matplotlib==3.9.0
requests==2.32.3
numpy==1.26.4
```

### .dockerignore

```
__pycache__/
*.pyc
.git/
data/
output/
*.zip
*.csv
```

---

## README.md Template

```markdown
# Vessel Collision Detection

Identifies two colliding vessels in Danish AIS data (December 2021)
using PySpark inside Docker.

## Requirements
- Docker + Docker Compose
- 8 GB RAM minimum

## Build
docker compose build

## Run
docker compose up

## Output
- output/collision_map.html — interactive trajectory map
- output/collision_map.png  — static map for report
- Console prints: MMSI pair, vessel names, timestamp, coordinates

## Docker Hub
docker pull <your-dockerhub-username>/vessel-collision:latest
```

---

## Optimizations — Grading-Critical

Document each of these in `report.md` to score well on "Computational Efficiency":

| Optimization | Where | Impact |
|---|---|---|
| Explicit schema on CSV load | `preprocess.py` | Avoids full-file scan for type inference |
| Native Spark SQL Haversine (no UDF) | `preprocess.py` | JVM execution, no Python serialization overhead |
| Time-bucket self-join (not Cartesian) | `detect.py` | O(n × bucket_size) vs O(n²) |
| `broadcast()` on moving_mmsis allowlist | `preprocess.py` | Avoids shuffle join for small DF |
| `.cache()` after cleaning | `preprocess.py` | Prevents re-reading/re-cleaning for subsequent actions |
| `spark.sql.shuffle.partitions = 200` | `main.py` | Right-sizes shuffle for local mode |
| `MMSI_A < MMSI_B` filter on join | `detect.py` | Halves join output before distance calc |
| `toPandas()` only on tiny result DF | `visualize.py` | Never collect large DFs to driver |

---

## report.md Outline

Write this file yourself after running the pipeline. Use this structure:

```
# Vessel Collision Detection — Report

## 1. Methodology
- Definition of collision (proximity threshold, time alignment rationale)
- Choice of 1-minute time buckets and ±1 slack justification

## 2. Data Quality & Noise Removal
- GPS jump filter: why 50 knots (fastest known ship ≈ 65 knots; 50 gives margin for noise)
- Stationary filter: nav status codes + SOG threshold
- Duplicate/null handling

## 3. Computational Strategy
- Why time-bucket join avoids O(n²) Cartesian product
- Haversine in native Spark SQL vs Python UDF
- Broadcast join for allowlists
- Partition and cache decisions

## 4. Results
[Fill in after running]
- MMSI A: ______  Name: ______
- MMSI B: ______  Name: ______
- Timestamp: ______
- Coordinates: ______°N, ______°E
- Separation: ______ nm (______ m)

## 5. Visualization
[Describe the map: two trajectories, collision marker, time window]

## 6. Limitations & Future Work
- AIS ping frequency varies — some collisions may be between pings
- No interpolation applied between pings (conservative approach)
```

---

## Claude Code Usage Tips

When working in Claude Code (VS Code), use these prompts for maximum efficiency:

- **"Implement `preprocess.py` following the CLAUDE.md spec"** — generates the full file
- **"Write the Haversine SQL expression in detect.py for the pair version"** — targeted
- **"Add error handling to ingest.py for failed downloads and missing files"** — robustness
- **"Write a pytest unit test for the GPS jump filter logic"** — verifiability
- **"Lint and type-check src/ using ruff and mypy"** — code quality
- **"Build the Docker image and report any errors"** — integration test
- **"Run the full pipeline with a small 1-day sample first (Dec 1 only)"** — fast iteration

### Recommended Claude Code workflow

1. Implement `config.py` first — everything else imports from it
2. Implement and test `ingest.py` standalone (just downloads)
3. Implement `preprocess.py` — test with 1-day file, check row counts at each stage
4. Implement `detect.py` — add logging of candidate count at each threshold
5. Implement `enrich.py` and `visualize.py`
6. Wire up `main.py`
7. Build Docker image and do a full end-to-end run
8. Write `report.md` from the actual output

---

## Common Pitfalls — Avoid These

- **Do not use `inferSchema=True`** — too slow; always use explicit `StructType`
- **Do not `.collect()` large DataFrames** — only collect the single collision result row
- **Do not cross-join** — the time-bucket join is the required approach
- **Do not use a Python UDF for Haversine** — use native Spark SQL functions
- **Do not forget `fill="none"` if drawing paths** — (Folium handles this)
- **Do not hardcode file paths** — always use `config.py` constants
- **Do not skip the noise removal section** — it is explicitly graded
- **Do not submit without a working Dockerfile** — reproducibility is 25% of the grade

---

## Git Hygiene

```bash
# .gitignore
data/*.csv
data/*.zip
output/
__pycache__/
*.pyc
.env
*.egg-info/
dist/
.DS_Store
```

Commit messages should follow: `feat:`, `fix:`, `docs:`, `refactor:`, `test:`

Minimum expected commits:
- `feat: project scaffolding and config`
- `feat: ingest AIS data for December 2021`
- `feat: preprocess — schema, filter, noise removal`
- `feat: detect — time-bucket collision algorithm`
- `feat: visualize — folium trajectory map`
- `feat: dockerize application`
- `docs: report and README`
