import os

# --- Geographic filter ---
CENTER_LAT = 55.225000
CENTER_LON = 14.245000
RADIUS_NM  = 50.0

# --- Temporal filter ---
START_DATE = "2021-12-01"
END_DATE   = "2021-12-31"

# --- Noise removal thresholds ---
MAX_SPEED_KNOTS      = 50.0
MIN_MOVING_SOG_KNOTS = 0.5
STATIONARY_NAV_CODES = ["At anchor", "Moored"]

# --- Collision detection ---
COLLISION_RADIUS_NM   = 0.1
TIME_BUCKET_SECONDS   = 60
TIME_BUCKET_SLACK     = 1
TRAJECTORY_WINDOW_MIN = 10

# --- Paths ---
DATA_DIR   = os.getenv("DATA_DIR",   "/data")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/output")
AIS_URL_TEMPLATE = "http://aisdata.ais.dk/download/aisdk-{year}-{month:02d}-{day:02d}.zip"

# --- AIS CSV column names (Danish AIS schema) ---
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
