import os
from datetime import date, timedelta
from src.config import DATA_DIR


def load_december_2021(data_dir: str = DATA_DIR) -> list[str]:
    """Return sorted list of local AIS CSV paths for December 2021."""
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
