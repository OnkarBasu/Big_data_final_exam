import os
from datetime import timedelta
from typing import Dict
import folium
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from src.config import OUTPUT_DIR, TRAJECTORY_WINDOW_MIN
from src.detect import CollisionResult


def plot_trajectories(
    df: DataFrame,
    result: CollisionResult,
    vessel_names: Dict[int, str],
    output_dir: str = OUTPUT_DIR,
) -> None:
    os.makedirs(output_dir, exist_ok=True)

    window_start = result.event_time - timedelta(minutes=TRAJECTORY_WINDOW_MIN)
    window_end   = result.event_time + timedelta(minutes=TRAJECTORY_WINDOW_MIN)

    traj = (
        df.filter(
            F.col("mmsi").isin([result.mmsi_a, result.mmsi_b]) &
            (F.col("timestamp") >= window_start) &
            (F.col("timestamp") <= window_end)
        )
        .orderBy("mmsi", "timestamp")
    )
    traj_pd = traj.toPandas()

    _save_folium(traj_pd, result, vessel_names, output_dir)
    _save_matplotlib(traj_pd, result, vessel_names, output_dir)


def _save_folium(traj_pd, result, vessel_names, output_dir):
    m = folium.Map(
        location=[result.event_lat, result.event_lon],
        zoom_start=12,
        tiles="OpenStreetMap",
    )
    colors = {result.mmsi_a: "blue", result.mmsi_b: "red"}

    for mmsi, group in traj_pd.groupby("mmsi"):
        coords = list(zip(group["lat"], group["lon"]))
        name = vessel_names.get(mmsi, str(mmsi))
        folium.PolyLine(
            coords,
            color=colors[mmsi],
            weight=3,
            tooltip=f"MMSI {mmsi} — {name}",
        ).add_to(m)
        folium.CircleMarker(coords[0],  radius=5, color=colors[mmsi], fill=True, tooltip="Start").add_to(m)
        folium.CircleMarker(coords[-1], radius=5, color=colors[mmsi], fill=True, tooltip="End").add_to(m)

    folium.Marker(
        [result.event_lat, result.event_lon],
        popup=(
            f"Collision at {result.event_time}<br>"
            f"Distance: {result.distance_nm:.4f} nm"
        ),
        icon=folium.Icon(color="black", icon="warning-sign", prefix="glyphicon"),
    ).add_to(m)

    path = os.path.join(output_dir, "collision_map.html")
    m.save(path)
    print(f"[visualize] Saved {path}")


def _save_matplotlib(traj_pd, result, vessel_names, output_dir):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    colors = {result.mmsi_a: "blue", result.mmsi_b: "red"}

    for mmsi, group in traj_pd.groupby("mmsi"):
        name = vessel_names.get(mmsi, str(mmsi))
        ax1.plot(group["lon"], group["lat"], color=colors[mmsi], label=f"{name} ({mmsi})", linewidth=2)
        ax1.scatter(group["lon"].iloc[0], group["lat"].iloc[0], color=colors[mmsi], marker="o", s=60)
        ax1.scatter(group["lon"].iloc[-1], group["lat"].iloc[-1], color=colors[mmsi], marker="s", s=60)

    ax1.scatter(result.event_lon, result.event_lat, color="black", marker="x", s=150, zorder=5, label="Collision")
    ax1.set_xlabel("Longitude")
    ax1.set_ylabel("Latitude")
    ax1.set_title("Vessel Trajectories (±10 min)")
    ax1.legend()

    for mmsi, group in traj_pd.groupby("mmsi"):
        name = vessel_names.get(mmsi, str(mmsi))
        ax2.plot(group["timestamp"], group["sog"], color=colors[mmsi], label=f"{name} SOG")

    ax2.axvline(result.event_time, color="black", linestyle="--", label="Collision time")
    ax2.set_xlabel("Time")
    ax2.set_ylabel("Speed Over Ground (knots)")
    ax2.set_title("Speed Profile")
    ax2.legend()
    fig.tight_layout()

    path = os.path.join(output_dir, "collision_map.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"[visualize] Saved {path}")
