"""
entity_linker.py — Cross-domain join across NIH, Weather, and NDMA
harmonized outputs, joined on (district_canonical, epi_week_key).

This is the entity-linking deliverable required by the assessment:
"align outbreak events, environmental anomalies, and disaster alerts
with administrative regions ... enabling cross-domain joins."

Run AFTER spark_consumer.py has written at least one batch of output
for nih_harmonized, weather_harmonized, and ndma_harmonized.

Usage:
    python -m src.processors.entity_linker
"""

import os
import pandas as pd

BASE = "data/sample_output"
OUT_DIR = "data/sample_output/cross_source_linked"


def _load(name):
    path = os.path.join(BASE, f"{name}_harmonized")
    if not os.path.exists(path):
        print(f"  [SKIP] {name}: no output yet at {path}")
        return pd.DataFrame()
    df = pd.read_parquet(path)
    print(f"  [OK] {name}: {len(df)} rows loaded")
    return df


def build_linked_dataset():
    print("Loading harmonized sources...")
    nih = _load("nih")
    weather = _load("weather")
    ndma = _load("ndma")

    if nih.empty or weather.empty:
        print(
            "\nNeed at least NIH + Weather data to build a join. Run the "
            "producers + spark_consumer first."
        )
        return None

    # Aggregate weather to one row per (district, epi_week) since PMD
    # arrives at finer granularity than NIH per the assessment's temporal spec.
    weather_agg = (
        weather.groupby(["district_canonical", "epi_week_key"])
        .agg(
            avg_temperature_c=("temperature_c", "mean"),
            avg_humidity_pct=("humidity_pct", "mean"),
            avg_precipitation_mm=("precipitation_mm", "mean"),
            avg_wind_speed_kmh=("wind_speed_kmh", "mean"),
        )
        .reset_index()
    )

    linked = nih.merge(
        weather_agg,
        on=["district_canonical", "epi_week_key"],
        how="left",
        suffixes=("", "_weather"),
    )

    if not ndma.empty:
        ndma_agg = (
            ndma.groupby(["district_canonical", "epi_week_key"])
            .agg(
                disaster_event_count=("alert_id", "count"),
                max_severity=(
                    "severity",
                    lambda s: s.mode().iloc[0] if not s.mode().empty else None,
                ),
                event_types=("event_type", lambda s: ", ".join(sorted(set(s)))),
            )
            .reset_index()
        )
        linked = linked.merge(
            ndma_agg,
            on=["district_canonical", "epi_week_key"],
            how="left",
        )
    else:
        print("  [INFO] No NDMA data to link yet — joined NIH + Weather only.")

    linked["has_concurrent_disaster"] = (
        linked.get("disaster_event_count", pd.Series([0] * len(linked))).fillna(0) > 0
    )

    os.makedirs(OUT_DIR, exist_ok=True)
    out_path = os.path.join(OUT_DIR, "nih_weather_ndma_linked.parquet")
    linked.to_parquet(out_path, index=False)

    print(f"\n✓ Wrote {len(linked)} linked rows to {out_path}")
    print("\nSample of linked, cross-domain rows:")
    cols = [
        "district_canonical",
        "epi_week_key",
        "disease",
        "case_count",
        "avg_temperature_c",
        "avg_precipitation_mm",
    ]
    if "disaster_event_count" in linked.columns:
        cols += ["disaster_event_count", "max_severity", "has_concurrent_disaster"]
    print(linked[cols].head(10).to_string(index=False))

    return linked


if __name__ == "__main__":
    build_linked_dataset()
