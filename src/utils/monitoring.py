"""
monitoring.py — Basic ingestion monitoring/metrics.
"""

import json
import os
from datetime import datetime, timezone

LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "logs")
LOG_FILE = os.path.join(LOG_DIR, "ingestion_metrics.jsonl")


def log_ingestion(
    source: str, records_attempted: int, records_succeeded: int, errors=None
):
    os.makedirs(LOG_DIR, exist_ok=True)
    entry = {
        "source": source,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "records_attempted": records_attempted,
        "records_succeeded": records_succeeded,
        "records_failed": records_attempted - records_succeeded,
        "success_rate": (
            round(records_succeeded / records_attempted, 3)
            if records_attempted
            else 0.0
        ),
        "errors": errors or [],
    }
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(entry) + "\n")
    print(
        f"  [METRICS] {source}: {records_succeeded}/{records_attempted} succeeded ({entry['success_rate']*100:.1f}%)"
    )
    return entry


def summarize():
    if not os.path.exists(LOG_FILE):
        print("No ingestion metrics logged yet.")
        return
    with open(LOG_FILE) as f:
        rows = [json.loads(line) for line in f]
    by_source = {}
    for r in rows:
        by_source.setdefault(r["source"], []).append(r)
    for source, runs in by_source.items():
        total_a = sum(r["records_attempted"] for r in runs)
        total_s = sum(r["records_succeeded"] for r in runs)
        print(
            f"{source:20s} runs={len(runs):3d}  {total_s}/{total_a} records succeeded"
        )


if __name__ == "__main__":
    summarize()
