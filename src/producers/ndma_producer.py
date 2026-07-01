import random
import time
import uuid
from datetime import datetime, timezone
from src.config.kafka_config import get_kafka_producer
from src.config.settings import NDMA_TOPIC, DISTRICTS

from src.utils.district_resolver import resolve_district_geocoded
from src.utils.epi_week import current_epi_week_key
from src.utils.monitoring import log_ingestion
from src.utils.temporal_tagger import tag_record_temporal_key

random.seed(7)
EVENT_TYPES = ["flood", "landslide", "drought", "heatwave"]
SEVERITIES = ["minor", "moderate", "severe", "catastrophic"]


def generate_alert():
    d = random.choice(DISTRICTS)
    resolution = resolve_district_geocoded(d["name"])
    epi_key = current_epi_week_key()
    epi_year, epi_week = epi_key.split("-W")
    severity = random.choices(SEVERITIES, weights=[30, 35, 25, 10])[0]
    event_type = random.choice(EVENT_TYPES)

    description = (
        f"{event_type.title()} reported in {d['name']} on "
        f"{datetime.now(timezone.utc).strftime('%d %B %Y')}; severity {severity}."
    )

    alert = {
        "record_id": str(uuid.uuid4()),
        "alert_id": f"NDMA-{uuid.uuid4().hex[:8].upper()}",
        "source": "NDMA",
        "event_type": event_type,
        "district": d["name"],
        "district_canonical": resolution["canonical"],
        "province": resolution["province"],
        "lat": resolution["lat"],
        "lon": resolution["lon"],
        "severity": severity,
        "epi_year": int(epi_year),
        "epi_week": int(epi_week),
        "issued_at": datetime.now(timezone.utc).isoformat(),
        "description": description,
    }

    # HeidelTime extracts the date mentioned in the free-text description
    # and converts it to our epi_week_key format; falls back to the
    # ingestion-time epi_key if no date expression is found in the text.
    alert["extracted_temporal_key"] = tag_record_temporal_key(description) or epi_key

    return alert


if __name__ == "__main__":
    producer = get_kafka_producer()
    n_alerts = 5
    for i in range(n_alerts):
        alert = generate_alert()
        producer.send(NDMA_TOPIC, alert)
        print(
            f"  → {alert['alert_id']} | {alert['event_type']} | {alert['district_canonical']} | "
            f"{alert['severity']} | temporal_key={alert['extracted_temporal_key']}"
        )
        time.sleep(random.uniform(0.2, 0.6))
    producer.flush()
    log_ingestion(
        "NDMA",
        records_attempted=n_alerts,
        records_succeeded=n_alerts,
    )
    print(f"\n✓ Published {n_alerts} alerts to '{NDMA_TOPIC}'")
