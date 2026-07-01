import random
import uuid
from datetime import datetime, timezone

from src.config.kafka_config import get_kafka_producer
from src.config.settings import POLICY_TOPIC
from src.utils.monitoring import log_ingestion

random.seed(21)

POLICY_TYPES = [
    "National Policy",
    "Emergency Response",
    "Reforestation Programme",
    "Energy Policy",
    "City Action Plan",
    "Water Policy",
]
STATUSES = ["Draft", "Active", "Completed", "Under Review"]
SECTORS_POOL = [
    "Agriculture",
    "Water",
    "Energy",
    "Health",
    "Forests",
    "Disaster Risk",
    "Urban Planning",
]

SAMPLE_POLICIES = [
    {
        "title": "National Climate Change Policy 2023",
        "type": "National Policy",
        "summary": "Framework for adaptation and mitigation across all sectors.",
        "year": 2023,
    },
    {
        "title": "Pakistan Floods Emergency Response Plan 2022",
        "type": "Emergency Response",
        "summary": "Coordinated response to 2022 monsoon flooding affecting 33M people.",
        "year": 2022,
    },
    {
        "title": "Billion Tree Tsunami Programme Phase II",
        "type": "Reforestation Programme",
        "summary": "Extension of afforestation across KPK, Balochistan, and GB.",
        "year": 2021,
    },
    {
        "title": "Renewable Energy Policy 2025",
        "type": "Energy Policy",
        "summary": "Target 60% renewable energy in national grid by 2030.",
        "year": 2025,
    },
    {
        "title": "Heat Action Plan - Karachi 2024",
        "type": "City Action Plan",
        "summary": "Early warning and cool centers for heatwave response.",
        "year": 2024,
    },
    {
        "title": "Indus Waters & Climate Adaptation Strategy",
        "type": "Water Policy",
        "summary": "Long-term water security strategy under climate stress.",
        "year": 2026,
    },
]


def generate_records():
    now = datetime.now(timezone.utc)
    records = []
    for p in SAMPLE_POLICIES:
        sectors = random.sample(SECTORS_POOL, k=random.randint(2, 4))
        records.append(
            {
                "record_id": str(uuid.uuid4()),
                "source": "MOCC_PAKISTAN",
                "title": p["title"],
                "policy_type": p["type"],
                "status": random.choice(STATUSES),
                "year": p["year"],
                "summary": p["summary"],
                "sectors": sectors,
                "report_month": now.month,
                "report_year": now.year,
                "report_month_label": now.strftime("%Y-%m"),
                "ingestion_ts": now.isoformat(),
            }
        )
    return records


if __name__ == "__main__":
    producer = get_kafka_producer()
    records = generate_records()
    for rec in records:
        producer.send(POLICY_TOPIC, rec)
        print(f"  → {rec['title']} ({rec['policy_type']}, {rec['status']})")
    producer.flush()
    print(f"\n✓ Published {len(records)} policy records to '{POLICY_TOPIC}'")
    log_ingestion(
        "Policy", records_attempted=len(records), records_succeeded=len(records)
    )
