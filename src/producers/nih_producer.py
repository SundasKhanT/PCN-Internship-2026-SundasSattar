import random
import uuid
from datetime import datetime, timezone
from src.config.kafka_config import get_kafka_producer
from src.config.settings import NIH_TOPIC, DISTRICTS
from src.utils.district_resolver import resolve_district_geocoded
from src.utils.epi_week import current_epi_week_key
from src.utils.monitoring import log_ingestion

random.seed(42)
DISEASES = ["dengue", "diarrhoea", "ili", "malaria"]
BASE_RATE = {"dengue": 8, "diarrhoea": 25, "ili": 40, "malaria": 12}


def generate_records():
    epi_key = current_epi_week_key()
    epi_year, epi_week = epi_key.split("-W")
    records = []
    for d in DISTRICTS:
        resolution = resolve_district_geocoded(d["name"])
        for disease in DISEASES:
            base = BASE_RATE[disease]
            case_count = max(0, int(base * random.uniform(0.6, 1.8)))
            records.append(
                {
                    "record_id": str(uuid.uuid4()),
                    "source": "NIH_PAKISTAN",
                    "epi_year": int(epi_year),
                    "epi_week": int(epi_week),
                    "district": d["name"],
                    "district_canonical": resolution["canonical"],
                    "province": resolution["province"],
                    "lat": resolution["lat"],
                    "lon": resolution["lon"],
                    "disease": disease,
                    "case_count": case_count,
                    "idsr_compliance_pct": round(random.uniform(65, 99), 1),
                    "ingestion_ts": datetime.now(timezone.utc).isoformat(),
                }
            )
    return records


if __name__ == "__main__":
    producer = get_kafka_producer()
    records = generate_records()
    for rec in records:
        producer.send(NIH_TOPIC, rec)
    producer.flush()
    print(f"✓ Published {len(records)} NIH surveillance records to '{NIH_TOPIC}'")
    log_ingestion("NIH", records_attempted=len(records), records_succeeded=len(records))
