import requests
from time import time

from src.config.kafka_config import get_kafka_producer
from src.config.settings import WEATHER_TOPIC, OPEN_METEO_BASE_URL, DISTRICTS
from src.utils.district_resolver import resolve_district_geocoded
from src.utils.epi_week import current_epi_week_key
from src.utils.monitoring import log_ingestion


def fetch_weather(lat, lon):
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation",
    }
    resp = requests.get(OPEN_METEO_BASE_URL, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()["current"]


if __name__ == "__main__":
    producer = get_kafka_producer()
    epi_key = current_epi_week_key()
    epi_year, epi_week = epi_key.split("-W")

    published = 0
    for d in DISTRICTS:
        resolution = resolve_district_geocoded(d["name"])
        try:
            current = fetch_weather(d["lat"], d["lon"])
        except Exception as e:
            print(f"  [SKIP] {d['name']}: {e}")
            continue

        record = {
            "source": "PMD",
            "district": d["name"],
            "district_canonical": resolution["canonical"],
            "province": resolution["province"],
            "lat": resolution["lat"],
            "lon": resolution["lon"],
            "epi_year": int(epi_year),
            "epi_week": int(epi_week),
            "temperature_c": current["temperature_2m"],
            "humidity_pct": current["relative_humidity_2m"],
            "wind_speed_kmh": current["wind_speed_10m"],
            "precipitation_mm": current.get("precipitation", 0.0),
            "timestamp": time(),
        }
        producer.send(WEATHER_TOPIC, record)
        published += 1
        print(
            f"  ✓ {d['name']} -> {resolution['canonical']}: {current['temperature_2m']}°C"
        )

    producer.flush()
    log_ingestion(
        "WEATHER",
        records_attempted=len(DISTRICTS),
        records_succeeded=published,
    )
    print(
        f"\n✓ Published {published}/{len(DISTRICTS)} weather records to '{WEATHER_TOPIC}'"
    )
