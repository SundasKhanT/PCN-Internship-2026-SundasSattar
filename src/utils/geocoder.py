"""
geocoder.py — Geocoding with GeoPy + OpenStreetMap (Nominatim).
"""

import json
import os
import time

from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

CACHE_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "geocode_cache.json"
)

_geolocator = Nominatim(user_agent="chip-task3-pipeline")


def _load_cache():
    if os.path.exists(CACHE_PATH):
        with open(CACHE_PATH) as f:
            return json.load(f)
    return {}


def _save_cache(cache):
    os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
    with open(CACHE_PATH, "w") as f:
        json.dump(cache, f, indent=2)


_cache = _load_cache()


def geocode_district(name: str, country: str = "Pakistan", retries: int = 2):
    key = f"{name.strip().lower()}|{country.lower()}"
    if key in _cache:
        return {**_cache[key], "source": "cache"}

    query = f"{name}, {country}"
    for attempt in range(retries):
        try:
            location = _geolocator.geocode(query, timeout=10)
            if location:
                result = {
                    "lat": location.latitude,
                    "lon": location.longitude,
                    "display_name": location.address,
                }
                _cache[key] = result
                _save_cache(_cache)
                return {**result, "source": "nominatim"}
            return None
        except (GeocoderTimedOut, GeocoderServiceError):
            time.sleep(1.5)
        finally:
            time.sleep(1)
    return None


if __name__ == "__main__":
    from src.config.settings import DISTRICTS

    for d in DISTRICTS:
        print(d["name"], "->", geocode_district(d["name"]))
