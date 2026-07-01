"""
district_resolver.py
---------------------
Entity resolution for Pakistani district name variants
(e.g. "D.G Khan" -> "Dera Ghazi Khan").
"""

import re

DISTRICT_REGISTRY = {
    "Dera Ghazi Khan": {
        "province": "Punjab",
        "aliases": ["d.g khan", "dg khan", "d g khan", "dera ghazi khan"],
    },
    "Rahim Yar Khan": {
        "province": "Punjab",
        "aliases": [
            "r.y khan",
            "ry khan",
            "r. y khan",
            "rahim yar khan",
            "rahimyar khan",
        ],
    },
    "Mandi Bahauddin": {
        "province": "Punjab",
        "aliases": [
            "m.b din",
            "mb din",
            "m. b din",
            "mandi bahauddin",
            "mandi bahaudin",
        ],
    },
    "Lahore": {"province": "Punjab", "aliases": ["lahore"]},
    "Faisalabad": {"province": "Punjab", "aliases": ["faisalabad", "lyallpur"]},
    "Multan": {"province": "Punjab", "aliases": ["multan"]},
    "Rawalpindi": {"province": "Punjab", "aliases": ["rawalpindi", "pindi"]},
    "Karachi": {"province": "Sindh", "aliases": ["karachi"]},
    "Hyderabad": {"province": "Sindh", "aliases": ["hyderabad"]},
    "Sukkur": {"province": "Sindh", "aliases": ["sukkur"]},
    "Peshawar": {"province": "Khyber Pakhtunkhwa", "aliases": ["peshawar"]},
    "Quetta": {"province": "Balochistan", "aliases": ["quetta"]},
    "Islamabad": {
        "province": "Islamabad Capital Territory",
        "aliases": ["islamabad", "isb"],
    },
}


def _normalize(raw: str) -> str:
    s = raw.lower().strip()
    s = re.sub(r"[.\-_,]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


_ALIAS_LOOKUP = {}
_ALIAS_LOOKUP_TIGHT = {}
for canon, info in DISTRICT_REGISTRY.items():
    for alias in info["aliases"]:
        norm_alias = _normalize(alias)
        _ALIAS_LOOKUP[norm_alias] = canon
        _ALIAS_LOOKUP_TIGHT[norm_alias.replace(" ", "")] = canon


def resolve_district(raw_name: str) -> dict:
    if not raw_name:
        return {
            "canonical": "UNKNOWN",
            "province": "UNKNOWN",
            "matched": False,
            "raw_input": raw_name,
        }

    norm = _normalize(raw_name)

    if norm in _ALIAS_LOOKUP:
        canon = _ALIAS_LOOKUP[norm]
        return {
            "canonical": canon,
            "province": DISTRICT_REGISTRY[canon]["province"],
            "matched": True,
            "raw_input": raw_name,
        }

    tight = norm.replace(" ", "")
    if tight in _ALIAS_LOOKUP_TIGHT:
        canon = _ALIAS_LOOKUP_TIGHT[tight]
        return {
            "canonical": canon,
            "province": DISTRICT_REGISTRY[canon]["province"],
            "matched": True,
            "raw_input": raw_name,
        }

    for alias, canon in _ALIAS_LOOKUP.items():
        if alias in norm or norm in alias:
            return {
                "canonical": canon,
                "province": DISTRICT_REGISTRY[canon]["province"],
                "matched": True,
                "raw_input": raw_name,
            }

    return {
        "canonical": raw_name.strip().title(),
        "province": "UNKNOWN",
        "matched": False,
        "raw_input": raw_name,
    }


def resolve_district_geocoded(raw_name: str) -> dict:
    """
    Entity resolution + geocoding in one step. Resolves the alias to a
    canonical district name/province, then enriches with lat/lon via
    GeoPy + Nominatim (OpenStreetMap), as required by the assessment.
    If GeoPy/Nominatim is unavailable (network issue, rate limit, or
    cache miss), falls back to the known coordinates already curated
    in settings.DISTRICTS, so the pipeline never silently drops lat/lon.
    """
    from src.utils.geocoder import geocode_district
    from src.config.settings import DISTRICTS

    resolution = resolve_district(raw_name)
    geo = geocode_district(resolution["canonical"], country="Pakistan")

    if geo:
        resolution["lat"] = geo["lat"]
        resolution["lon"] = geo["lon"]
        resolution["geocode_source"] = geo["source"]
    else:
        fallback = next(
            (d for d in DISTRICTS if d["name"].lower() == raw_name.strip().lower()),
            None,
        )
        if fallback:
            resolution["lat"] = fallback["lat"]
            resolution["lon"] = fallback["lon"]
            resolution["geocode_source"] = "settings_fallback"
        else:
            resolution["lat"] = None
            resolution["lon"] = None
            resolution["geocode_source"] = "unresolved"

    return resolution


if __name__ == "__main__":
    for t in ["D.G Khan", "R. Y Khan", "Lahore", "Unknown Place"]:
        r = resolve_district_geocoded(t)
        print(
            f"{t:20s} -> {r['canonical']:20s} ({r['province']}) "
            f"lat={r['lat']} lon={r['lon']} [{r['geocode_source']}]"
        )
