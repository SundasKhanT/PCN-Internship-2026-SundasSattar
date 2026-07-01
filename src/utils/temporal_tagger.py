"""
temporal_tagger.py — Temporal normalization with HeidelTime.
"""

import re
from datetime import date

from py_heideltime import heideltime

from src.utils.epi_week import get_epi_week, epi_week_key


def extract_temporal_expressions(text: str, language: str = "ENGLISH"):
    if not text:
        return []
    try:
        return heideltime(text, language=language)
    except Exception as e:
        print(f"  [WARN] HeidelTime failed: {e}")
        return []


def _timex_value_to_epi_week_key(value: str):
    if not value:
        return None
    m = re.match(r"^(\d{4})-W(\d{2})$", value)
    if m:
        return f"{m.group(1)}-W{m.group(2)}"
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", value)
    if m:
        d = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        y, w = get_epi_week(d)
        return epi_week_key(y, w)
    return None


def tag_record_temporal_key(text: str):
    for expr in extract_temporal_expressions(text):
        if expr.get("type") in ("DATE", "TIME"):
            key = _timex_value_to_epi_week_key(expr.get("value"))
            if key:
                return key
    return None


if __name__ == "__main__":
    samples = [
        "Flash floods reported in Dera Ghazi Khan on 25 July 2024 following heavy upstream rainfall.",
        "Last week's monsoon rains triggered landslide alerts across Khyber Pakhtunkhwa.",
    ]
    for s in samples:
        print(s, "->", tag_record_temporal_key(s))
