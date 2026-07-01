"""
epi_week.py — CDC/MMWR-style epidemiological week calculation.
"""

from datetime import date, datetime, timedelta


def get_epi_week(d: date) -> tuple:
    days_since_sunday = (d.weekday() + 1) % 7
    week_start = d - timedelta(days=days_since_sunday)
    thursday = week_start + timedelta(days=4)
    epi_year = thursday.year

    jan1 = date(epi_year, 1, 1)
    jan1_days_since_sunday = (jan1.weekday() + 1) % 7
    jan1_week_start = jan1 - timedelta(days=jan1_days_since_sunday)
    first_thursday_week_start = jan1_week_start
    if (jan1_week_start + timedelta(days=4)).year < epi_year:
        first_thursday_week_start += timedelta(days=7)

    epi_week = ((week_start - first_thursday_week_start).days // 7) + 1
    return epi_year, epi_week


def get_epi_week_from_str(date_str: str) -> tuple:
    if "T" in date_str:
        d = datetime.fromisoformat(date_str.replace("Z", "+00:00")).date()
    else:
        d = date.fromisoformat(date_str)
    return get_epi_week(d)


def epi_week_key(epi_year: int, epi_week: int) -> str:
    return f"{epi_year}-W{epi_week:02d}"


def current_epi_week_key() -> str:
    y, w = get_epi_week(date.today())
    return epi_week_key(y, w)


if __name__ == "__main__":
    print(current_epi_week_key())
    print(epi_week_key(*get_epi_week_from_str("2024-07-25")))
