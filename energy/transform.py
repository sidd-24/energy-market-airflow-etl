"""
Builds one row per hour for a local (Europe/Berlin) calendar day and checks it.
A day has 23, 24 or 25 hours because of the clock changes.
"""
from datetime import date, datetime, time, timedelta, timezone

from energy.config import (GENERATION, GENERATION_LOAD_RATIO, LOAD, PRICE, PRICE_LIMITS,
                           RENEWABLE, TZ)


class DataNotReady(Exception):
    """SMARD has not published the whole day yet. Try again later."""


class DataQualityError(Exception):
    """The data is complete but implausible. Do not load it."""


def day_bounds(day: date) -> tuple[int, int]:
    """Start and end of a local day in UTC milliseconds."""
    start = datetime.combine(day, time(0), tzinfo=TZ)
    end = datetime.combine(day + timedelta(days=1), time(0), tzinfo=TZ)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def expected_hours(day: date) -> list[int]:
    start_ms, end_ms = day_bounds(day)
    return list(range(start_ms, end_ms, 3_600_000))


def build_rows(day: date, series: dict[str, dict[int, float | None]]) -> list[dict]:
    """series: {"day_ahead_price": {ts: value}, "load": {...}, "solar": {...}, ...}"""
    rows = []
    for ts in expected_hours(day):
        utc = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        row = {"ts_utc": utc.isoformat(), "ts_local": utc.astimezone(TZ).isoformat(), "day": day.isoformat()}
        for name, values in series.items():
            row[name] = values.get(ts)
        rows.append(row)
    return rows


def missing_values(rows: list[dict]) -> dict[str, int]:
    """Hours without a value, per series."""
    names = [PRICE[0], LOAD[0], *GENERATION]
    return {n: c for n in names if (c := sum(r.get(n) is None for r in rows))}


def validate(rows: list[dict]) -> list[str]:
    """Raises DataNotReady or DataQualityError. Returns warnings that do not block the load."""
    missing = missing_values(rows)
    if missing:
        raise DataNotReady(f"Missing hourly values: {missing}")

    low, high = PRICE_LIMITS
    problems = [f"{r['ts_local']}: price {r[PRICE[0]]} outside {PRICE_LIMITS}"
                for r in rows if not low <= r[PRICE[0]] <= high]
    problems += [f"{r['ts_local']}: load {r[LOAD[0]]} is not positive" for r in rows if r[LOAD[0]] <= 0]
    problems += [f"{r['ts_local']}: {n} generation {r[n]} is negative"
                 for r in rows for n in GENERATION if r[n] < 0]
    if problems:
        raise DataQualityError("; ".join(problems[:5]) + (f" (+{len(problems) - 5} more)" if len(problems) > 5 else ""))

    warnings = []
    ratio_low, ratio_high = GENERATION_LOAD_RATIO
    for r in rows:
        ratio = sum(r[n] for n in GENERATION) / r[LOAD[0]]
        if not ratio_low <= ratio <= ratio_high:
            warnings.append(f"{r['ts_local']}: generation is {ratio:.0%} of load")
    return warnings


def renewable_share(rows: list[dict]) -> float:
    total = sum(r[n] for r in rows for n in GENERATION)
    return sum(r[n] for r in rows for n in RENEWABLE) / total if total else 0.0
