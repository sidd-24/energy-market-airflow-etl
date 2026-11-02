"""
Pipeline steps, used by the Airflow DAGs and by run_local.py.

    extract   SMARD chunks -> data/raw/<day>.json (the day's values per series)
    transform day extract  -> hourly rows, validated
    load      rows         -> energy_hourly and daily_summary (replaces the day)
"""
import json
from datetime import date, datetime, timezone
from pathlib import Path

import requests

from energy import smard
from energy.config import DB_PATH, GENERATION, LOAD, PRICE, RAW_DIR, TZ
from energy.db import replace_day, save_outlook
from energy.kpis import daily_summary, shift_recommendation
from energy.transform import DataNotReady, build_rows, day_bounds, missing_values, validate

SERIES = {PRICE[0]: PRICE[1:], LOAD[0]: LOAD[1:], **{name: (fid, "DE") for name, fid in GENERATION.items()}}


def _fetch(day: date, names, session=None, raw_dir: str = RAW_DIR, memo: dict | None = None) -> dict:
    session = session or requests.Session()
    start_ms, end_ms = day_bounds(day)
    return {name: smard.get_series(*SERIES[name], start_ms, end_ms, session, raw_dir, memo) for name in names}


def is_complete(day: date, session=None, raw_dir: str = RAW_DIR) -> bool:
    """Used by the Airflow sensor: has SMARD published every hour of every series?"""
    return not missing_values(build_rows(day, _fetch(day, SERIES, session, raw_dir)))


def extract(day: date, session=None, raw_dir: str = RAW_DIR, memo: dict | None = None) -> Path:
    series = _fetch(day, SERIES, session, raw_dir, memo)
    path = Path(raw_dir) / f"{day.isoformat()}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"day": day.isoformat(), "extracted_at": datetime.now(timezone.utc).isoformat(),
                                "series": series}))
    return path


def transform(day: date, raw_dir: str = RAW_DIR) -> tuple[list[dict], list[str]]:
    data = json.loads((Path(raw_dir) / f"{day.isoformat()}.json").read_text())
    series = {name: {int(ts): v for ts, v in values.items()} for name, values in data["series"].items()}
    rows = build_rows(day, series)
    return rows, validate(rows)


def load(day: date, rows: list[dict], db_path: str = DB_PATH) -> dict:
    summary = daily_summary(rows)
    replace_day(db_path, day.isoformat(), rows, summary)
    return summary


def run_day(day: date, session=None, raw_dir: str = RAW_DIR, db_path: str = DB_PATH,
            memo: dict | None = None) -> tuple[dict, list[str]]:
    """memo: share downloads between days of one backfill."""
    extract(day, session, raw_dir, memo)
    rows, warnings = transform(day, raw_dir)
    return load(day, rows, db_path), warnings


def price_outlook(day: date, job_hours: int, job_mw: float, planned_start_hour: int = 8,
                  session=None, raw_dir: str = RAW_DIR, db_path: str | None = DB_PATH) -> dict:
    """Cheapest start for an energy-intensive job on `day`, from the published day-ahead prices."""
    prices = _fetch(day, [PRICE[0]], session, raw_dir)[PRICE[0]]
    rows = build_rows(day, {PRICE[0]: prices})
    if any(r[PRICE[0]] is None for r in rows):
        raise DataNotReady(f"Day-ahead prices for {day} are not published yet")
    hourly = [(datetime.fromisoformat(r["ts_local"]), r[PRICE[0]]) for r in rows]
    outlook = shift_recommendation(hourly, job_hours, job_mw, planned_start_hour)
    if db_path:
        save_outlook(db_path, day.isoformat(), job_hours, job_mw, outlook)
    return outlook


def today_local() -> date:
    return datetime.now(TZ).date()
