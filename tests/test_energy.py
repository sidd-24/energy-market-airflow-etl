"""Tests for the energy pipeline. A fake SMARD server replaces the network."""
import sqlite3
from datetime import date, datetime
from pathlib import Path

import pytest

from energy import pipeline, smard
from energy.config import GENERATION, TZ
from energy.kpis import cheapest_window, daily_summary, shift_recommendation
from energy.transform import (DataNotReady, DataQualityError, build_rows, day_bounds, expected_hours,
                              renewable_share, validate)

WEEKDAY = date(2026, 11, 3)  # a Tuesday


def full_series(day, price=100.0, load=50_000.0, gen=4_000.0):
    hours = expected_hours(day)
    series = {"day_ahead_price": {t: price for t in hours}, "load": {t: load for t in hours}}
    series.update({name: {t: gen for t in hours} for name in GENERATION})
    return series


# ─── Days and chunks ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("day, hours", [(date(2026, 3, 29), 23), (date(2026, 10, 25), 25), (WEEKDAY, 24)])
def test_local_days_follow_the_clock_changes(day, hours):
    assert len(expected_hours(day)) == hours


def test_chunks_overlapping_an_interval():
    assert smard.chunks_for_interval([0, 100, 200], 150, 250) == [100, 200]
    assert smard.chunks_for_interval([0, 100, 200], 50, 60) == [0]


# ─── Validation ────────────────────────────────────────────────────────────────

def test_complete_plausible_day_passes():
    assert validate(build_rows(WEEKDAY, full_series(WEEKDAY))) == []


def test_missing_hour_means_not_ready():
    series = full_series(WEEKDAY)
    series["solar"][expected_hours(WEEKDAY)[-1]] = None
    with pytest.raises(DataNotReady, match="solar"):
        validate(build_rows(WEEKDAY, series))


@pytest.mark.parametrize("change", [
    lambda s, t: s["day_ahead_price"].__setitem__(t, 5000.0),   # above the market price limit
    lambda s, t: s["lignite"].__setitem__(t, -10.0),            # negative generation
    lambda s, t: s["load"].__setitem__(t, 0.0),                 # no load
])
def test_implausible_values_are_rejected(change):
    series = full_series(WEEKDAY)
    change(series, expected_hours(WEEKDAY)[5])
    with pytest.raises(DataQualityError):
        validate(build_rows(WEEKDAY, series))


def test_negative_prices_are_valid_and_odd_generation_only_warns():
    rows = build_rows(WEEKDAY, full_series(WEEKDAY, price=-20.0, gen=500.0))   # generation is 11% of load
    warnings = validate(rows)
    assert len(warnings) == 24 and "generation is 11% of load" in warnings[0]


def test_renewable_share():
    rows = build_rows(WEEKDAY, full_series(WEEKDAY))
    assert renewable_share(rows) == pytest.approx(6 / 11)   # 6 of the 11 sources are renewable


# ─── KPIs and load shifting ────────────────────────────────────────────────────

def test_cheapest_and_priciest_window():
    assert cheapest_window([5, 1, 1, 9, 0, 0], 2) == (4, 0)
    assert cheapest_window([5, 1, 1, 9, 0, 0], 2, cheapest=False) == (2, 5)
    assert cheapest_window([1, 2], 3) is None


def test_daily_summary_peak_and_negative_hours():
    series = full_series(WEEKDAY)
    for t in expected_hours(WEEKDAY):
        hour = datetime.fromtimestamp(t / 1000, TZ).hour
        series["day_ahead_price"][t] = 200.0 if 8 <= hour < 20 else -5.0
    summary = daily_summary(build_rows(WEEKDAY, series))
    assert summary["peak_avg_price"] == 200.0 and summary["off_peak_avg_price"] == -5.0
    assert summary["negative_price_hours"] == 12 and summary["cheapest_window_start"] == "00:00"


def test_weekend_has_no_peak():
    saturday = date(2026, 11, 7)
    assert daily_summary(build_rows(saturday, full_series(saturday)))["peak_avg_price"] is None


def test_shift_recommendation_saving():
    prices = [(datetime(2026, 11, 3, h, tzinfo=TZ), p) for h, p in enumerate([50] * 8 + [150] * 4 + [100] * 12)]
    o = shift_recommendation(prices, job_hours=4, job_mw=2.0, planned_start_hour=8)
    assert o["best_start"] == "2026-11-03 00:00" and o["best_cost_eur"] == 400.0         # 4 h x 2 MW x 50
    assert o["planned_cost_eur"] == 1200.0 and o["saving_vs_planned_eur"] == 800.0      # 08:00-12:00 at 150


# ─── Pipeline against a fake SMARD server ──────────────────────────────────────

class FakeResponse:
    def __init__(self, payload):
        self.status_code, self._payload = 200, payload

    def json(self):
        return self._payload

    def raise_for_status(self):
        pass


class FakeSmard:
    """Serves one weekly chunk per series covering `day`; `price` sets the day-ahead price."""
    def __init__(self, day, price=100.0, missing_last_hour=False):
        self.day, self.price, self.missing, self.calls = day, price, missing_last_hour, 0
        self.chunk = day_bounds(day)[0] - 86_400_000

    def get(self, url, timeout=None):
        self.calls += 1
        if url.endswith("index_hour.json"):
            return FakeResponse({"timestamps": [self.chunk]})
        hours = expected_hours(self.day)
        value = self.price if "/4169/" in url else 4_000.0 if "/410/" not in url else 50_000.0
        series = [[t, value] for t in range(self.chunk, self.chunk + 8 * 86_400_000, 3_600_000)]
        if self.missing:
            series = [[t, None if t == hours[-1] else v] for t, v in series]
        return FakeResponse({"series": series})


def test_run_day_is_idempotent(tmp_path):
    day, db = date(2026, 10, 25), str(tmp_path / "energy.db")
    for price in (100.0, 120.0):   # second run with revised data replaces the first
        summary, _ = pipeline.run_day(day, FakeSmard(day, price), str(tmp_path / "raw"), db)
    with sqlite3.connect(db) as c:
        assert c.execute("SELECT COUNT(*) FROM energy_hourly").fetchone()[0] == 25
        assert c.execute("SELECT COUNT(*), avg_price FROM daily_summary").fetchone() == (1, 120.0)


def test_incomplete_day_is_not_loaded(tmp_path):
    day = WEEKDAY
    assert not pipeline.is_complete(day, FakeSmard(day, missing_last_hour=True), str(tmp_path))
    with pytest.raises(DataNotReady):
        pipeline.run_day(day, FakeSmard(day, missing_last_hour=True), str(tmp_path / "raw"), str(tmp_path / "e.db"))


def test_backfill_downloads_each_chunk_once(tmp_path):
    fake, memo = FakeSmard(WEEKDAY), {}
    for day in (WEEKDAY, date(2026, 11, 4)):   # same weekly chunk
        pipeline.extract(day, fake, str(tmp_path), memo)
    assert fake.calls == 2 * 13                   # index + chunk for 13 series, not repeated for day two
    assert len(list(tmp_path.glob("4169_DE-LU_*.json"))) == 1   # raw source file kept for lineage


def test_price_outlook(tmp_path):
    day = WEEKDAY
    outlook = pipeline.price_outlook(day, 4, 2.0, session=FakeSmard(day), raw_dir=str(tmp_path), db_path=None)
    assert outlook["best_cost_eur"] == 800.0 and outlook["saving_vs_planned_eur"] == 0.0


# ─── DAG integrity (runs where Airflow is installed, e.g. in CI) ───────────────

def test_dags_load_without_errors():
    pytest.importorskip("airflow")
    from airflow.models import DagBag
    bag = DagBag(dag_folder=str(Path(__file__).parent.parent / "dags"), include_examples=False)
    assert bag.import_errors == {}
    assert set(bag.dag_ids) == {"energy_market_etl", "energy_price_outlook"}
    assert [t.task_id for t in bag.get_dag("energy_market_etl").topological_sort()] == \
        ["wait_for_data", "extract", "transform", "load"]
