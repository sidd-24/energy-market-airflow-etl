"""
SQLite storage. Every load replaces the rows of its day in one transaction,
so re-running or backfilling a day never creates duplicates.
"""
import sqlite3
from pathlib import Path

from energy.config import GENERATION, LOAD, PRICE

HOURLY_COLUMNS = ["ts_utc", "ts_local", "day", PRICE[0], LOAD[0], *GENERATION]
DAILY_COLUMNS = ["day", "hours", "avg_price", "min_price", "max_price", "peak_avg_price", "off_peak_avg_price",
                 "negative_price_hours", "renewable_share", "total_load_mwh", "cheapest_window_start",
                 "cheapest_window_avg_price", "priciest_window_start", "priciest_window_avg_price"]


def connect(path: str) -> sqlite3.Connection:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.execute(f"""CREATE TABLE IF NOT EXISTS energy_hourly (
        ts_utc TEXT PRIMARY KEY, ts_local TEXT NOT NULL, day TEXT NOT NULL,
        {", ".join(f"{c} REAL" for c in HOURLY_COLUMNS[3:])})""")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_hourly_day ON energy_hourly(day)")
    connection.execute(f"""CREATE TABLE IF NOT EXISTS daily_summary (
        day TEXT PRIMARY KEY, {", ".join(f"{c}" for c in DAILY_COLUMNS[1:])})""")
    connection.execute("""CREATE TABLE IF NOT EXISTS price_outlook (
        day TEXT, job_hours INTEGER, job_mw REAL, best_start TEXT, best_avg_price REAL, best_cost_eur REAL,
        planned_start TEXT, planned_cost_eur REAL, saving_vs_planned_eur REAL, saving_vs_day_average_eur REAL,
        PRIMARY KEY (day, job_hours, job_mw))""")
    return connection


def replace_day(path: str, day: str, rows: list[dict], summary: dict) -> None:
    with connect(path) as connection:
        connection.execute("DELETE FROM energy_hourly WHERE day = ?", [day])
        connection.executemany(
            f"INSERT INTO energy_hourly VALUES ({', '.join('?' for _ in HOURLY_COLUMNS)})",
            [[r[c] for c in HOURLY_COLUMNS] for r in rows])
        connection.execute(f"INSERT OR REPLACE INTO daily_summary VALUES ({', '.join('?' for _ in DAILY_COLUMNS)})",
                           [summary[c] for c in DAILY_COLUMNS])


def save_outlook(path: str, day: str, job_hours: int, job_mw: float, outlook: dict) -> None:
    with connect(path) as connection:
        connection.execute("INSERT OR REPLACE INTO price_outlook VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                           [day, job_hours, job_mw, outlook["best_start"], outlook["best_avg_price"],
                            outlook["best_cost_eur"], outlook["planned_start"], outlook["planned_cost_eur"],
                            outlook["saving_vs_planned_eur"], outlook["saving_vs_day_average_eur"]])
