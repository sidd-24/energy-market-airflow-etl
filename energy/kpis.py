"""
Daily market figures and the load-shifting calculation: when should an
energy-intensive production job run to pay the lowest day-ahead price?
"""
from datetime import datetime

from energy.config import PEAK_HOURS, PRICE
from energy.transform import renewable_share


def _mean(values):
    return sum(values) / len(values) if values else None


def cheapest_window(prices: list[float], hours: int, cheapest: bool = True) -> tuple[int, float] | None:
    """Start index and average price of the cheapest (or most expensive) block of consecutive hours."""
    if hours <= 0 or len(prices) < hours:
        return None
    windows = [(i, _mean(prices[i:i + hours])) for i in range(len(prices) - hours + 1)]
    return (min if cheapest else max)(windows, key=lambda w: w[1])


def daily_summary(rows: list[dict], window_hours: int = 4) -> dict:
    prices = [r[PRICE[0]] for r in rows]
    local = [datetime.fromisoformat(r["ts_local"]) for r in rows]
    peak = [p for p, t in zip(prices, local) if t.weekday() < 5 and t.hour in PEAK_HOURS]
    off_peak = [p for p, t in zip(prices, local) if not (t.weekday() < 5 and t.hour in PEAK_HOURS)]
    low = cheapest_window(prices, window_hours)
    high = cheapest_window(prices, window_hours, cheapest=False)
    return {
        "day": rows[0]["day"],
        "hours": len(rows),
        "avg_price": round(_mean(prices), 2),
        "min_price": min(prices),
        "max_price": max(prices),
        "peak_avg_price": round(_mean(peak), 2) if peak else None,
        "off_peak_avg_price": round(_mean(off_peak), 2) if off_peak else None,
        "negative_price_hours": sum(p < 0 for p in prices),
        "renewable_share": round(renewable_share(rows), 4),
        "total_load_mwh": round(sum(r["load"] for r in rows), 1),
        "cheapest_window_start": local[low[0]].strftime("%H:%M"),
        "cheapest_window_avg_price": round(low[1], 2),
        "priciest_window_start": local[high[0]].strftime("%H:%M"),
        "priciest_window_avg_price": round(high[1], 2),
    }


def shift_recommendation(hourly_prices: list[tuple[datetime, float]], job_hours: int, job_mw: float,
                         planned_start_hour: int = 8) -> dict | None:
    """
    For a job that draws job_mw for job_hours consecutive hours: the cheapest start
    time, its energy cost, and the saving against the planned start and against
    the day's average price.
    """
    prices = [p for _, p in hourly_prices]
    best = cheapest_window(prices, job_hours)
    if best is None:
        return None
    start_index, best_price = best
    energy_mwh = job_hours * job_mw
    planned_index = next((i for i, (t, _) in enumerate(hourly_prices) if t.hour == planned_start_hour), None)
    planned = (sum(prices[planned_index:planned_index + job_hours]) / job_hours
               if planned_index is not None and planned_index + job_hours <= len(prices) else None)
    return {
        "best_start": hourly_prices[start_index][0].strftime("%Y-%m-%d %H:%M"),
        "best_avg_price": round(best_price, 2),
        "best_cost_eur": round(best_price * energy_mwh, 2),
        "planned_start": f"{planned_start_hour:02d}:00",
        "planned_cost_eur": round(planned * energy_mwh, 2) if planned is not None else None,
        "saving_vs_planned_eur": round((planned - best_price) * energy_mwh, 2) if planned is not None else None,
        "saving_vs_day_average_eur": round((_mean(prices) - best_price) * energy_mwh, 2),
    }
