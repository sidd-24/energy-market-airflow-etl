"""
Runs the pipeline without Airflow, e.g. to backfill a range or check a day.

    python run_local.py --start 2026-10-01 --end 2026-10-31
    python run_local.py --outlook                 # tomorrow's cheapest window for a 4 h, 2 MW job
    python run_local.py --outlook --job-hours 6 --job-mw 1.5
"""
import argparse
from datetime import date, timedelta

import requests

from energy.pipeline import price_outlook, run_day, today_local
from energy.transform import DataNotReady, DataQualityError


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", type=date.fromisoformat)
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--outlook", action="store_true", help="cheapest window for tomorrow (or --date)")
    parser.add_argument("--date", type=date.fromisoformat, help="day for the outlook")
    parser.add_argument("--job-hours", type=int, default=4)
    parser.add_argument("--job-mw", type=float, default=2.0)
    parser.add_argument("--planned-start", type=int, default=8, help="hour the job would normally start")
    args = parser.parse_args()
    session = requests.Session()

    if args.outlook:
        day = args.date or today_local() + timedelta(days=1)
        try:
            o = price_outlook(day, args.job_hours, args.job_mw, args.planned_start, session)
        except DataNotReady as error:
            print(f"{error}. Day-ahead prices are published after the auction, around 13:00 the day before.")
            return
        print(f"{day}: run the {args.job_hours} h / {args.job_mw} MW job at {o['best_start'][-5:]} "
              f"(avg {o['best_avg_price']} EUR/MWh, cost {o['best_cost_eur']:,.0f} EUR).")
        print(f"  Saving vs {o['planned_start']} start: {o['saving_vs_planned_eur']:,.0f} EUR; "
              f"vs day average: {o['saving_vs_day_average_eur']:,.0f} EUR")
        return

    start = args.start or today_local() - timedelta(days=2)
    end = args.end or start
    day, memo = start, {}
    while day <= end:
        try:
            summary, warnings = run_day(day, session, memo=memo)
            print(f"{day}: {summary['hours']} h, avg {summary['avg_price']} EUR/MWh, "
                  f"renewables {summary['renewable_share']:.0%}, negative hours {summary['negative_price_hours']}, "
                  f"cheapest 4 h from {summary['cheapest_window_start']}"
                  + (f", {len(warnings)} warning(s)" if warnings else ""))
        except (DataNotReady, DataQualityError) as error:
            print(f"{day}: skipped - {type(error).__name__}: {error}")
        day += timedelta(days=1)


if __name__ == "__main__":
    main()
