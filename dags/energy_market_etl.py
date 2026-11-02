"""
Daily ETL of the German electricity market (SMARD, Bundesnetzagentur).

One run per Berlin calendar day. It waits until SMARD has published every
hour of the day, then extracts, validates and loads it. Loads replace the
day, so retries and backfills are safe:

    airflow dags backfill energy_market_etl -s 2026-10-01 -e 2026-10-31
"""
import json
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

from energy import pipeline
from energy.config import RAW_DIR
from energy.transform import DataQualityError

STAGING_DIR = Path(RAW_DIR).parent / "staging"


def _day(data_interval_start):
    return data_interval_start.in_timezone("Europe/Berlin").date()


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 1, tz="Europe/Berlin"),
    catchup=True,
    max_active_runs=3,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=10)},
    tags=["energy", "etl", "smard"],
    doc_md=__doc__,
)
def energy_market_etl():

    # SMARD publishes generation data with about a day's delay. Reschedule mode
    # frees the worker slot between checks.
    @task.sensor(poke_interval=3600, timeout=48 * 3600, mode="reschedule")
    def wait_for_data(data_interval_start=None) -> bool:
        return pipeline.is_complete(_day(data_interval_start))

    @task
    def extract(data_interval_start=None) -> str:
        return str(pipeline.extract(_day(data_interval_start)))

    @task
    def transform(extract_path: str, data_interval_start=None) -> str:
        day = _day(data_interval_start)
        try:
            rows, warnings = pipeline.transform(day)
        except DataQualityError as error:
            # Bad data does not fix itself: fail without retrying
            raise AirflowFailException(str(error)) from error
        for warning in warnings:
            print(f"WARNING {warning}")
        path = STAGING_DIR / f"{day.isoformat()}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(rows))
        return str(path)

    @task
    def load(staging_path: str, data_interval_start=None) -> dict:
        rows = json.loads(Path(staging_path).read_text())
        summary = pipeline.load(_day(data_interval_start), rows)
        print(f"Loaded {summary['hours']} hours, average price {summary['avg_price']} EUR/MWh")
        return summary

    # The sensor must gate extract. `wait_for_data() >> load(transform(extract()))`
    # would only link it to load, and extract would start before the data is complete.
    extracted = extract()
    wait_for_data() >> extracted
    load(transform(extracted))


energy_market_etl()
