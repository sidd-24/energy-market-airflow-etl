"""
Load-shifting outlook: after the day-ahead auction, find tomorrow's cheapest
window for an energy-intensive production job and the saving against its
planned start. Job size and duration are DAG params (Trigger DAG w/ config).
"""
import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param

from energy import pipeline
from energy.transform import DataNotReady


def _tomorrow(data_interval_end):
    # The 14:00 run on day D has data_interval_end = D 14:00, so this is D + 1
    return data_interval_end.in_timezone("Europe/Berlin").add(days=1).date()


@dag(
    # The auction closes at 12:00 and SMARD publishes the results shortly after
    schedule="0 14 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="Europe/Berlin"),
    catchup=False,
    params={
        "job_hours": Param(4, type="integer", minimum=1, maximum=24),
        "job_mw": Param(2.0, type="number", minimum=0.1),
        "planned_start_hour": Param(8, type="integer", minimum=0, maximum=23),
    },
    tags=["energy", "smard", "load-shifting"],
    doc_md=__doc__,
)
def energy_price_outlook():

    @task.sensor(poke_interval=1800, timeout=6 * 3600, mode="reschedule")
    def wait_for_prices(data_interval_end=None) -> bool:
        try:
            pipeline.price_outlook(_tomorrow(data_interval_end), 1, 1.0, db_path=None)
            return True
        except DataNotReady:
            return False

    @task
    def recommend(data_interval_end=None, params=None) -> dict:
        day = _tomorrow(data_interval_end)
        outlook = pipeline.price_outlook(day, params["job_hours"], params["job_mw"], params["planned_start_hour"])
        print(f"{day}: start the job at {outlook['best_start']} "
              f"(saving {outlook['saving_vs_planned_eur']} EUR vs {outlook['planned_start']})")
        return outlook

    wait_for_prices() >> recommend()


energy_price_outlook()
