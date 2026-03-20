from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import sqlite3
import json
import os

DB_PATH = "/opt/airflow/data/weather.db"
CITY = "Munich"
URL = f"https://wttr.in/{CITY}?format=j1"


def extract(**context):
    response = requests.get(URL, timeout=10)
    response.raise_for_status()
    data = response.json()
    context["ti"].xcom_push(key="raw_data", value=json.dumps(data))
    print(f"Extracted weather data for {CITY}")


def transform(**context):
    raw = context["ti"].xcom_pull(key="raw_data", task_ids="extract")
    data = json.loads(raw)

    current = data["current_condition"][0]
    weather = data["weather"][0]

    transformed = {
        "city": CITY,
        "timestamp": datetime.utcnow().isoformat(),
        "temp_c": int(current["temp_C"]),
        "feels_like_c": int(current["FeelsLikeC"]),
        "humidity": int(current["humidity"]),
        "weather_desc": current["weatherDesc"][0]["value"],
        "wind_speed_kmph": int(current["windspeedKmph"]),
        "max_temp_c": int(weather["maxtempC"]),
        "min_temp_c": int(weather["mintempC"]),
    }

    context["ti"].xcom_push(key="transformed_data", value=json.dumps(transformed))
    print(f"Transformed data: {transformed}")


def load(**context):
    raw = context["ti"].xcom_pull(key="transformed_data", task_ids="transform")
    data = json.loads(raw)

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT,
            timestamp TEXT,
            temp_c INTEGER,
            feels_like_c INTEGER,
            humidity INTEGER,
            weather_desc TEXT,
            wind_speed_kmph INTEGER,
            max_temp_c INTEGER,
            min_temp_c INTEGER
        )
    """)

    cursor.execute("""
        INSERT INTO weather (
            city, timestamp, temp_c, feels_like_c, humidity,
            weather_desc, wind_speed_kmph, max_temp_c, min_temp_c
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data["city"], data["timestamp"], data["temp_c"],
        data["feels_like_c"], data["humidity"], data["weather_desc"],
        data["wind_speed_kmph"], data["max_temp_c"], data["min_temp_c"]
    ))

    conn.commit()
    conn.close()
    print(f"Loaded weather data into {DB_PATH}")


default_args = {
    "owner": "sidd",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="weather_etl",
    default_args=default_args,
    description="Fetches weather data for Munich and loads into SQLite",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "weather"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    extract_task >> transform_task >> load_task