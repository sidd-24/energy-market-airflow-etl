# 🌤 Automated Weather ETL Pipeline

An automated ETL pipeline that fetches hourly weather data for Munich, transforms it, and loads it into a SQLite database. Fully orchestrated with Apache Airflow and containerized with Docker.

## Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow 2.10 |
| Containerization | Docker + Docker Compose |
| Data Source | wttr.in (free, no API key) |
| Storage | SQLite |
| Language | Python |

## Pipeline

```
Extract                  Transform                Load
-------                  ---------                ----
Fetch weather data   →   Clean and structure  →   Insert into
from wttr.in API         the JSON response        SQLite database
(Munich)                 into a flat record
```

The pipeline runs on an **hourly schedule** and stores one record per run with the following fields:

| Field | Description |
|-------|-------------|
| city | City name |
| timestamp | UTC timestamp of the run |
| temp_c | Current temperature in Celsius |
| feels_like_c | Feels like temperature |
| humidity | Humidity percentage |
| weather_desc | Weather description |
| wind_speed_kmph | Wind speed |
| max_temp_c | Day's maximum temperature |
| min_temp_c | Day's minimum temperature |

## Project Structure

```
airflow-etl/
├── dags/
│   ├── __init__.py
│   └── weather_etl.py     # ETL DAG with extract, transform, load tasks
├── data/                  # SQLite database stored here (auto-created)
├── docker-compose.yml     # Airflow + PostgreSQL setup
└── .gitignore
```

## Setup

**1. Clone the repo**
```bash
git clone https://github.com/sidd-24/airflow-etl
cd airflow-etl
```

**2. Make sure Docker Desktop is running**

**3. Start Airflow**
```bash
docker compose up -d
```

This will start:
- PostgreSQL (Airflow metadata database)
- Airflow Webserver
- Airflow Scheduler

**4. Open the Airflow UI**

Go to `http://localhost:8080` and login with:
- Username: `admin`
- Password: `admin`

**5. Trigger the DAG**

Find `weather_etl` in the DAGs list, toggle it on, and click the play button to trigger a manual run.

## How it works

The DAG has 3 tasks connected in sequence:

```
extract >> transform >> load
```

- **extract** — calls the wttr.in API and pushes raw JSON to XCom
- **transform** — pulls from XCom, parses the response, and structures it into a flat record
- **load** — creates the SQLite table if it doesn't exist and inserts the record

Each task communicates via Airflow's XCom mechanism so no intermediate files are needed.

## Stopping Airflow

```bash
docker compose down
```