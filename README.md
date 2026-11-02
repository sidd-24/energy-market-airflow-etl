# ⚡ German Electricity Market ETL with Apache Airflow

![tests](https://github.com/sidd-24/airflow-etl/actions/workflows/tests.yml/badge.svg)

An Airflow pipeline that loads hourly data on the German electricity market into a database:
- **day-ahead prices** for the DE-LU bidding zone
- **grid load**
- **generation by source** (11 sources)

All of it comes from [SMARD](https://www.smard.de), the market data platform of the Bundesnetzagentur. A second DAG uses the next day's auction prices to tell a factory **when to run an energy-intensive production job**.

Why this matters for manufacturing: on a spot-indexed tariff, the same production step can cost very different amounts depending on the hour. In October 2026 the daily gap between the cheapest and the most expensive 4-hour window averaged **€125/MWh**.

## What it does

| DAG | Schedule | Steps |
|---|---|---|
| `energy_market_etl` | Daily, per Berlin calendar day, with catch-up | Wait for complete data (sensor) → extract → validate → load → daily summary |
| `energy_price_outlook` | 14:00 daily, after the day-ahead auction | Wait for tomorrow's prices → cheapest window for a production job → saving vs the planned start |

**Pipeline design:**
- **Idempotent loads.** A run replaces its day in one transaction, so retries and `airflow dags backfill` never create duplicates. Every run downloads fresh data, so revisions published by SMARD reach the database.
- **Local calendar days.** Days are Europe/Berlin calendar days, so a day has 23, 24 or 25 hours at the clock changes.
- **Waiting instead of loading half a day.** SMARD publishes generation data with about a day's delay. A sensor in `reschedule` mode checks every hour without holding a worker slot.
- **Two kinds of failure:**
  - *Data not ready* (a missing hour) → wait and try again.
  - *Bad data* → stop without retrying, because bad data does not fix itself. Examples: a price outside the European day-ahead limits of −500 to 4,000 €/MWh, negative generation, zero load.
  - Generation vs load outside 50–150% is only logged as a warning, because imports and exports explain normal gaps.
- **Traceable.** Raw SMARD files → day extract → staged rows → database. Tasks pass file paths, not datasets through XCom.

## Results: October 2026

Backfilled with `python run_local.py --start 2026-10-01 --end 2026-10-31`. Daily figures are in [`docs/daily_summary_2026-10.csv`](docs/daily_summary_2026-10.csv).

| | |
|---|---|
| Days / hours loaded | 31 / 745 (25 hours on 25 October) |
| Average day-ahead price | 169.5 €/MWh (daily average from 13.1 to 314.9) |
| Average renewable share of generation | 58% |
| Correlation between renewable share and daily price | **r = −0.92** |
| Average gap between cheapest and priciest 4-hour window | 124.8 €/MWh |

The last week of October shows the effect. A wind lull cut the renewable share to 31–34%, and the daily price rose from 13 €/MWh to 315 €/MWh.

**Load shifting.** Take a 2 MW job that runs 4 hours a day and normally starts at 08:00:
- **Cost at 08:00:** €42,809 for October.
- **Cost at the cheapest window:** €27,759, saving **€15,050 (35%)**. That window usually started at 11:00 or 12:00, when solar output is highest.

This assumes the job can run at any hour and the plant pays day-ahead prices. Grid fees, taxes and staffing constraints are not included.

```
$ python run_local.py --outlook --date 2026-10-29 --job-hours 6 --job-mw 1.5
2026-10-29: run the 6 h / 1.5 MW job at 00:00 (avg 236.15 EUR/MWh, cost 2,125 EUR).
  Saving vs 08:00 start: 534 EUR; vs day average: 708 EUR
```

## Data model (SQLite)

| Table | Content |
|---|---|
| `energy_hourly` | One row per hour: UTC and local time, day-ahead price (€/MWh), load and 11 generation sources (MWh) |
| `daily_summary` | Average, min and max price; peak (Mon–Fri 08–20) and off-peak price; negative-price hours; renewable share; load; cheapest and priciest 4-hour window |
| `price_outlook` | Recommended start, cost and saving per day and job size |

Renewables are biomass, hydro, wind (on- and offshore), solar and other renewables. Pumped storage counts as generation but not as renewable.

## Running it

**With Airflow (Docker):**
```bash
docker compose up -d          # Airflow 2.10.5 + PostgreSQL, UI at http://localhost:8080 (admin / admin)
```
Unpause `energy_market_etl` and it catches up from 1 October 2026. Run `energy_price_outlook` with *Trigger DAG w/ config* to set the job size (`job_hours`, `job_mw`, `planned_start_hour`). The admin login is for local use only.

**Without Airflow:** the same pipeline code, for a quick backfill or check:
```bash
pip install -r requirements.txt
python run_local.py --start 2026-10-01 --end 2026-10-31
python run_local.py --outlook --job-hours 4 --job-mw 2
python -m pytest -q
```

The tests use a fake SMARD server, so they need no network. They cover the clock-change days, validation, idempotent re-runs, backfill downloads and the load-shifting calculation. GitHub Actions also parses both DAGs in a real Airflow 2.10.5 install.

## Project structure

```
├── dags/
│   ├── energy_market_etl.py      # Daily ETL: sensor → extract → transform → load
│   └── energy_price_outlook.py   # Day-ahead load-shifting recommendation
├── energy/                       # Pipeline logic, independent of Airflow
│   ├── smard.py                  # SMARD API client (weekly chunks, retries)
│   ├── transform.py              # Local-day rows, completeness and plausibility checks
│   ├── kpis.py                   # Daily figures, cheapest window, shift saving
│   ├── db.py                     # SQLite schema, day-replacing loads
│   ├── pipeline.py               # extract / transform / load steps
│   └── config.py                 # Series ids, price limits, peak hours
├── run_local.py                  # Run without Airflow
├── docs/daily_summary_2026-10.csv
├── tests/
└── docker-compose.yml
```

## Data source

Bundesnetzagentur | SMARD.de, licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). Since 1 October 2025 the day-ahead market trades 15-minute products. This pipeline uses SMARD's hourly series so that prices line up with the hourly generation data.

## History

This repo started as an hourly weather ETL (wttr.in → SQLite). That version stored the time of each run rather than the time of the weather observation, and it inserted a new row on every run, so the committed database held the same reading twice. Both problems are fixed in this version by the idempotent day loads.
