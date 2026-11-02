"""
Settings for the German electricity market pipeline. All data comes from
SMARD (www.smard.de), the market data platform of the Bundesnetzagentur.
"""
import os
from zoneinfo import ZoneInfo

SMARD_URL = "https://www.smard.de/app/chart_data"
TZ = ZoneInfo("Europe/Berlin")

DB_PATH = os.getenv("ENERGY_DB", "data/energy.db")
RAW_DIR = os.getenv("ENERGY_RAW_DIR", "data/raw")

# name -> (SMARD filter id, region). Values are hourly: EUR/MWh for the price, MWh otherwise.
PRICE = ("day_ahead_price", 4169, "DE-LU")
LOAD = ("load", 410, "DE")
GENERATION = {
    "biomass": 4066,
    "hydro": 1226,
    "wind_offshore": 1225,
    "wind_onshore": 4067,
    "solar": 4068,
    "other_renewable": 1228,
    "lignite": 1223,
    "hard_coal": 4069,
    "natural_gas": 4071,
    "pumped_storage": 4070,
    "other_conventional": 1227,
}
RENEWABLE = {"biomass", "hydro", "wind_offshore", "wind_onshore", "solar", "other_renewable"}

# Harmonised clearing price limits of the European day-ahead market (SDAC).
# A price outside them means bad data, not a market event.
PRICE_LIMITS = (-500.0, 4000.0)

# Total generation vs load outside this ratio is unusual (imports and exports
# explain normal gaps), so it is reported as a warning, not an error
GENERATION_LOAD_RATIO = (0.5, 1.5)

# Peak hours as traded on the German market: Monday to Friday, 08:00 to 20:00 local time
PEAK_HOURS = range(8, 20)
