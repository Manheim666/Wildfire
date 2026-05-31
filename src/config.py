"""
WildFire Prediction — Central Configuration
============================================
All shared constants, paths, city definitions, and column lists.
Import from any notebook:  from src.config import *
"""
import os, sys, subprocess
from pathlib import Path

# ── Project root detection ────────────────────────────────────────────────
def detect_project_root() -> Path:
    # Check WILDFIRE_ROOT first, then ARIAN_ROOT for backward compatibility
    root_env = os.environ.get("WILDFIRE_ROOT") or os.environ.get("ARIAN_ROOT")
    if root_env:
        return Path(root_env).expanduser().resolve()
    if "google.colab" in sys.modules:
        from google.colab import drive
        if not os.path.ismount("/content/drive"):
            drive.mount("/content/drive")
        return Path("/content/drive/MyDrive/WildFire_Data")
    here = Path.cwd().resolve()
    for cand in [here, *here.parents]:
        if (cand / "data").is_dir() and (cand / "notebooks").is_dir():
            return cand
    return here.parent if here.name == "notebooks" else here

ROOT       = detect_project_root()
RAW        = ROOT / "data" / "raw"
LEGACY     = RAW  / "legacy"
FIRMS_DIR  = RAW  / "firms"
PROCESSED  = ROOT / "data" / "processed"
REFERENCE  = ROOT / "data" / "reference"
OUTPUTS    = ROOT / "outputs"
MODELS     = ROOT / "models"
MODELS_W   = MODELS / "weather"
MODELS_F   = MODELS / "wildfire"
REPORTS    = ROOT / "reports"
FIGURES    = REPORTS / "figures"
MAPS       = REPORTS / "maps"
METRICS    = REPORTS / "metrics"
SRC        = ROOT / "src"

ALL_DIRS = [RAW, LEGACY, FIRMS_DIR, PROCESSED, REFERENCE, OUTPUTS,
            MODELS, MODELS_W, MODELS_F, REPORTS, FIGURES, MAPS, METRICS]

def ensure_dirs():
    for p in ALL_DIRS:
        p.mkdir(parents=True, exist_ok=True)

ensure_dirs()

# ── Cities ────────────────────────────────────────────────────────────────
CITIES = {
    "Baku":        (40.4093, 49.8671),
    "Ganja":       (40.6828, 46.3606),
    "Mingachevir": (40.7639, 47.0595),
    "Shirvan":     (39.9317, 48.9299),
    "Lankaran":    (38.7523, 48.8475),
    "Shaki":       (41.1975, 47.1694),
    "Nakhchivan":  (39.2089, 45.4122),
    "Yevlakh":     (40.6183, 47.1500),
    "Quba":        (41.3611, 48.5261),
    "Khachmaz":    (41.4635, 48.8060),
    "Gabala":      (40.9982, 47.8468),
    "Shamakhi":    (40.6303, 48.6414),
    "Jalilabad":   (39.2089, 48.2986),
    "Barda":       (40.3744, 47.1266),
    "Zaqatala":    (41.6296, 46.6433),
    "Shabran":     (41.2156, 48.8544),
}

CITY_LIST = sorted(CITIES.keys())

# ── Time constants ────────────────────────────────────────────────────────
HISTORY_START  = "2012-01-20"
FIRE_BUFFER_KM = 20
FRESHNESS_H    = 24
RANDOM_SEED    = 42
SPLIT_DATE     = "2025-01-01"
HOLDOUT_DAYS   = 30
HOLDOUT_HOURS  = 168

# ── Weather variable mappings ─────────────────────────────────────────────
ERA5_VARS = [
    "temperature_2m", "relative_humidity_2m", "precipitation",
    "wind_speed_10m", "wind_direction_10m", "surface_pressure",
    "shortwave_radiation",
]
ERA5_LAND_VARS = ["soil_temperature_0_to_7cm", "soil_moisture_0_to_7cm"]
ALL_HOURLY_VARS = ERA5_VARS + ERA5_LAND_VARS

NICE_NAMES = {
    "temperature_2m": "Temperature_C",
    "relative_humidity_2m": "Humidity_percent",
    "precipitation": "Rain_mm",
    "wind_speed_10m": "Wind_Speed_kmh",
    "wind_direction_10m": "Wind_Dir_deg",
    "surface_pressure": "Pressure_hPa",
    "shortwave_radiation": "Solar_Radiation_Wm2",
    "soil_temperature_0_to_7cm": "Soil_Temp_C",
    "soil_moisture_0_to_7cm": "Soil_Moisture",
}

# ── Target and drop columns ──────────────────────────────────────────────
TARGET_COL = "Fire_Occurred"
LEAK_COLS  = ["fire_count", "mean_brightness", "max_frp", "Burned_Area_hectares"]
ID_COLS    = ["City", "Date", "Timestamp"]
DROP_COLS  = ID_COLS + [TARGET_COL] + LEAK_COLS + ["Latitude", "Longitude"]

# ── Daily weather targets for NB03 ───────────────────────────────────────
WEATHER_TARGETS_DAILY = [
    "Temperature_C_mean", "Humidity_percent_mean",
    "Rain_mm_sum", "Wind_Speed_kmh_mean",
    "Pressure_hPa_mean", "Solar_Radiation_Wm2_mean",
    "Soil_Temp_C_mean", "Soil_Moisture_mean",
]

WEATHER_TARGETS_HOURLY = [
    "Temperature_C", "Humidity_percent",
    "Wind_Speed_kmh", "Solar_Radiation_Wm2",
]

# ── Key file paths ────────────────────────────────────────────────────────
MASTER_DAILY   = PROCESSED / "master_daily.parquet"
MASTER_HOURLY  = PROCESSED / "master_hourly.parquet"
ENG_DAILY      = PROCESSED / "engineered_daily.parquet"
ENG_HOURLY     = PROCESSED / "engineered_hourly.parquet"
FIRE_DAILY     = PROCESSED / "fires_daily.parquet"
WILDFIRE_DS    = PROCESSED / "wildfire_ml_dataset.parquet"
FORECAST_30D   = OUTPUTS / "weather_forecast_30d.parquet"
FORECAST_168H  = OUTPUTS / "weather_forecast_168h.parquet"
RISK_30D       = OUTPUTS / "wildfire_risk_30d.parquet"
RISK_168H      = OUTPUTS / "wildfire_risk_168h.parquet"

# ── GPU detection ────────────────────────────────────────────────────────
def _detect_gpu() -> bool:
    """Return True if an NVIDIA GPU is available."""
    try:
        r = subprocess.run(
            ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
            capture_output=True, text=True, timeout=5,
        )
        return r.returncode == 0 and len(r.stdout.strip()) > 0
    except Exception:
        return False

GPU_AVAILABLE = _detect_gpu()

# Per-library GPU keyword arguments (empty dict when no GPU)
XGB_GPU_PARAMS = {"device": "cuda"} if GPU_AVAILABLE else {}
CB_GPU_PARAMS  = {"task_type": "GPU"} if GPU_AVAILABLE else {}
LGB_GPU_PARAMS = {"device": "gpu"} if GPU_AVAILABLE else {}
