#!/usr/bin/env python3
"""
Update wildfire risk forecast for dashboard.

Fetches fresh hourly weather from Open-Meteo API (free, no key required),
applies saved CatBoost models, writes:
  docs/data/forecast_30_days.json
  docs/data/hourly_forecast_168h.json
  docs/data/metrics.json

Designed to run daily via GitHub Actions.
"""

from __future__ import annotations

import json
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import requests

ROOT = Path(__file__).resolve().parent.parent

# ── City definitions ───────────────────────────────────────────────────────
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

# Static geographic features per city
# Approximate values derived from Azerbaijan topography and land-use data.
CITY_STATIC: dict[str, dict[str, float]] = {
    "Baku":        {"NDBI": 0.10, "NDVI": 0.12, "EVI": 0.10, "Elevation": 27.0,  "Slope": 2.0,  "Trees_pct": 5.0,  "Urban_pct": 35.0},
    "Ganja":       {"NDBI": 0.05, "NDVI": 0.25, "EVI": 0.20, "Elevation": 414.0, "Slope": 5.0,  "Trees_pct": 15.0, "Urban_pct": 20.0},
    "Mingachevir": {"NDBI": 0.04, "NDVI": 0.28, "EVI": 0.22, "Elevation": 56.0,  "Slope": 3.0,  "Trees_pct": 20.0, "Urban_pct": 15.0},
    "Shirvan":     {"NDBI": 0.04, "NDVI": 0.20, "EVI": 0.16, "Elevation": 65.0,  "Slope": 3.0,  "Trees_pct": 10.0, "Urban_pct": 15.0},
    "Lankaran":    {"NDBI": 0.02, "NDVI": 0.45, "EVI": 0.38, "Elevation": 15.0,  "Slope": 2.0,  "Trees_pct": 45.0, "Urban_pct": 10.0},
    "Shaki":       {"NDBI": 0.02, "NDVI": 0.55, "EVI": 0.45, "Elevation": 706.0, "Slope": 12.0, "Trees_pct": 55.0, "Urban_pct": 8.0},
    "Nakhchivan":  {"NDBI": 0.03, "NDVI": 0.10, "EVI": 0.08, "Elevation": 870.0, "Slope": 8.0,  "Trees_pct": 5.0,  "Urban_pct": 18.0},
    "Yevlakh":     {"NDBI": 0.03, "NDVI": 0.22, "EVI": 0.18, "Elevation": 56.0,  "Slope": 2.0,  "Trees_pct": 12.0, "Urban_pct": 12.0},
    "Quba":        {"NDBI": 0.02, "NDVI": 0.50, "EVI": 0.42, "Elevation": 600.0, "Slope": 10.0, "Trees_pct": 50.0, "Urban_pct": 8.0},
    "Khachmaz":    {"NDBI": 0.03, "NDVI": 0.30, "EVI": 0.25, "Elevation": 74.0,  "Slope": 3.0,  "Trees_pct": 22.0, "Urban_pct": 12.0},
    "Gabala":      {"NDBI": 0.02, "NDVI": 0.52, "EVI": 0.44, "Elevation": 624.0, "Slope": 9.0,  "Trees_pct": 52.0, "Urban_pct": 7.0},
    "Shamakhi":    {"NDBI": 0.03, "NDVI": 0.30, "EVI": 0.25, "Elevation": 850.0, "Slope": 8.0,  "Trees_pct": 20.0, "Urban_pct": 12.0},
    "Jalilabad":   {"NDBI": 0.03, "NDVI": 0.28, "EVI": 0.23, "Elevation": 46.0,  "Slope": 2.0,  "Trees_pct": 18.0, "Urban_pct": 12.0},
    "Barda":       {"NDBI": 0.04, "NDVI": 0.22, "EVI": 0.18, "Elevation": 133.0, "Slope": 3.0,  "Trees_pct": 10.0, "Urban_pct": 15.0},
    "Zaqatala":    {"NDBI": 0.01, "NDVI": 0.60, "EVI": 0.52, "Elevation": 455.0, "Slope": 14.0, "Trees_pct": 65.0, "Urban_pct": 5.0},
    "Shabran":     {"NDBI": 0.03, "NDVI": 0.28, "EVI": 0.23, "Elevation": 35.0,  "Slope": 3.0,  "Trees_pct": 20.0, "Urban_pct": 12.0},
}

RISK_COLORS = {"Low": "#3FA773", "Moderate": "#D8A31D", "High": "#D96C3B", "Extreme": "#B73333"}

HOURLY_VARS = [
    "temperature_2m", "relative_humidity_2m", "precipitation",
    "wind_speed_10m", "wind_direction_10m", "surface_pressure",
    "shortwave_radiation", "soil_temperature_0_to_7cm", "soil_moisture_0_to_7cm",
]


# ── Helpers ────────────────────────────────────────────────────────────────
def risk_level(prob: float) -> str:
    if prob >= 0.60: return "Extreme"
    if prob >= 0.35: return "High"
    if prob >= 0.15: return "Moderate"
    return "Low"


def confidence_score(prob: float) -> float:
    return float(np.clip(0.55 + abs(prob - 0.5) * 0.8, 0.55, 0.95))


# ── Weather fetch ──────────────────────────────────────────────────────────
def fetch_weather(city: str, lat: float, lon: float,
                  past_days: int = 60, forecast_days: int = 16) -> pd.DataFrame:
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(HOURLY_VARS),
        "wind_speed_unit": "kmh",
        "past_days": past_days,
        "forecast_days": forecast_days,
        "timezone": "auto",
    }
    for attempt in range(5):
        try:
            r = requests.get(url, params=params, timeout=(10, 60))
            r.raise_for_status()
            h = r.json()["hourly"]
            df = pd.DataFrame({
                "Timestamp":           pd.to_datetime(h["time"]),
                "Temperature_C":       h["temperature_2m"],
                "Humidity_percent":    h["relative_humidity_2m"],
                "Rain_mm":             h["precipitation"],
                "Wind_Speed_kmh":      h["wind_speed_10m"],
                "Wind_Dir_deg":        h["wind_direction_10m"],
                "Pressure_hPa":        h["surface_pressure"],
                "Solar_Radiation_Wm2": h["shortwave_radiation"],
                "Soil_Temp_C":         h["soil_temperature_0_to_7cm"],
                "Soil_Moisture":       h["soil_moisture_0_to_7cm"],
            })
            df["City"] = city
            return df
        except Exception as e:
            if attempt < 4:
                time.sleep(2 ** attempt)
            else:
                raise RuntimeError(f"Open-Meteo failed for {city}: {e}")


# ── Daily aggregation ──────────────────────────────────────────────────────
def aggregate_to_daily(df_h: pd.DataFrame) -> pd.DataFrame:
    df = df_h.copy()
    df["Date"] = df["Timestamp"].dt.normalize()
    df["Hour"] = df["Timestamp"].dt.hour

    base = df.groupby(["City", "Date"]).agg(
        Temperature_C_mean      =("Temperature_C",       "mean"),
        Temperature_C_max       =("Temperature_C",       "max"),
        Temperature_C_min       =("Temperature_C",       "min"),
        Humidity_percent_mean   =("Humidity_percent",    "mean"),
        Humidity_percent_max    =("Humidity_percent",    "max"),
        Humidity_percent_min    =("Humidity_percent",    "min"),
        Rain_mm_sum             =("Rain_mm",             "sum"),
        Wind_Speed_kmh_mean     =("Wind_Speed_kmh",      "mean"),
        Wind_Speed_kmh_max      =("Wind_Speed_kmh",      "max"),
        Wind_Speed_kmh_min      =("Wind_Speed_kmh",      "min"),
        Wind_Dir_deg_mean       =("Wind_Dir_deg",        "mean"),
        Pressure_hPa_mean       =("Pressure_hPa",        "mean"),
        Solar_Radiation_Wm2_mean=("Solar_Radiation_Wm2", "mean"),
        Soil_Temp_C_max         =("Soil_Temp_C",         "max"),
        Soil_Temp_C_mean        =("Soil_Temp_C",         "mean"),
        Soil_Moisture_max       =("Soil_Moisture",       "max"),
        Soil_Moisture_mean      =("Soil_Moisture",       "mean"),
        hours_temp_above_30     =("Temperature_C",       lambda x: (x > 30).sum()),
        hours_humidity_below_30 =("Humidity_percent",    lambda x: (x < 30).sum()),
        hours_wind_above_20     =("Wind_Speed_kmh",      lambda x: (x > 20).sum()),
        Humidity_percent_diurnal_range=("Humidity_percent", lambda x: x.max() - x.min()),
    ).reset_index()

    base["temp_range"] = base["Temperature_C_max"] - base["Temperature_C_min"]

    # Night min temperature (hours 0-5)
    night = df[df["Hour"] <= 5].groupby(["City", "Date"])["Temperature_C"].min().reset_index()
    night.columns = ["City", "Date", "temp_night_min"]
    base = base.merge(night, on=["City", "Date"], how="left")
    base["temp_night_min"] = base["temp_night_min"].fillna(base["Temperature_C_min"])

    # Solar peak hour
    def _peak_hour(sub):
        if sub["Solar_Radiation_Wm2"].max() > 0:
            return int(sub.loc[sub["Solar_Radiation_Wm2"].idxmax(), "Hour"])
        return 12
    peak = df.groupby(["City", "Date"]).apply(_peak_hour).reset_index()
    peak.columns = ["City", "Date", "solar_peak_hour"]
    base = base.merge(peak, on=["City", "Date"], how="left")
    base["solar_peak_hour"] = base["solar_peak_hour"].fillna(12)

    return base


# ── Daily feature engineering ──────────────────────────────────────────────
def _vpd(T, H):
    es = 0.6108 * np.exp((17.27 * T) / (T + 237.3))
    return (es - es * (H / 100)).clip(lower=0)


def _dew_point(T, H):
    a, b = 17.27, 237.3
    alpha = (a * T) / (b + T) + np.log((H.clip(lower=1) / 100))
    return (b * alpha) / (a - alpha)


def build_daily_features(df_all: pd.DataFrame) -> pd.DataFrame:
    parts = []
    for city, grp in df_all.groupby("City"):
        g = grp.copy().sort_values("Date").reset_index(drop=True)
        dt = g["Date"]

        # Calendar
        g["Month"]      = dt.dt.month
        g["DayOfYear"]  = dt.dt.dayofyear
        g["DayOfWeek"]  = dt.dt.dayofweek
        g["Year"]       = dt.dt.year
        g["WeekOfYear"] = dt.dt.isocalendar().week.astype(int)
        g["Month_sin"]  = np.sin(2 * np.pi * g["Month"] / 12)
        g["Month_cos"]  = np.cos(2 * np.pi * g["Month"] / 12)
        g["DoY_sin"]    = np.sin(2 * np.pi * g["DayOfYear"] / 365)
        g["DoY_cos"]    = np.cos(2 * np.pi * g["DayOfYear"] / 365)
        g["DoW_sin"]    = np.sin(2 * np.pi * g["DayOfWeek"] / 7)
        g["DoW_cos"]    = np.cos(2 * np.pi * g["DayOfWeek"] / 7)
        g["is_summer"]     = g["Month"].isin([6, 7, 8]).astype(int)
        g["is_winter"]     = g["Month"].isin([12, 1, 2]).astype(int)
        g["is_fire_season"] = g["Month"].isin([5, 6, 7, 8, 9]).astype(int)

        T = g["Temperature_C_mean"]
        H = g["Humidity_percent_mean"]
        W = g["Wind_Speed_kmh_mean"]
        R = g["Rain_mm_sum"]

        # FWI proxies
        g["FFMC_proxy"] = (100 - (H * 0.5 + R.clip(0, 10) * 3 - T.clip(0, 40) * 0.5)).clip(0, 100)
        g["DMC_proxy"]  = (T.clip(0) * 0.3 - R * 0.8 + (100 - H) * 0.1).rolling(14, min_periods=1).mean().clip(0)
        g["DC_proxy"]   = (T.clip(0) * 0.2 - R * 0.5).rolling(30, min_periods=1).sum().clip(0)
        g["ISI_proxy"]  = (g["FFMC_proxy"] / 100) * (W * 0.3)
        g["BUI_proxy"]  = (g["DMC_proxy"] + g["DC_proxy"]) / 2
        g["FWI_proxy"]  = (g["ISI_proxy"] * g["BUI_proxy"] / 50).clip(0)

        # Dry days streak
        is_dry = (R < 0.1).astype(int)
        groups = is_dry.ne(is_dry.shift()).cumsum()
        g["dry_days_streak"] = is_dry.groupby(groups).cumsum()

        # Lag features
        lag_map = {
            "Temperature_C_max":        [2],
            "Humidity_percent_mean":    [1, 3, 5, 7, 14, 30],
            "Humidity_percent_min":     [1, 2, 3, 5, 7, 14, 30],
            "Wind_Speed_kmh_mean":      [1, 2, 3, 5, 7, 14, 30],
            "Wind_Speed_kmh_max":       [1, 2, 3, 5, 7, 14, 30],
            "Rain_mm_sum":              [1, 2, 3, 5, 7, 14, 30],
            "Solar_Radiation_Wm2_mean": [1, 3, 5, 7, 14, 30],
            "Soil_Temp_C_mean":         [14, 30],
            "Soil_Moisture_mean":       [2, 5, 14, 30],
            "FWI_proxy":                [1, 3, 5, 7, 14, 30],
        }
        for var, lags in lag_map.items():
            if var in g.columns:
                for lag in lags:
                    g[f"{var}_lag{lag}"] = g[var].shift(lag)

        # Rolling features
        roll_map = {
            "Temperature_C_mean":       [3, 7, 14, 30],
            "Temperature_C_max":        [3, 7, 14, 30],
            "Humidity_percent_mean":    [3, 7, 14, 30],
            "Humidity_percent_min":     [3, 7, 14, 30],
            "Wind_Speed_kmh_mean":      [3, 7, 14, 30],
            "Wind_Speed_kmh_max":       [3, 7, 14, 30],
            "Rain_mm_sum":              [3, 7, 14, 30],
            "Solar_Radiation_Wm2_mean": [3, 7, 14, 30],
            "Soil_Temp_C_mean":         [3, 7, 14, 30],
            "Soil_Moisture_mean":       [3, 7, 14, 30],
            "FWI_proxy":                [3, 7, 14, 30],
        }
        for var, windows in roll_map.items():
            if var in g.columns:
                shifted = g[var].shift(1)
                for w in windows:
                    rolled = shifted.rolling(w, min_periods=1)
                    g[f"{var}_roll{w}_mean"] = rolled.mean()
                    g[f"{var}_roll{w}_std"]  = rolled.std()

        # Derived weather features
        g["VPD_kPa"]    = _vpd(T, H)
        g["Dew_Point_C"] = _dew_point(T, H)

        # Extreme flags
        g["heatwave_flag"]    = (T > T.quantile(0.95)).astype(int)
        g["low_humidity_flag"] = (H < H.quantile(0.10)).astype(int)
        g["high_wind_flag"]   = (W > W.quantile(0.90)).astype(int)
        g["dry_spell_flag"]   = (g["dry_days_streak"] >= 7).astype(int)

        # Interaction features
        g["temp_x_low_hum"]  = T * (100 - H) / 100
        g["temp_x_wind"]     = T * W / 100
        g["dry_days_x_wind"] = g["dry_days_streak"] * W / 100
        g["hot_dry_windy"]   = g["heatwave_flag"] * g["low_humidity_flag"] * g["high_wind_flag"]

        # Rainfall deficit
        rain_roll30 = R.shift(1).rolling(30, min_periods=1).sum()
        monthly_means = g.groupby("Month")["Rain_mm_sum"].transform("mean")
        g["Rain_roll30_sum"]   = rain_roll30
        g["Rainfall_Deficit"]  = monthly_means * 30 - rain_roll30.fillna(0)

        # Anomaly features
        for var in ["Temperature_C_mean", "Wind_Speed_kmh_mean", "Solar_Radiation_Wm2_mean"]:
            if var in g.columns:
                monthly_mean = g.groupby("Month")[var].transform("mean")
                g[f"{var}_anomaly"] = g[var] - monthly_mean

        # Prophet approximations: use actual forecast values, residuals = 0
        g["prophet_Temperature_C_mean_pred"]     = T
        g["prophet_Temperature_C_mean_resid"]    = 0.0
        g["prophet_Humidity_percent_mean_pred"]  = H
        g["prophet_Humidity_percent_mean_resid"] = 0.0
        g["prophet_Rain_mm_sum_pred"]            = R
        g["prophet_Wind_Speed_kmh_mean_pred"]    = W

        # Static geographic features
        for k, v in CITY_STATIC[city].items():
            g[k] = v
        g["Latitude"]  = CITIES[city][0]
        g["Longitude"] = CITIES[city][1]

        # Vegetation interactions
        g["NDVI_x_drought"]    = g["NDVI"] * g.get("Rainfall_Deficit", 0) / 100
        g["forest_x_dry_days"] = g["Trees_pct"] * g["dry_days_streak"] / 100
        g["NDVI_x_VPD"]        = g["NDVI"] * g["VPD_kPa"]

        # Historical fire features (no real-time fire data in this pipeline)
        for col in ["fire_count_7d", "fire_count_14d", "fire_count_30d", "fire_count_90d"]:
            g[col] = 0.0
        g["days_since_last_fire"]  = 365.0
        g["city_month_fire_rate"]  = 0.02
        g["city_fire_rate"]        = 0.02

        parts.append(g)

    return pd.concat(parts, ignore_index=True)


# ── Hourly feature engineering ─────────────────────────────────────────────
def build_hourly_features(df_h: pd.DataFrame) -> pd.DataFrame:
    parts = []
    for city, grp in df_h.groupby("City"):
        h = grp.copy().sort_values("Timestamp").reset_index(drop=True)
        dt = h["Timestamp"]

        h["Hour"]        = dt.dt.hour
        h["Month"]       = dt.dt.month
        h["DayOfWeek"]   = dt.dt.dayofweek
        h["DayOfYear"]   = dt.dt.dayofyear
        h["Month_sin"]   = np.sin(2 * np.pi * h["Month"] / 12)
        h["Month_cos"]   = np.cos(2 * np.pi * h["Month"] / 12)
        h["DoY_sin"]     = np.sin(2 * np.pi * h["DayOfYear"] / 365)
        h["DoY_cos"]     = np.cos(2 * np.pi * h["DayOfYear"] / 365)
        h["DoW_sin"]     = np.sin(2 * np.pi * h["DayOfWeek"] / 7)
        h["DoW_cos"]     = np.cos(2 * np.pi * h["DayOfWeek"] / 7)
        h["Hour_sin"]    = np.sin(2 * np.pi * h["Hour"] / 24)
        h["Hour_cos"]    = np.cos(2 * np.pi * h["Hour"] / 24)
        h["is_daytime"]  = h["Hour"].between(6, 20).astype(int)
        h["is_fire_season"] = h["Month"].isin([5, 6, 7, 8, 9]).astype(int)

        lag_map_h = {
            "Temperature_C":       [6, 12],
            "Humidity_percent":    [1, 3, 6, 12, 24],
            "Wind_Speed_kmh":      [1, 6, 12, 24],
            "Rain_mm":             [1, 3, 6, 12, 24],
            "Solar_Radiation_Wm2": [1, 6, 12, 24],
        }
        for var, lags in lag_map_h.items():
            if var in h.columns:
                for lag in lags:
                    h[f"{var}_lag{lag}h"] = h[var].shift(lag)

        roll_map_h = {
            "Temperature_C":       [6, 12, 24],
            "Humidity_percent":    [6, 12, 24],
            "Wind_Speed_kmh":      [6, 12, 24],
            "Rain_mm":             [6, 12, 24],
            "Solar_Radiation_Wm2": [6, 12, 24],
        }
        for var, windows in roll_map_h.items():
            if var in h.columns:
                shifted = h[var].shift(1)
                for w in windows:
                    rolled = shifted.rolling(w, min_periods=1)
                    h[f"{var}_roll{w}h_mean"] = rolled.mean()
                    h[f"{var}_roll{w}h_std"]  = rolled.std()

        for k, v in CITY_STATIC[city].items():
            h[k] = v

        parts.append(h)

    return pd.concat(parts, ignore_index=True)


# ── Main ───────────────────────────────────────────────────────────────────
def main() -> None:
    try:
        from catboost import CatBoostClassifier
    except ImportError:
        print("ERROR: catboost not installed. Run: pip install catboost")
        sys.exit(1)

    today = date.today()
    print(f"[update_forecast] date={today}")

    models_dir = ROOT / "models" / "wildfire"
    docs_data  = ROOT / "docs" / "data"
    docs_data.mkdir(parents=True, exist_ok=True)

    # Load models and feature columns
    model_d = CatBoostClassifier()
    model_d.load_model(str(models_dir / "best_fire_model.json"))
    feat_d   = json.loads((models_dir / "feature_columns.json").read_text())
    manifest = json.loads((models_dir / "model_manifest.json").read_text())

    model_h = CatBoostClassifier()
    model_h.load_model(str(models_dir / "best_fire_model_hourly.json"))
    feat_h    = json.loads((models_dir / "feature_columns_hourly.json").read_text())
    manifest_h = json.loads((models_dir / "model_manifest_hourly.json").read_text())

    threshold_d = float(manifest.get("optimal_threshold", 0.44))
    threshold_h = float(manifest_h.get("optimal_threshold", 0.58))

    # Fetch weather (past 60 days + next 16 days)
    print("Fetching weather from Open-Meteo...")
    all_hourly = []
    for city, (lat, lon) in CITIES.items():
        print(f"  {city}", end=" ... ", flush=True)
        df_c = fetch_weather(city, lat, lon, past_days=60, forecast_days=16)
        all_hourly.append(df_c)
        print("ok")
        time.sleep(1)

    df_h_all = pd.concat(all_hourly, ignore_index=True)

    # Daily path
    print("Building daily features...")
    df_daily = aggregate_to_daily(df_h_all)
    df_daily_feat = build_daily_features(df_daily)

    X_d = df_daily_feat[feat_d].fillna(0).astype(float).values
    proba_d = model_d.predict_proba(X_d)[:, 1]
    df_daily_feat["probability"] = proba_d

    today_ts = pd.Timestamp(today)
    forecast_daily = df_daily_feat[df_daily_feat["Date"] >= today_ts].copy()
    forecast_daily = forecast_daily.sort_values(["Date", "City"]).reset_index(drop=True)

    daily_records = []
    for _, row in forecast_daily.iterrows():
        p = float(row["probability"])
        daily_records.append({
            "date":           row["Date"].strftime("%Y-%m-%d"),
            "region":         row["City"],
            "risk_level":     risk_level(p),
            "probability":    round(p, 10),
            "confidence":     round(confidence_score(p), 10),
            "risk_score":     round(p * 100, 1),
            "predicted_fire": int(p >= threshold_d),
            "temperature":    round(float(row.get("Temperature_C_mean", 0) or 0), 1),
            "wind":           round(float(row.get("Wind_Speed_kmh_mean", 0) or 0), 1),
            "humidity":       round(float(row.get("Humidity_percent_mean", 0) or 0), 1),
            "rain":           round(float(row.get("Rain_mm_sum", 0) or 0), 2),
            "Temperature_C_mean":        round(float(row.get("Temperature_C_mean", 0) or 0), 4),
            "Humidity_percent_mean":     round(float(row.get("Humidity_percent_mean", 0) or 0), 4),
            "Rain_mm_sum":               round(float(row.get("Rain_mm_sum", 0) or 0), 4),
            "Wind_Speed_kmh_mean":       round(float(row.get("Wind_Speed_kmh_mean", 0) or 0), 4),
            "Pressure_hPa_mean":         round(float(row.get("Pressure_hPa_mean", 0) or 0), 4),
            "Solar_Radiation_Wm2_mean":  round(float(row.get("Solar_Radiation_Wm2_mean", 0) or 0), 4),
            "Soil_Temp_C_mean":          round(float(row.get("Soil_Temp_C_mean", 0) or 0), 4),
            "Soil_Moisture_mean":        round(float(row.get("Soil_Moisture_mean", 0) or 0), 4),
            "Latitude":       CITIES[row["City"]][0],
            "Longitude":      CITIES[row["City"]][1],
        })

    # Hourly path
    print("Building hourly features...")
    df_h_feat = build_hourly_features(df_h_all)

    end_168h = today_ts + pd.Timedelta(hours=168)
    df_h_future = df_h_feat[
        (df_h_feat["Timestamp"] >= today_ts) &
        (df_h_feat["Timestamp"] < end_168h)
    ].copy()

    X_h = df_h_future[feat_h].fillna(0).astype(float).values
    proba_h = model_h.predict_proba(X_h)[:, 1]
    df_h_future["probability"] = proba_h

    # Apply daily ceiling to hourly probabilities
    daily_prob_lookup = forecast_daily.set_index(["City", "Date"])["probability"].to_dict()
    def _cap(row):
        day_key = (row["City"], pd.Timestamp(row["Timestamp"]).normalize())
        daily_p = daily_prob_lookup.get(day_key, None)
        if daily_p is not None:
            ceiling = max(float(daily_p) * 2.5, 0.15)
            return min(row["probability"], ceiling)
        return row["probability"]
    df_h_future["probability"] = df_h_future.apply(_cap, axis=1)

    hourly_records = []
    for _, row in df_h_future.sort_values(["Timestamp", "City"]).iterrows():
        p = float(row["probability"])
        hourly_records.append({
            "timestamp":      pd.Timestamp(row["Timestamp"]).strftime("%Y-%m-%dT%H:%M"),
            "region":         row["City"],
            "probability":    round(p, 10),
            "risk_level":     risk_level(p),
            "risk_score":     round(p * 100, 1),
            "predicted_fire": int(p >= threshold_h),
            "confidence":     round(confidence_score(p), 10),
            "risk_color":     RISK_COLORS[risk_level(p)],
            "temperature":    round(float(row.get("Temperature_C", 0) or 0), 1),
            "humidity":       round(float(row.get("Humidity_percent", 0) or 0), 1),
            "wind":           round(float(row.get("Wind_Speed_kmh", 0) or 0), 1),
            "solar":          round(float(row.get("Solar_Radiation_Wm2", 0) or 0), 1),
            "Latitude":       CITIES[row["City"]][0],
            "Longitude":      CITIES[row["City"]][1],
        })

    # Metrics
    metrics = {
        "generated_at": pd.Timestamp.utcnow().isoformat() + "Z",
        "prediction_horizon_days":    len(set(r["date"] for r in daily_records)),
        "target": "Daily probability of a NASA FIRMS wildfire detection within the city risk area",
        "selected_model":   manifest.get("model_name", "CatBoost_Optuna"),
        "optimal_threshold": threshold_d,
        "roc_auc":  manifest.get("metrics", {}).get("roc_auc", 0),
        "recall":   manifest.get("metrics", {}).get("recall", 0),
        "hourly_model": {
            "model_name":               manifest_h.get("model_name", "CatBoost_H_Optuna"),
            "prediction_horizon_hours": 168,
            "optimal_threshold":        threshold_h,
        },
    }

    # Write outputs
    (docs_data / "forecast_30_days.json").write_text(
        json.dumps(daily_records, separators=(",", ":")), encoding="utf-8"
    )
    (docs_data / "hourly_forecast_168h.json").write_text(
        json.dumps(hourly_records, separators=(",", ":")), encoding="utf-8"
    )
    (docs_data / "metrics.json").write_text(
        json.dumps(metrics, indent=2), encoding="utf-8"
    )

    end_date = (today + timedelta(days=15)).strftime("%Y-%m-%d")
    print(f"Done — daily: {len(daily_records)} records ({today} → {end_date})")
    print(f"       hourly: {len(hourly_records)} records")
    print(f"       outputs → {docs_data}")


if __name__ == "__main__":
    main()
