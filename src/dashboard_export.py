"""
WildFire Prediction — Unified Dashboard Export
===============================================
Single source of truth for all dashboard JSON/CSV exports.
Ensures daily and hourly outputs use consistent column names,
risk level thresholds, and daily-anchored hourly risk capping.
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Dict, Optional

import numpy as np
import pandas as pd

from src.config import CITIES, OUTPUTS

# ── Risk thresholds (single place — change here to propagate everywhere) ──
RISK_THRESHOLDS = {"Low": 0.0, "Moderate": 0.15, "High": 0.35, "Extreme": 0.60}
RISK_COLORS = {
    "Low":      "#3FA773",
    "Moderate": "#D8A31D",
    "High":     "#D96C3B",
    "Extreme":  "#B73333",
}

# Hourly ceiling multiplier: hourly prob capped at daily_prob * HOURLY_CEIL_MULT
# This prevents hourly model from showing "Extreme" when daily says "Low".
HOURLY_CEIL_MULT = 2.5
HOURLY_CEIL_FLOOR = 0.15  # minimum ceiling (even if daily is 0)


def risk_level(prob: float) -> str:
    if prob >= RISK_THRESHOLDS["Extreme"]:
        return "Extreme"
    if prob >= RISK_THRESHOLDS["High"]:
        return "High"
    if prob >= RISK_THRESHOLDS["Moderate"]:
        return "Moderate"
    return "Low"


def risk_level_series(probs: pd.Series) -> pd.Series:
    return pd.cut(
        probs.clip(0, 1),
        bins=[-0.001, RISK_THRESHOLDS["Moderate"],
              RISK_THRESHOLDS["High"], RISK_THRESHOLDS["Extreme"], 1.001],
        labels=["Low", "Moderate", "High", "Extreme"],
    ).astype(str)


def confidence(prob: float) -> float:
    return float(np.clip(0.55 + abs(prob - 0.5) * 0.8, 0.55, 0.95))


def _city_coords() -> Dict[str, tuple]:
    return {c: (lat, lon) for c, (lat, lon) in CITIES.items()}


def apply_daily_anchor_to_hourly(
    h_proba: np.ndarray,
    hourly_cities: pd.Series,
    hourly_dates: pd.Series,
    daily_risk_df: pd.DataFrame,
    daily_prob_col: str = "probability",
    ceil_mult: float = HOURLY_CEIL_MULT,
    ceil_floor: float = HOURLY_CEIL_FLOOR,
) -> np.ndarray:
    """Cap hourly probabilities by daily risk to enforce consistency.

    For each city-date in the hourly forecast, the hourly probability is capped
    at max(daily_prob * ceil_mult, ceil_floor).  This prevents the hourly model
    from producing "Extreme" when the daily model says "Low" or "Moderate".

    Parameters
    ----------
    h_proba : raw hourly probabilities (N,)
    hourly_cities : city names aligned with h_proba
    hourly_dates : date (date-normalized) aligned with h_proba
    daily_risk_df : DataFrame with City, Date, <daily_prob_col>
    """
    daily_risk_df = daily_risk_df.copy()
    daily_risk_df["Date"] = pd.to_datetime(daily_risk_df["Date"]).dt.normalize()
    daily_dict = {
        (row["City"], row["Date"]): row[daily_prob_col]
        for _, row in daily_risk_df.iterrows()
        if daily_prob_col in daily_risk_df.columns
    }

    h_proba_capped = h_proba.copy().astype(float)
    hourly_dates_norm = pd.to_datetime(hourly_dates).dt.normalize()

    for i in range(len(h_proba_capped)):
        city = hourly_cities.iloc[i]
        day = hourly_dates_norm.iloc[i]
        daily_p = daily_dict.get((city, day))
        if daily_p is not None:
            ceiling = max(float(daily_p) * ceil_mult, ceil_floor)
            h_proba_capped[i] = min(h_proba_capped[i], ceiling)

    return h_proba_capped


def export_daily_forecast(
    risk_df: pd.DataFrame,
    daily_prob_col: str = "fire_probability",
    threshold: float = 0.15,
    outputs_dir: Optional[Path] = None,
) -> pd.DataFrame:
    """Build and write daily forecast JSON/CSV.

    Standardises column names so the dashboard always receives:
      date, region, probability, risk_level, risk_score, predicted_fire,
      confidence, risk_color, temperature, humidity, wind, rain, …
    """
    if outputs_dir is None:
        outputs_dir = OUTPUTS

    out = risk_df.copy()
    out["Date"] = pd.to_datetime(out["Date"])

    # Unified probability column
    if daily_prob_col in out.columns and "probability" not in out.columns:
        out["probability"] = out[daily_prob_col]
    elif "probability" not in out.columns:
        raise ValueError(f"No probability column found in daily risk DataFrame")

    out["probability"] = out["probability"].clip(0, 1)
    out["risk_level"] = out["probability"].map(risk_level)
    out["risk_score"] = (out["probability"] * 100).round(1)
    out["predicted_fire"] = (out["probability"] >= threshold).astype(int)
    out["confidence"] = out["probability"].map(confidence)
    out["risk_color"] = out["risk_level"].map(RISK_COLORS)
    out["date"] = out["Date"].dt.strftime("%Y-%m-%d")
    out["region"] = out["City"]

    coords = _city_coords()
    out["Latitude"] = out["City"].map(lambda c: coords.get(c, (np.nan, np.nan))[0])
    out["Longitude"] = out["City"].map(lambda c: coords.get(c, (np.nan, np.nan))[1])

    col_map = {
        "Temperature_C_mean": "temperature",
        "Humidity_percent_mean": "humidity",
        "Wind_Speed_kmh_mean": "wind",
        "Rain_mm_sum": "rain",
    }
    for src, dst in col_map.items():
        if src in out.columns and dst not in out.columns:
            out[dst] = out[src].round(1)

    public_cols = [
        "date", "region", "probability", "risk_level", "risk_score",
        "predicted_fire", "confidence", "risk_color",
        "temperature", "humidity", "wind", "rain",
        "Temperature_C_mean", "Humidity_percent_mean", "Rain_mm_sum",
        "Wind_Speed_kmh_mean", "Pressure_hPa_mean", "Solar_Radiation_Wm2_mean",
        "Soil_Temp_C_mean", "Soil_Moisture_mean",
        "Latitude", "Longitude",
    ]
    public_cols = [c for c in public_cols if c in out.columns]
    public = out[public_cols].copy()

    # Replace NaN with null-safe defaults before JSON export
    public = public.where(public.notna(), other=None)

    csv_path = outputs_dir / "forecast_30_days.csv"
    json_path = outputs_dir / "forecast_30_days.json"
    public.to_csv(csv_path, index=False)
    json_path.write_text(public.to_json(orient="records", indent=2), encoding="utf-8")

    # Map points (latest per city)
    latest = public.sort_values("date").groupby("region", as_index=False).tail(1)
    (outputs_dir / "map_points.json").write_text(
        json.dumps(latest.to_dict(orient="records"), indent=2), encoding="utf-8"
    )

    _copy_to_destinations(outputs_dir, ["forecast_30_days.csv", "forecast_30_days.json",
                                         "map_points.json"])
    return public


def export_hourly_forecast(
    hourly_df: pd.DataFrame,
    daily_risk_df: Optional[pd.DataFrame] = None,
    h_proba_col: str = "fire_probability",
    h_threshold: float = 0.15,
    outputs_dir: Optional[Path] = None,
) -> pd.DataFrame:
    """Build and write hourly forecast JSON with daily-anchored risk capping.

    hourly_df must have: City, Timestamp, <weather cols>, <h_proba_col>.
    daily_risk_df (optional): used for ceiling. If None, no ceiling applied.
    """
    if outputs_dir is None:
        outputs_dir = OUTPUTS

    h_out = hourly_df.copy()
    h_out["Timestamp"] = pd.to_datetime(h_out["Timestamp"])
    if "Date" not in h_out.columns:
        h_out["Date"] = h_out["Timestamp"].dt.normalize()

    h_proba = h_out[h_proba_col].clip(0, 1).values.copy()

    # Apply daily anchor ceiling
    if daily_risk_df is not None and not daily_risk_df.empty:
        daily_prob_col = ("probability" if "probability" in daily_risk_df.columns
                          else "fire_probability")
        h_proba = apply_daily_anchor_to_hourly(
            h_proba,
            h_out["City"].reset_index(drop=True),
            h_out["Date"].reset_index(drop=True),
            daily_risk_df,
            daily_prob_col=daily_prob_col,
        )

    h_out["probability"] = np.clip(h_proba, 0, 1)
    h_out["risk_level"] = h_out["probability"].map(risk_level)
    h_out["risk_score"] = (h_out["probability"] * 100).round(1)
    h_out["predicted_fire"] = (h_out["probability"] >= h_threshold).astype(int)
    h_out["confidence"] = h_out["probability"].map(confidence)
    h_out["risk_color"] = h_out["risk_level"].map(RISK_COLORS)
    h_out["timestamp"] = h_out["Timestamp"].dt.strftime("%Y-%m-%dT%H:%M")
    h_out["region"] = h_out["City"]

    coords = _city_coords()
    h_out["Latitude"] = h_out["City"].map(lambda c: coords.get(c, (np.nan, np.nan))[0])
    h_out["Longitude"] = h_out["City"].map(lambda c: coords.get(c, (np.nan, np.nan))[1])

    weather_col_map = {
        "Temperature_C": "temperature",
        "Humidity_percent": "humidity",
        "Wind_Speed_kmh": "wind",
        "Solar_Radiation_Wm2": "solar",
    }
    for src, dst in weather_col_map.items():
        if src in h_out.columns and dst not in h_out.columns:
            h_out[dst] = h_out[src].round(1)

    public_cols = [
        "timestamp", "region", "probability", "risk_level", "risk_score",
        "predicted_fire", "confidence", "risk_color",
        "temperature", "humidity", "wind", "solar",
        "Latitude", "Longitude",
    ]
    public_cols = [c for c in public_cols if c in h_out.columns]
    public = h_out[public_cols].copy()
    public = public.where(public.notna(), other=None)

    json_path = outputs_dir / "hourly_forecast_168h.json"
    json_path.write_text(public.to_json(orient="records", indent=2), encoding="utf-8")
    _copy_to_destinations(outputs_dir, ["hourly_forecast_168h.json"])

    return public


def export_metrics(
    metrics_dict: dict,
    outputs_dir: Optional[Path] = None,
) -> None:
    if outputs_dir is None:
        outputs_dir = OUTPUTS
    path = outputs_dir / "metrics.json"
    path.write_text(json.dumps(metrics_dict, indent=2), encoding="utf-8")
    _copy_to_destinations(outputs_dir, ["metrics.json"])


def _copy_to_destinations(outputs_dir: Path, fnames: list) -> None:
    root = outputs_dir.parent
    for dest_dir in [root / "dashboard" / "data", root / "docs" / "data"]:
        dest_dir.mkdir(parents=True, exist_ok=True)
        for fname in fnames:
            src = outputs_dir / fname
            if src.exists():
                shutil.copy2(src, dest_dir / fname)
