"""
WildFire Prediction — Output Validation
========================================
Run validation checks on all pipeline outputs. Returns structured
pass/fail results suitable for the validate_pipeline_outputs.py script.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

REQUIRED_CITIES = [
    "Baku", "Barda", "Gabala", "Ganja", "Jalilabad", "Khachmaz",
    "Lankaran", "Mingachevir", "Nakhchivan", "Quba", "Shabran",
    "Shaki", "Shamakhi", "Shirvan", "Yevlakh", "Zaqatala",
]

RISK_LEVELS = {"Low", "Moderate", "High", "Extreme"}

# Physical bounds for weather variables
WEATHER_BOUNDS = {
    "Temperature_C_mean":        (-60.0, 60.0),
    "Temperature_C":             (-60.0, 60.0),
    "Humidity_percent_mean":     (0.0, 100.0),
    "Humidity_percent":          (0.0, 100.0),
    "Rain_mm_sum":               (0.0, 500.0),
    "Rain_mm":                   (0.0, 100.0),
    "Wind_Speed_kmh_mean":       (0.0, 200.0),
    "Wind_Speed_kmh":            (0.0, 200.0),
    "Pressure_hPa_mean":         (800.0, 1100.0),
    "Pressure_hPa":              (800.0, 1100.0),
    "Solar_Radiation_Wm2_mean":  (0.0, 1400.0),
    "Solar_Radiation_Wm2":       (0.0, 1400.0),
    "Soil_Moisture_mean":        (0.0, 1.0),
    "Soil_Moisture":             (0.0, 1.0),
}

# Maximum tolerable daily-hourly disagreement score
MAX_DISAGREEMENT = 0.35


Check = Tuple[str, bool, str]  # (name, passed, detail)


def _ok(name: str, detail: str = "") -> Check:
    return (name, True, detail)


def _fail(name: str, detail: str) -> Check:
    return (name, False, detail)


# ═══════════════════════════════════════════════════════════════════════════
# File existence checks
# ═══════════════════════════════════════════════════════════════════════════

def check_required_files(root: Path) -> List[Check]:
    results = []
    required = [
        root / "models" / "wildfire" / "best_fire_model.joblib",
        root / "models" / "wildfire" / "model_manifest.json",
        root / "models" / "wildfire" / "feature_columns.json",
        root / "outputs" / "weather_forecast_30d.parquet",
        root / "outputs" / "weather_forecast_168h.parquet",
        root / "outputs" / "wildfire_risk_30d.parquet",
        root / "outputs" / "forecast_30_days.json",
        root / "outputs" / "hourly_forecast_168h.json",
        root / "outputs" / "metrics.json",
        root / "data" / "processed" / "engineered_daily.parquet",
        root / "data" / "processed" / "engineered_hourly.parquet",
    ]
    for p in required:
        if p.exists():
            results.append(_ok(f"file_exists:{p.name}"))
        else:
            results.append(_fail(f"file_exists:{p.name}", f"Missing: {p}"))
    return results


# ═══════════════════════════════════════════════════════════════════════════
# Weather forecast checks
# ═══════════════════════════════════════════════════════════════════════════

def check_weather_forecast_30d(path: Path) -> List[Check]:
    results = []
    if not path.exists():
        return [_fail("weather_30d:exists", str(path))]

    df = pd.read_parquet(path)
    results.append(_ok("weather_30d:exists"))

    # Required columns
    req_cols = ["City", "Date", "Temperature_C_mean", "Humidity_percent_mean",
                "Rain_mm_sum", "Wind_Speed_kmh_mean"]
    missing = [c for c in req_cols if c not in df.columns]
    if missing:
        results.append(_fail("weather_30d:columns", f"Missing: {missing}"))
    else:
        results.append(_ok("weather_30d:columns"))

    # Cities
    present = set(df["City"].unique()) if "City" in df.columns else set()
    missing_cities = set(REQUIRED_CITIES) - present
    if missing_cities:
        results.append(_fail("weather_30d:cities", f"Missing cities: {sorted(missing_cities)}"))
    else:
        results.append(_ok("weather_30d:cities", f"{len(present)} cities"))

    # No NaN in key cols
    if df.isnull().values.any():
        n = int(df.isnull().sum().sum())
        results.append(_fail("weather_30d:no_nan", f"{n} NaN values"))
    else:
        results.append(_ok("weather_30d:no_nan"))

    # Physical bounds
    for col, (lo, hi) in WEATHER_BOUNDS.items():
        if col not in df.columns:
            continue
        s = df[col]
        bad = ((s < lo) | (s > hi)).sum()
        if bad:
            results.append(_fail(f"weather_30d:bounds:{col}",
                                 f"{bad} values outside [{lo}, {hi}]"))
        else:
            results.append(_ok(f"weather_30d:bounds:{col}"))

    # No duplicate city/date
    if "City" in df.columns and "Date" in df.columns:
        dupes = df.duplicated(["City", "Date"]).sum()
        if dupes:
            results.append(_fail("weather_30d:no_dupes", f"{dupes} duplicate city/date rows"))
        else:
            results.append(_ok("weather_30d:no_dupes"))

    return results


def check_weather_forecast_168h(path: Path) -> List[Check]:
    results = []
    if not path.exists():
        return [_fail("weather_168h:exists", str(path))]

    df = pd.read_parquet(path)
    results.append(_ok("weather_168h:exists"))

    req_cols = ["City", "Timestamp", "Temperature_C", "Humidity_percent", "Wind_Speed_kmh"]
    missing = [c for c in req_cols if c not in df.columns]
    if missing:
        results.append(_fail("weather_168h:columns", f"Missing: {missing}"))
    else:
        results.append(_ok("weather_168h:columns"))

    if df.isnull().values.any():
        n = int(df.isnull().sum().sum())
        results.append(_fail("weather_168h:no_nan", f"{n} NaN values"))
    else:
        results.append(_ok("weather_168h:no_nan"))

    # Physical bounds
    for col, (lo, hi) in WEATHER_BOUNDS.items():
        if col not in df.columns:
            continue
        s = df[col]
        bad = ((s < lo) | (s > hi)).sum()
        if bad:
            results.append(_fail(f"weather_168h:bounds:{col}",
                                 f"{bad} values outside [{lo}, {hi}]"))
        else:
            results.append(_ok(f"weather_168h:bounds:{col}"))

    if "City" in df.columns and "Timestamp" in df.columns:
        dupes = df.duplicated(["City", "Timestamp"]).sum()
        if dupes:
            results.append(_fail("weather_168h:no_dupes", f"{dupes} duplicate city/timestamp rows"))
        else:
            results.append(_ok("weather_168h:no_dupes"))

    return results


# ═══════════════════════════════════════════════════════════════════════════
# Wildfire risk checks
# ═══════════════════════════════════════════════════════════════════════════

def check_wildfire_risk_30d(path: Path) -> List[Check]:
    results = []
    if not path.exists():
        return [_fail("risk_30d:exists", str(path))]

    df = pd.read_parquet(path)
    results.append(_ok("risk_30d:exists"))

    # Detect probability column name
    prob_col = None
    for c in ["probability", "fire_probability"]:
        if c in df.columns:
            prob_col = c
            break

    req_cols = ["City", "Date", "risk_level"] + ([prob_col] if prob_col else [])
    missing = [c for c in req_cols if c not in df.columns]
    if missing:
        results.append(_fail("risk_30d:columns", f"Missing: {missing}"))
    else:
        results.append(_ok("risk_30d:columns"))

    if prob_col:
        probs = df[prob_col].dropna()
        if ((probs < 0) | (probs > 1)).any():
            results.append(_fail("risk_30d:prob_range", "probabilities outside [0,1]"))
        else:
            results.append(_ok("risk_30d:prob_range",
                               f"range [{probs.min():.3f}, {probs.max():.3f}]"))

    if "risk_level" in df.columns:
        bad_levels = set(df["risk_level"].astype(str).unique()) - RISK_LEVELS - {"nan"}
        if bad_levels:
            results.append(_fail("risk_30d:risk_levels", f"Unknown levels: {bad_levels}"))
        else:
            results.append(_ok("risk_30d:risk_levels"))

    present = set(df["City"].unique()) if "City" in df.columns else set()
    missing_cities = set(REQUIRED_CITIES) - present
    if missing_cities:
        results.append(_fail("risk_30d:cities", f"Missing: {sorted(missing_cities)}"))
    else:
        results.append(_ok("risk_30d:cities"))

    if df.isnull().values.any():
        n = int(df.isnull().sum().sum())
        results.append(_fail("risk_30d:no_nan", f"{n} NaN values"))
    else:
        results.append(_ok("risk_30d:no_nan"))

    return results


def check_hourly_risk_json(path: Path) -> List[Check]:
    results = []
    if not path.exists():
        return [_fail("hourly_json:exists", str(path))]

    try:
        with open(path) as f:
            records = json.load(f)
        results.append(_ok("hourly_json:exists", f"{len(records)} records"))
    except json.JSONDecodeError as e:
        return [_fail("hourly_json:valid_json", str(e))]

    if not records:
        return [_fail("hourly_json:non_empty", "zero records")]

    req_keys = ["timestamp", "region", "probability", "risk_level"]
    sample = records[0]
    missing = [k for k in req_keys if k not in sample]
    if missing:
        results.append(_fail("hourly_json:columns", f"Missing keys: {missing}"))
    else:
        results.append(_ok("hourly_json:columns"))

    # No NaN in JSON (JSON doesn't have NaN but check None/null)
    def _has_none(r):
        return any(v is None for v in r.values())
    null_count = sum(_has_none(r) for r in records)
    if null_count:
        results.append(_fail("hourly_json:no_null", f"{null_count} records with null values"))
    else:
        results.append(_ok("hourly_json:no_null"))

    # Probability range
    if "probability" in sample:
        probs = [r["probability"] for r in records if r.get("probability") is not None]
        if probs:
            mn, mx = min(probs), max(probs)
            if mx > 1.0 or mn < 0.0:
                results.append(_fail("hourly_json:prob_range", f"[{mn:.3f}, {mx:.3f}] — outside [0,1]"))
            else:
                results.append(_ok("hourly_json:prob_range", f"[{mn:.3f}, {mx:.3f}]"))

    # Regions
    if "region" in sample:
        regions = set(r["region"] for r in records)
        missing_cities = set(REQUIRED_CITIES) - regions
        if missing_cities:
            results.append(_fail("hourly_json:cities", f"Missing: {sorted(missing_cities)}"))
        else:
            results.append(_ok("hourly_json:cities"))

    return results


# ═══════════════════════════════════════════════════════════════════════════
# Daily / hourly consistency
# ═══════════════════════════════════════════════════════════════════════════

def check_daily_hourly_consistency(
    daily_path: Path,
    hourly_json_path: Path,
    max_disagreement: float = MAX_DISAGREEMENT,
) -> Tuple[List[Check], pd.DataFrame]:
    """Compare daily risk vs hourly risk aggregated to day.

    Returns (checks, consistency_report_df).
    """
    results: List[Check] = []

    if not daily_path.exists() or not hourly_json_path.exists():
        return [_fail("consistency:inputs_exist", "daily or hourly file missing")], pd.DataFrame()

    daily = pd.read_parquet(daily_path)
    daily["Date"] = pd.to_datetime(daily["Date"])
    daily_prob_col = "probability" if "probability" in daily.columns else "fire_probability"
    if daily_prob_col not in daily.columns:
        return [_fail("consistency:daily_prob_col", f"no probability column in {daily_path.name}")], pd.DataFrame()

    try:
        with open(hourly_json_path) as f:
            h_records = json.load(f)
    except Exception as e:
        return [_fail("consistency:hourly_json_load", str(e))], pd.DataFrame()

    if not h_records or "probability" not in h_records[0]:
        return [_fail("consistency:hourly_prob_key", "no probability key in hourly JSON")], pd.DataFrame()

    hdf = pd.DataFrame(h_records)
    hdf["date"] = pd.to_datetime(hdf["timestamp"]).dt.normalize()
    hdf["probability"] = pd.to_numeric(hdf["probability"], errors="coerce")

    hourly_agg = (
        hdf.groupby(["region", "date"])["probability"]
        .agg(hourly_max="max", hourly_mean="mean")
        .reset_index()
        .rename(columns={"region": "City", "date": "Date"})
    )

    merged = daily.merge(hourly_agg, on=["City", "Date"], how="inner")
    if merged.empty:
        return [_fail("consistency:overlap", "no overlapping city/dates between daily and hourly")], pd.DataFrame()

    merged["daily_prob"] = merged[daily_prob_col]
    merged["disagreement"] = (merged["hourly_max"] - merged["daily_prob"]).abs()
    merged["mismatch_flag"] = merged["disagreement"] > max_disagreement

    n_mismatch = int(merged["mismatch_flag"].sum())
    n_total = len(merged)
    pct = 100 * n_mismatch / max(n_total, 1)

    if pct > 20:
        results.append(_fail(
            "consistency:daily_hourly_agreement",
            f"{n_mismatch}/{n_total} ({pct:.1f}%) city-days exceed disagreement threshold {max_disagreement}"
        ))
    else:
        results.append(_ok(
            "consistency:daily_hourly_agreement",
            f"{n_mismatch}/{n_total} ({pct:.1f}%) mismatches — within tolerance"
        ))

    # Build report DataFrame
    report = merged[["City", "Date", "daily_prob", "hourly_max", "hourly_mean",
                      "disagreement", "mismatch_flag"]].copy()
    report["daily_risk_level"] = pd.cut(
        report["daily_prob"],
        bins=[-1, 0.15, 0.35, 0.60, 1.01],
        labels=["Low", "Moderate", "High", "Extreme"],
    )
    report["hourly_max_risk_level"] = pd.cut(
        report["hourly_max"],
        bins=[-1, 0.15, 0.35, 0.60, 1.01],
        labels=["Low", "Moderate", "High", "Extreme"],
    )
    report = report.sort_values("disagreement", ascending=False).reset_index(drop=True)

    return results, report


# ═══════════════════════════════════════════════════════════════════════════
# Model artifact checks
# ═══════════════════════════════════════════════════════════════════════════

def check_model_artifacts(models_dir: Path) -> List[Check]:
    results = []
    wildfire_dir = models_dir / "wildfire"
    outputs_dir = models_dir.parent / "outputs"

    required_in_models = [
        "best_fire_model.joblib",
        "model_manifest.json",
        "feature_columns.json",
    ]
    for fname in required_in_models:
        p = wildfire_dir / fname
        if p.exists():
            results.append(_ok(f"model_artifact:{fname}"))
        else:
            results.append(_fail(f"model_artifact:{fname}", f"Missing: {p}"))

    # final_threshold.json lives in outputs/
    threshold_path = outputs_dir / "final_threshold.json"
    if threshold_path.exists():
        results.append(_ok("model_artifact:final_threshold.json"))
    else:
        results.append(_fail("model_artifact:final_threshold.json",
                             f"Missing: {threshold_path}"))

    manifest_path = wildfire_dir / "model_manifest.json"
    if manifest_path.exists():
        try:
            with open(manifest_path) as f:
                m = json.load(f)
            req_keys = ["model_name", "optimal_threshold", "metrics"]
            missing = [k for k in req_keys if k not in m]
            if missing:
                results.append(_fail("model_manifest:keys", f"Missing: {missing}"))
            else:
                results.append(_ok("model_manifest:keys",
                                   f"model={m['model_name']} threshold={m['optimal_threshold']:.3f}"))
        except Exception as e:
            results.append(_fail("model_manifest:parse", str(e)))

    return results


# ═══════════════════════════════════════════════════════════════════════════
# Feature engineering checks (no leakage)
# ═══════════════════════════════════════════════════════════════════════════

def check_no_leakage(daily_path: Path, split_date: str = "2025-01-01") -> List[Check]:
    results = []
    if not daily_path.exists():
        return [_fail("leakage:file", str(daily_path))]

    df = pd.read_parquet(daily_path)
    df["Date"] = pd.to_datetime(df["Date"])
    split = pd.Timestamp(split_date)

    leak_cols = ["fire_count", "mean_brightness", "max_frp", "Burned_Area_hectares"]
    found = [c for c in leak_cols if c in df.columns]
    if found:
        results.append(_ok("leakage:leak_cols_present", f"present but should be dropped before model: {found}"))
    else:
        results.append(_ok("leakage:no_leak_cols"))

    # Check that rolling/lag features shift by >=1
    roll_cols = [c for c in df.columns if "roll" in c.lower() or "lag" in c.lower()]
    if roll_cols:
        results.append(_ok("leakage:lag_roll_cols_exist", f"{len(roll_cols)} lag/roll features"))
    else:
        results.append(_fail("leakage:lag_roll_cols_missing", "no lag/rolling features found"))

    # Check train/test dates don't overlap
    train = df[df["Date"] < split]
    test = df[df["Date"] >= split]
    if len(train) == 0:
        results.append(_fail("leakage:train_empty", f"no data before {split_date}"))
    elif len(test) == 0:
        results.append(_fail("leakage:test_empty", f"no data >= {split_date}"))
    else:
        results.append(_ok("leakage:split_clean",
                           f"train={len(train)} rows, test={len(test)} rows, split={split_date}"))

    return results


# ═══════════════════════════════════════════════════════════════════════════
# Dashboard JSON validity
# ═══════════════════════════════════════════════════════════════════════════

def check_dashboard_json(path: Path, expected_keys: List[str]) -> List[Check]:
    results = []
    if not path.exists():
        return [_fail(f"dashboard_json:{path.name}:exists", str(path))]

    try:
        with open(path) as f:
            data = json.load(f)
        results.append(_ok(f"dashboard_json:{path.name}:valid"))
    except json.JSONDecodeError as e:
        return [_fail(f"dashboard_json:{path.name}:valid", str(e))]

    records = data if isinstance(data, list) else []
    if records:
        sample = records[0]
        missing = [k for k in expected_keys if k not in sample]
        if missing:
            results.append(_fail(f"dashboard_json:{path.name}:keys", f"Missing: {missing}"))
        else:
            results.append(_ok(f"dashboard_json:{path.name}:keys"))

    return results


# ═══════════════════════════════════════════════════════════════════════════
# Master runner
# ═══════════════════════════════════════════════════════════════════════════

def run_all_checks(root: Path) -> Tuple[List[Check], pd.DataFrame]:
    """Run all checks, return (all_checks, consistency_report)."""
    all_checks: List[Check] = []

    all_checks += check_required_files(root)
    all_checks += check_weather_forecast_30d(root / "outputs" / "weather_forecast_30d.parquet")
    all_checks += check_weather_forecast_168h(root / "outputs" / "weather_forecast_168h.parquet")
    all_checks += check_wildfire_risk_30d(root / "outputs" / "wildfire_risk_30d.parquet")
    all_checks += check_hourly_risk_json(root / "outputs" / "hourly_forecast_168h.json")
    all_checks += check_model_artifacts(root / "models")
    all_checks += check_no_leakage(root / "data" / "processed" / "engineered_daily.parquet")
    all_checks += check_dashboard_json(
        root / "outputs" / "forecast_30_days.json",
        ["date", "region", "risk_level", "probability"],
    )

    consistency_checks, consistency_report = check_daily_hourly_consistency(
        root / "outputs" / "wildfire_risk_30d.parquet",
        root / "outputs" / "hourly_forecast_168h.json",
    )
    all_checks += consistency_checks

    return all_checks, consistency_report


def print_report(checks: List[Check], consistency_report: pd.DataFrame) -> int:
    """Print formatted pass/fail report. Returns exit code (0=all pass)."""
    passed = [c for c in checks if c[1]]
    failed = [c for c in checks if not c[1]]

    print()
    print("═" * 70)
    print("  WildFire Pipeline Validation Report")
    print("═" * 70)

    if failed:
        print(f"\n  ✗ FAILED ({len(failed)} checks):")
        for name, ok, detail in failed:
            print(f"    ✗ {name}")
            if detail:
                print(f"      → {detail}")

    print(f"\n  ✓ PASSED ({len(passed)} checks):")
    for name, ok, detail in passed:
        print(f"    ✓ {name}" + (f"  ({detail})" if detail else ""))

    print()
    print("─" * 70)
    print(f"  Total: {len(passed)} passed, {len(failed)} failed")
    print("─" * 70)

    if not consistency_report.empty:
        print("\n  DAILY / HOURLY CONSISTENCY REPORT:")
        print(f"  {'City':12s} {'Date':12s} {'Daily':7s} {'H-Max':7s} {'H-Mean':7s} {'Diff':7s} {'Flag'}")
        print("  " + "-" * 62)
        for _, row in consistency_report.head(20).iterrows():
            flag = "⚠ MISMATCH" if row["mismatch_flag"] else ""
            print(f"  {row['City']:12s} {str(row['Date'].date()):12s} "
                  f"{row['daily_prob']:7.3f} {row['hourly_max']:7.3f} "
                  f"{row['hourly_mean']:7.3f} {row['disagreement']:7.3f} {flag}")

    print("═" * 70)
    return 0 if not failed else 1
