"""
WildFire 30-day wildfire risk prediction pipeline.

This script trains several forecast-compatible wildfire classifiers using a
strict temporal split, selects the best calibrated probability model, scores the
30-day weather forecast, and writes dashboard-ready CSV/JSON outputs.
"""
from __future__ import annotations

import json
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import ExtraTreesClassifier, HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, precision_recall_fscore_support, roc_auc_score, f1_score as sklearn_f1
from sklearn.neural_network import MLPClassifier
from sklearn.pipeline import Pipeline
from sklearn.isotonic import IsotonicRegression
from sklearn.preprocessing import StandardScaler
from pandas.errors import PerformanceWarning
from sklearn.model_selection import StratifiedKFold

try:
    from xgboost import XGBClassifier
except Exception:  # pragma: no cover - optional dependency
    XGBClassifier = None

try:
    from lightgbm import LGBMClassifier
except Exception:  # pragma: no cover - optional dependency
    LGBMClassifier = None

try:
    from catboost import CatBoostClassifier
except Exception:  # pragma: no cover - optional dependency
    CatBoostClassifier = None

try:
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)
except ImportError:
    optuna = None

from src.config import CITIES, ENG_DAILY, ENG_HOURLY, FORECAST_30D, FORECAST_168H, MODELS_F, OUTPUTS, RANDOM_SEED
from src.features import add_calendar_features, add_wildfire_weather_features, compute_fwi_proxy

warnings.filterwarnings("ignore", category=PerformanceWarning)
warnings.filterwarnings("ignore", message="X does not have valid feature names")


TRAIN_END = pd.Timestamp("2024-01-01")
TEST_START = pd.Timestamp("2025-01-01")
TARGET = "Fire_Occurred"
RISK_ORDER = ["Low", "Moderate", "High", "Extreme"]
RISK_COLORS = {
    "Low": "#3FA773",
    "Moderate": "#D8A31D",
    "High": "#D96C3B",
    "Extreme": "#B73333",
}

BASE_WEATHER = [
    "Temperature_C_mean",
    "Humidity_percent_mean",
    "Rain_mm_sum",
    "Wind_Speed_kmh_mean",
    "Pressure_hPa_mean",
    "Solar_Radiation_Wm2_mean",
    "Soil_Temp_C_mean",
    "Soil_Moisture_mean",
]
STATIC_FEATURES = [
    "Latitude",
    "Longitude",
    "Elevation",
    "Slope",
    "Trees_pct",
    "Urban_pct",
    "Pop_Total",
    "NDBI",
    "NDVI",
    "EVI",
]
DROP_COLUMNS = {
    "Date",
    "City",
    TARGET,
    "fire_count",
    "mean_brightness",
    "max_frp",
    "Burned_Area_hectares",
}


@dataclass
class ModelResult:
    name: str
    estimator: Pipeline
    calibrator: IsotonicRegression
    threshold: float
    metrics: Dict[str, float]


def _risk_level(probability: float) -> str:
    if probability >= 0.60:
        return "Extreme"
    if probability >= 0.35:
        return "High"
    if probability >= 0.15:
        return "Moderate"
    return "Low"


def _confidence(probability: float) -> float:
    """Readable confidence proxy: distance from the uncertain middle."""
    return float(np.clip(0.55 + abs(probability - 0.5) * 0.8, 0.55, 0.95))


def _climate_summary(row: pd.Series) -> str:
    temp = row.get("Temperature_C_mean", np.nan)
    wind = row.get("Wind_Speed_kmh_mean", np.nan)
    humidity = row.get("Humidity_percent_mean", np.nan)
    rain = row.get("Rain_mm_sum", np.nan)
    fragments = []
    if pd.notna(temp) and temp >= 28:
        fragments.append("hot conditions")
    elif pd.notna(temp) and temp <= 12:
        fragments.append("cool conditions")
    else:
        fragments.append("mild temperatures")
    if pd.notna(wind) and wind >= 18:
        fragments.append("strong wind")
    if pd.notna(humidity) and humidity <= 40:
        fragments.append("dry air")
    if pd.notna(rain) and rain >= 2:
        fragments.append("recent rainfall")
    return ", ".join(fragments).capitalize() + "."


def _warning_text(row: pd.Series) -> str:
    if row["risk_level"] in {"High", "Extreme"}:
        return "High temperature, dry air, and wind can accelerate wildfire spread."
    if row.get("Wind_Speed_kmh_mean", 0) >= 18:
        return "Wind is elevated, so small ignitions could spread faster."
    if row.get("Humidity_percent_mean", 100) <= 40:
        return "Low humidity can dry vegetation and raise ignition sensitivity."
    return "Current conditions suggest limited short-term wildfire pressure."


def _add_lag_roll_features(df: pd.DataFrame, variables: Iterable[str]) -> pd.DataFrame:
    df = df.sort_values(["City", "Date"]).copy()
    for var in variables:
        if var not in df.columns:
            continue
        grouped = df.groupby("City", group_keys=False)[var]
        for lag in [1, 2, 3, 5, 7, 14, 30]:
            df[f"{var}_lag{lag}"] = grouped.shift(lag)
        shifted = grouped.shift(1)
        for window in [3, 7, 14, 30]:
            df[f"{var}_roll{window}_mean"] = shifted.groupby(df["City"]).rolling(window, min_periods=1).mean().reset_index(level=0, drop=True)
            df[f"{var}_roll{window}_std"] = shifted.groupby(df["City"]).rolling(window, min_periods=2).std().reset_index(level=0, drop=True)
    return df


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Build the shared feature surface used for training and future scoring."""
    out = df.copy()
    out["Date"] = pd.to_datetime(out["Date"])
    out = add_calendar_features(out, "Date")
    out = pd.concat(
        [compute_fwi_proxy(group) for _, group in out.groupby("City", sort=False)],
        ignore_index=True,
    )
    out = add_wildfire_weather_features(out)
    lag_vars = BASE_WEATHER + ["FWI_proxy", "VPD_kPa", "dry_days_streak"]
    out = _add_lag_roll_features(out, lag_vars)
    out = pd.get_dummies(out, columns=["City"], prefix="city", dtype=int)
    return out


def load_training_frame() -> pd.DataFrame:
    df = pd.read_parquet(ENG_DAILY)
    df["Date"] = pd.to_datetime(df["Date"])
    needed = ["City", "Date", TARGET] + BASE_WEATHER + STATIC_FEATURES
    existing = [c for c in needed if c in df.columns]
    train = df[existing].copy()
    for col in STATIC_FEATURES:
        if col not in train.columns:
            train[col] = 0.0
    return build_features(train)


def load_forecast_frame(history_raw: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    forecast = pd.read_parquet(FORECAST_30D)
    forecast["Date"] = pd.to_datetime(forecast["Date"])
    latest_static = (
        history_raw.sort_values("Date")
        .groupby("City", as_index=False)
        .tail(1)[["City"] + [c for c in STATIC_FEATURES if c in history_raw.columns]]
    )
    forecast = forecast.merge(latest_static, on="City", how="left")
    for col in STATIC_FEATURES:
        if col not in forecast.columns:
            forecast[col] = 0.0
    forecast[TARGET] = np.nan

    history_tail = history_raw[history_raw["Date"] >= forecast["Date"].min() - pd.Timedelta(days=45)].copy()
    combined = pd.concat([history_tail, forecast], ignore_index=True, sort=False)
    features = build_features(combined)

    # ── Fix quantile-based flags ─────────────────────────────────────────
    # build_features → add_wildfire_weather_features computes heatwave /
    # low-humidity / high-wind flags using in-sample quantiles.  The small
    # prediction window (~75 days, one season) produces drastically wrong
    # thresholds vs. the full multi-year training set, inflating fire risk.
    # Recompute flags using full-history quantiles so they match training.
    T_full = history_raw["Temperature_C_mean"].dropna()
    H_full = history_raw["Humidity_percent_mean"].dropna()
    W_full = history_raw["Wind_Speed_kmh_mean"].dropna()
    t95 = float(T_full.quantile(0.95))
    h10 = float(H_full.quantile(0.10))
    w90 = float(W_full.quantile(0.90))

    T_feat = features.get("Temperature_C_mean", pd.Series(0, index=features.index))
    H_feat = features.get("Humidity_percent_mean", pd.Series(50, index=features.index))
    W_feat = features.get("Wind_Speed_kmh_mean", pd.Series(0, index=features.index))
    features["heatwave_flag"]     = (T_feat > t95).astype(int)
    features["low_humidity_flag"] = (H_feat < h10).astype(int)
    features["high_wind_flag"]    = (W_feat > w90).astype(int)
    features["hot_dry_windy"]     = (features["heatwave_flag"]
                                     * features["low_humidity_flag"]
                                     * features["high_wind_flag"])

    # ── Fix Rainfall_Deficit ─────────────────────────────────────────────
    # The "long-term monthly mean" was computed on the small prediction
    # window rather than full history, distorting the drought proxy.
    if "Rainfall_Deficit" in features.columns and "Rain_mm_sum" in history_raw.columns:
        hr = history_raw.copy()
        hr["Month"] = pd.to_datetime(hr["Date"]).dt.month
        full_monthly_avg = hr.groupby(["City", "Month"])["Rain_mm_sum"].mean()

        feat_month = features["Date"].dt.month
        feat_city = None
        city_cols = [c for c in features.columns if c.startswith("city_")]
        if city_cols:
            feat_city = features[city_cols].idxmax(axis=1).str.replace("city_", "", regex=False)
        elif "City" in features.columns:
            feat_city = features["City"]

        if feat_city is not None:
            mapped_avg = pd.Series(
                [full_monthly_avg.get((c, m), np.nan) for c, m in zip(feat_city, feat_month)],
                index=features.index,
            )
            roll30 = features.get("Rain_roll30_sum", pd.Series(0, index=features.index))
            features["Rainfall_Deficit"] = mapped_avg * 30 - roll30.fillna(0)

    future_features = features[features["Date"].isin(forecast["Date"])].copy()
    return forecast, future_features


def candidate_models(pos_weight: float) -> Dict[str, object]:
    models: Dict[str, object] = {
        "LogisticRegression": Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            ("model", LogisticRegression(max_iter=1000, class_weight="balanced", random_state=RANDOM_SEED)),
        ]),
        "RandomForest": Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("model", RandomForestClassifier(n_estimators=260, max_depth=16, min_samples_leaf=3, class_weight="balanced", n_jobs=-1, random_state=RANDOM_SEED)),
        ]),
        "ExtraTrees": Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("model", ExtraTreesClassifier(n_estimators=320, max_depth=18, min_samples_leaf=2, class_weight="balanced", n_jobs=-1, random_state=RANDOM_SEED)),
        ]),
        "HistGradientBoosting": Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("model", HistGradientBoostingClassifier(max_iter=220, max_leaf_nodes=31, learning_rate=0.055, l2_regularization=0.05, class_weight="balanced", random_state=RANDOM_SEED)),
        ]),
    }
    if XGBClassifier is not None:
        models["XGBoost"] = Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("model", XGBClassifier(n_estimators=260, max_depth=5, learning_rate=0.045, subsample=0.85, colsample_bytree=0.85, eval_metric="aucpr", scale_pos_weight=pos_weight, random_state=RANDOM_SEED, n_jobs=-1)),
        ])
    if LGBMClassifier is not None:
        models["LightGBM"] = Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("model", LGBMClassifier(n_estimators=360, max_depth=7, learning_rate=0.04, subsample=0.85, colsample_bytree=0.85, is_unbalance=True, random_state=RANDOM_SEED, n_jobs=-1, verbose=-1)),
        ])
    if CatBoostClassifier is not None:
        models["CatBoost"] = Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("model", CatBoostClassifier(iterations=280, depth=6, learning_rate=0.045, auto_class_weights="Balanced", eval_metric="AUC", random_seed=RANDOM_SEED, verbose=False)),
        ])
    # Neural Network (MLP)
    models["MLP_Neural"] = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler()),
        ("model", MLPClassifier(
            hidden_layer_sizes=(256, 128, 64),
            activation="relu", solver="adam", alpha=1e-3,
            batch_size=512, learning_rate="adaptive",
            learning_rate_init=1e-3, max_iter=300,
            early_stopping=True, validation_fraction=0.15,
            n_iter_no_change=15, random_state=RANDOM_SEED,
        )),
    ])
    return models


def feature_matrix(df: pd.DataFrame, feature_columns: List[str] | None = None) -> Tuple[pd.DataFrame, List[str]]:
    numeric = df.select_dtypes(include=[np.number]).copy()
    numeric = numeric.drop(columns=[c for c in DROP_COLUMNS if c in numeric.columns], errors="ignore")
    if feature_columns is None:
        feature_columns = sorted([c for c in numeric.columns if c != TARGET])
    return numeric.reindex(columns=feature_columns), feature_columns


def threshold_from_validation(y_true: np.ndarray, probabilities: np.ndarray,
                              min_precision: float = 0.30,
                              min_recall: float = 0.70) -> float:
    """Find optimal threshold enforcing precision >= 0.30 and recall >= 0.70.

    Fallback cascade:
      1. Primary: precision >= min_precision, recall >= min_recall
      2. Relax precision to 20%
      3. Relax recall to 50%, keep precision >= 20%
      4. Maximise F1 as last resort
    """
    grid = np.arange(0.05, 0.85, 0.005)
    pr_auc = average_precision_score(y_true, probabilities)

    def _search(prec_floor, rec_floor):
        best_t, best_s = None, -1.0
        for t in grid:
            preds = (probabilities >= t).astype(int)
            precision, recall, f1, _ = precision_recall_fscore_support(
                y_true, preds, average="binary", zero_division=0)
            if precision < prec_floor or recall < rec_floor:
                continue
            score = 0.35 * pr_auc + 0.30 * f1 + 0.20 * recall + 0.15 * precision
            if score > best_s:
                best_s = score
                best_t = float(t)
        return best_t

    # Primary
    t = _search(min_precision, min_recall)
    if t is not None:
        return t
    # Fallback 1: relax precision to 20%
    t = _search(0.20, min_recall)
    if t is not None:
        return t
    # Fallback 2: relax recall to 50%
    t = _search(0.20, 0.50)
    if t is not None:
        return t
    # Fallback 3: maximise F1
    best_t, best_f1 = 0.5, -1.0
    for t in grid:
        preds = (probabilities >= t).astype(int)
        f1v = sklearn_f1(y_true, preds, zero_division=0)
        if f1v > best_f1:
            best_f1 = f1v
            best_t = float(t)
    return best_t


def evaluate_model(name: str, model: Pipeline, X_val: pd.DataFrame, y_val: pd.Series, X_test: pd.DataFrame, y_test: pd.Series) -> ModelResult:
    val_prob_raw = model.predict_proba(X_val)[:, 1]
    calibrator = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0)
    calibrator.fit(val_prob_raw, y_val.to_numpy())
    val_prob = calibrator.predict(val_prob_raw)
    threshold = threshold_from_validation(y_val.to_numpy(), val_prob)
    test_prob = calibrator.predict(model.predict_proba(X_test)[:, 1])
    test_pred = (test_prob >= threshold).astype(int)
    precision, recall, f1, _ = precision_recall_fscore_support(y_test, test_pred, average="binary", zero_division=0)
    pr_auc_val = float(average_precision_score(y_test, test_prob))
    metrics = {
        "threshold": threshold,
        "precision": float(precision),
        "recall": float(recall),
        "f1": float(f1),
        "pr_auc": pr_auc_val,
        "roc_auc": float(roc_auc_score(y_test, test_prob)),
        "positive_rate": float(test_pred.mean()),
        "selection_score": float(0.35 * pr_auc_val + 0.30 * f1 + 0.20 * recall + 0.15 * precision),
    }
    return ModelResult(name=name, estimator=model, calibrator=calibrator, threshold=threshold, metrics=metrics)


# ═══════════════════════════════════════════════════════════════════════════
# Optuna Hyperparameter Tuning
# ═══════════════════════════════════════════════════════════════════════════

def _optuna_xgb(X_tr, y_tr, X_v, y_v, pos_weight, n_trials=100):
    """Optuna-tune XGBoost and return best Pipeline."""
    if XGBClassifier is None or optuna is None:
        return None
    def objective(trial):
        p = {
            "n_estimators": trial.suggest_int("n_estimators", 150, 600),
            "max_depth": trial.suggest_int("max_depth", 3, 8),
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.15, log=True),
            "subsample": trial.suggest_float("subsample", 0.6, 1.0),
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
            "min_child_weight": trial.suggest_int("min_child_weight", 1, 10),
            "gamma": trial.suggest_float("gamma", 0.0, 5.0),
            "reg_alpha": trial.suggest_float("reg_alpha", 1e-4, 10.0, log=True),
            "reg_lambda": trial.suggest_float("reg_lambda", 1e-4, 10.0, log=True),
        }
        pipe = Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("model", XGBClassifier(**p, scale_pos_weight=pos_weight,
                                     eval_metric="aucpr", random_state=RANDOM_SEED, n_jobs=-1)),
        ])
        pipe.fit(X_tr, y_tr)
        prob = pipe.predict_proba(X_v)[:, 1]
        return average_precision_score(y_v, prob)
    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.TPESampler(seed=RANDOM_SEED))
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)
    best = study.best_params
    pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("model", XGBClassifier(**best, scale_pos_weight=pos_weight,
                                 eval_metric="aucpr", random_state=RANDOM_SEED, n_jobs=-1)),
    ])
    pipe.fit(X_tr, y_tr)
    return pipe


def _optuna_lgb(X_tr, y_tr, X_v, y_v, n_trials=100):
    """Optuna-tune LightGBM and return best Pipeline."""
    if LGBMClassifier is None or optuna is None:
        return None
    def objective(trial):
        p = {
            "n_estimators": trial.suggest_int("n_estimators", 150, 600),
            "max_depth": trial.suggest_int("max_depth", 3, 10),
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.15, log=True),
            "subsample": trial.suggest_float("subsample", 0.6, 1.0),
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
            "num_leaves": trial.suggest_int("num_leaves", 20, 150),
            "min_child_samples": trial.suggest_int("min_child_samples", 5, 100),
            "reg_alpha": trial.suggest_float("reg_alpha", 1e-4, 10.0, log=True),
            "reg_lambda": trial.suggest_float("reg_lambda", 1e-4, 10.0, log=True),
        }
        pipe = Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("model", LGBMClassifier(**p, is_unbalance=True,
                                      random_state=RANDOM_SEED, n_jobs=-1, verbose=-1)),
        ])
        pipe.fit(X_tr, y_tr)
        prob = pipe.predict_proba(X_v)[:, 1]
        return average_precision_score(y_v, prob)
    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.TPESampler(seed=RANDOM_SEED))
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)
    best = study.best_params
    pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("model", LGBMClassifier(**best, is_unbalance=True,
                                  random_state=RANDOM_SEED, n_jobs=-1, verbose=-1)),
    ])
    pipe.fit(X_tr, y_tr)
    return pipe


def _optuna_cb(X_tr, y_tr, X_v, y_v, n_trials=100):
    """Optuna-tune CatBoost and return best Pipeline."""
    if CatBoostClassifier is None or optuna is None:
        return None
    def objective(trial):
        p = {
            "iterations": trial.suggest_int("iterations", 150, 600),
            "depth": trial.suggest_int("depth", 4, 10),
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.15, log=True),
            "l2_leaf_reg": trial.suggest_float("l2_leaf_reg", 1e-2, 10.0, log=True),
            "bagging_temperature": trial.suggest_float("bagging_temperature", 0.0, 5.0),
            "random_strength": trial.suggest_float("random_strength", 1e-2, 10.0, log=True),
            "border_count": trial.suggest_int("border_count", 32, 255),
        }
        pipe = Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("model", CatBoostClassifier(**p, auto_class_weights="Balanced",
                                          eval_metric="AUC", random_seed=RANDOM_SEED, verbose=False)),
        ])
        pipe.fit(X_tr, y_tr)
        prob = pipe.predict_proba(X_v)[:, 1]
        return average_precision_score(y_v, prob)
    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.TPESampler(seed=RANDOM_SEED))
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)
    best = study.best_params
    pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("model", CatBoostClassifier(**best, auto_class_weights="Balanced",
                                      eval_metric="AUC", random_seed=RANDOM_SEED, verbose=False)),
    ])
    pipe.fit(X_tr, y_tr)
    return pipe


# ═══════════════════════════════════════════════════════════════════════════
# Stacking Ensemble
# ═══════════════════════════════════════════════════════════════════════════

class StackingEnsemble:
    """Stacking ensemble: base models produce OOF probabilities, meta-learner
    (LogisticRegression) learns optimal combination. Also averages probabilities
    as a SoftVoting fallback."""

    def __init__(self, base_models: Dict[str, Pipeline], n_folds: int = 5):
        self.base_models = base_models
        self.n_folds = n_folds
        self.meta_model = None
        self.fitted_bases: Dict[str, List[Pipeline]] = {}

    def fit(self, X: pd.DataFrame, y: pd.Series):
        X_arr = X.values if hasattr(X, "values") else np.asarray(X)
        y_arr = y.values if hasattr(y, "values") else np.asarray(y)
        oof_probs = np.zeros((len(y_arr), len(self.base_models)))
        skf = StratifiedKFold(n_splits=self.n_folds, shuffle=True, random_state=RANDOM_SEED)

        for j, (name, model_template) in enumerate(self.base_models.items()):
            self.fitted_bases[name] = []
            for fold_idx, (tr_idx, val_idx) in enumerate(skf.split(X_arr, y_arr)):
                import copy
                fold_model = copy.deepcopy(model_template)
                fold_model.fit(X_arr[tr_idx], y_arr[tr_idx])
                oof_probs[val_idx, j] = fold_model.predict_proba(X_arr[val_idx])[:, 1]
                self.fitted_bases[name].append(fold_model)

        self.meta_model = LogisticRegression(max_iter=1000, random_state=RANDOM_SEED)
        self.meta_model.fit(oof_probs, y_arr)
        return self

    def predict_proba(self, X):
        X_arr = X.values if hasattr(X, "values") else np.asarray(X)
        base_probs = np.zeros((X_arr.shape[0], len(self.base_models)))
        for j, (name, fold_models) in enumerate(self.fitted_bases.items()):
            fold_preds = np.column_stack(
                [m.predict_proba(X_arr)[:, 1] for m in fold_models])
            base_probs[:, j] = fold_preds.mean(axis=1)
        meta_probs = self.meta_model.predict_proba(base_probs)[:, 1]
        # Blend: 60% meta-learner + 40% soft-vote average
        soft_vote = base_probs.mean(axis=1)
        blended = 0.6 * meta_probs + 0.4 * soft_vote
        return np.column_stack([1 - blended, blended])


# ═══════════════════════════════════════════════════════════════════════════
# Role-Based Blended Ensemble  (every model contributes its strength)
# ═══════════════════════════════════════════════════════════════════════════

class RoleBlendedEnsemble:
    """Blends ALL trained models with recall-first weighting.

    Strategy:
      1. Classify each model as recall-specialist, precision-specialist,
         or balanced based on validation metrics.
      2. Assign weights: recall-specialists get 2×, precision-specialists 1×,
         balanced 1.5×.  Then normalise so they sum to 1.
      3. Final probability = weighted average of all model probabilities.
      4. Bonus: if >= 60% of models agree on fire, bump probability by 10%
         (capped at 1.0).  This rewards consensus.
    """

    def __init__(self, models_with_metrics: List[Tuple[str, object, Dict]]):
        """models_with_metrics: list of (name, fitted_model, metrics_dict)"""
        self.models = []
        self.weights = []
        self._assign_roles(models_with_metrics)

    def _assign_roles(self, mwm):
        recall_vals = [m[2]["recall"] for m in mwm]
        prec_vals   = [m[2]["precision"] for m in mwm]
        med_recall  = float(np.median(recall_vals))
        med_prec    = float(np.median(prec_vals))

        raw_weights = []
        for name, model, met in mwm:
            r, p = met["recall"], met["precision"]
            if r >= med_recall and p < med_prec:
                role, w = "recall", 2.0
            elif p >= med_prec and r < med_recall:
                role, w = "precision", 1.0
            else:
                role, w = "balanced", 1.5
            # Bonus for PR-AUC (overall ranking quality)
            w *= (0.5 + met.get("pr_auc", 0.3))
            self.models.append((name, model, role))
            raw_weights.append(w)

        total = sum(raw_weights)
        self.weights = [w / total for w in raw_weights]

    def fit(self, X, y):
        return self  # models are already fitted

    def predict_proba(self, X):
        X_arr = X.values if hasattr(X, "values") else np.asarray(X)
        probs = []
        for name, model, role in self.models:
            try:
                p = model.predict_proba(X_arr)[:, 1]
            except Exception:
                p = np.full(X_arr.shape[0], 0.0)
            probs.append(p)
        probs = np.column_stack(probs)  # (N, n_models)

        # Weighted average
        weights = np.array(self.weights)
        blended = probs @ weights  # (N,)

        # Consensus boost: if >= 60% of models predict fire (prob > median
        # threshold ~0.15), bump by 10%
        fire_votes = (probs > 0.15).sum(axis=1)
        consensus = fire_votes >= (0.6 * len(self.models))
        blended[consensus] = np.minimum(blended[consensus] * 1.10, 1.0)

        # Ensure no model's strong signal is completely drowned out:
        # If ANY recall-specialist has prob > 0.5, floor the blend at 0.10
        recall_mask = np.array([role == "recall" for _, _, role in self.models])
        if recall_mask.any():
            max_recall_prob = probs[:, recall_mask].max(axis=1)
            floor_mask = max_recall_prob > 0.5
            blended[floor_mask] = np.maximum(blended[floor_mask], 0.10)

        return np.column_stack([1 - blended, blended])


# ═══════════════════════════════════════════════════════════════════════════
# Train & Select
# ═══════════════════════════════════════════════════════════════════════════

OPTUNA_TRIALS = 100

def train_and_select(features: pd.DataFrame) -> Tuple[ModelResult, List[Dict[str, float]], List[str]]:
    train_mask = features["Date"] < TRAIN_END
    val_mask = (features["Date"] >= TRAIN_END) & (features["Date"] < TEST_START)
    test_mask = features["Date"] >= TEST_START

    X_train, feature_columns = feature_matrix(features[train_mask])
    X_val, _ = feature_matrix(features[val_mask], feature_columns)
    X_test, _ = feature_matrix(features[test_mask], feature_columns)
    y_train = features.loc[train_mask, TARGET].astype(int)
    y_val = features.loc[val_mask, TARGET].astype(int)
    y_test = features.loc[test_mask, TARGET].astype(int)

    neg = max((y_train == 0).sum(), 1)
    pos = max((y_train == 1).sum(), 1)
    pos_weight = min(neg / pos, 20)

    # ── Phase 1: Baseline models ─────────────────────────────────────────
    print("Phase 1: Training baseline models...")
    results: List[ModelResult] = []
    for name, model in candidate_models(pos_weight).items():
        model.fit(X_train, y_train)
        results.append(evaluate_model(name, model, X_val, y_val, X_test, y_test))
        print(f"  {name:25s} PR-AUC={results[-1].metrics['pr_auc']:.3f}  "
              f"P={results[-1].metrics['precision']:.3f}  R={results[-1].metrics['recall']:.3f}")

    # ── Phase 2: Optuna deep-tuning (top 3 GBTs) ────────────────────────
    if optuna is not None:
        print(f"Phase 2: Optuna tuning ({OPTUNA_TRIALS} trials each)...")
        X_fit = pd.concat([X_train, X_val])
        y_fit = pd.concat([y_train, y_val])

        tuned_models = {}
        for label, tune_fn, kwargs in [
            ("XGB_Optuna",  _optuna_xgb, {"pos_weight": pos_weight}),
            ("LGB_Optuna",  _optuna_lgb, {}),
            ("CB_Optuna",   _optuna_cb,  {}),
        ]:
            print(f"  Tuning {label}...")
            pipe = tune_fn(X_train, y_train, X_val, y_val, n_trials=OPTUNA_TRIALS, **kwargs)
            if pipe is not None:
                # Re-fit on train+val for final evaluation
                import copy
                full_pipe = copy.deepcopy(pipe)
                full_pipe.fit(X_fit, y_fit)
                result = evaluate_model(label, full_pipe, X_val, y_val, X_test, y_test)
                results.append(result)
                tuned_models[label] = full_pipe
                print(f"    {label:25s} PR-AUC={result.metrics['pr_auc']:.3f}  "
                      f"P={result.metrics['precision']:.3f}  R={result.metrics['recall']:.3f}")
    else:
        print("Optuna not available, skipping Phase 2")
        tuned_models = {}

    # ── Phase 3: Stacking ensemble (top 3 models) ────────────────────────
    print("Phase 3: Building stacking ensemble...")
    # Select top-3 models by PR-AUC for stacking bases
    sorted_results = sorted(results, key=lambda r: r.metrics["pr_auc"], reverse=True)
    top3_names = [r.name for r in sorted_results[:3]]
    top3_models = {}
    for r in sorted_results[:3]:
        top3_models[r.name] = r.estimator

    # Build stacking ensemble on train data, evaluate on val/test
    try:
        stacker = StackingEnsemble(top3_models, n_folds=5)
        stacker.fit(X_train, y_train)
        stacking_result = evaluate_model("Stacking_Top3", stacker, X_val, y_val, X_test, y_test)
        results.append(stacking_result)
        print(f"  Stacking_Top3  PR-AUC={stacking_result.metrics['pr_auc']:.3f}  "
              f"P={stacking_result.metrics['precision']:.3f}  R={stacking_result.metrics['recall']:.3f}")
    except Exception as e:
        print(f"  Stacking failed: {e}")

    # ── Also add SoftVoting (simple average of top-3 probabilities) ──────
    try:
        class _SoftVoter:
            def __init__(self, models):
                self.models = models
            def fit(self, X, y): return self
            def predict_proba(self, X):
                probs = np.column_stack([m.predict_proba(X)[:, 1] for m in self.models.values()])
                avg = probs.mean(axis=1)
                return np.column_stack([1 - avg, avg])
        voter = _SoftVoter(top3_models)
        sv_result = evaluate_model("SoftVoting_Top3", voter, X_val, y_val, X_test, y_test)
        results.append(sv_result)
        print(f"  SoftVoting_Top3  PR-AUC={sv_result.metrics['pr_auc']:.3f}  "
              f"P={sv_result.metrics['precision']:.3f}  R={sv_result.metrics['recall']:.3f}")
    except Exception as e:
        print(f"  SoftVoting failed: {e}")

    # ── Phase 4: Role-based blended ensemble (ALL models) ────────────────
    print("Phase 4: Building role-blended ensemble (all models)...")
    try:
        # Collect every individually-trained model with its metrics
        all_models_with_metrics = [
            (r.name, r.estimator, r.metrics) for r in results
            if not r.name.startswith("Stacking") and not r.name.startswith("SoftVoting")
        ]
        if len(all_models_with_metrics) >= 3:
            blender = RoleBlendedEnsemble(all_models_with_metrics)
            blend_result = evaluate_model("RoleBlend_All", blender, X_val, y_val, X_test, y_test)
            results.append(blend_result)
            # Show role assignments
            for (name, _, role), w in zip(blender.models, blender.weights):
                print(f"    {name:25s} role={role:10s} weight={w:.3f}")
            print(f"  RoleBlend_All    PR-AUC={blend_result.metrics['pr_auc']:.3f}  "
                  f"P={blend_result.metrics['precision']:.3f}  R={blend_result.metrics['recall']:.3f}")
    except Exception as e:
        print(f"  RoleBlend failed: {e}")

    # ── Select best ──────────────────────────────────────────────────────
    results = sorted(results, key=lambda r: r.metrics["selection_score"], reverse=True)
    leaderboard = [{"model": r.name, **r.metrics} for r in results]
    print(f"\nBest model: {results[0].name} (score={results[0].metrics['selection_score']:.4f})")
    return results[0], leaderboard, feature_columns


def write_outputs(best: ModelResult, leaderboard: List[Dict[str, float]], feature_columns: List[str], forecast_raw: pd.DataFrame, forecast_features: pd.DataFrame) -> None:
    X_future, _ = feature_matrix(forecast_features, feature_columns)
    probabilities = best.calibrator.predict(best.estimator.predict_proba(X_future)[:, 1])

    out = forecast_raw.copy().sort_values(["Date", "City"]).reset_index(drop=True)
    # Override Lat/Lon with canonical CITIES coordinates (history data may have stale values)
    city_coords = {c: (lat, lon) for c, (lat, lon) in CITIES.items()}
    out["Latitude"] = out["City"].map(lambda c: city_coords.get(c, (np.nan, np.nan))[0])
    out["Longitude"] = out["City"].map(lambda c: city_coords.get(c, (np.nan, np.nan))[1])
    out["probability"] = probabilities
    out["confidence"] = out["probability"].map(_confidence)
    out["risk_level"] = out["probability"].map(_risk_level)
    out["predicted_fire"] = (out["probability"] >= best.threshold).astype(int)
    out["risk_score"] = (out["probability"] * 100).round(1)
    out["temperature"] = out["Temperature_C_mean"].round(1)
    out["wind"] = out["Wind_Speed_kmh_mean"].round(1)
    out["humidity"] = out["Humidity_percent_mean"].round(1)
    out["rain"] = out["Rain_mm_sum"].round(2)
    out["climate_summary"] = out.apply(_climate_summary, axis=1)
    out["warning"] = out.apply(_warning_text, axis=1)
    out["risk_color"] = out["risk_level"].map(RISK_COLORS)
    out["date"] = pd.to_datetime(out["Date"]).dt.strftime("%Y-%m-%d")
    out["region"] = out["City"]

    public_cols = [
        "date",
        "region",
        "risk_level",
        "probability",
        "confidence",
        "risk_score",
        "predicted_fire",
        "temperature",
        "wind",
        "humidity",
        "rain",
        "Temperature_C_mean",
        "Humidity_percent_mean",
        "Rain_mm_sum",
        "Wind_Speed_kmh_mean",
        "Pressure_hPa_mean",
        "Solar_Radiation_Wm2_mean",
        "Soil_Temp_C_mean",
        "Soil_Moisture_mean",
        "Latitude",
        "Longitude",
        "climate_summary",
        "warning",
        "risk_color",
    ]
    out_public = out[public_cols].copy()
    out_public.to_csv(OUTPUTS / "forecast_30_days.csv", index=False)
    (OUTPUTS / "forecast_30_days.json").write_text(out_public.to_json(orient="records", indent=2), encoding="utf-8")

    latest = out_public.sort_values("date").groupby("region", as_index=False).tail(1)
    map_points = latest.to_dict(orient="records")
    (OUTPUTS / "map_points.json").write_text(json.dumps(map_points, indent=2), encoding="utf-8")

    metrics = {
        "generated_at": pd.Timestamp.now(tz="Asia/Baku").isoformat(),
        "prediction_horizon_days": 30,
        "target": "Daily probability of a NASA FIRMS wildfire detection within the city risk area",
        "selected_model": best.name,
        "selected_threshold": best.threshold,
        "temporal_split": {
            "train": f"< {TRAIN_END.date()}",
            "validation": f"{TRAIN_END.date()} to {TEST_START.date()}",
            "test": f">= {TEST_START.date()}",
        },
        "leaderboard": leaderboard,
        "feature_count": len(feature_columns),
        "risk_levels": {
            "Low": "< 15%",
            "Moderate": "15% to 35%",
            "High": "35% to 60%",
            "Extreme": ">= 60%",
        },
        "data_sources": ["NASA FIRMS MODIS/VIIRS", "Open-Meteo ERA5/ERA5-Land", "Open-Elevation/static geography"],
    }
    (OUTPUTS / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    if FORECAST_168H.exists():
        hourly = pd.read_parquet(FORECAST_168H)
        hourly_out = pd.DataFrame({
            "timestamp": pd.to_datetime(hourly["Timestamp"]).dt.strftime("%Y-%m-%dT%H:%M"),
            "region": hourly["City"],
            "temperature": hourly["Temperature_C"].round(1),
            "humidity": hourly["Humidity_percent"].round(1),
            "wind": hourly["Wind_Speed_kmh"].round(1),
            "solar": hourly["Solar_Radiation_Wm2"].round(1),
        })
        (OUTPUTS / "hourly_forecast_168h.json").write_text(
            hourly_out.to_json(orient="records", indent=2), encoding="utf-8")

        # ── Hourly fire risk scoring ─────────────────────────────────────
        hourly_model_path = MODELS_F / "best_fire_model_hourly.joblib"
        hourly_manifest_path = MODELS_F / "model_manifest_hourly.json"
        hourly_feat_path = MODELS_F / "feature_columns_hourly.json"
        if hourly_model_path.exists() and hourly_feat_path.exists():
            import json as _json
            h_model = joblib.load(hourly_model_path)
            with open(hourly_feat_path) as _f:
                h_feature_cols = _json.load(_f)
            with open(hourly_manifest_path) as _f:
                h_manifest = _json.load(_f)
            h_threshold = h_manifest.get("optimal_threshold", 0.5)

            # ── Properly engineer hourly features (lag, rolling, calendar) ──
            from src.config import ENG_HOURLY, REFERENCE
            hf = hourly.copy()
            hf["Timestamp"] = pd.to_datetime(hf["Timestamp"])
            if "Date" not in hf.columns:
                hf["Date"] = hf["Timestamp"].dt.normalize()

            # Load hourly history for lag context
            h_hist = None
            if ENG_HOURLY.exists():
                h_hist = pd.read_parquet(ENG_HOURLY)
                h_hist["Timestamp"] = pd.to_datetime(h_hist["Timestamp"])
                h_hist = h_hist.sort_values(["City", "Timestamp"])

            # Static geography
            static_path = REFERENCE / "static_geography.parquet"
            if static_path.exists():
                static_geo = pd.read_parquet(static_path)
            else:
                static_geo = pd.DataFrame([
                    {"City": c, "Latitude": lat, "Longitude": lon,
                     "Elevation": 0, "Slope": 0, "Trees_pct": 0,
                     "Urban_pct": 0, "Pop_Total": 0, "NDBI": 0, "NDVI": 0, "EVI": 0}
                    for c, (lat, lon) in CITIES.items()])

            h_frames = []
            for city in sorted(hf["City"].unique()):
                cf = hf[hf["City"] == city].sort_values("Timestamp").copy()
                if h_hist is not None:
                    ch = h_hist[h_hist["City"] == city].sort_values("Timestamp")
                    cutoff = cf["Timestamp"].min() - pd.Timedelta(hours=168 * 2)
                    recent = ch[ch["Timestamp"] >= cutoff]
                    common = ["City", "Date", "Timestamp"] + [
                        c for c in cf.columns if c in recent.columns
                        and c not in ("City", "Date", "Timestamp")]
                    combo = pd.concat([recent[common], cf[common]],
                                      ignore_index=True)
                    combo = combo.sort_values("Timestamp").drop_duplicates(
                        ["City", "Timestamp"], keep="last")
                else:
                    combo = cf.copy()

                # Calendar features
                combo["Year"] = combo["Timestamp"].dt.year
                combo["Month"] = combo["Timestamp"].dt.month
                combo["DayOfYear"] = combo["Timestamp"].dt.dayofyear
                combo["DayOfWeek"] = combo["Timestamp"].dt.dayofweek
                combo["Hour"] = combo["Timestamp"].dt.hour
                combo["WeekOfYear"] = combo["Timestamp"].dt.isocalendar().week.astype(int)
                combo["Hour_sin"] = np.sin(2 * np.pi * combo["Hour"] / 24)
                combo["Hour_cos"] = np.cos(2 * np.pi * combo["Hour"] / 24)
                combo["Month_sin"] = np.sin(2 * np.pi * combo["Month"] / 12)
                combo["Month_cos"] = np.cos(2 * np.pi * combo["Month"] / 12)
                combo["DoY_sin"] = np.sin(2 * np.pi * combo["DayOfYear"] / 365)
                combo["DoY_cos"] = np.cos(2 * np.pi * combo["DayOfYear"] / 365)
                combo["DoW_sin"] = np.sin(2 * np.pi * combo["DayOfWeek"] / 7)
                combo["DoW_cos"] = np.cos(2 * np.pi * combo["DayOfWeek"] / 7)
                combo["Season"] = combo["Month"].map(
                    {12: 0, 1: 0, 2: 0, 3: 1, 4: 1, 5: 1,
                     6: 2, 7: 2, 8: 2, 9: 3, 10: 3, 11: 3})
                combo["is_summer"] = combo["Month"].isin([6, 7, 8]).astype(int)
                combo["is_winter"] = combo["Month"].isin([12, 1, 2]).astype(int)
                combo["is_fire_season"] = combo["Month"].isin([5, 6, 7, 8, 9]).astype(int)
                combo["is_daytime"] = combo["Hour"].between(6, 20).astype(int)

                # Hourly lag/rolling features
                hvars = [c for c in ["Temperature_C", "Humidity_percent",
                                     "Wind_Speed_kmh", "Rain_mm",
                                     "Solar_Radiation_Wm2"]
                         if c in combo.columns]
                for v in hvars:
                    for lag in [1, 3, 6, 12, 24]:
                        combo[f"{v}_lag{lag}h"] = combo[v].shift(lag)
                    for w in [6, 12, 24]:
                        shifted = combo[v].shift(1)
                        combo[f"{v}_roll{w}h_mean"] = shifted.rolling(w, min_periods=1).mean()
                        combo[f"{v}_roll{w}h_std"] = shifted.rolling(w, min_periods=2).std()

                # Static geography merge
                combo = combo.merge(static_geo, on="City", how="left",
                                    suffixes=("", "_static"))
                for sc in static_geo.columns:
                    if sc == "City":
                        continue
                    if f"{sc}_static" in combo.columns:
                        combo[sc] = combo[sc].fillna(combo[f"{sc}_static"])
                        combo = combo.drop(columns=[f"{sc}_static"])

                h_frames.append(combo[combo["Timestamp"].isin(cf["Timestamp"].values)])

            h_feat_df = pd.concat(h_frames, ignore_index=True)

            # ── Fill missing features with historical city medians (not zero) ──
            # Zero-filling weather features like Soil_Moisture causes extreme
            # predictions because the model interprets 0 as severe drought.
            # Only fill columns that were missing entirely or are base weather
            # variables that should never be zero.
            CALENDAR_COLS = {"is_daytime", "is_summer", "is_fire_season",
                             "Season", "Hour", "Month", "DayOfWeek",
                             "DayOfYear", "Year", "WeekOfYear",
                             "Hour_sin", "Hour_cos", "Month_sin",
                             "DoY_cos", "DoW_sin", "DoW_cos"}
            BASE_WEATHER_H = {"Rain_mm", "Wind_Speed_kmh", "Wind_Dir_deg",
                              "Solar_Radiation_Wm2", "Soil_Temp_C",
                              "Soil_Moisture", "NDBI", "NDVI", "EVI",
                              "Elevation", "Slope", "Trees_pct", "Urban_pct"}

            newly_added = set()
            for col in h_feature_cols:
                if col not in h_feat_df.columns:
                    h_feat_df[col] = np.nan
                    newly_added.add(col)

            if h_hist is not None:
                city_medians = h_hist.groupby("City").median(numeric_only=True)
                for col in h_feature_cols:
                    if col in CALENDAR_COLS:
                        h_feat_df[col] = h_feat_df[col].fillna(0)
                        continue
                    # Fill if: column was newly added OR is a base weather var
                    should_fill = col in newly_added or col in BASE_WEATHER_H
                    if should_fill and col in city_medians.columns:
                        mask = h_feat_df[col].isna()
                        if col in BASE_WEATHER_H:
                            mask = mask | (h_feat_df[col] == 0)
                        if mask.any():
                            h_feat_df.loc[mask, col] = h_feat_df.loc[mask, "City"].map(
                                city_medians[col]).values
                    # For lag/rolling NaN, fill with column median (not zero)
                    elif h_feat_df[col].isna().any() and col in city_medians.columns:
                        h_feat_df[col] = h_feat_df[col].fillna(
                            h_feat_df["City"].map(city_medians[col]))
                h_feat_df = h_feat_df.fillna(0)
            else:
                for col in h_feature_cols:
                    if col not in h_feat_df.columns:
                        h_feat_df[col] = 0
                h_feat_df = h_feat_df.fillna(0)

            X_h = h_feat_df[h_feature_cols]
            h_proba = h_model.predict_proba(X_h)[:, 1]

            # ── Daily-anchored ceiling ──────────────────────────────────
            # The hourly model may over-predict when features are approximate.
            # Cap each city-day's hourly prob at max(daily_prob * 2.5, 0.15).
            daily_prob_dict = {}
            for _, dr in out.iterrows():
                dkey = (dr["City"], pd.Timestamp(dr["Date"]).normalize())
                daily_prob_dict[dkey] = dr["probability"]
            h_dates = pd.to_datetime(hourly_out["timestamp"]).dt.normalize()
            for i in range(len(hourly_out)):
                city = hourly_out.iloc[i]["region"]
                day = h_dates.iloc[i]
                daily_p = daily_prob_dict.get((city, day))
                if daily_p is not None:
                    ceiling = max(daily_p * 2.5, 0.15)
                    h_proba[i] = min(h_proba[i], ceiling)

            hourly_out["probability"] = h_proba.round(4)
            hourly_out["risk_level"] = hourly_out["probability"].map(_risk_level)
            hourly_out["risk_score"] = (h_proba * 100).round(1)
            hourly_out["predicted_fire"] = (h_proba >= h_threshold).astype(int)
            hourly_out["confidence"] = hourly_out["probability"].map(_confidence)
            hourly_out["risk_color"] = hourly_out["risk_level"].map(RISK_COLORS)

            # Add hourly model info to metrics
            metrics["hourly_model"] = {
                "model_name": h_manifest.get("model_name", "Unknown"),
                "optimal_threshold": h_threshold,
                "prediction_horizon_hours": 168,
                "n_features": len(h_feature_cols),
            }
            (OUTPUTS / "metrics.json").write_text(
                json.dumps(metrics, indent=2), encoding="utf-8")

        # Add Latitude/Longitude for map rendering
        coords = {c: (lat, lon) for c, (lat, lon) in CITIES.items()}
        hourly_out["Latitude"] = hourly_out["region"].map(lambda r: coords.get(r, (0, 0))[0])
        hourly_out["Longitude"] = hourly_out["region"].map(lambda r: coords.get(r, (0, 0))[1])

        # Re-write with fire risk columns included
        (OUTPUTS / "hourly_forecast_168h.json").write_text(
            hourly_out.to_json(orient="records", indent=2), encoding="utf-8")

    # ── Copy outputs to dashboard/data/ ──────────────────────────────────
    import shutil
    for dest_dir in [Path(OUTPUTS).parent / "dashboard" / "data",
                     Path(OUTPUTS).parent / "docs" / "data"]:
        dest_dir.mkdir(parents=True, exist_ok=True)
        for fname in ["forecast_30_days.csv", "forecast_30_days.json",
                       "metrics.json", "hourly_forecast_168h.json"]:
            src_f = OUTPUTS / fname
            if src_f.exists():
                shutil.copy2(src_f, dest_dir / fname)

    MODELS_F.mkdir(parents=True, exist_ok=True)
    joblib.dump(
        {"model": best.estimator, "calibrator": best.calibrator, "threshold": best.threshold, "features": feature_columns},
        MODELS_F / "forecast_compatible_fire_model.joblib",
    )


def score_only() -> None:
    """Load the saved champion model and score the current weather forecast.

    Does NOT retrain.  Requires:
      - models/wildfire/forecast_compatible_fire_model.joblib
      - outputs/weather_forecast_30d.parquet
    Writes dashboard outputs the same way as main().
    """
    OUTPUTS.mkdir(parents=True, exist_ok=True)
    model_path = MODELS_F / "forecast_compatible_fire_model.joblib"
    if not model_path.exists():
        raise FileNotFoundError(
            f"No saved model at {model_path}. Run with --mode train first."
        )

    bundle = joblib.load(model_path)
    estimator:    Pipeline            = bundle["model"]
    calibrator:   IsotonicRegression  = bundle["calibrator"]
    threshold:    float               = bundle["threshold"]
    feature_cols: List[str]           = bundle["features"]

    best = ModelResult(
        name="loaded_champion",
        estimator=estimator,
        calibrator=calibrator,
        threshold=threshold,
        metrics={},
    )

    raw = pd.read_parquet(ENG_DAILY)
    raw["Date"] = pd.to_datetime(raw["Date"])
    forecast_raw, forecast_features = load_forecast_frame(raw)
    write_outputs(best, [], feature_cols, forecast_raw, forecast_features)
    print(f"Score-only complete. Outputs → {OUTPUTS}")


def main() -> None:
    """Full train-and-score pipeline (re-trains every time)."""
    OUTPUTS.mkdir(parents=True, exist_ok=True)
    MODELS_F.mkdir(parents=True, exist_ok=True)
    raw = pd.read_parquet(ENG_DAILY)
    raw["Date"] = pd.to_datetime(raw["Date"])
    train_features = load_training_frame()
    best, leaderboard, feature_columns = train_and_select(train_features)
    forecast_raw, forecast_features = load_forecast_frame(raw)
    write_outputs(best, leaderboard, feature_columns, forecast_raw, forecast_features)
    print(f"Selected model: {best.name}")
    print(f"Outputs written to: {OUTPUTS}")
