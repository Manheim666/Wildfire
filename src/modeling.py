"""
WildFire Prediction — Modeling Utilities
=========================================
Model factories, training helpers, hyperparameter search utilities.
"""
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings("ignore")

from sklearn.ensemble import (
    RandomForestRegressor, ExtraTreesRegressor,
    HistGradientBoostingRegressor,
)
from sklearn.linear_model import Ridge, ElasticNet, LogisticRegression
from sklearn.calibration import CalibratedClassifierCV

from src.config import XGB_GPU_PARAMS, CB_GPU_PARAMS, LGB_GPU_PARAMS, GPU_AVAILABLE

SEED = 42


# ═══════════════════════════════════════════════════════════════════════════
# Weather Model Factory
# ═══════════════════════════════════════════════════════════════════════════

def get_weather_models():
    """Return dict of {name: model} for weather forecasting comparison."""
    models = {
        "Ridge": Ridge(alpha=1.0),
        "ElasticNet": ElasticNet(alpha=0.1, l1_ratio=0.5, max_iter=2000),
        "RandomForest": RandomForestRegressor(
            n_estimators=300, max_depth=15, min_samples_split=5,
            random_state=SEED, n_jobs=-1),
        "ExtraTrees": ExtraTreesRegressor(
            n_estimators=300, max_depth=15, min_samples_split=5,
            random_state=SEED, n_jobs=-1),
        "HistGBR": HistGradientBoostingRegressor(
            max_iter=500, max_depth=8, learning_rate=0.05,
            random_state=SEED),
    }

    try:
        import xgboost as xgb
        models["XGBoost"] = xgb.XGBRegressor(
            n_estimators=500, max_depth=8, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8,
            random_state=SEED, n_jobs=-1, verbosity=0,
            **XGB_GPU_PARAMS)
    except ImportError:
        pass

    try:
        import lightgbm as lgb
        models["LightGBM"] = lgb.LGBMRegressor(
            n_estimators=500, max_depth=8, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8,
            random_state=SEED, n_jobs=-1, verbose=-1,
            **LGB_GPU_PARAMS)
    except ImportError:
        pass

    try:
        import catboost as cb
        models["CatBoost"] = cb.CatBoostRegressor(
            iterations=500, depth=8, learning_rate=0.05,
            random_seed=SEED, verbose=0,
            **CB_GPU_PARAMS)
    except ImportError:
        pass

    return models


# ═══════════════════════════════════════════════════════════════════════════
# Fire Detection Model Factory
# ═══════════════════════════════════════════════════════════════════════════

def get_fire_models(imbalance_ratio=10.0):
    """Return dict of {name: (model, imbalance_strategy)} for fire detection.

    Final selection: 1 baseline + 3 strong gradient boosters.
    Other models (MLP, TabNet, EasyEnsemble, BalancedRF) were tested during
    experimentation and found to be redundant or inferior; they are excluded
    from the final pipeline to keep the codebase clean.
    """
    models = {}

    # ── Baseline: LogisticRegression ──────────────────────────────────────
    models["LogReg_baseline"] = (
        LogisticRegression(class_weight="balanced", max_iter=1000,
                           random_state=SEED, n_jobs=-1),
        "class_weight=balanced")

    # ── Candidate 1: CatBoost (best PR-AUC in experiments) ───────────────
    try:
        import catboost as cb
        models["CatBoost"] = (
            cb.CatBoostClassifier(
                iterations=500, depth=8, learning_rate=0.05,
                auto_class_weights="Balanced", eval_metric="F1",
                random_seed=SEED, verbose=0,
                **CB_GPU_PARAMS),
            "auto_class_weights=Balanced")
    except ImportError:
        pass

    # ── Candidate 2: XGBoost (strong recall specialist) ──────────────────
    try:
        import xgboost as xgb
        models["XGBoost"] = (
            xgb.XGBClassifier(
                n_estimators=500, max_depth=8, learning_rate=0.05,
                subsample=0.8, colsample_bytree=0.8,
                scale_pos_weight=imbalance_ratio,
                eval_metric="aucpr", random_state=SEED,
                use_label_encoder=False, n_jobs=-1,
                **XGB_GPU_PARAMS),
            f"scale_pos_weight={imbalance_ratio:.1f}")
    except ImportError:
        pass

    # ── Candidate 3: LightGBM (fast, good generalization) ────────────────
    try:
        import lightgbm as lgb
        models["LightGBM"] = (
            lgb.LGBMClassifier(
                n_estimators=500, max_depth=8, learning_rate=0.05,
                subsample=0.8, colsample_bytree=0.8,
                is_unbalance=True, random_state=SEED, n_jobs=-1, verbose=-1,
                **LGB_GPU_PARAMS),
            "is_unbalance=True")
    except ImportError:
        pass

    return models


def calibrate_model(model, X_val, y_val, method="isotonic"):
    """Return a CalibratedClassifierCV wrapper."""
    cal = CalibratedClassifierCV(model, method=method, cv="prefit")
    cal.fit(X_val, y_val)
    return cal
