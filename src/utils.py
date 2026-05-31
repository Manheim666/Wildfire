"""
WildFire Prediction — General Utilities
===============================================
Helpers for data loading, saving, and misc operations.
"""
import json, warnings
import numpy as np
import pandas as pd
from pathlib import Path

warnings.filterwarnings("ignore")


def load_parquet_safe(path, fallback_path=None, date_cols=None):
    """Load parquet with fallback and date parsing."""
    p = Path(path)
    if p.exists():
        df = pd.read_parquet(p)
    elif fallback_path and Path(fallback_path).exists():
        df = pd.read_parquet(Path(fallback_path))
        print(f"  ⚠ Using fallback: {fallback_path}")
    else:
        raise FileNotFoundError(f"Neither {path} nor {fallback_path} exist")

    if date_cols:
        for c in date_cols:
            if c in df.columns:
                df[c] = pd.to_datetime(df[c])
    return df


def save_model_artifact(obj, path, format="joblib"):
    """Save model in joblib or json format."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if format == "json" and hasattr(obj, "save_model"):
        obj.save_model(str(path))
    elif format == "joblib":
        from joblib import dump
        dump(obj, path)
    else:
        import pickle
        with open(path, "wb") as f:
            pickle.dump(obj, f)
    print(f"  Saved: {path}")


def load_model_artifact(path, format="joblib"):
    """Load model from disk."""
    path = Path(path)
    if format == "json":
        import xgboost as xgb
        model = xgb.XGBClassifier()
        model.load_model(str(path))
        return model
    elif format == "joblib":
        from joblib import load
        return load(path)
    else:
        import pickle
        with open(path, "rb") as f:
            return pickle.load(f)


def get_numeric_features(df, drop_cols=None):
    """Get list of numeric feature columns, excluding drop_cols."""
    if drop_cols is None:
        drop_cols = []
    return [c for c in df.columns
            if c not in drop_cols
            and df[c].dtype in ["float64", "float32", "int64", "int32"]]


def reduce_mem_usage(df):
    """Downcast numeric columns to reduce memory."""
    for col in df.select_dtypes(include=["float64"]).columns:
        df[col] = pd.to_numeric(df[col], downcast="float")
    for col in df.select_dtypes(include=["int64"]).columns:
        df[col] = pd.to_numeric(df[col], downcast="integer")
    return df
