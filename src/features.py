"""
WildFire Prediction — Feature Engineering Utilities
====================================================
Reusable functions for creating weather and wildfire features.
"""
import numpy as np
import pandas as pd


# ═══════════════════════════════════════════════════════════════════════════
# Calendar / Cyclical Features
# ═══════════════════════════════════════════════════════════════════════════

def add_calendar_features(df, date_col="Date"):
    """Add calendar and cyclical time features."""
    dt = pd.to_datetime(df[date_col])
    df["Year"]       = dt.dt.year
    df["Month"]      = dt.dt.month
    df["DayOfYear"]  = dt.dt.dayofyear
    df["DayOfWeek"]  = dt.dt.dayofweek
    df["WeekOfYear"] = dt.dt.isocalendar().week.astype(int)

    # Cyclical encodings
    df["Month_sin"]  = np.sin(2 * np.pi * df["Month"] / 12)
    df["Month_cos"]  = np.cos(2 * np.pi * df["Month"] / 12)
    df["DoY_sin"]    = np.sin(2 * np.pi * df["DayOfYear"] / 365)
    df["DoY_cos"]    = np.cos(2 * np.pi * df["DayOfYear"] / 365)
    df["DoW_sin"]    = np.sin(2 * np.pi * df["DayOfWeek"] / 7)
    df["DoW_cos"]    = np.cos(2 * np.pi * df["DayOfWeek"] / 7)

    # Season flags
    df["Season"] = df["Month"].map(
        {12: 0, 1: 0, 2: 0, 3: 1, 4: 1, 5: 1,
         6: 2, 7: 2, 8: 2, 9: 3, 10: 3, 11: 3})
    df["is_summer"]      = df["Month"].isin([6, 7, 8]).astype(int)
    df["is_winter"]      = df["Month"].isin([12, 1, 2]).astype(int)
    df["is_fire_season"] = df["Month"].isin([5, 6, 7, 8, 9]).astype(int)
    return df


def add_hourly_calendar(df, ts_col="Timestamp"):
    """Add hour-level cyclical features."""
    dt = pd.to_datetime(df[ts_col])
    df["Hour"]      = dt.dt.hour
    df["Hour_sin"]  = np.sin(2 * np.pi * df["Hour"] / 24)
    df["Hour_cos"]  = np.cos(2 * np.pi * df["Hour"] / 24)
    df["is_daytime"] = df["Hour"].between(6, 20).astype(int)
    return df


# ═══════════════════════════════════════════════════════════════════════════
# Lag & Rolling Features
# ═══════════════════════════════════════════════════════════════════════════

def build_lag_features(group, variables, lag_days, date_col="Date"):
    """Create lag features for a single city group (pre-sorted by date)."""
    g = group.sort_values(date_col).copy()
    for var in variables:
        if var not in g.columns:
            continue
        for lag in lag_days:
            g[f"{var}_lag{lag}"] = g[var].shift(lag)
    return g


def build_rolling_features(group, variables, windows, date_col="Date"):
    """Create rolling mean/std features (shifted by 1 to prevent leakage)."""
    g = group.sort_values(date_col).copy()
    for var in variables:
        if var not in g.columns:
            continue
        shifted = g[var].shift(1)
        for w in windows:
            rolled = shifted.rolling(w, min_periods=1)
            g[f"{var}_roll{w}_mean"] = rolled.mean()
            g[f"{var}_roll{w}_std"]  = rolled.std()
    return g


# ═══════════════════════════════════════════════════════════════════════════
# Fire Weather Index (FWI) Proxy
# ═══════════════════════════════════════════════════════════════════════════

def compute_fwi_proxy(group, date_col="Date"):
    """Compute simplified Canadian FWI system proxies for one city."""
    g = group.sort_values(date_col).copy()

    T = g.get("Temperature_C_mean", pd.Series(0, index=g.index))
    H = g.get("Humidity_percent_mean", pd.Series(50, index=g.index))
    W = g.get("Wind_Speed_kmh_mean", pd.Series(0, index=g.index))
    R = g.get("Rain_mm_sum", pd.Series(0, index=g.index))

    # Fine Fuel Moisture Code
    g["FFMC_proxy"] = (100 - (H * 0.5 + R.clip(0, 10) * 3 - T.clip(0, 40) * 0.5)).clip(0, 100)
    # Duff Moisture Code
    g["DMC_proxy"] = (T.clip(0) * 0.3 - R * 0.8 + (100 - H) * 0.1).rolling(14, min_periods=1).mean().clip(0)
    # Drought Code
    g["DC_proxy"] = (T.clip(0) * 0.2 - R * 0.5).rolling(30, min_periods=1).sum().clip(0)
    # Initial Spread Index
    g["ISI_proxy"] = (g["FFMC_proxy"] / 100) * (W * 0.3)
    # Build-Up Index
    g["BUI_proxy"] = (g["DMC_proxy"] + g["DC_proxy"]) / 2
    # Fire Weather Index
    g["FWI_proxy"] = (g["ISI_proxy"] * g["BUI_proxy"] / 50).clip(0)

    return g


# ═══════════════════════════════════════════════════════════════════════════
# Wildfire-Specific Features
# ═══════════════════════════════════════════════════════════════════════════

def compute_vpd(temp_c, rh_pct):
    """Vapor Pressure Deficit (kPa) — higher = drier air = more fire risk."""
    es = 0.6108 * np.exp((17.27 * temp_c) / (temp_c + 237.3))
    ea = es * (rh_pct / 100.0)
    return (es - ea).clip(lower=0)


def compute_dew_point(temp_c, rh_pct):
    """Approximate dew point temperature (Magnus formula)."""
    a, b = 17.27, 237.3
    rh_safe = rh_pct.clip(lower=1) / 100.0
    alpha = (a * temp_c) / (b + temp_c) + np.log(rh_safe)
    return (b * alpha) / (a - alpha)


def compute_heat_index(temp_c, rh_pct):
    """Simplified heat index (valid for T > 27°C)."""
    T = temp_c * 9 / 5 + 32  # to Fahrenheit
    HI = (-42.379 + 2.04901523 * T + 10.14333127 * rh_pct
           - 0.22475541 * T * rh_pct - 6.83783e-3 * T**2
           - 5.481717e-2 * rh_pct**2 + 1.22874e-3 * T**2 * rh_pct
           + 8.5282e-4 * T * rh_pct**2 - 1.99e-6 * T**2 * rh_pct**2)
    HI_C = (HI - 32) * 5 / 9
    # Only valid when temp > 27°C
    return np.where(temp_c > 27, HI_C, temp_c)


def add_wildfire_weather_features(df):
    """Add VPD, dew point, heat index, drought proxy, dry-day features."""
    T = df.get("Temperature_C_mean", df.get("Temperature_C", pd.Series(0, index=df.index)))
    H = df.get("Humidity_percent_mean", df.get("Humidity_percent", pd.Series(50, index=df.index)))
    R = df.get("Rain_mm_sum", df.get("Rain_mm", pd.Series(0, index=df.index)))
    W = df.get("Wind_Speed_kmh_mean", df.get("Wind_Speed_kmh", pd.Series(0, index=df.index)))

    df["VPD_kPa"]    = compute_vpd(T, H)
    df["Dew_Point_C"] = compute_dew_point(T, H)
    df["Heat_Index"]  = compute_heat_index(T, H)

    # Drought proxy: rolling 30-day rainfall deficit from long-term monthly mean
    if "Rain_mm_sum" in df.columns:
        df["Rain_roll30_sum"] = df.groupby("City")["Rain_mm_sum"].transform(
            lambda x: x.shift(1).rolling(30, min_periods=1).sum())
        monthly_avg = df.groupby(["City", "Month"])["Rain_mm_sum"].transform("mean")
        df["Rainfall_Deficit"] = monthly_avg * 30 - df["Rain_roll30_sum"].fillna(0)

    # Dry-day streak
    if "Rain_mm_sum" in df.columns:
        def _dry_streak(s):
            is_dry = (s < 0.1).astype(int)
            groups = is_dry.ne(is_dry.shift()).cumsum()
            return is_dry.groupby(groups).cumsum()
        df["dry_days_streak"] = df.groupby("City")["Rain_mm_sum"].transform(_dry_streak)

    # Extreme weather flags
    df["heatwave_flag"]   = (T > T.quantile(0.95)).astype(int)
    df["low_humidity_flag"] = (H < H.quantile(0.10)).astype(int)
    df["high_wind_flag"]  = (W > W.quantile(0.90)).astype(int)
    df["dry_spell_flag"]  = (df.get("dry_days_streak", pd.Series(0, index=df.index)) >= 7).astype(int)

    # Interaction features
    df["temp_x_low_hum"]   = T * (100 - H) / 100
    df["temp_x_wind"]      = T * W / 100
    df["dry_days_x_wind"]  = df.get("dry_days_streak", 0) * W / 100
    df["hot_dry_windy"]    = df["heatwave_flag"] * df["low_humidity_flag"] * df["high_wind_flag"]

    return df


def add_historical_fire_features(df, date_col="Date"):
    """Add historical fire-count features per city (strictly lagged)."""
    if "Fire_Occurred" not in df.columns:
        return df

    df = df.sort_values(["City", date_col]).copy()

    for window in [7, 14, 30, 90]:
        df[f"fire_count_{window}d"] = df.groupby("City")["Fire_Occurred"].transform(
            lambda x: x.shift(1).rolling(window, min_periods=1).sum())

    # Days since last fire
    def _days_since_fire(s):
        result = pd.Series(np.nan, index=s.index)
        last_fire_idx = None
        for i, (idx, val) in enumerate(s.items()):
            if last_fire_idx is not None:
                result.iloc[i] = i - last_fire_idx
            else:
                result.iloc[i] = 9999  # no fire seen yet
            if val == 1:
                last_fire_idx = i
        return result

    df["days_since_last_fire"] = df.groupby("City")["Fire_Occurred"].transform(_days_since_fire)
    df["days_since_last_fire"] = df["days_since_last_fire"].clip(upper=365)

    # Same-month historical fire rate per city
    df["city_month_fire_rate"] = df.groupby(["City", "Month"])["Fire_Occurred"].transform(
        lambda x: x.shift(1).expanding().mean())
    df["city_month_fire_rate"] = df["city_month_fire_rate"].fillna(0)

    # City overall historical fire rate
    df["city_fire_rate"] = df.groupby("City")["Fire_Occurred"].transform(
        lambda x: x.shift(1).expanding().mean())
    df["city_fire_rate"] = df["city_fire_rate"].fillna(0)

    return df


def add_vegetation_interactions(df):
    """Create interaction features between vegetation and weather."""
    ndvi = df.get("NDVI", pd.Series(0, index=df.index))
    drought = df.get("Rainfall_Deficit", df.get("DC_proxy", pd.Series(0, index=df.index)))
    dry = df.get("dry_days_streak", pd.Series(0, index=df.index))
    trees = df.get("Trees_pct", pd.Series(0, index=df.index))

    df["NDVI_x_drought"]     = ndvi * drought / 100
    df["forest_x_dry_days"]  = trees * dry / 100
    df["NDVI_x_VPD"]         = ndvi * df.get("VPD_kPa", 0)

    return df


# ═══════════════════════════════════════════════════════════════════════════
# Anomaly Features
# ═══════════════════════════════════════════════════════════════════════════

def add_anomaly_features(df, variables=None, train_monthly_means=None):
    """Compute anomaly = value - city monthly mean.

    To avoid leakage: pass train_monthly_means computed from training data only.
    If None, computes from df itself (use only during EDA, not during model training).

    train_monthly_means: dict {(city, month): {variable: mean_value}}
    """
    if variables is None:
        variables = ["Temperature_C_mean", "Humidity_percent_mean", "Rain_mm_sum"]
    for var in variables:
        if var not in df.columns:
            continue
        if train_monthly_means is not None:
            def _lookup_mean(row, v=var):
                key = (row.get("City", ""), row.get("Month", 0))
                city_month = train_monthly_means.get(key, {})
                return city_month.get(v, np.nan)
            means = df.apply(_lookup_mean, axis=1)
        else:
            means = df.groupby(["City", "Month"])[var].transform("mean")
        df[f"{var}_anomaly"] = df[var] - means
    return df


def compute_train_monthly_means(train_df, variables=None):
    """Compute city×month means from training data for leak-free anomaly features.

    Returns dict {(city, month): {variable: mean_value}}.
    """
    if variables is None:
        variables = ["Temperature_C_mean", "Humidity_percent_mean", "Rain_mm_sum"]
    result = {}
    for (city, month), grp in train_df.groupby(["City", "Month"]):
        result[(city, month)] = {v: grp[v].mean() for v in variables if v in grp.columns}
    return result
