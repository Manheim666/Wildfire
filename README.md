# WildFire — Azerbaijan Wildfire Risk Intelligence Platform

> **End-to-end wildfire risk intelligence pipeline for 16 Azerbaijani cities.**
> Six sequential Jupyter notebooks and a shared `src/` Python module covering data ingestion, exploratory analysis and feature engineering, weather forecasting, wildfire detection (multi-model + Optuna + SHAP), 30-day risk prediction with interactive maps, and climate trend analysis — producing publication-quality visualizations, automated hypothesis reports, and a demo-ready geospatial dashboard.

Runs identically on **Google Colab** and **local environments** (JupyterLab / VS Code) with zero configuration changes.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Team & Task Allocation](#2-team--task-allocation)
3. [10-Day Project Timeline](#3-10-day-project-timeline)
4. [Pipeline Architecture](#4-pipeline-architecture)
5. [Notebook Descriptions](#5-notebook-descriptions)
6. [Shared `src/` Module](#6-shared-src-module)
7. [Folder Structure](#7-folder-structure)
8. [Key Definitions & Glossary](#8-key-definitions--glossary)
9. [Setup & Execution](#9-setup--execution)
10. [Dependencies](#10-dependencies)
11. [Cities Covered](#11-cities-covered)
12. [Data Sources & Provenance](#12-data-sources--provenance)
13. [Key Outputs & Deliverables](#13-key-outputs--deliverables)
14. [Evaluation Protocol & Model Targets](#14-evaluation-protocol--model-targets)

---

## 1. Project Overview

**WildFire** builds a complete, production-grade data science pipeline that:

- **Ingests** multi-source data: Open-Meteo weather APIs (ERA5 + ERA5-Land, historical + forecast), NASA FIRMS satellite fire detections (MODIS C6.1, VIIRS C2 — 3 sensors), Open-Elevation terrain, and GEE vegetation indices
- **Explores & engineers** 200+ features through city-by-city statistical analysis, FWI fire weather indices, VPD/dew point/heat index calculations, lag/rolling aggregates (1–30 day), Prophet seasonal residuals, and cyclical time encodings
- **Forecasts** 8 weather variables over daily (30-day) and hourly (168-hour) horizons using Prophet + XGBoost stacking ensembles with multi-model comparison (Ridge, ElasticNet, RF, ExtraTrees, HistGBR, XGB, LGB, CatBoost)
- **Detects wildfires** using 9 classification models (LogReg, RF, ExtraTrees, HistGBC, XGBoost, LightGBM, CatBoost, BalancedRF, EasyEnsemble) with cost-sensitive learning, conservative SMOTE, Optuna hyperparameter tuning (precision-constrained), isotonic probability calibration, and SHAP explainability
- **Predicts 30-day wildfire risk** per city using weather forecasts + trained fire model — 4-tier risk classification (Low / Moderate / High / Extreme)
- **Generates** interactive Folium risk maps (date-selectable + heatmap layers), Plotly animated dashboards, city-level timelines, and automated hypothesis reports
- **Analyses climate trends** — forecast vs last year, vs decade average, long-term warming signals, jury-ready conclusions
- **Evaluates rigorously** with 3-way temporal split (train/val/test), overfitting diagnostics, confusion matrices, PR-AUC curves, and feature importance rankings

All reusable functions are factored into a shared **`src/` Python module** (configuration, feature builders, model factories, evaluation metrics, visualization helpers) that every notebook imports — keeping the notebooks focused on analysis narrative while the logic stays DRY and testable.

---

## 2. Team & Task Allocation

| Team Member | Role | Assigned Tasks |
|---|---|---|
| **Asif Habilov** | Data Ingestion / ML Engineering / Architecture | T1 — Data ingestion pipeline (NB1); T5 — Multi-model fire classifier & calibration (NB4); T8 — ML optimization & `src/` module design |
| **Raul Ibrahimov** | Data Ingestion / Presentation Design | T2 — Fire label normalization & static geography (NB1); T10 — Presentation design & delivery; T11 — Documentation & README |
| **Ilaha Shafizada** | Data Analysis / Feature Engineering | T3 — Exploratory data analysis (NB2 §1–§5); T6 — FWI & lag feature engineering (NB2 §7); T9 — Outlier detection & data quality |
| **Nurana Aliyarli** | Data Analysis / Feature Engineering | T4 — Fire-weather relationship analysis (NB2 §6); T7 — Calendar & rolling feature engineering (NB2 §7); T9 — Outlier detection & data quality |
| **Aysu Mammadova** | Data Analysis / ML Engineering | T5 — Weather forecasting ensemble (NB3); T8 — Risk prediction & hypothesis testing (NB5–NB6); T12 — Geospatial visualization |

### Task Breakdown

| ID | Task | Description | Owner(s) | Notebook |
|----|------|-------------|----------|----------|
| T1 | Weather Data Ingestion | Historical hourly weather (Open-Meteo Archive), live 16-day forecast, HTTP caching & retry logic | Asif, Raul | NB1 |
| T2 | Fire Labels & Geography | NASA FIRMS archive parsing, daily fire-label normalization (20 km buffer), Open-Elevation terrain fetch | Raul | NB1 |
| T3 | Exploratory Data Analysis | Per-city quality audit, descriptive statistics, distribution analysis, correlation heatmaps, seasonal decomposition | Ilaha | NB2 |
| T4 | Fire-Weather Analysis | Fire-day vs. non-fire-day comparison per city, Welch's t-test for significance, feature–fire correlation ranking | Nurana | NB2 |
| T5 | Weather Forecasting Ensemble | Prophet + XGBoost stacking per city per feature, multi-model comparison, 30-day predictions | Aysu | NB3 |
| T6 | FWI Feature Engineering | FFMC, DMC, DC, ISI, BUI, FWI index computation; VPD, dew point, heat index; dry-spell tracking | Ilaha | NB2 |
| T7 | Temporal Feature Engineering | Lag features (1–30 day), rolling statistics (3/7/14/30 day), cyclical calendar encodings, seasonal flags | Nurana | NB2 |
| T8 | Wildfire Detection & Evaluation | 8 classifiers + Optuna tuning + SHAP, isotonic calibration, PR-AUC evaluation, threshold selection | Asif, Aysu | NB4 |
| T9 | Data Quality & Outlier Detection | IQR-based outlier analysis per city, missing-value audit, data integrity validation | Ilaha, Nurana | NB2 |
| T10 | Presentation Design & Delivery | Slide deck creation, narrative structure, visual design, rehearsal, final presentation | Raul | — |
| T11 | Documentation & `src/` Module | README, code comments, shared module documentation, reproducibility guide | Raul, Asif | — |
| T12 | Geospatial Visualization | Folium risk maps, Plotly animated dashboards, city-level timelines, climate report figures | Aysu | NB5, NB6 |

---

## 3. 10-Day Project Timeline

| Day | Phase | Milestones | Team Focus |
|-----|-------|-----------|------------|
| **1** | Data Collection | Open-Meteo API integration, FIRMS archive loading, legacy CSV fallback | Asif, Raul |
| **2** | Data Collection | Static geography fetch, fire-label normalization, `master_daily.parquet` finalized | Asif, Raul |
| **3** | EDA & Feature Engineering | Per-city quality audit, descriptive statistics, distribution analysis | Ilaha, Nurana |
| **4** | EDA & Feature Engineering | Correlation analysis, seasonal decomposition, fire-weather t-tests | Ilaha, Nurana, Aysu |
| **5** | Feature Engineering | FWI indices, VPD/dew point, lag/rolling features, calendar encodings → `engineered_daily.parquet` | Ilaha, Nurana |
| **6** | ML — Weather Forecasting | Prophet + XGBoost + multi-model ensemble per city → `weather_forecast_30d.parquet` | Asif, Aysu |
| **7** | ML — Wildfire Detection | 8-model comparison, Optuna tuning, SHAP, isotonic calibration | Asif, Aysu |
| **8** | Risk Prediction & Visualization | 30-day risk scoring, Folium/Plotly maps, climate trend analysis | Asif, Aysu |
| **9** | Integration & Presentation Prep | End-to-end pipeline validation, `src/` module cleanup, slide deck | All team |
| **10** | **Presentation** | Final rehearsal and delivery | All team |

---

## 4. Pipeline Architecture

```
┌───────────────────────────────────────────────────────────┐
│             WildFire Pipeline Flow                        │
│       Run: NB1 → NB2 → NB3 → NB4 → NB5 → NB6            │
│                                                           │
│  ┌─────────────────────────────────┐                      │
│  │  NB1 — Data Ingestion           │                      │
│  │  Open-Meteo + FIRMS + GEE       │                      │
│  │  24h smart freshness check      │                      │
│  └───────────────┬─────────────────┘                      │
│                  │                                        │
│                  ▼                                        │
│   master_daily + master_hourly + weather_hourly.parquet   │
│                  │                                        │
│                  ▼                                        │
│  ┌─────────────────────────────────┐                      │
│  │  NB2 — EDA & Feature Eng.       │                      │
│  │  200+ features: FWI, VPD,       │                      │
│  │  lags, rolling, Prophet resid   │                      │
│  └───────────────┬─────────────────┘                      │
│                  │                                        │
│                  ▼                                        │
│   engineered_daily + engineered_hourly.parquet            │
│                  │                                        │
│         ┌────────┴────────────────┐                       │
│         ▼                         ▼                       │
│  ┌──────────────┐  ┌──────────────────────────────┐       │
│  │  NB3 Weather │  │  NB4 Wildfire Detection      │       │
│  │  Prophet+XGB │  │  8 models + Optuna + SHAP    │       │
│  │  30d + 168h  │  │  3-way split, calibration    │       │
│  └──────┬───────┘  └──────────────┬───────────────┘       │
│         │                         │                       │
│         └──────────┬──────────────┘                       │
│                    ▼                                      │
│  ┌──────────────────────────────────────┐                 │
│  │  NB5 — Risk Prediction & Dashboard   │                 │
│  │  30-day risk + Folium + Plotly maps  │                 │
│  └──────────────────┬───────────────────┘                 │
│                     ▼                                     │
│  ┌──────────────────────────────────────┐                 │
│  │  NB6 — Climate Report                │                 │
│  │  Trends + comparisons + jury report  │                 │
│  └──────────────────────────────────────┘                 │
│                                                           │
│  ┌──────────────────────────────────────┐                 │
│  │  src/ — Shared Python Module         │                 │
│  │  config · features · modeling ·      │                 │
│  │  evaluation · visualization · utils  │                 │
│  └──────────────────────────────────────┘                 │
└───────────────────────────────────────────────────────────┘
```

---

## 5. Notebook Descriptions

### NB1 — Data Ingestion (`01_Data_Ingestion.ipynb`)

| Aspect | Detail |
|--------|--------|
| **Purpose** | Collect, unify, and persist all raw data for the pipeline |
| **Weather Source** | Open-Meteo Archive API (ERA5 + ERA5-Land, hourly, 2012–present) + 16-day live forecast |
| **Weather Variables** | Temperature, humidity, rain, wind speed/direction, pressure, solar radiation, soil temperature, soil moisture (9 features) |
| **Fire Source** | NASA FIRMS CSVs (MODIS C6.1, SUOMI-NPP VIIRS, J1 VIIRS, J2 VIIRS) |
| **Fire Normalization** | Binary daily label per city within a 20 km buffer radius |
| **Geography** | Open-Elevation API + derived slope; land-cover, population, road network from supplementary CSVs; GEE vegetation indices (NDVI, NDBI) with CSV fallback |
| **Fallback Logic** | Local parquet cache → legacy `merged.csv` → API (with retry/rate-limit handling) |
| **Atomic Writes** | Writes to `.tmp.parquet` then renames — prevents corruption on failure |
| **Outputs** | `master_daily.parquet`, `master_hourly.parquet`, `weather_hourly.parquet`, `fires_daily.parquet`, `cities.parquet`, `static_geography.parquet` |
| **Runtime** | ~5–15 min (cached) |

### NB2 — EDA & Feature Engineering (`02_EDA_FeatureEngineering.ipynb`)

| Aspect | Detail |
|--------|--------|
| **Purpose** | Comprehensive city-by-city analysis + feature engineering at both daily and hourly granularity |
| **Quality Audit** | Per-city missing-value report, duplicate detection, dtype checks |
| **Descriptive Stats** | Mean, std, quartiles, skewness, kurtosis per city per feature |
| **Visualizations** | Boxplots, histograms, correlation heatmaps, seasonal decomposition, diurnal cycles |
| **Fire-Weather Analysis** | Welch's t-test comparing fire-day vs. non-fire-day weather per city |
| **Feature Engineering** | FWI family (FFMC, DMC, DC, ISI, BUI, FWI), VPD, dew point, heat index, drought proxy, dry-spell tracking, extreme-temperature flags, lag features (1–30 day), rolling stats (3/7/14/30 day), Prophet seasonal residuals, cyclical calendar encodings, historical fire features, vegetation interactions, anomaly features |
| **Outlier Detection** | IQR-based per-city per-feature outlier counts with 1st/99th percentile winsorization |
| **Outputs** | `engineered_daily.parquet` (200+ features), `engineered_hourly.parquet`, per-city profiles CSV, correlation heatmaps |
| **Runtime** | ~3–5 min |

### NB3 — Weather Forecasting (`03_Weather_TimeSeries.ipynb`)

| Aspect | Detail |
|--------|--------|
| **Purpose** | Forecast key weather variables at daily (30-day) and hourly (168-hour) horizons |
| **Daily Models** | Prophet (yearly/weekly seasonality, Azerbaijan holidays) + XGBoost (recursive multi-step with lag/rolling features) |
| **Hourly Models** | Prophet (daily seasonality) + XGBoost (hourly lags) |
| **Stacking** | Weighted ensemble — weight optimized per city/variable on holdout |
| **Multi-Model Comparison** | Ridge, ElasticNet, RF, ExtraTrees, HistGBR, XGB, LGB, CatBoost evaluated on holdout with MAE/RMSE leaderboard |
| **Scale** | 128+ model bundles (16 cities × 8 targets) |
| **Outputs** | `weather_forecast_30d.parquet`, `weather_forecast_168h.parquet`, `weather_leaderboard.csv` |
| **Runtime** | ~60–120 min |

### NB4 — Wildfire Detection (`04_Wildfire_Detection.ipynb`)

| Aspect | Detail |
|--------|--------|
| **Purpose** | Production-quality multi-model wildfire classification with anti-overfitting safeguards |
| **3-Way Split** | Train (< 2024), Validation (2024), Test (≥ 2025) — test **never** seen during training or tuning |
| **Feature Pruning** | Remove near-zero-variance + highly correlated (r > 0.95) columns to reduce noise |
| **9 Base Models** | LogReg, RF, ExtraTrees, HistGBC, XGBoost, LightGBM, CatBoost, BalancedRF, EasyEnsemble — all cost-sensitive |
| **SMOTE** | Conservative ratios (0.2–0.3) only on top gradient boosters; SMOTEENN comparison |
| **Optuna Tuning** | 50 trials per top-2 model; precision floor ≥ 12 %; scale_pos_weight capped at 1.5× |
| **Threshold** | Tuned on **validation** set; minimum precision ≥ 10 % |
| **Calibration** | Isotonic probability calibration on validation set for meaningful risk scores |
| **Overfitting Monitor** | Train-vs-val F1 gap printed for every model; gap > 15 % flagged |
| **Explainability** | SHAP TreeExplainer + top-25 feature importance chart |
| **Outputs** | `models/wildfire/best_fire_model.joblib`, `reports/metrics/fire_leaderboard.csv`, SHAP plots |
| **Runtime** | ~10–20 min |

### NB5 — Risk Prediction & Dashboard (`05_Risk_Prediction_Dashboard.ipynb`)

| Aspect | Detail |
|--------|--------|
| **Purpose** | Predict 30-day wildfire risk per city + generate interactive maps for demo |
| **Features** | Replicates NB2 feature engineering on forecast data with historical lag proxies |
| **Risk Levels** | Low (<15 %), Moderate (15–35 %), High (35–60 %), Extreme (>60 %) |
| **Folium Maps** | One HTML map per forecast date — colour-coded markers with click popups |
| **Plotly Dashboard** | Animated scatter-geo with date slider — all cities, all dates |
| **Weather Maps** | Separate animated maps for temperature, humidity, wind, precipitation |
| **Outputs** | `wildfire_risk_30d.parquet`, `reports/maps/*.html` |
| **Runtime** | ~2–5 min |

### NB6 — Climate Report (`06_Climate_Report.ipynb`)

| Aspect | Detail |
|--------|--------|
| **Purpose** | Answer jury-ready climate shift and fire risk trend questions |
| **vs Last Year** | Forecast temperature and risk compared to same period last year |
| **vs Decade** | Temperature anomaly relative to 2015–2024 average |
| **Long-term Trends** | Annual temperature, extreme heat days, precipitation, fire count trends |
| **Risk Outlook** | Which cities have increasing risk and why |
| **Jury Summary** | Non-technical conclusions with key statistics |
| **Outputs** | `reports/figures/climate_*.png`, `reports/metrics/climate_summary.csv` |
| **Runtime** | ~1–2 min |

---

## 6. Shared `src/` Module

All reusable logic is factored into a thin Python package that every notebook imports. This keeps notebook cells focused on analysis narrative, avoids code duplication, and makes functions independently testable.

```python
from src.config import ROOT, PROCESSED, CITIES, TARGET_COL, ...
from src.features import add_calendar_features, build_lag_features, compute_vpd, ...
from src.evaluation import fire_metrics, find_optimal_threshold, build_fire_leaderboard
from src.visualization import plot_confusion_matrix, plot_pr_curves, plot_feature_importance
```

| Module | Purpose | Key Functions |
|--------|---------|---------------|
| **`config.py`** | Central paths, constants, city coordinates, column definitions | `detect_project_root()`, `ensure_dirs()`, `CITIES`, `TARGET_COL`, `DROP_COLS`, `RANDOM_SEED` |
| **`features.py`** | All feature engineering builders | `add_calendar_features()`, `build_lag_features()`, `build_rolling_features()`, `compute_fwi_proxy()`, `compute_vpd()`, `compute_dew_point()`, `compute_heat_index()`, `add_wildfire_weather_features()`, `add_historical_fire_features()`, `add_anomaly_features()` |
| **`modeling.py`** | Model factory functions for weather and fire classifiers | `get_weather_models()`, `get_fire_models()` |
| **`evaluation.py`** | Metrics, threshold tuning, leaderboard construction | `fire_metrics()`, `find_optimal_threshold()`, `build_fire_leaderboard()`, `weather_metrics()` |
| **`visualization.py`** | Reusable plotting helpers | `plot_confusion_matrix()`, `plot_pr_curves()`, `plot_feature_importance()`, `plot_leaderboard()` |
| **`utils.py`** | Data loading, model saving, misc helpers | `load_parquet_safe()`, `save_model()`, `load_model()` |
| **`prediction_pipeline.py`** | End-to-end 30-day risk scoring pipeline | `build_features()`, `load_training_frame()`, `load_forecast_frame()`, `candidate_models()`, `train_and_select()`, `write_outputs()`, `main()` |

---

## 7. Folder Structure

```
Wildfire/
├── notebooks/                              Run in order: NB1 → NB2 → NB3 → NB4 → NB5 → NB6
│   ├── 01_Data_Ingestion.ipynb             Data collection & unification
│   ├── 02_EDA_FeatureEngineering.ipynb     EDA + FWI + VPD + lag/rolling + Prophet
│   ├── 03_Weather_TimeSeries.ipynb         Prophet + XGBoost + multi-model forecasting
│   ├── 04_Wildfire_Detection.ipynb         8 models + SMOTE + Optuna + SHAP + calibration
│   ├── 05_Risk_Prediction_Dashboard.ipynb  30-day risk + Folium + Plotly maps
│   └── 06_Climate_Report.ipynb             Climate trends + jury report
│
├── src/                                    Shared Python module (imported by all notebooks)
│   ├── __init__.py
│   ├── config.py                           Central paths, constants, city list
│   ├── features.py                         Feature engineering (VPD, FWI, lags, rolling)
│   ├── modeling.py                         Model factories (weather + fire classifiers)
│   ├── evaluation.py                       Metrics, threshold tuning, leaderboards
│   ├── visualization.py                    Plotting helpers (CM, PR curves, SHAP)
│   ├── utils.py                            Data loading, model saving
│   ├── dashboard_export.py                 Unified dashboard JSON/CSV export utilities
│   ├── validation.py                       Pipeline output validation checks
│   └── prediction_pipeline.py             End-to-end 30-day risk scoring pipeline
│
├── data/
│   ├── raw/
│   │   ├── firms/                          NASA FIRMS sensor archives
│   │   │   ├── MODIS C6.1/
│   │   │   ├── SUOMI VIIRS C2/
│   │   │   └── J1 VIIRS C2/
│   │   ├── legacy/                         Fallback: merged.csv + supplementary CSVs
│   │   ├── weather_hourly__*.parquet       Per-city weather cache (16 files)
│   │   └── weather_hourly.parquet          Combined hourly weather
│   ├── processed/
│   │   ├── master_daily.parquet            Core daily dataset (NB1)
│   │   ├── master_hourly.parquet           Core hourly dataset (NB1)
│   │   ├── engineered_daily.parquet        200+ daily features (NB2)
│   │   ├── engineered_hourly.parquet       100+ hourly features (NB2)
│   │   └── fires_daily.parquet             Fire labels
│   └── reference/
│       ├── cities.parquet                  16 city coordinates
│       └── static_geography.parquet        Elevation, slope, land cover, population
│
├── models/
│   ├── wildfire/                           Fire detection models (NB4)
│   │   ├── best_fire_model.joblib          Best calibrated daily classifier
│   │   ├── best_fire_model.json            CatBoost JSON format (used by GitHub Actions)
│   │   ├── best_fire_model_hourly.joblib   Best calibrated hourly classifier
│   │   ├── best_fire_model_hourly.json     CatBoost JSON format (used by GitHub Actions)
│   │   ├── forecast_compatible_fire_model.joblib  Pipeline-retrained model (NB5)
│   │   ├── model_manifest.json             Daily threshold, features, metrics
│   │   ├── model_manifest_hourly.json      Hourly threshold, features, metrics
│   │   ├── feature_columns.json            Daily feature list for inference
│   │   └── feature_columns_hourly.json     Hourly feature list for inference
│   └── prophet_cache/                      Cached Prophet models per city/var
│
├── outputs/                                All pipeline artefacts
│   ├── weather_forecast_30d.parquet        30-day weather forecast (NB3)
│   ├── weather_forecast_168h.parquet       168-hour weather forecast (NB3)
│   ├── weather_leaderboard.csv             Model comparison (NB3)
│   ├── wildfire_risk_30d.parquet           30-day risk predictions (NB5)
│   └── *.png / *.csv                       EDA plots, hypothesis tests
│
├── reports/
│   ├── figures/                            Publication-quality figures (NB4, NB6)
│   ├── maps/                               Interactive HTML maps (NB5)
│   └── metrics/                            CSV leaderboards and summaries (NB4, NB6)
│
├── dashboard/                              Standalone web dashboard (HTML/JS/CSS)
│   ├── index.html
│   ├── app.js
│   ├── styles.css
│   └── data/                              forecast_30_days.json, hourly_forecast_168h.json, metrics.json
│
├── docs/                                   GitHub Pages deployment (mirrors dashboard/)
│   ├── index.html
│   ├── app.js
│   ├── styles.css
│   ├── .nojekyll
│   └── data/
│
├── scripts/
│   ├── update_forecast.py                  Daily scoring script (run by GitHub Actions)
│   ├── validate_pipeline_outputs.py        Pipeline output validation runner
│   ├── threshold_analysis.py               Threshold sensitivity analysis
│   └── generate_presentation_outputs.py    Generate presentation-ready figures
│
├── .github/
│   └── workflows/
│       ├── update_forecast.yml             Daily forecast update + commit
│       └── pages.yml                       GitHub Pages deployment
│
├── requirements.txt                        pip install -r requirements.txt
└── README.md
```

> All files under `outputs/`, `models/`, and `reports/` are generated programmatically — nothing is hand-edited.

---

## 8. Key Definitions & Glossary

| Term | Full Name | Description |
|------|-----------|-------------|
| **FWI** | Fire Weather Index | Composite index estimating wildfire danger from weather conditions |
| **FFMC** | Fine Fuel Moisture Code | Moisture content of surface litter; high FFMC = dry fine fuels = easy ignition |
| **DMC** | Duff Moisture Code | Moisture of loosely compacted organic layers; proxy for moderate-depth drought |
| **DC** | Drought Code | Deep soil moisture deficit; tracks long-term drying trends |
| **ISI** | Initial Spread Index | Expected rate of fire spread, combining wind speed and FFMC |
| **BUI** | Buildup Index | Total fuel available for combustion, combining DMC and DC |
| **VPD** | Vapor Pressure Deficit | Difference between saturated and actual water vapor pressure; higher VPD = drier air = higher fire risk |
| **FIRMS** | Fire Information for Resource Management System | NASA's global near-real-time active fire detection system using MODIS and VIIRS satellite sensors |
| **MODIS** | Moderate Resolution Imaging Spectroradiometer | Satellite instrument detecting thermal anomalies at ~1 km resolution |
| **VIIRS** | Visible Infrared Imaging Radiometer Suite | Successor to MODIS with ~375 m resolution (NPP and NOAA-20/21 satellites) |
| **NDVI** | Normalized Difference Vegetation Index | Satellite-derived vegetation greenness measure; dead/dry vegetation is more flammable |
| **NDBI** | Normalized Difference Built-up Index | Satellite-derived urban density measure |
| **PR-AUC** | Precision-Recall Area Under the Curve | Primary evaluation metric for imbalanced classification — preferred over accuracy |
| **SMOTE** | Synthetic Minority Over-sampling Technique | Generates synthetic minority-class samples to address class imbalance |
| **SHAP** | SHapley Additive exPlanations | Game-theory-based method for explaining individual model predictions |
| **Optuna** | — | Bayesian hyperparameter optimization framework with pruning |
| **Isotonic Calibration** | — | Non-parametric probability calibration ensuring predicted probabilities match observed frequencies |
| **ERA5-Land** | — | ECMWF's high-resolution (9 km) land-surface reanalysis dataset |

---

## 9. Setup & Execution

### Google Colab

1. Upload the project folder to your Google Drive root (→ `/MyDrive/WildFire_Data/`)
2. Open any notebook in Colab
3. Run the first cell — it auto-mounts Drive, detects the project root, and creates directories
4. Execute cells top-to-bottom in order: **NB1 → NB2 → NB3 → NB4 → NB5 → NB6**

Override the project path if needed:
```python
import os; os.environ["WILDFIRE_ROOT"] = "/content/drive/MyDrive/path/to/WildFire_Data"
```

### Local (JupyterLab / VS Code)

```bash
cd Wildfire
pip install -r requirements.txt
jupyter lab notebooks/
```

The setup cell in each notebook auto-detects the project root by walking up the directory tree. Override with:
```bash
export WILDFIRE_ROOT=/absolute/path/to/project
```

### Pipeline Runner (command line)

```bash
# Run the full pipeline (all 6 notebooks in order)
python run_pipeline.py

# Run only the training phase (NB1–NB4)
python run_pipeline.py --mode train

# Run only the prediction/dashboard phase (NB5–NB6)
python run_pipeline.py --mode predict

# Pure Python execution — bypasses notebooks entirely (fast)
python run_pipeline.py --mode predict --script

# Force Optuna re-tuning in NB4
python run_pipeline.py --retune

# Skip SHAP computation (speeds up NB4)
python run_pipeline.py --skip-shap

# Run and validate outputs afterwards
python run_pipeline.py --validate
```

### Environment Variables

| Variable | Purpose |
|----------|---------|
| `WILDFIRE_ROOT` | Absolute path to project root. Overrides auto-detection. |
| `WILDFIRE_RETUNE` | Set to `1` to force Optuna re-tuning in NB4. |
| `WILDFIRE_SKIP_SHAP` | Set to `1` to skip SHAP computation in NB4. |

### Git LFS

Large binary and columnar data files are tracked via Git LFS (see `.gitattributes`):

| Pattern | LFS tracked |
|---------|------------|
| `*.pkl`, `*.joblib`, `*.h5`, `*.pt`, `*.pth` | Yes — all locations |
| `data/**/*.parquet`, `outputs/**/*.parquet` | Yes |
| `data/**/*.csv`, `outputs/**/*.csv`, `reports/**/*.csv` | Yes |
| `docs/data/*.json`, `docs/data/*.csv` | **No** — plain text so GitHub Pages can serve them directly |

After cloning:
```bash
git lfs install
git lfs pull
```

---

## 10. Dependencies

See `requirements.txt` for pinned versions. Summary by notebook:

| Group | Packages | Used By |
|-------|----------|---------|
| **Core** | pandas, numpy, pyarrow, joblib, tqdm | All |
| **Data Ingestion** | openmeteo-requests, requests-cache, retry-requests, requests | NB1 |
| **EDA** | scipy, statsmodels, prophet, matplotlib, seaborn | NB2 |
| **Weather Forecasting** | xgboost | NB3 |
| **Wildfire Detection** | lightgbm, catboost, scikit-learn, imbalanced-learn, optuna, shap | NB4 |
| **Visualization** | folium, plotly | NB5, NB6 |
| **Optional** | earthengine-api (GEE enrichment in NB1) | NB1 |

Each notebook also installs missing packages automatically in its first code cell.

---

## 11. Cities Covered

| City | Latitude | Longitude | Key Characteristics |
|---|---:|---:|---|
| Baku | 40.409 | 49.867 | Capital city; highest urbanization; highest observed fire rate |
| Shabran | 41.206 | 48.987 | Affected by major wildfire events during 2021–2022 |
| Ganja | 40.683 | 46.361 | Western highland city; important regional center |
| Mingachevir | 40.764 | 47.060 | Central lowland city near the Kura River and Mingachevir Reservoir |
| Shirvan | 39.932 | 48.930 | Located in the Kura-Araz lowland; dry lowland climate |
| Lankaran | 38.752 | 48.848 | Southern subtropical coastal region |
| Shaki | 41.198 | 47.169 | Northern foothill region with forest and mountain influence |
| Nakhchivan | 39.209 | 45.412 | Exclave region; arid continental climate |
| Yevlakh | 40.618 | 47.150 | Central plains; dry lowland conditions |
| Quba | 41.361 | 48.526 | Northern mountainous region |
| Khachmaz | 41.464 | 48.806 | Northeastern coastal region near the Caspian Sea |
| Gabala | 40.998 | 47.847 | Highest elevation among selected cities; highly forested area |
| Shamakhi | 40.630 | 48.641 | Mountain plateau region |
| Jalilabad | 39.209 | 48.299 | Southern lowland region with agricultural land use |
| Barda | 40.374 | 47.127 | Karabakh region; central lowland area |
| Zaqatala | 41.630 | 46.643 | Northwestern mountain-foothill region; lowest observed fire rate |

Fire labels are aggregated daily within a **20 km radius** of each city centroid.

---

## 12. Data Sources & Provenance

| Source | Data | Access | Notes |
|--------|------|--------|-------|
| **Open-Meteo Archive** | Historical hourly weather (2012–present) | Free, no API key | ERA5 + ERA5-Land reanalysis; 9 variables |
| **Open-Meteo Forecast** | 16-day ahead hourly weather | Free, no API key | Updated daily |
| **NASA FIRMS** | Active fire detections | Free, archive CSVs | MODIS C6.1 + VIIRS C2 (3 sensors); confidence ≥ "n" (VIIRS) / ≥ 30 (MODIS) |
| **Open-Elevation** | Terrain elevation | Free, no API key | Slope derived from 4-neighbour 1 km DEM cross |
| **Google Earth Engine** | MODIS burned area, Sentinel-2 NDVI/NDBI | Free (GEE account) | CSV fallback if GEE unavailable |
| **Supplementary CSVs** | Land cover %, urban %, population, roads | Local reference files | Static per-city attributes |

### Caveats

- **Fire risk** represents the probability of any FIRMS-detected hotspot occurring, **not** predicted burn area or fire severity
- The classifier is calibrated with **isotonic regression** — use the operational threshold from `model_manifest.json`
- Evaluate with **PR-AUC and recall at the operational threshold** — accuracy is misleading given ~8–10 % fire-day prevalence
- Weather forecast errors compound over the 30-day horizon; days 1–7 are most reliable

---

## 13. Key Outputs & Deliverables

### Data Artefacts

| File | Description | Produced By |
|------|-------------|-------------|
| `data/processed/master_daily.parquet` | City-day rows × 29+ columns | NB1 |
| `data/processed/master_hourly.parquet` | Hourly weather observations | NB1 |
| `data/raw/weather_hourly.parquet` | Raw hourly weather (all cities) | NB1 |
| `data/processed/engineered_daily.parquet` | 200+ engineered features (daily) | NB2 |
| `data/processed/engineered_hourly.parquet` | 100+ engineered features (hourly) | NB2 |
| `outputs/weather_forecast_30d.parquet` | 30-day daily weather forecast | NB3 |
| `outputs/weather_forecast_168h.parquet` | 168-hour weather forecast | NB3 |
| `outputs/wildfire_risk_30d.parquet` | 30-day fire-risk per city | NB5 |

### ML Models

| File | Description | Produced By |
|------|-------------|-------------|
| `models/wildfire/best_fire_model.joblib` | Best calibrated daily fire classifier | NB4 |
| `models/wildfire/best_fire_model_hourly.joblib` | Best calibrated hourly fire classifier | NB4 |
| `models/wildfire/forecast_compatible_fire_model.joblib` | Pipeline-retrained model for NB5 scoring | NB5 |
| `models/wildfire/model_manifest.json` | Daily threshold, feature list, test metrics | NB4 |
| `models/wildfire/model_manifest_hourly.json` | Hourly threshold, feature list, test metrics | NB4 |
| `models/wildfire/feature_columns.json` | Daily feature order for inference | NB4 |
| `models/wildfire/feature_columns_hourly.json` | Hourly feature order for inference | NB4 |
| `models/prophet_cache/*.pkl` | Cached Prophet models (64 files) | NB3 |

### Interactive Visualizations

| File | Description | Produced By |
|------|-------------|-------------|
| `reports/maps/fire_risk_*.html` | Folium risk maps (per date) | NB5 |
| `reports/maps/fire_risk_dashboard.html` | Plotly animated dashboard | NB5 |
| `reports/figures/fire_shap_summary.png` | SHAP explainability plot | NB4 |
| `reports/figures/climate_*.png` | Climate trend figures | NB6 |

### Reports & Metrics

| File | Description | Produced By |
|------|-------------|-------------|
| `reports/metrics/fire_leaderboard.csv` | All models ranked by composite score | NB4 |
| `reports/metrics/climate_summary.csv` | Climate comparison conclusions | NB6 |
| `outputs/weather_leaderboard.csv` | Weather model comparison | NB3 |
| `outputs/hypothesis_tests.csv` | EDA hypothesis test results | NB2 |

---

## 14. Evaluation Protocol & Model Targets

### Wildfire Detection (NB4)

| Metric | Target | Rationale |
|--------|--------|-----------|
| **Recall** | ≥ 0.70 | Missing a real fire is far worse than a false alarm |
| **Precision** | ≥ 0.30 | Floor to prevent degenerate "predict all fire" models |
| **F1** | Maximize | Harmonic mean balancing recall and precision |
| **PR-AUC** | ≥ 0.20 | Primary ranking metric for imbalanced classification |
| **Overfit gap** | < 0.15 | Train-vs-val F1 difference; models exceeding this are flagged |

**Composite objective** used for Optuna and model ranking: `0.6 × Recall + 0.4 × F1`

### Weather Forecasting (NB3)

| Metric | Description |
|--------|-------------|
| **MAE** | Mean Absolute Error — primary comparison metric |
| **RMSE** | Root Mean Squared Error — penalizes large deviations |

Evaluated on a temporal holdout (last 30 days for daily, last 168 hours for hourly).

---

## 15. GitHub Actions & Automated Updates

The repository includes two GitHub Actions workflows:

### `update_forecast.yml` — Daily Forecast Update

Runs automatically every day at **03:00 UTC** (07:00 Baku time). Also triggerable manually via the Actions tab.

**What it does:**
1. Fetches fresh hourly weather for all 16 cities from Open-Meteo API
2. Applies the pre-trained CatBoost models (`models/wildfire/best_fire_model.json`)
3. Scores 30-day daily risk and 168-hour hourly risk per city
4. Writes updated JSON data files to `docs/data/`
5. Commits and pushes the updated data to the repository

**Why scheduled runs sometimes needed manual re-run (now fixed):**
- Missing `persist-credentials: true` on checkout caused push failures
- No retry logic on the forecast script — a single Open-Meteo API timeout would fail the entire run
- Missing model artifact validation — silent failures when JSON models were absent
- `git pull --rebase origin main` without `--ff-only` could create merge commits

### `pages.yml` — GitHub Pages Deployment

Deploys the `docs/` folder to GitHub Pages whenever commits land on `main` or `master`, or when triggered manually. Validates that the docs folder contains real HTML files (not Git LFS pointers) before deploying.

### Repository Settings Required

For the workflows to function correctly, ensure the following settings are configured in the repository:

1. **GitHub Pages source**: Set to `GitHub Actions` in *Settings → Pages → Source*
2. **Actions permissions**: Ensure *Settings → Actions → General → Workflow permissions* is set to **Read and write permissions**
3. **Pages environment**: The `github-pages` deployment environment is created automatically on first deployment