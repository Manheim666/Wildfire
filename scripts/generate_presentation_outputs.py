"""Generate all missing presentation outputs from existing model/data artifacts."""
import json, sys, warnings
from pathlib import Path
import numpy as np, pandas as pd
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_theme(style="whitegrid")

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.config import (ENG_DAILY, MODELS_F, OUTPUTS, REPORTS, FIGURES, METRICS,
                         CITIES, RANDOM_SEED, TARGET_COL)
from src.prediction_pipeline import (
    build_features, feature_matrix, TARGET, TRAIN_END, TEST_START,
    BASE_WEATHER, STATIC_FEATURES, RoleBlendedEnsemble, StackingEnsemble,
)
import src.prediction_pipeline as _pp
for cls_name in ("RoleBlendedEnsemble", "StackingEnsemble", "_SoftVoter"):
    cls = getattr(_pp, cls_name, None)
    if cls: setattr(sys.modules["__main__"], cls_name, cls)

warnings.filterwarnings("ignore")
import joblib
from sklearn.metrics import (confusion_matrix, precision_recall_fscore_support,
    average_precision_score, roc_auc_score, roc_curve, precision_recall_curve,
    classification_report)

# ── Load model + data ────────────────────────────────────────────────────
print("Loading model and data...")
bundle = joblib.load(MODELS_F / "forecast_compatible_fire_model.joblib")
model, calibrator, feature_columns = bundle["model"], bundle["calibrator"], bundle["features"]

df = pd.read_parquet(ENG_DAILY); df["Date"] = pd.to_datetime(df["Date"])
needed = ["City", "Date", TARGET] + BASE_WEATHER + STATIC_FEATURES
existing = [c for c in needed if c in df.columns]
tr = df[existing].copy()
for col in STATIC_FEATURES:
    if col not in tr.columns: tr[col] = 0.0
features = build_features(tr)

test_mask = features["Date"] >= TEST_START
val_mask = (features["Date"] >= TRAIN_END) & (features["Date"] < TEST_START)
X_test, _ = feature_matrix(features[test_mask], feature_columns)
y_test = features.loc[test_mask, TARGET].astype(int).values
cities_test = df.loc[df["Date"] >= TEST_START, "City"].values[:len(y_test)]
months_test = df.loc[df["Date"] >= TEST_START, "Date"].dt.month.values[:len(y_test)]

test_prob = calibrator.predict(model.predict_proba(X_test)[:, 1])
pr_auc = average_precision_score(y_test, test_prob)
roc_auc = roc_auc_score(y_test, test_prob)

# Load threshold
with open(OUTPUTS / "final_threshold.json") as f:
    thr_data = json.load(f)
THR = thr_data["selected_threshold"]
test_pred = (test_prob >= THR).astype(int)
tn, fp, fn, tp = confusion_matrix(y_test, test_pred, labels=[0,1]).ravel()
p, r, f1, _ = precision_recall_fscore_support(y_test, test_pred, average="binary", zero_division=0)
f2 = (5*p*r)/(4*p+r) if (4*p+r)>0 else 0

FIGURES.mkdir(parents=True, exist_ok=True)
OUTPUTS.mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════════════════════════════════
# 1. final_metrics.json + final_metrics.csv
# ═══════════════════════════════════════════════════════════════════════
print("1. final_metrics...")
metrics = {
    "model": "RoleBlend_All", "threshold": THR,
    "recall": round(float(r), 4), "precision": round(float(p), 4),
    "F1": round(float(f1), 4), "F2": round(float(f2), 4),
    "PR_AUC": round(float(pr_auc), 4), "ROC_AUC": round(float(roc_auc), 4),
    "true_positives": int(tp), "false_positives": int(fp),
    "false_negatives": int(fn), "true_negatives": int(tn),
    "test_samples": int(len(y_test)), "fire_rate": round(float(y_test.mean()), 4),
}
(OUTPUTS / "final_metrics.json").write_text(json.dumps(metrics, indent=2))
pd.DataFrame([metrics]).to_csv(OUTPUTS / "final_metrics.csv", index=False)

# ═══════════════════════════════════════════════════════════════════════
# 2. final_predictions.csv
# ═══════════════════════════════════════════════════════════════════════
print("2. final_predictions.csv...")
pred_df = pd.DataFrame({
    "city": cities_test, "month": months_test,
    "y_true": y_test, "y_pred": test_pred,
    "probability": test_prob.round(4),
})
pred_df.to_csv(OUTPUTS / "final_predictions.csv", index=False)

# ═══════════════════════════════════════════════════════════════════════
# 3. city_risk_summary.csv
# ═══════════════════════════════════════════════════════════════════════
print("3. city_risk_summary.csv...")
rows = []
for city in sorted(pred_df["city"].unique()):
    c = pred_df[pred_df["city"] == city]
    yt, yp, ypr = c["y_true"].values, c["y_pred"].values, c["probability"].values
    _p, _r, _f1, _ = precision_recall_fscore_support(yt, yp, average="binary", zero_division=0)
    rows.append({
        "city": city, "total_days": len(c),
        "actual_fires": int(yt.sum()), "predicted_fires": int(yp.sum()),
        "recall": round(float(_r), 4), "precision": round(float(_p), 4),
        "F1": round(float(_f1), 4),
        "mean_probability": round(float(ypr.mean()), 4),
        "max_probability": round(float(ypr.max()), 4),
        "fire_rate": round(float(yt.mean()), 4),
        "false_negatives": int(((yt==1)&(yp==0)).sum()),
        "false_positives": int(((yt==0)&(yp==1)).sum()),
    })
pd.DataFrame(rows).to_csv(OUTPUTS / "city_risk_summary.csv", index=False)

# ═══════════════════════════════════════════════════════════════════════
# 4. confusion_matrix.png
# ═══════════════════════════════════════════════════════════════════════
print("4. confusion_matrix.png...")
fig, ax = plt.subplots(figsize=(6, 5))
cm = confusion_matrix(y_test, test_pred, labels=[0,1])
sns.heatmap(cm, annot=True, fmt="d", cmap="Blues", ax=ax,
            xticklabels=["No Fire","Fire"], yticklabels=["No Fire","Fire"])
ax.set_title(f"Confusion Matrix (threshold={THR})\nRecall={r:.3f}  Precision={p:.3f}  F1={f1:.3f}", fontsize=12)
ax.set_ylabel("Actual"); ax.set_xlabel("Predicted")
fig.tight_layout(); fig.savefig(FIGURES / "confusion_matrix.png", dpi=150); plt.close()

# ═══════════════════════════════════════════════════════════════════════
# 5. precision_recall_curve.png
# ═══════════════════════════════════════════════════════════════════════
print("5. precision_recall_curve.png...")
prec_arr, rec_arr, _ = precision_recall_curve(y_test, test_prob)
fig, ax = plt.subplots(figsize=(8, 6))
ax.plot(rec_arr, prec_arr, color="#D96C3B", lw=2)
ax.axhline(p, color="gray", ls="--", alpha=0.5, label=f"Precision@thr={p:.3f}")
ax.axvline(r, color="gray", ls=":", alpha=0.5, label=f"Recall@thr={r:.3f}")
ax.fill_between(rec_arr, prec_arr, alpha=0.15, color="#D96C3B")
ax.set_xlabel("Recall"); ax.set_ylabel("Precision")
ax.set_title(f"Precision-Recall Curve (PR-AUC={pr_auc:.3f})", fontsize=13)
ax.legend(); ax.set_xlim([0,1.02]); ax.set_ylim([0,1.02])
fig.tight_layout(); fig.savefig(FIGURES / "precision_recall_curve.png", dpi=150); plt.close()

# ═══════════════════════════════════════════════════════════════════════
# 6. roc_curve.png
# ═══════════════════════════════════════════════════════════════════════
print("6. roc_curve.png...")
fpr, tpr, _ = roc_curve(y_test, test_prob)
fig, ax = plt.subplots(figsize=(7, 6))
ax.plot(fpr, tpr, color="#4A90D9", lw=2, label=f"ROC (AUC={roc_auc:.3f})")
ax.plot([0,1],[0,1], "k--", alpha=0.3)
ax.set_xlabel("False Positive Rate"); ax.set_ylabel("True Positive Rate")
ax.set_title(f"ROC Curve (AUC={roc_auc:.3f})", fontsize=13)
ax.legend(); fig.tight_layout()
fig.savefig(FIGURES / "roc_curve.png", dpi=150); plt.close()

# ═══════════════════════════════════════════════════════════════════════
# 7. feature_importance.png (from CatBoost_Optuna if available)
# ═══════════════════════════════════════════════════════════════════════
print("7. feature_importance.png...")
try:
    fi_path = METRICS / "fire_feature_importance.csv"
    if fi_path.exists():
        fi = pd.read_csv(fi_path).head(25)
        fig, ax = plt.subplots(figsize=(10, 8))
        sns.barplot(data=fi, x=fi.columns[1], y=fi.columns[0], ax=ax, palette="viridis")
        ax.set_title("Top 25 Feature Importances — Wildfire Detection", fontsize=13)
        fig.tight_layout(); fig.savefig(FIGURES / "feature_importance.png", dpi=150); plt.close()
except Exception as e:
    print(f"  Skipped: {e}")

# ═══════════════════════════════════════════════════════════════════════
# 8. Recall by city chart
# ═══════════════════════════════════════════════════════════════════════
print("8. city_recall.png...")
city_df = pd.read_csv(OUTPUTS / "city_risk_summary.csv").sort_values("recall", ascending=True)
fig, ax = plt.subplots(figsize=(10, 7))
colors = ["#B73333" if r < 0.5 else "#D8A31D" if r < 0.7 else "#3FA773" for r in city_df["recall"]]
ax.barh(city_df["city"], city_df["recall"], color=colors)
ax.axvline(0.70, color="red", ls="--", alpha=0.5, label="Target recall=0.70")
for i, (_, row) in enumerate(city_df.iterrows()):
    ax.text(row["recall"]+0.01, i, f'{row["recall"]:.2f} ({int(row["actual_fires"])} fires)', va="center", fontsize=9)
ax.set_xlabel("Recall"); ax.set_title("Recall by City — Wildfire Detection", fontsize=13)
ax.legend(); fig.tight_layout()
fig.savefig(FIGURES / "city_recall.png", dpi=150); plt.close()

print("\n✓ All presentation outputs generated.")
for f in ["final_metrics.json","final_metrics.csv","final_predictions.csv",
          "city_risk_summary.csv"]:
    print(f"  outputs/{f}")
for f in ["confusion_matrix.png","precision_recall_curve.png","roc_curve.png",
          "feature_importance.png","city_recall.png"]:
    print(f"  reports/figures/{f}")
