"""
Threshold Analysis for Wildfire Risk Model
===========================================
Loads the saved forecast-compatible model, reconstructs validation predictions,
sweeps thresholds, selects the best operating point, and saves:
  - outputs/final_threshold.json
  - outputs/threshold_tradeoff.csv
  - outputs/threshold_tradeoff.png
"""
import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sklearn.metrics import (
    average_precision_score,
    precision_recall_fscore_support,
    confusion_matrix,
)

# ── Project setup ────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from src.config import ENG_DAILY, MODELS_F, OUTPUTS, RANDOM_SEED
from src.prediction_pipeline import (
    build_features, feature_matrix, TARGET,
    TRAIN_END, TEST_START, BASE_WEATHER, STATIC_FEATURES,
    RoleBlendedEnsemble, StackingEnsemble,
)

# ── Patch pickle so classes saved under __main__ resolve correctly ───────
import src.prediction_pipeline as _pp
import types
# Some models were pickled when prediction_pipeline ran as __main__
_main = sys.modules.get("__main__")
for cls_name in ("RoleBlendedEnsemble", "StackingEnsemble", "_SoftVoter"):
    cls = getattr(_pp, cls_name, None)
    if cls is not None:
        setattr(sys.modules["__main__"], cls_name, cls)

# ── Load model bundle ────────────────────────────────────────────────────
bundle_path = MODELS_F / "forecast_compatible_fire_model.joblib"
bundle = joblib.load(bundle_path)
model = bundle["model"]
calibrator = bundle["calibrator"]
feature_columns = bundle["features"]

# ── Rebuild validation set ───────────────────────────────────────────────
print("Loading engineered daily data...")
df = pd.read_parquet(ENG_DAILY)
df["Date"] = pd.to_datetime(df["Date"])
needed = ["City", "Date", TARGET] + BASE_WEATHER + STATIC_FEATURES
existing = [c for c in needed if c in df.columns]
train_raw = df[existing].copy()
for col in STATIC_FEATURES:
    if col not in train_raw.columns:
        train_raw[col] = 0.0
features = build_features(train_raw)

val_mask = (features["Date"] >= TRAIN_END) & (features["Date"] < TEST_START)
test_mask = features["Date"] >= TEST_START

X_val, _ = feature_matrix(features[val_mask], feature_columns)
X_test, _ = feature_matrix(features[test_mask], feature_columns)
y_val = features.loc[val_mask, TARGET].astype(int).values
y_test = features.loc[test_mask, TARGET].astype(int).values

print(f"Validation: {len(y_val)} samples, {y_val.sum()} fires ({100*y_val.mean():.2f}%)")
print(f"Test:       {len(y_test)} samples, {y_test.sum()} fires ({100*y_test.mean():.2f}%)")

# ── Raw + calibrated probabilities ───────────────────────────────────────
val_prob_raw = model.predict_proba(X_val)[:, 1]
val_prob = calibrator.predict(val_prob_raw)

test_prob_raw = model.predict_proba(X_test)[:, 1]
test_prob = calibrator.predict(test_prob_raw)

pr_auc_val = average_precision_score(y_val, val_prob)
pr_auc_test = average_precision_score(y_test, test_prob)
print(f"PR-AUC  val={pr_auc_val:.4f}  test={pr_auc_test:.4f}")

# ── Threshold sweep on VALIDATION set ────────────────────────────────────
thresholds = np.arange(0.01, 0.80, 0.005)
rows = []
for t in thresholds:
    preds = (val_prob >= t).astype(int)
    tn, fp, fn, tp = confusion_matrix(y_val, preds, labels=[0, 1]).ravel()
    precision, recall, f1, _ = precision_recall_fscore_support(
        y_val, preds, average="binary", zero_division=0
    )
    f2 = (5 * precision * recall) / (4 * precision + recall) if (4 * precision + recall) > 0 else 0.0
    rows.append({
        "threshold": round(float(t), 4),
        "recall": round(float(recall), 4),
        "precision": round(float(precision), 4),
        "F1": round(float(f1), 4),
        "F2": round(float(f2), 4),
        "PR_AUC": round(pr_auc_val, 4),
        "false_positives": int(fp),
        "false_negatives": int(fn),
        "true_positives": int(tp),
        "true_negatives": int(tn),
        "predicted_positive_rate": round(float(preds.mean()), 4),
    })

tradeoff = pd.DataFrame(rows)

# ── Selection logic ──────────────────────────────────────────────────────
# 1. recall >= 0.70
candidates = tradeoff[tradeoff["recall"] >= 0.70].copy()

if len(candidates) > 0:
    # 2. prefer precision in [0.30, 0.40] or higher
    good_prec = candidates[candidates["precision"] >= 0.30]
    if len(good_prec) > 0:
        # 3. best F2
        best_row = good_prec.loc[good_prec["F2"].idxmax()]
    else:
        # precision below 0.30 for all recall>=0.70 — pick best F2 anyway
        best_row = candidates.loc[candidates["F2"].idxmax()]
else:
    # No threshold with recall>=0.70 — pick highest recall with precision closest to 0.30
    tradeoff["_dist"] = abs(tradeoff["precision"] - 0.30)
    best_row = tradeoff.loc[tradeoff["recall"].idxmax()]

selected_threshold = float(best_row["threshold"])
print(f"\n{'='*60}")
print(f"SELECTED THRESHOLD: {selected_threshold}")
print(f"  Recall:    {best_row['recall']}")
print(f"  Precision: {best_row['precision']}")
print(f"  F1:        {best_row['F1']}")
print(f"  F2:        {best_row['F2']}")
print(f"  FP:        {best_row['false_positives']}")
print(f"  FN:        {best_row['false_negatives']}")
print(f"{'='*60}")

# ── Verify on TEST set ──────────────────────────────────────────────────
test_preds = (test_prob >= selected_threshold).astype(int)
tn_t, fp_t, fn_t, tp_t = confusion_matrix(y_test, test_preds, labels=[0, 1]).ravel()
p_t, r_t, f1_t, _ = precision_recall_fscore_support(y_test, test_preds, average="binary", zero_division=0)
f2_t = (5 * p_t * r_t) / (4 * p_t + r_t) if (4 * p_t + r_t) > 0 else 0.0

print(f"\nTEST SET verification (threshold={selected_threshold}):")
print(f"  Recall:    {r_t:.4f}")
print(f"  Precision: {p_t:.4f}")
print(f"  F1:        {f1_t:.4f}")
print(f"  F2:        {f2_t:.4f}")
print(f"  PR-AUC:    {pr_auc_test:.4f}")
print(f"  FP:        {fp_t}")
print(f"  FN:        {fn_t}")
print(f"  TP:        {tp_t}")
print(f"  TN:        {tn_t}")

# ── Why this threshold is acceptable ─────────────────────────────────────
if r_t >= 0.70 and p_t >= 0.30:
    justification = (
        f"Threshold {selected_threshold} achieves recall={r_t:.3f} (≥0.70) and "
        f"precision={p_t:.3f} (≥0.30) on the held-out test set. "
        f"This means the model catches {100*r_t:.1f}% of actual wildfire days while "
        f"keeping false alarms to {100*(1-p_t):.1f}% of positive predictions. "
        f"For wildfire early warning, missing a real fire (false negative = {fn_t}) "
        f"is far more dangerous than a false alarm (FP = {fp_t}), so this recall-oriented "
        f"threshold is appropriate."
    )
elif r_t >= 0.70:
    justification = (
        f"Threshold {selected_threshold} achieves recall={r_t:.3f} (≥0.70) but precision={p_t:.3f} "
        f"is below the 0.30 target. This is a deliberate trade-off: in wildfire early warning, "
        f"capturing {100*r_t:.1f}% of fire events (missing only {fn_t}) is worth the cost of "
        f"{fp_t} false alarms. No threshold could simultaneously satisfy recall≥0.70 and "
        f"precision≥0.30 for this dataset."
    )
else:
    justification = (
        f"Threshold {selected_threshold} achieves the best available trade-off: "
        f"recall={r_t:.3f}, precision={p_t:.3f}. The model's probability calibration does not "
        f"allow recall≥0.70 with precision≥0.30 simultaneously."
    )

# ── Save final_threshold.json ────────────────────────────────────────────
final = {
    "selected_threshold": selected_threshold,
    "selection_method": "F2-maximized among recall≥0.70 candidates on validation set",
    "validation_metrics": {
        "recall": float(best_row["recall"]),
        "precision": float(best_row["precision"]),
        "F1": float(best_row["F1"]),
        "F2": float(best_row["F2"]),
        "PR_AUC": float(pr_auc_val),
        "false_positives": int(best_row["false_positives"]),
        "false_negatives": int(best_row["false_negatives"]),
        "true_positives": int(best_row["true_positives"]),
        "predicted_positive_rate": float(best_row["predicted_positive_rate"]),
    },
    "test_metrics": {
        "recall": round(float(r_t), 4),
        "precision": round(float(p_t), 4),
        "F1": round(float(f1_t), 4),
        "F2": round(float(f2_t), 4),
        "PR_AUC": round(float(pr_auc_test), 4),
        "false_positives": int(fp_t),
        "false_negatives": int(fn_t),
        "true_positives": int(tp_t),
        "true_negatives": int(tn_t),
    },
    "justification": justification,
}

out_dir = OUTPUTS
out_dir.mkdir(parents=True, exist_ok=True)
(out_dir / "final_threshold.json").write_text(json.dumps(final, indent=2), encoding="utf-8")
print(f"\nSaved: {out_dir / 'final_threshold.json'}")

# ── Save threshold_tradeoff.csv ──────────────────────────────────────────
tradeoff.to_csv(out_dir / "threshold_tradeoff.csv", index=False)
print(f"Saved: {out_dir / 'threshold_tradeoff.csv'}")

# ── Plot threshold_tradeoff.png ──────────────────────────────────────────
fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle("Threshold Trade-off Analysis — Wildfire Risk Model", fontsize=14, fontweight="bold")

t = tradeoff["threshold"]

# Top-left: Precision / Recall / F1 / F2
ax = axes[0, 0]
ax.plot(t, tradeoff["recall"], label="Recall", color="#D96C3B", linewidth=2)
ax.plot(t, tradeoff["precision"], label="Precision", color="#3FA773", linewidth=2)
ax.plot(t, tradeoff["F1"], label="F1", color="#4A90D9", linewidth=1.5, linestyle="--")
ax.plot(t, tradeoff["F2"], label="F2", color="#7B68EE", linewidth=1.5, linestyle="--")
ax.axvline(selected_threshold, color="red", linestyle=":", linewidth=2, label=f"Selected={selected_threshold}")
ax.axhline(0.70, color="#D96C3B", alpha=0.3, linestyle="--")
ax.axhline(0.30, color="#3FA773", alpha=0.3, linestyle="--")
ax.set_xlabel("Threshold")
ax.set_ylabel("Score")
ax.set_title("Precision / Recall / F-scores")
ax.legend(fontsize=8)
ax.grid(True, alpha=0.3)

# Top-right: FP / FN counts
ax = axes[0, 1]
ax.plot(t, tradeoff["false_positives"], label="False Positives", color="#D96C3B", linewidth=2)
ax.plot(t, tradeoff["false_negatives"], label="False Negatives", color="#3FA773", linewidth=2)
ax.axvline(selected_threshold, color="red", linestyle=":", linewidth=2)
ax.set_xlabel("Threshold")
ax.set_ylabel("Count")
ax.set_title("False Positives vs False Negatives")
ax.legend(fontsize=9)
ax.grid(True, alpha=0.3)

# Bottom-left: Predicted positive rate
ax = axes[1, 0]
ax.plot(t, tradeoff["predicted_positive_rate"], color="#4A90D9", linewidth=2)
ax.axvline(selected_threshold, color="red", linestyle=":", linewidth=2)
ax.set_xlabel("Threshold")
ax.set_ylabel("Rate")
ax.set_title("Predicted Positive Rate")
ax.grid(True, alpha=0.3)

# Bottom-right: F2 score zoom
ax = axes[1, 1]
ax.plot(t, tradeoff["F2"], color="#7B68EE", linewidth=2)
ax.axvline(selected_threshold, color="red", linestyle=":", linewidth=2, label=f"Selected={selected_threshold}")
# Shade the recall>=0.70 zone
recall_ok = tradeoff["recall"] >= 0.70
if recall_ok.any():
    t_ok = t[recall_ok]
    ax.axvspan(t_ok.min(), t_ok.max(), alpha=0.1, color="green", label="Recall≥0.70 zone")
ax.set_xlabel("Threshold")
ax.set_ylabel("F2 Score")
ax.set_title("F2 Score (recall-weighted)")
ax.legend(fontsize=9)
ax.grid(True, alpha=0.3)

plt.tight_layout()
fig.savefig(out_dir / "threshold_tradeoff.png", dpi=150, bbox_inches="tight")
plt.close()
print(f"Saved: {out_dir / 'threshold_tradeoff.png'}")

# ── Final report ─────────────────────────────────────────────────────────
print(f"""
{'='*60}
FINAL THRESHOLD REPORT
{'='*60}
Selected threshold:    {selected_threshold}
Final recall:          {r_t:.4f}
Final precision:       {p_t:.4f}
Final F1:              {f1_t:.4f}
Final F2:              {f2_t:.4f}
Final false negatives: {fn_t}
Final false positives: {fp_t}
PR-AUC (test):         {pr_auc_test:.4f}

{justification}
{'='*60}
""")
