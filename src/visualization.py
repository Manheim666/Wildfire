"""
WildFire Prediction — Visualization Utilities
==============================================
Reusable plotting functions for EDA, evaluation, and maps.
"""
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_theme(style="whitegrid")


def plot_confusion_matrix(y_true, y_pred, title="", ax=None, labels=None):
    """Plot a confusion matrix heatmap."""
    from sklearn.metrics import confusion_matrix, recall_score, f1_score
    if labels is None:
        labels = ["No Fire", "Fire"]
    cm = confusion_matrix(y_true, y_pred)
    if ax is None:
        fig, ax = plt.subplots(figsize=(5, 4))
    sns.heatmap(cm, annot=True, fmt="d", cmap="Blues", ax=ax,
                xticklabels=labels, yticklabels=labels)
    rec = recall_score(y_true, y_pred, zero_division=0)
    f1  = f1_score(y_true, y_pred, zero_division=0)
    ax.set_title(f"{title}\nRecall={rec:.3f}  F1={f1:.3f}", fontsize=11)
    ax.set_ylabel("Actual")
    ax.set_xlabel("Predicted")
    return ax


def plot_pr_curves(curves_dict, y_true, title="PR Curves", ax=None):
    """Plot multiple PR curves.
    curves_dict: {label: y_prob}
    """
    from sklearn.metrics import precision_recall_curve, average_precision_score
    if ax is None:
        fig, ax = plt.subplots(figsize=(8, 6))
    colors = plt.cm.tab10(np.linspace(0, 1, len(curves_dict)))
    for (label, y_prob), color in zip(curves_dict.items(), colors):
        prec, rec, _ = precision_recall_curve(y_true, y_prob)
        ap = average_precision_score(y_true, y_prob)
        ax.plot(rec, prec, label=f"{label} (AP={ap:.3f})", color=color, lw=2)
    ax.set_xlabel("Recall")
    ax.set_ylabel("Precision")
    ax.set_title(title, fontsize=14)
    ax.legend(loc="best", fontsize=9)
    ax.set_xlim([0, 1.02])
    ax.set_ylim([0, 1.02])
    return ax


def plot_feature_importance(feature_names, importances, top_n=25,
                            title="Feature Importance", ax=None):
    """Horizontal bar chart of top-N feature importances."""
    fi = pd.DataFrame({"Feature": feature_names, "Importance": importances})
    fi = fi.sort_values("Importance", ascending=False).head(top_n)
    if ax is None:
        fig, ax = plt.subplots(figsize=(10, max(6, top_n * 0.35)))
    sns.barplot(data=fi, x="Importance", y="Feature", ax=ax, palette="viridis")
    ax.set_title(title, fontsize=14)
    return ax


def plot_leaderboard(lb_df, metric_cols=None, title="Model Leaderboard"):
    """Bar chart comparing models on key metrics."""
    if metric_cols is None:
        metric_cols = ["recall", "f1", "precision", "pr_auc"]
    available = [c for c in metric_cols if c in lb_df.columns]
    if not available:
        return

    fig, axes = plt.subplots(1, len(available), figsize=(5 * len(available), 6))
    if len(available) == 1:
        axes = [axes]

    for ax, metric in zip(axes, available):
        data = lb_df.sort_values(metric, ascending=True)
        ax.barh(data["model"], data[metric], color=plt.cm.viridis(
            np.linspace(0.2, 0.8, len(data))))
        ax.set_title(metric.upper(), fontsize=12)
        ax.set_xlim(0, 1)
        for i, v in enumerate(data[metric]):
            ax.text(v + 0.01, i, f"{v:.3f}", va="center", fontsize=9)

    fig.suptitle(title, fontsize=14)
    plt.tight_layout()
    return fig
