import json
import numpy as np
import pandas as pd
import joblib
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    r2_score, mean_absolute_error, mean_squared_error,
    accuracy_score, f1_score, roc_auc_score, roc_curve, confusion_matrix,
    classification_report,
)

from .config import FIGURES_DIR, MODELS_DIR, RANDOM_STATE, DEFAULT_N_CLUSTERS
from .features import build_feature_lists


def _build_preprocessor(num_cols, cat_cols):
    transformers = []
    if num_cols:
        transformers.append(("num", StandardScaler(), num_cols))
    if cat_cols:
        transformers.append(("cat", OneHotEncoder(handle_unknown="ignore", min_frequency=20), cat_cols))
    return ColumnTransformer(transformers)


def _save_fig(fig, name):
    path = FIGURES_DIR / name
    fig.tight_layout()
    fig.savefig(path, dpi=130, bbox_inches="tight")
    plt.close(fig)
    return path


def train_price_model(df):
    num_cols, cat_cols = build_feature_lists(df, "price")
    X = df[num_cols + cat_cols].copy()
    y = df["price"].values

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=RANDOM_STATE
    )

    pipe = Pipeline([
        ("pre", _build_preprocessor(num_cols, cat_cols)),
        ("rf", RandomForestRegressor(
            n_estimators=200, max_depth=22, min_samples_leaf=2,
            n_jobs=-1, random_state=RANDOM_STATE,
        )),
    ])
    pipe.fit(X_train, y_train)
    y_pred = pipe.predict(X_test)

    metrics = {
        "r2": float(r2_score(y_test, y_pred)),
        "mae": float(mean_absolute_error(y_test, y_pred)),
        "rmse": float(np.sqrt(mean_squared_error(y_test, y_pred))),
        "n_train": len(X_train),
        "n_test": len(X_test),
        "features_numeric": num_cols,
        "features_categorical": cat_cols,
    }

    fig, ax = plt.subplots(figsize=(7, 7))
    sample_idx = np.random.RandomState(RANDOM_STATE).choice(
        len(y_test), size=min(2000, len(y_test)), replace=False
    )
    ax.scatter(y_test[sample_idx], y_pred[sample_idx], alpha=0.3, s=15)
    lims = [min(y_test.min(), y_pred.min()), max(y_test.max(), y_pred.max())]
    ax.plot(lims, lims, "r--", label="Ideal y=x")
    ax.set_xscale("log"); ax.set_yscale("log")
    ax.set_xlabel("Actual Price (Rs)")
    ax.set_ylabel("Predicted Price (Rs)")
    ax.set_title(f"RF Price Predictor: Predicted vs Actual\n"
                 f"R2={metrics['r2']:.3f}  MAE=Rs.{metrics['mae']:.0f}  RMSE=Rs.{metrics['rmse']:.0f}")
    ax.legend()
    _save_fig(fig, "rf_predicted_vs_actual.png")

    residuals = y_test - y_pred
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.scatter(y_pred[sample_idx], residuals[sample_idx], alpha=0.3, s=15)
    ax.axhline(0, color="red", linestyle="--")
    ax.set_xscale("log")
    ax.set_xlabel("Predicted Price (Rs, log)")
    ax.set_ylabel("Residual (Actual − Predicted)")
    ax.set_title("RF Price Predictor: Residuals")
    _save_fig(fig, "rf_residuals.png")

    rf = pipe.named_steps["rf"]
    feature_names = pipe.named_steps["pre"].get_feature_names_out()
    importances = rf.feature_importances_
    top = pd.Series(importances, index=feature_names).sort_values(ascending=False).head(20)
    fig, ax = plt.subplots(figsize=(10, 8))
    sns.barplot(x=top.values, y=top.index, ax=ax, palette="mako")
    ax.set_title("Top 20 Feature Importances — RF Price Model")
    ax.set_xlabel("Importance")
    _save_fig(fig, "rf_feature_importance.png")

    joblib.dump(pipe, MODELS_DIR / "rf_price_model.joblib")
    return pipe, metrics


def train_discount_classifier(df):
    num_cols, cat_cols = build_feature_lists(df, "discount")
    X = df[num_cols + cat_cols].copy()
    y = df["is_high_discount"].values

    if len(np.unique(y)) < 2:
        return None, {"error": "only one class for discount target"}

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=RANDOM_STATE, stratify=y
    )

    pipe = Pipeline([
        ("pre", _build_preprocessor(num_cols, cat_cols)),
        ("rf", RandomForestClassifier(
            n_estimators=250, max_depth=18, min_samples_leaf=3,
            class_weight="balanced", n_jobs=-1, random_state=RANDOM_STATE,
        )),
    ])
    pipe.fit(X_train, y_train)
    y_pred = pipe.predict(X_test)
    y_proba = pipe.predict_proba(X_test)[:, 1]

    metrics = {
        "accuracy": float(accuracy_score(y_test, y_pred)),
        "f1": float(f1_score(y_test, y_pred)),
        "roc_auc": float(roc_auc_score(y_test, y_proba)),
        "report": classification_report(y_test, y_pred, output_dict=True),
        "n_train": len(X_train),
        "n_test": len(X_test),
    }

    cm = confusion_matrix(y_test, y_pred)
    fig, ax = plt.subplots(figsize=(6, 5))
    sns.heatmap(cm, annot=True, fmt="d", cmap="Blues", ax=ax,
                xticklabels=["Low/no", "High"], yticklabels=["Low/no", "High"])
    ax.set_xlabel("Predicted"); ax.set_ylabel("Actual")
    ax.set_title(f"Discount Classifier Confusion Matrix\n"
                 f"Accuracy={metrics['accuracy']:.2%}  F1={metrics['f1']:.2f}")
    _save_fig(fig, "discount_classifier_confusion.png")

    fpr, tpr, _ = roc_curve(y_test, y_proba)
    fig, ax = plt.subplots(figsize=(7, 6))
    ax.plot(fpr, tpr, label=f"ROC (AUC={metrics['roc_auc']:.3f})", linewidth=2)
    ax.plot([0, 1], [0, 1], "k--", label="Random")
    ax.set_xlabel("False Positive Rate"); ax.set_ylabel("True Positive Rate")
    ax.set_title("Discount Classifier — ROC Curve")
    ax.legend()
    _save_fig(fig, "discount_classifier_roc.png")

    joblib.dump(pipe, MODELS_DIR / "rf_discount_classifier.joblib")
    return pipe, metrics


def train_clusters(df, n_clusters=DEFAULT_N_CLUSTERS):
    num_cols, _ = build_feature_lists(df, "cluster")
    X = df[num_cols].copy().fillna(df[num_cols].median())

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    km = KMeans(n_clusters=n_clusters, random_state=RANDOM_STATE, n_init=10)
    labels = km.fit_predict(X_scaled)

    pca = PCA(n_components=2, random_state=RANDOM_STATE)
    coords = pca.fit_transform(X_scaled)

    fig, ax = plt.subplots(figsize=(10, 7))
    palette = sns.color_palette("Set2", n_clusters)
    for c in range(n_clusters):
        mask = labels == c
        ax.scatter(coords[mask, 0], coords[mask, 1], s=12, alpha=0.5,
                   color=palette[c], label=f"Cluster {c} (n={mask.sum()})")
    ax.set_title("Product Segmentation — KMeans on PCA(2D)")
    ax.set_xlabel("PC1"); ax.set_ylabel("PC2")
    ax.legend()
    _save_fig(fig, "kmeans_clusters_pca.png")

    df_with_labels = df.copy()
    df_with_labels["cluster"] = labels
    profile = df_with_labels.groupby("cluster")[num_cols].mean()

    fig, ax = plt.subplots(figsize=(10, 5))
    sns.heatmap(profile.T, annot=True, fmt=".1f", cmap="YlOrRd", ax=ax)
    ax.set_title("KMeans Cluster Profiles (mean values)")
    ax.set_xlabel("Cluster")
    _save_fig(fig, "kmeans_cluster_profiles.png")

    metrics = {
        "n_clusters": n_clusters,
        "cluster_sizes": {int(k): int(v) for k, v in pd.Series(labels).value_counts().items()},
        "features": num_cols,
    }

    joblib.dump({"scaler": scaler, "kmeans": km, "pca": pca, "features": num_cols},
                MODELS_DIR / "kmeans_clusters.joblib")
    return df_with_labels, metrics


def save_metrics(metrics_dict):
    path = MODELS_DIR.parent / "reports" / "metrics.json"
    with open(path, "w") as f:
        json.dump(metrics_dict, f, indent=2, default=str)
    return path
