"""End-to-end ML pipeline.

Usage:
    python pipeline.py --input data/blinkit_final_extraction.csv
    python pipeline.py --input data/zepto_export.csv --tag zepto

Reads any q-commerce CSV that follows the expected schema (see
reports/OBSERVATIONS.md), cleans + feature-engineers it, generates EDA
charts, trains three ML models (RF price predictor, RF discount
classifier, KMeans clusters), and writes everything under reports/ and
models/.
"""
import argparse
import json
import time
from pathlib import Path

from src.ml_pipeline.cleaner import load_csv, clean
from src.ml_pipeline.features import add_features
from src.ml_pipeline.eda import run_all_eda
from src.ml_pipeline.models import (
    train_price_model, train_discount_classifier, train_clusters, save_metrics,
)
from src.ml_pipeline.config import CLEAN_DATA_DIR, REPORTS_DIR


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/blinkit_final_extraction.csv")
    parser.add_argument("--tag", default="blinkit",
                        help="Name to prefix cleaned-data output file")
    args = parser.parse_args()

    t0 = time.time()
    print(f"\n=== Q-Commerce ML Pipeline ===")
    print(f"Input: {args.input}\n")

    print("[1/5] Loading & cleaning...")
    raw = load_csv(args.input)
    cleaned, clean_report = clean(raw)
    print(f"   rows: {clean_report['input_rows']} -> {clean_report['output_rows']}")
    print(f"   removed duplicates: {clean_report['dedupe_removed']}")
    print(f"   dropped constant cols: {clean_report['dropped_constant_columns']}")

    print("\n[2/5] Feature engineering...")
    df = add_features(cleaned)
    clean_path = CLEAN_DATA_DIR / f"{args.tag}_clean.parquet"
    df.to_parquet(clean_path, index=False)
    print(f"   features: {list(df.columns)}")
    print(f"   saved cleaned dataset -> {clean_path}")

    print("\n[3/5] EDA charts...")
    saved_charts = run_all_eda(df)
    print(f"   produced {len(saved_charts)} charts: {saved_charts}")

    print("\n[4/5] Training models...")
    print("   - Random Forest price predictor...")
    _, price_metrics = train_price_model(df)
    print(f"     R²={price_metrics['r2']:.3f}  MAE=₹{price_metrics['mae']:.2f}  RMSE=₹{price_metrics['rmse']:.2f}")

    print("   - Random Forest discount classifier...")
    _, disc_metrics = train_discount_classifier(df)
    if "error" not in disc_metrics:
        print(f"     Accuracy={disc_metrics['accuracy']:.2%}  F1={disc_metrics['f1']:.3f}  AUC={disc_metrics['roc_auc']:.3f}")
    else:
        print(f"     skipped: {disc_metrics['error']}")

    print("   - KMeans clustering...")
    df_clustered, cluster_metrics = train_clusters(df)
    df_clustered.to_parquet(CLEAN_DATA_DIR / f"{args.tag}_clustered.parquet", index=False)
    print(f"     clusters: {cluster_metrics['cluster_sizes']}")

    print("\n[5/5] Saving metrics...")
    metrics = {
        "tag": args.tag,
        "input": args.input,
        "clean_report": clean_report,
        "price_model": price_metrics,
        "discount_classifier": disc_metrics,
        "clusters": cluster_metrics,
        "elapsed_sec": round(time.time() - t0, 1),
    }
    metrics_path = save_metrics(metrics)
    print(f"   saved metrics -> {metrics_path}")

    print(f"\n=== Done in {metrics['elapsed_sec']}s ===")
    print(f"Charts: {REPORTS_DIR / 'figures'}/")
    print(f"Models: {Path('models')}/")
    print(f"Cleaned data: {clean_path}")
    print(f"\nNext: streamlit run dashboard/app.py")


if __name__ == "__main__":
    main()
