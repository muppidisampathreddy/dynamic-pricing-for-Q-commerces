# Data Observations & Pipeline Spec

This document captures patterns observed in Q-commerce scraped data. Each observation drives a step in the reusable pipeline (`src/ml_pipeline/`). When you bring in new data (Zepto, Instamart, BigBasket) with similar columns, the pipeline applies these rules automatically.

## Expected schema (input CSV)

| Column | Type | Required | Notes |
|---|---|---|---|
| `scraped_at` | timestamp | yes | When the row was captured |
| `keyword` | string | yes | Search term used to find the product |
| `product_id` | int | yes | Stable product identifier |
| `merchant_id` | int | optional | Dark store / vendor identifier |
| `product_name` | string | yes | Display name |
| `brand` | string | optional | May be empty for unbranded |
| `price` | float | yes | Selling price (₹) |
| `mrp` | float | yes | Maximum retail price (₹) |
| `unit_value` | float | yes | Numeric quantity (e.g., 500 for "500 g") |
| `unit_type` | string | yes | Unit ("g", "ml", "pc", etc.) |
| `inventory` | int | yes | Available stock |
| `unavail_qty` | int | optional | Often always 0 — drop if constant |
| `rating` | float | optional | 0–5 |

## Observations from Q-Commerce snapshot (14,215 rows, 2026-03-06)

### O1. Duplicate product_ids — same product appears across multiple keywords
- **Observation:** 6,638 unique product_ids in 14,215 rows (~2.1× duplication).
- **Why:** "Milk", "Milk pack", "Milk discount" all return the same Amul Milk product.
- **Pipeline step:** Dedupe on `product_id` before modeling. Keep first occurrence's keyword OR aggregate.

### O2. Rating is missing for ~15% of rows
- **Observation:** 2,147 nulls in `rating` (out of 14,215).
- **Why:** New products with no reviews yet.
- **Pipeline step:** Median imputation per category (or global median fallback).

### O3. Brand field can be empty
- **Observation:** 297 rows with empty string `brand` (not null, just `""`).
- **Pipeline step:** Replace `""` with `"unbranded"`. Treat as a real category.

### O4. `unavail_qty` is always 0 in this snapshot
- **Observation:** Constant column = zero predictive value.
- **Pipeline step:** Auto-drop columns with zero variance (works for any new dataset too).

### O5. `unit_type` has a long tail with multi-pack notation
- **Observation:** Top values: `g, pc, pcs, ml, unit`. But also `"x 200 g"`, `"x 950 ml"` for multi-packs.
- **Why:** The original parser only handled simple units; multi-packs are written as `"unit_value × unit_type"` style.
- **Pipeline step:**
  - Normalize singular/plural: `pc` ↔ `pcs`, `piece` → `pc`.
  - Detect multi-pack: if `unit_type` starts with `x `, parse out the inner unit and multiply `unit_value` accordingly.
  - Bucket rare unit_types (<50 occurrences) into `"other"`.

### O6. `merchant_id` has very low cardinality
- **Observation:** Only 4 distinct merchants in 14k rows.
- **Why:** dark stores serving the lat/lon used during scraping.
- **Pipeline step:** Treat as categorical (one-hot). For new platforms cardinality will differ — pipeline auto-detects.

### O7. Discount is the most interesting derived feature
- **Observation:** `discount_pct = (mrp - price) / mrp * 100`. 82% of rows have discount > 0; mean 26%, median 19%; 56% have >15%.
- **Sanity:** 0 rows where `price > mrp` ✓. 0 rows where `mrp == 0` ✓.
- **Pipeline step:** Always compute `discount_pct`. Use it as both a feature (for price prediction) and a target (for the discount classifier). Threshold for "high discount" defaults to 15% (median 19%, so this is a meaningful split).

### O8. Price has extreme right skew
- **Observation:** Price min=₹1, median=₹199, mean=₹372, max=₹60,990.
- **Why:** Mostly grocery (₹50–500), but electronics push the tail.
- **Pipeline step:** Log-transform price for the regression target (`log1p(price)`). RF doesn't strictly need it, but residual plots look much cleaner.

### O9. Out-of-stock is rare (1.5%)
- **Observation:** Only 218 rows with `inventory == 0`.
- **Pipeline step:** Class imbalance — note in EDA. Can build OOS classifier but flag the imbalance to the user.

### O10. No `category` column — must be derived from `keyword`
- **Observation:** Categories are only available in `main.py`'s scraping config dict.
- **Pipeline step:** Reverse-map keyword → category at load time using a lookup dictionary. For unmapped keywords (e.g., "X pack", "X discount"), strip the suffix and try again. Unmatched → `"misc"`.

### O11. Time axis is not usable — single snapshot
- **Observation:** `scraped_at` spans 1 hour on a single day.
- **Pipeline step:** All time-series modeling code is gated. Pipeline detects # of distinct dates; only enables forecasting if ≥7 days. Otherwise produces snapshot-only models.

## Auto-pipeline behavior on new datasets

When a new CSV is dropped in:

1. **Schema check:** required columns present? error out clean if not.
2. **Constant-column drop:** any column with 1 unique value → drop.
3. **Type coerce:** numeric cols → float/int; strings → strip whitespace.
4. **Empty-string → null:** all string columns.
5. **Dedupe:** by `product_id` (keep first).
6. **Impute:** rating → median; brand → "unbranded".
7. **Feature engineer:** add `discount_pct`, `is_discounted`, `is_high_discount`, `price_per_unit`, `is_multipack`, `category`, `is_oos`.
8. **Auto-EDA:** generate charts to `reports/figures/` based on which columns exist.
9. **Train models:** RF regressor (price), RF classifier (discount), KMeans (segmentation).
10. **Save artifacts:** models to `models/`, cleaned data to `data/clean/`, metrics to `reports/metrics.json`.

## Charts the pipeline always tries to produce

(Each is skipped gracefully if its required columns are missing.)

- **price_distribution.png** — histogram + KDE of price (log scale)
- **discount_by_category.png** — bar chart of mean discount % per category
- **top_brands.png** — bar chart of top 15 brands by product count
- **price_vs_rating.png** — scatter, hue=category
- **inventory_heatmap.png** — heatmap of stock levels by category × brand (top 20)
- **discount_distribution.png** — histogram of discount %
- **out_of_stock_by_category.png** — % OOS per category
- **rf_feature_importance.png** — bar chart of RF feature importances
- **rf_predicted_vs_actual.png** — scatter, ideal y=x line
- **rf_residuals.png** — residual plot
- **discount_classifier_confusion.png** — confusion matrix heatmap
- **discount_classifier_roc.png** — ROC curve
- **kmeans_clusters_pca.png** — 2D PCA projection colored by cluster
- **kmeans_cluster_profiles.png** — per-cluster mean of key features
