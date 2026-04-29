# Demo Talking Points

Use these as a guide while presenting. Each section maps to a tab in the dashboard.

---

## 1. Project pitch (30 seconds)

> "We built a complete data science pipeline for Q-commerce price intelligence. It scrapes live product data from Blinkit, cleans and enriches it, trains three machine-learning models, and surfaces insights through an interactive dashboard. The pipeline is reusable — drop in a CSV from Zepto, Instamart, or BigBasket with the same schema and everything re-runs automatically."

## 2. The data layer (1 minute)

- **Source:** scraped 14,215 product records from Blinkit across 1,000+ search keywords, spanning 27 categories.
- **Anti-bot:** TLS-fingerprint impersonation via `curl_cffi`, Playwright session refresh on 429/401.
- **Storage:** DuckDB for analytical queries, CSV exports for portability.

> "We applied production-grade scraping techniques because Q-commerce APIs are aggressively rate-limited."

## 3. The ML pipeline (2 minutes)

Walk through the **Pipeline tab** in the dashboard. Show that one command runs everything:

```bash
python pipeline.py --input data/blinkit_final_extraction.csv
```

Highlight:
- **Schema validation** — pipeline rejects malformed input cleanly.
- **Auto-cleaning** — drops constant columns (`unavail_qty`), dedupes by `product_id` (14,215 → 6,638), normalizes multi-pack units (`"x 200 g"` → 200g × pack count).
- **Feature engineering** — derives `discount_pct`, `is_high_discount`, `price_per_unit`, `is_multipack`, `category` (reverse-mapped from keyword), `is_oos`.
- **Documented in `reports/OBSERVATIONS.md`** — every cleaning rule was driven by a real pattern observed in the data.

## 4. Models (3 minutes)

### Model 1 — Random Forest price predictor (regression)

> "Given product attributes (MRP, brand, category, unit, rating), predict the selling price."

- **Why Random Forest?** Handles mixed numeric + categorical features natively, robust to outliers, gives feature importance for free.
- **Result:** R² = 0.825, MAE ≈ Rs.40 on a held-out test set.
- **What this means:** The model can predict the selling price within ~₹40 average error — useful for catalog-price auditing or detecting mispriced products.
- **Show:** Predicted-vs-actual plot, feature importance chart, live demo (input attributes → predicted price).

### Model 2 — Random Forest discount classifier

> "Given product attributes, predict whether it's heavily discounted (≥15% off MRP)."

- **Result:** Accuracy 85.9%, F1 = 0.877, ROC AUC = 0.94.
- **Use case:** Spot products likely to be on promotion — interesting for consumers, useful for competitor analysis.
- **Show:** Confusion matrix, ROC curve.

### Model 3 — KMeans clustering (unsupervised)

> "Without labels, group products into segments by price, discount behavior, unit size, and rating."

- **4 clusters** emerged automatically. Cluster sizes show clear segmentation (mass-market vs premium vs outlier electronics).
- **Show:** PCA 2D scatter, cluster profile heatmap, per-cluster product browser.

## 5. The reusable pipeline angle (1 minute)

This is what differentiates the project from a one-off notebook.

> "The pipeline is fully data-agnostic within its schema. We documented every pattern we observed in `OBSERVATIONS.md` and turned each into a generalized rule. So when we get Zepto data tomorrow, we just run `python pipeline.py --input zepto.csv --tag zepto` and get the same EDA, the same models, the same dashboard — without touching code."

## 6. Honest "Future Work" (1 minute)

> "We deliberately scoped out time-series forecasting (Prophet, RF with lag features) because it requires longitudinal data — multiple snapshots per product over weeks. Our DuckDB schema already stores `scraped_at`, so the moment we run daily scrapes, the time series accumulates and we plug in Prophet for forecasting. The architecture is ready; only the data history is missing."

This shows engineering judgment — you understand forecasting and consciously chose not to fake it.

---

## Likely evaluator questions & answers

**Q: Why Random Forest and not deep learning?**
A: With 6,638 rows and ~30 mixed features, RF is the right complexity. Deep learning would overfit and gain nothing. RF gives interpretability via feature importance, which we can show on the slide.

**Q: How do you know your model isn't overfitting?**
A: 80/20 train/test split, R² reported on held-out test set, residual plot shows no systematic pattern.

**Q: What was your biggest data challenge?**
A: Multi-pack unit notation. Items like "Amul Taaza 2-pack" came in as `unit_value=2, unit_type="x 200 ml"`. We wrote a regex parser that detects multi-pack format and re-derives total quantity (2 × 200 = 400 ml). Without this, `price_per_unit` would have been wildly wrong.

**Q: How would you scale this?**
A: (1) Replace single-merchant scraping with multi-vendor (Zepto, Instamart) via the same pipeline. (2) Move DuckDB to a managed warehouse. (3) Schedule daily scrapes for time-series. (4) Add Prophet forecasting once we have ≥7 days of history.

**Q: Why did you dedupe so aggressively (14k → 6.6k)?**
A: The same product appeared under multiple search keywords (e.g., "Milk", "Milk pack", "Milk discount"). Modeling without dedup would have over-weighted popular products. We kept the first occurrence per `product_id`.

**Q: What's the business value?**
A: (1) Price-prediction model = mispricing detection. (2) Discount classifier = real-time promo flagging for consumers/competitors. (3) Clustering = product portfolio segmentation. (4) Dashboard = decision-support tool for category managers.
