# Q-Commerce Price Intelligence — Data + ML Pipeline

An end-to-end data science project on the **Quick-Commerce (Q-Commerce)** sector. The system collects live product catalogue data from a hyper-local Q-Commerce delivery platform, cleans and enriches it through a reusable pipeline, trains three machine-learning models, and exposes the insights through an interactive dashboard.

The pipeline is **schema-driven and platform-agnostic** — once a new dataset is dropped in with the same column structure, every step (cleaning, feature engineering, EDA charts, model training) re-runs automatically.

---

## Highlights

| Phase | What it does |
|---|---|
| **1. Acquisition** | Asynchronous scraper with TLS-fingerprint impersonation, self-healing browser sessions, and adaptive backoff on rate limits |
| **2. Storage** | Analytical database (DuckDB) with append-only `price_history` schema |
| **3. Cleaning** | Schema validation, constant-column auto-drop, product deduplication, multi-pack unit parsing, missing-value imputation |
| **4. Feature engineering** | Discount %, price-per-unit, log-price, multi-pack flag, out-of-stock flag, automatic category mapping |
| **5. EDA** | 8 publication-quality charts auto-generated to `reports/figures/` |
| **6. Modeling** | Random Forest regressor (price prediction), Random Forest classifier (high-discount detection), KMeans + PCA (product segmentation) |
| **7. Dashboard** | Streamlit app with 6 tabs including a live RF inference page |

---

## Project structure

```text
colllege_project/
├── main.py                         # Scraper orchestrator
├── pipeline.py                     # ML pipeline entry point
│
├── src/
│   ├── scraper/                    # Phase 1: data acquisition
│   │   ├── session.py              # Playwright-based session management
│   │   └── client.py               # Async HTTP client with retry logic
│   ├── pipeline/                   # Phase 2: JSON → tabular
│   │   └── processor.py            # Polars data extraction & unit normalization
│   ├── database/                   # Phase 3: storage
│   │   └── db.py                   # DuckDB schema + persistence
│   └── ml_pipeline/                # Phase 4–6: ML pipeline (reusable)
│       ├── config.py               # Schema, thresholds, category map, paths
│       ├── cleaner.py              # Validation, dedup, normalization, imputation
│       ├── features.py             # Feature engineering & feature lists
│       ├── eda.py                  # Auto-EDA chart generators
│       └── models.py               # RF regressor, RF classifier, KMeans
│
├── dashboard/
│   └── app.py                      # Streamlit dashboard (Phase 7)
│
├── data/                           # Raw scraped CSV/DB (gitignored)
│   └── clean/                      # Pipeline-cleaned parquet outputs
│
├── reports/
│   ├── figures/                    # All auto-generated charts
│   ├── metrics.json                # Last-run model metrics
│   ├── OBSERVATIONS.md             # Data patterns + cleaning rules
│   └── DEMO_TALKING_POINTS.md      # Presentation notes
│
├── models/                         # Trained .joblib artifacts (gitignored)
├── requirements.txt
└── README.md
```

---

## Technology stack

| Layer | Tools | Why |
|---|---|---|
| **Scraping** | `curl_cffi`, `playwright`, `asyncio` | TLS fingerprint impersonation, dynamic auth-token capture, high-concurrency request handling |
| **Processing** | `polars`, `pandas` | Fast columnar operations on tabular data |
| **Storage** | `duckdb`, `parquet` | Analytical, in-process, zero-config |
| **ML** | `scikit-learn`, `joblib` | Random Forest, KMeans, pipelines, model serialization |
| **Visualization** | `matplotlib`, `seaborn`, `plotly` | Static (reports) + interactive (dashboard) |
| **Dashboard** | `streamlit` | Rapid data-app prototyping |

---

## Quick start

### Installation

```bash
# Clone the repository
git clone <your-repo-url>
cd colllege_project

# Setup environment
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

### Run the data acquisition layer

```bash
python main.py
```

Outputs:
- `data/<platform>_data.db` — DuckDB analytical store
- `data/<platform>_final_extraction.csv` — flat CSV snapshot
- `data/progress.log` — per-batch checkpoint log

### Run the ML pipeline

```bash
python pipeline.py --input data/<platform>_final_extraction.csv
```

Runs end-to-end in ~10 seconds. Generates:
- Cleaned dataset → `data/clean/*.parquet`
- 14 charts → `reports/figures/`
- 3 trained models → `models/`
- Metrics → `reports/metrics.json`

### Launch the dashboard

```bash
streamlit run dashboard/app.py
```

Opens at `http://localhost:8501`. Six tabs: **Overview · EDA · Price Predictor · Discount Insights · Clusters · Pipeline**.

---

## ML models

### Model 1 — Random Forest price predictor (regression)

> Given a product's attributes (MRP, brand, category, unit, rating, inventory), predict its expected selling price.

- **Held-out R²:** 0.825
- **Mean absolute error:** ~Rs.40
- **Top features:** MRP, log-MRP, price-per-unit, brand, category
- **Use case:** catalogue auditing, mispricing detection, dynamic price recommendation

### Model 2 — Random Forest discount classifier

> Given a product's attributes, predict whether it is heavily discounted (≥15% off MRP).

- **Accuracy:** 85.9%
- **F1 score:** 0.88
- **ROC AUC:** 0.94
- **Use case:** real-time promotion flagging, competitor-pricing intelligence

### Model 3 — KMeans + PCA product segmentation (unsupervised)

> Group products into segments based on price, MRP, discount behavior, unit size, rating, and inventory — without any labels.

- **k = 4** (chosen via inertia / domain reasoning)
- Cluster sizes reveal a clear hierarchy: mass-market → mid-tier → premium → outlier electronics
- **Use case:** assortment planning, persona-style customer experiences

---

## Reusable, schema-driven pipeline

The ML pipeline is intentionally decoupled from any specific Q-Commerce platform. It works on any CSV that exposes the canonical schema (see `reports/OBSERVATIONS.md`):

```bash
python pipeline.py --input data/another_platform.csv --tag platform_b
streamlit run dashboard/app.py
```

The pipeline auto-handles:

1. **Schema validation** — exits cleanly if required columns are missing.
2. **Type coercion** — numeric strings → numbers, empty strings → null.
3. **Constant-column drop** — any column with one unique value is removed.
4. **Product deduplication** — keep first occurrence per `product_id`.
5. **Multi-pack unit parsing** — `"x 200 g"` with `unit_value=2` → 400g equivalent.
6. **Rare-unit bucketing** — long-tail unit types collapsed into `"other"`.
7. **Missing-value imputation** — median for numeric, `"unbranded"` for brand.
8. **Feature engineering** — derives 9+ analytical features.
9. **Auto-EDA** — every chart whose required columns exist gets generated.
10. **Model training** — three models trained and serialized.
11. **Metrics export** — JSON snapshot for downstream reporting.

Every cleaning rule is documented and justified in [`reports/OBSERVATIONS.md`](reports/OBSERVATIONS.md).

---

## Auto-generated charts

Each chart is produced by the pipeline only if its required columns exist in the dataset.

**EDA**
- Price distribution (log scale)
- Discount-percentage distribution
- Average discount by category
- Top 15 brands by product count
- Price vs Rating (sample, hue = category)
- Out-of-stock rate by category
- Price box-plots by category
- Numeric correlation heatmap

**Models**
- RF: predicted vs actual (log-log scatter, ideal y=x line)
- RF: residual plot
- RF: top-20 feature importances
- Classifier: confusion matrix
- Classifier: ROC curve (with AUC)
- KMeans: PCA(2D) cluster scatter
- KMeans: per-cluster mean profile heatmap

---

## Data observations layer

The file [`reports/OBSERVATIONS.md`](reports/OBSERVATIONS.md) is treated as a **specification, not a log**. Every data pattern observed during exploration drives a generalized rule that the pipeline applies to any future dataset of the same shape:

| Observation | Pipeline rule |
|---|---|
| `unavail_qty` is always 0 in this snapshot | Auto-drop columns with one unique value |
| Same product appears under multiple search keywords | Dedup on `product_id`, keep first |
| Multi-pack notation `"x 200 g"` co-exists with simple units | Regex parser detects multi-pack, normalizes total quantity |
| ~15% of rows have missing rating | Median imputation per dataset |
| Brand can be empty string (not null) | Replace with `"unbranded"` and treat as a real category |
| Price is right-skewed (₹1 to ₹60,990) | Log-transform target for residual diagnostics |
| Categories are not in the data — only in the scraper config | Reverse-map keyword → category at load time |

When a new platform's data arrives, no code changes — the pipeline applies the same rules and produces the same artifacts.

---

## Future work

The architecture is forecasting-ready. The `scraped_at` column and `price_history` schema already support time-series, but a single snapshot is insufficient for forecasting. Once daily snapshots accumulate (≥7 days):

- **Prophet** (open-source forecasting library) → trend + seasonality decomposition for hero products
- **Random Forest with lag features** (`price_t-1`, `price_t-7`, rolling means) → tabular forecasting
- **Time-series cross-validation** for honest evaluation
- A new "Forecasts" tab in the dashboard, with confidence-interval bands

Other directions:

- Multi-platform price comparison via the same pipeline applied to several Q-Commerce sources
- Geographic price comparison by varying scraping coordinates
- Inventory anomaly detection (stockout prediction)
- A FastAPI service exposing the trained models over HTTP for downstream apps

---

## Disclaimer

This project is for **educational and research purposes only**. The codebase demonstrates production-style scraping techniques in a controlled setting; before running against any real platform ensure compliance with the platform's Terms of Service, `robots.txt`, and applicable laws.
