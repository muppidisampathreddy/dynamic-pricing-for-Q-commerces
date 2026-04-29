# 🏙️ Blinkit High-Frequency Data Pipeline (2026 Edition)

A production-grade, asynchronous data pipeline designed to scrape, process, and analyze hyper-local commerce data from **Blinkit**. This project was built to bypass advanced anti-bot measures and handle high-volume extractions (~20k+ rows) with professional-standard modularity.

---

## 🚀 Repository Highlights
*   **Anti-Bot Stealth**: Integrated `curl_cffi` for TLS fingerprint impersonation (Chrome 110) and randomized jitter intervals.
*   **Self-Healing Sessions**: Automatic session refresh via Playwright if tokens expire or throttles occur.
*   **Production Architecture**: Modular directory structure (`src/scraper`, `src/pipeline`, `src/database`) for industrial scalability.
*   **Big Data Ready**: Uses **Polars** for multi-threaded data flattening and **DuckDB** for high-performance SQL storage.
*   **Adaptive Backoff**: Smart retry logic that detects `HTTP 429` errors and automatically cools down IP signatures.

---

## 📂 Project Structure
```text
├── main.py                # Central Orchestrator (Run this!)
├── src/
│   ├── scraper/
│   │   ├── session.py     # Playwright-based credential management
│   │   └── client.py      # Async curl_cffi HTTP networking
│   ├── pipeline/
│   │   └── processor.py   # Polars data extraction & unit normalization
│   └── database/
│       └── db.py          # DuckDB schema and storage logic
├── data/                  # [Ignored] Local storage for .db, .csv, and .log files
├── requirements.txt       # Project dependencies
└── .gitignore             # Configured to protect production data
```

---

## 🛠️ Technology Stack
| Layer | Technology | Purpose |
| :--- | :--- | :--- |
| **Networking** | `curl_cffi` | Bypassing TLS Fingerprinting blocks |
| **Automation** | `playwright` | Dynamic `auth_key` and token extraction |
| **Processing**| `polars` | 100x faster than Pandas for JS-flattening |
| **Database** | `duckdb` | In-process SQL database for analytical queries |
| **Async Engine**| `asyncio` | High-concurrency task management |

---

## 📝 Features & Logic
### 1. Unit Normalization (Demand Forecasting ready)
The pipeline automatically parses mixed unit strings to standard numeric values:
*   `"1.5 kg"` → `1500.0 g`
*   `"500 ml"` → `500.0 ml`
*   `"1 liter"` → `1000.0 ml`

### 2. Analytical Schema
We track 13+ critical metrics per product record, including:
*   `price` vs `mrp` (Discount analysis)
*   `inventory` & `unavail_qty` (Stock availability tracking)
*   `rating` (Consumer sentiment)
*   `merchant_id` (Hyper-local mapping)

### 3. Checkpointing
The system saves progress every 50 keywords. If the process is interrupted, your data is safe in `blinkit_data.db`.

---

## ⚡ Quick Start
### Installation
```bash
# Clone the repository
git clone <your-repo-url>
cd blinkit-scraper

# Setup environment
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

### Execution
```bash
python main.py
```

---

## 📊 Dataset Stats
Currently, the pipeline generates ~18,000 to 22,000 rows across **27+ categories** including:
*   Dairy & Breakfast
*   Pharmacy & Wellness
*   Meat & Seafood
*   Home Office Tech
*   Festive & Party Supplies
*   Gourmet Specialities

---

---

## 🤖 ML Pipeline & Dashboard (Phase 2)

After scraping, run a fully automated ML pipeline that cleans the data, engineers features, generates EDA charts, trains three machine-learning models, and exposes everything via a Streamlit dashboard.

### Architecture

```text
src/ml_pipeline/
  config.py        # schema, thresholds, category map
  cleaner.py       # schema validation, dedup, multi-pack unit parsing, imputation
  features.py      # discount_pct, price_per_unit, log_price, category mapping
  eda.py           # auto-generates 8 charts to reports/figures/
  models.py        # RF regressor, RF classifier, KMeans + PCA
pipeline.py        # single entry point — runs everything end-to-end
dashboard/app.py   # Streamlit dashboard with 6 tabs
reports/
  OBSERVATIONS.md  # data patterns + pipeline spec
  DEMO_TALKING_POINTS.md
  metrics.json     # last-run metrics
  figures/*.png    # all charts
models/*.joblib    # trained model artifacts
```

### One-command pipeline

```bash
python pipeline.py --input data/blinkit_final_extraction.csv
```

The pipeline auto-handles: schema validation → constant-column drop → product dedup → multi-pack unit parsing → rare-unit bucketing → median imputation → feature engineering → 8 EDA charts → RF price model → RF discount classifier → KMeans + PCA clustering → metrics export.

### Dashboard

```bash
streamlit run dashboard/app.py
```

Tabs: **Overview · EDA · Price Predictor (interactive) · Discount Insights · Clusters · Pipeline**

### Models trained on this snapshot

| Model | Task | Metric |
|---|---|---|
| RF Regressor | Predict `price` from product attributes | R² = 0.825, MAE ≈ Rs.40 |
| RF Classifier | Predict `is_high_discount` (≥15% off) | Accuracy 85.9%, F1 = 0.88, AUC = 0.94 |
| KMeans (k=4) | Unsupervised product segmentation | 4 clusters by price/discount/unit profile |

### Reusable for any Q-commerce platform

The pipeline is data-agnostic within its schema. Drop in a CSV from Zepto, Instamart, or BigBasket with the same columns and re-run:

```bash
python pipeline.py --input data/zepto_export.csv --tag zepto
streamlit run dashboard/app.py
```

All cleaning rules are documented and justified in [reports/OBSERVATIONS.md](reports/OBSERVATIONS.md).

### Future work — time-series forecasting

The `scraped_at` column and DuckDB `price_history` schema already support time series. Once daily scrapes accumulate ≥7 days of data, plug in **Prophet** or RF with lag features for true price forecasting. Architecture is ready; only the data history is missing.

---

## ⚖️ Disclaimer
This project is for educational and research purposes only. Ensure compliance with Blinkit’s Terms of Service and `robots.txt` before use.
