# ITCS 6190/8190 - Cloud Computing for Data Analysis

## Course Project: Data Analysis with Apache Spark

### CcMart - E-Commerce Transaction Analytics Pipeline

An end-to-end big data analytics pipeline built with Apache Spark to analyze transactional e-commerce data. The pipeline processes customer behavior, product catalogs, transactions, and clickstream data to generate actionable business insights, predictive models, and a real-time streaming intelligence layer.

---

### Dataset

**[E-commerce App Transactional Dataset](https://www.kaggle.com/datasets/bytadit/transactional-ecommerce)** (~2 GB)

| Table | Records | Description |
|-------|---------|-------------|
| Customer | ~100,000 | User profiles, demographics, device info, location |
| Product | ~44,000 | Fashion product catalog with categories, colors, seasons |
| Transaction | ~850,000 | Purchase orders with payment, shipping, promos |
| Click Stream | ~12.8M | User app activity events (clicks, searches, bookings) |

> **License:** CC BY-NC-ND (study/portfolio use only)
> **Storage:** Full dataset stored externally; small samples committed to `data/sample/`

---

### Pipeline Architecture

```
Raw CSVs (data/raw/)
    |
    v
[1] Data Ingestion  (src/ingestion.py)
    |   - Schema validation & type casting
    |   - Parquet output (10x compression vs CSV)
    v
[2] Exploratory Data Analysis  (src/eda.py)
    |   - Demographics, payment, temporal, cross-table analysis
    |   - 5 visualisation charts saved to outputs/eda/
    v
[3] Spark SQL - 6 Advanced Queries  (src/transformations.py)
    |   - Conversion funnel (CTEs + ROW_NUMBER)
    |   - Market basket analysis (LATERAL VIEW EXPLODE + lift)
    |   - Cohort retention (MONTHS_BETWEEN + Pivot)
    |   - RFM scoring (NTILE(5) + composite score)
    |   - Purchase velocity (LAG + LEAD + DATEDIFF)
    |   - Product affinity (browse-to-buy lift)
    v
[4] Structured Streaming  (src/streaming.py)
    |   - Simulated real-time clickstream ingestion
    |   - 5-minute windowed aggregations + traffic breakdown
    v
[5] Streaming ML Predictions  (src/streaming_predictions.py)
    |   - Phase 1: Train RF model on historical batch data
    |   - Phase 2: foreachBatch inference on live stream
    |   - Stream-Static JOIN for product catalog enrichment
    v
[6] ML Pipeline  (src/ml_pipeline.py)
    |   - Random Forest classifier (payment prediction, 95.4% accuracy)
    |   - KMeans clustering k=5 (customer segmentation, Silhouette 0.629)
    |   - ALS collaborative filtering (personalised recommender, RMSE 0.824)
    v
[7] Dashboard & Simulation
    |   - dashboard/index.html  -> 8-page BI command centre
    |   - dashboard/simulation.html -> live e-commerce simulation
    v
[8] Outputs
        - outputs/eda/           -> 5 chart PNGs
        - outputs/sql_results/   -> 6 query CSVs
        - outputs/ml/            -> model metrics JSON + cluster CSV
        - outputs/streaming/     -> session predictions + metrics
```

---

### Spark Components Used

| Component | Usage |
|-----------|-------|
| **Structured APIs** | DataFrame ingestion, transformations, aggregations, multi-table JOINs |
| **Spark SQL** | 6 complex queries: window functions, CTEs, LATERAL VIEW EXPLODE |
| **Structured Streaming** | Real-time clickstream with windowed aggregations + foreachBatch ML |
| **MLlib** | RF Classifier + KMeans Clustering + ALS Recommender (Pipeline API) |
| **Parquet** | Columnar storage format - 10x smaller, 30x faster reads than CSV |

---

### Quick Start

#### Prerequisites
- Python 3.11+
- Java 11 (JDK)
- Apache Spark / PySpark 3.5+

#### Install Dependencies
```bash
pip install -r requirements.txt
```

#### Run Full Pipeline
```bash
make run
# or
bash run.sh
```

#### Run Individual Components
```bash
python src/ingestion.py             # Step 1: Data ingestion
python src/eda.py                   # Step 2: Exploratory data analysis
python src/transformations.py       # Step 3: Spark SQL queries
python src/streaming.py             # Step 4: Streaming demo
python src/streaming_predictions.py # Step 5: Streaming ML predictions
python src/ml_pipeline.py           # Step 6: ML pipeline (RF + KMeans + ALS)
```

#### Open Dashboard
```
dashboard/index.html       -> Full BI Dashboard (8 pages)
dashboard/simulation.html  -> Live e-commerce simulation with analytics panel
```

#### Generate Presentation
```bash
python generate_final_ppt.py
# Output: dashboard/CcMart_Final_Presentation.pptx (12 slides)
```

---

### Project Structure

```
├── data/
│   ├── raw/                 # Full Kaggle dataset (not committed to git)
│   ├── sample/              # Small samples for testing
│   ├── stream_input/        # JSON event files for streaming simulation
│   ├── stream_output/       # Streaming results
│   └── processed/           # Parquet output (auto-generated)
├── docs/
│   ├── dataset_overview.md
│   ├── methodology.md
│   ├── results.md
│   ├── limitations.md
│   ├── reproduction_guide.md
│   └── slides/
│       └── CcMart_Final_Presentation.pptx
├── notebooks/
│   ├── ingestion.ipynb      # Data ingestion walkthrough
│   ├── eda.ipynb            # EDA with code + findings
│   ├── sql_queries.ipynb    # 6 SQL queries explained
│   ├── streaming_demo.ipynb # Streaming concepts + demo
│   └── ml_pipeline.ipynb   # RF + KMeans + ALS with rationale
├── src/
│   ├── ingestion.py         # Data ingestion pipeline
│   ├── eda.py               # Exploratory data analysis
│   ├── transformations.py   # Spark SQL queries (6 queries)
│   ├── streaming.py         # Structured Streaming pipeline
│   ├── streaming_predictions.py # Real-time ML scoring (foreachBatch)
│   ├── ml_pipeline.py       # ML pipeline (RF + KMeans + ALS)
│   ├── generate_sample_data.py  # Generates sample data for testing
│   └── utils.py             # Shared Spark session utilities
├── tests/
│   ├── test_ingestion.py
│   ├── test_ml.py
│   ├── test_sql.py
│   └── test_streaming.py
├── dashboard/
│   ├── index.html           # 8-page BI dashboard
│   ├── simulation.html      # Live e-commerce simulation
│   ├── app.js               # Dashboard JavaScript
│   ├── style.css            # Dashboard styles
│   └── CcMart_Final_Presentation.pptx
├── outputs/                 # Generated visualisations & results (auto-generated)
├── generate_final_ppt.py    # Generates CcMart_Final_Presentation.pptx
├── requirements.txt
├── Makefile
└── run.sh
```

---

### Team

**Group 6**

| Name | Role |
|------|------|
| | |
| | |
| | |
| | |
| | |
