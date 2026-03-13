# ITCS 6190/8190 – Cloud Computing for Data Analysis

## Course Project: Data Analysis with Apache Spark

### 🛒 Customer360: E-Commerce Transaction Analytics Pipeline

An end-to-end big data analytics pipeline built with Apache Spark to analyze transactional e-commerce data. The pipeline processes customer behavior, product catalogs, transactions, and click stream data to generate actionable business insights and predictive models.

---

### 📊 Dataset

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

### 🏗️ Pipeline Architecture

```
Raw CSVs (data/raw/)
    │
    ▼
[1] Data Ingestion (Structured APIs)
    │   • Schema validation & type casting
    │   • JSON metadata parsing
    │   • Parquet output
    ▼
[2] Exploratory Data Analysis
    │   • Summary statistics & null analysis
    │   • Distribution analysis (demographics, products, payments)
    │   • Temporal patterns & geographic insights
    │   • Cross-table joins & visualizations
    ▼
[3] Complex SQL Queries (Spark SQL)
    │   • Revenue ranking with window functions
    │   • Customer Lifetime Value (CLV) with CTEs
    │   • Conversion funnel analysis
    │   • Promo code effectiveness
    ▼
[4] Streaming (Structured Streaming)
    │   • Simulated real-time click stream ingestion
    │   • Windowed aggregations
    ▼
[5] ML Pipeline (MLlib)
    │   • Payment status classification
    │   • Customer segmentation (KMeans)
    ▼
[6] Outputs & Visualizations
```

---

### 🔧 Spark Components

| Component | Usage |
|-----------|-------|
| **Structured APIs** | DataFrame ingestion, transformations, aggregations, joins |
| **Spark SQL** | Complex queries with window functions, CTEs, subqueries |
| **Streaming** | Real-time click stream processing with Structured Streaming |
| **MLlib** | Classification (payment prediction) & clustering (customer segments) |

---

### 🚀 Quick Start

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
python src/ingestion.py        # Data ingestion
python src/eda.py              # Exploratory data analysis
python src/transformations.py  # Spark SQL queries
python src/streaming.py        # Streaming component
python src/ml_pipeline.py      # ML pipeline
```

---

### 📁 Project Structure

```
├── data/
│   ├── raw/                 # Full Kaggle dataset (not committed)
│   ├── sample/              # Small samples for testing
│   └── processed/           # Parquet output (generated)
├── docs/
│   ├── dataset_overview.md
│   ├── methodology.md
│   ├── results.md
│   ├── limitations.md
│   ├── reproduction_guide.md
│   └── slides/
├── notebooks/
│   ├── eda.ipynb
│   ├── ingestion.ipynb
│   ├── sql_queries.ipynb
│   ├── streaming_demo.ipynb
│   └── ml_pipeline.ipynb
├── src/
│   ├── ingestion.py         # Data ingestion pipeline
│   ├── eda.py               # Exploratory data analysis
│   ├── transformations.py   # Spark SQL queries
│   ├── streaming.py         # Streaming component
│   ├── ml_pipeline.py       # ML pipeline
│   └── utils.py             # Shared utilities
├── tests/
├── outputs/                 # Generated visualizations & results
├── requirements.txt
├── Makefile
└── run.sh
```

---

### 👥 Team

**Group 6**

<!-- Add team member details here -->
| Name | Role |
|------|------|
| | |
| | |
| | |
| | |
| | |
