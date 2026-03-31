# Methodology

## Pipeline Architecture

```
data/raw/ (CSVs)
    │
    ▼
┌─────────────────┐
│  src/ingestion.py │  ← Step 1: Load, validate schemas, parse JSON, save Parquet
└────────┬────────┘
         │
    data/processed/ (Parquet files)
         │
    ┌────┴────────────────────┬──────────────────┐
    ▼                         ▼                   ▼
┌──────────┐          ┌──────────────┐     ┌──────────────────────────┐
│ src/eda.py│          │src/transform.│     │  src/streaming.py        │
│           │          │  ations.py   │     │  (windowed aggregations) │
│ 5 EDA     │          │ 6 SQL        │     └──────────┬───────────────┘
│ dashboards│          │ queries      │                │
└──────────┘          └──────────────┘                ▼
                                          ┌──────────────────────────┐
                                          │src/streaming_predictions │
                                          │  Phase 1: Train RF model │
                                          │  Phase 2: Score sessions │
                                          │  via foreachBatch + MLlib│
                                          └──────────┬───────────────┘
         ┌────────────────────────────────────────────┘
         ▼
    ┌────────────────┐
    │src/ml_pipeline │  ← Classification + Clustering (batch)
    │    .py         │
    └────────────────┘
         │
    outputs/
    ├── eda/            (visualization PNGs)
    ├── sql_results/    (query results)
    ├── streaming/      (traffic breakdown + session_predictions.csv)
    └── ml/             (model metrics, cluster assignments)
```

## Component Details

### 1. Data Ingestion (`src/ingestion.py`)

**Spark Component**: Structured APIs (DataFrame API)

- Reads 4 CSV files with explicitly defined schemas (`StructType`)
- Falls back to `inferSchema` if schema mismatch detected
- Parses embedded JSON columns using `from_json()`:
  - `product_metadata` → struct with `items` array (product_id, quantity, price)
  - `event_metadata` → struct with page, duration_sec, product_id, query, promo_code
- Converts timestamp/date strings to proper Spark types
- Auto-detects data source: prefers `data/raw/` (real data), falls back to `data/sample/` (generated)
- Outputs cleaned Parquet files to `data/processed/`

### 2. Exploratory Data Analysis (`src/eda.py`)

**Spark Component**: Structured APIs + Matplotlib/Seaborn

Generates 5 multi-panel visualization dashboards:

| Dashboard | Panels | Insights |
|-----------|--------|----------|
| 01: Customer Demographics | Gender, device, country, join timeline | 64% Female, Android-dominant, Indonesia-only |
| 02: Product Catalog | Categories, seasons, colours, gender targets | Apparel dominant, Summer-heavy, Black #1 colour |
| 03: Transaction Analysis | Status, payment methods, revenue trends, hourly/daily patterns | 95.7% success, Credit Card dominates, exponential growth |
| 04: Click Stream | Event types, traffic sources, session metrics, hourly patterns | 12.8M events, 90% mobile, 14.3 events/session avg |
| 05: Cross-Table Insights | Revenue by country, device, payment success rates | Single market, Android 3.5x iOS revenue |

### 3. Spark SQL Queries (`src/transformations.py`)

**Spark Component**: Spark SQL

| # | Query | SQL Features |
|---|-------|-------------|
| 1 | Revenue by Payment Method with Ranking | `DENSE_RANK()`, `PARTITION BY`, window functions |
| 2 | Customer Lifetime Value (Top 15) | CTEs (`WITH`), `JOIN`, `DENSE_RANK()` |
| 3 | Promo Code Effectiveness | `CASE WHEN`, conditional aggregation, subquery |
| 4 | Monthly Revenue with Running Total & MoM Growth | `SUM() OVER`, `LAG()`, window frames |
| 5 | Click Stream Conversion Funnel | `CASE WHEN` pivoting, `NULLIF`, session-level aggregation |
| 6 | Revenue by Country & Customer Segment | Nested CTE, `CASE WHEN` segmentation, multi-level aggregation |

### 4. Structured Streaming (`src/streaming.py`)

**Spark Component**: Structured Streaming

**Stream Simulator** (`stream_simulator()`):
- Reads click stream CSV rows
- Writes them as JSON files to `data/stream_input/` with configurable batch size and delay
- Simulates real-time event arrival

**Streaming Consumer** (`run_streaming()`):
- Uses `spark.readStream` to read JSON files from `data/stream_input/`
- **Query 1**: 5-minute tumbling window aggregation — event counts and active sessions per event type
- **Query 2**: Traffic source breakdown per 5-minute window
- Implements watermarking (10-minute late data tolerance)
- Outputs to console (Query 1) and Parquet sink (Query 2)

### 4.5. Streaming ML Predictions (`src/streaming_predictions.py`)

**Spark Components**: Structured Streaming + MLlib + `foreachBatch`

Two-phase pipeline that answers the question *"what predictions can we make on the live stream?"*

**Phase 1 — Train (batch)**:
- Loads historical clickstream + transactions
- Engineers per-session features: `total_events`, `unique_event_types`, `viewed_product`, `added_to_cart`, `reached_checkout`, `applied_promo`, `searched`, `traffic_source`
- Labels each session: `converted = 1` if it has a successful transaction, else `0`
- Trains a **RandomForest** classifier (50 trees, max depth 6) inside a Spark `Pipeline`
- Persists the trained `PipelineModel` to `data/models/session_conversion/`
- Saves evaluation metrics (AUC, F1, Accuracy) to `outputs/streaming/stream_model_metrics.json`

**Phase 2 — Predict (streaming)**:
- `stream_simulator()` emits clickstream JSON files in real time
- `spark.readStream` picks them up in micro-batches
- `foreachBatch` callback on each micro-batch:
  1. Aggregates raw events into session feature vectors
  2. Loads the saved `PipelineModel`
  3. Calls `model.transform()` to get `conversion_probability` and a `risk_label`
  4. Labels sessions: `HIGH_CONVERSION (≥70%)`, `MEDIUM_CONVERSION (40–70%)`, `LOW_CONVERSION (<40%)`
  5. Appends scored rows to `outputs/streaming/session_predictions.csv`

| Output Column | Meaning |
|---------------|---------|
| `session_id` | The live session being scored |
| `conversion_probability` | Model's P(checkout succeeds) for that session |
| `predicted_conversion` | 1 = model predicts purchase, 0 = no purchase |
| `risk_label` | HIGH / MEDIUM / LOW conversion bucket |
| `batch_id` | Which streaming micro-batch scored this session |
| `scored_at` | Timestamp of scoring |

### 5. ML Pipeline (`src/ml_pipeline.py`)

**Spark Component**: MLlib

#### Task 1: Payment Status Classification

- **Target**: `payment_status` (Success / Failed)
- **Features**: `total_amount`, `shipment_fee`, `promo_amount`, `has_promo`, `payment_method`, `device_type`, `gender`
- **Preprocessing**: StringIndexer → VectorAssembler → StandardScaler
- **Models**: Logistic Regression, Random Forest (50 trees), Gradient-Boosted Trees
- **Evaluation**: AUC, F1, Precision, Recall, Accuracy, Confusion Matrix
- **Split**: 80/20 train/test

#### Task 2: Customer Segmentation (KMeans Clustering)

- **Features**: `total_spend`, `num_transactions`, `avg_order_value`, `num_sessions`, `promo_usage_rate`, `payment_methods_used`
- **Algorithm**: KMeans with automatic k selection (tested k = 3, 4, 5, 6, 8)
- **Evaluation**: Silhouette Score
- **Output**: Cluster assignments, cluster statistics, device/gender distribution per cluster

## Technology Stack

| Technology | Version | Purpose |
|-----------|---------|---------|
| Apache Spark / PySpark | ≥ 3.5.0 | Core processing engine |
| Java JDK | 11 | Spark runtime (JDK 17 incompatible) |
| Hadoop | Prebuilt winutils | Windows HDFS compatibility |
| Python | ≥ 3.9 | Application language |
| Matplotlib + Seaborn | ≥ 3.7 / ≥ 0.12 | Visualization |
| Pandas + NumPy | ≥ 2.0 / ≥ 1.24 | Data manipulation helpers |
| pytest | ≥ 7.0 | Unit testing |
