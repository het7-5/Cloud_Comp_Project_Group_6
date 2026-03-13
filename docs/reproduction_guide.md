# Reproduction Guide

## Prerequisites

### Software Requirements

| Software | Version | Notes |
|----------|---------|-------|
| Python | ≥ 3.9 | Tested with 3.11 |
| Java JDK | **11** | ⚠️ Java 17+ is NOT compatible with PySpark 3.5.x |
| Apache Spark | ≥ 3.5.0 | Included via PySpark pip package |
| Git | Any | For cloning the repository |

### Windows-Specific Requirements

1. **Java 11 JDK** installed (e.g., Eclipse Adoptium/Temurin JDK 11)
   - Download from: https://adoptium.net/temurin/releases/?version=11
   - Set `JAVA_HOME` to your JDK 11 installation path

2. **Hadoop winutils.exe** for Windows
   - Download `winutils.exe` and `hadoop.dll` for Hadoop 3.x
   - Place them in `C:\hadoop\bin\` (or update `HADOOP_HOME` in `src/utils.py`)

> **Note**: The `src/utils.py` file sets `JAVA_HOME` and `HADOOP_HOME` automatically. Update the paths in that file if your installations differ.

## Step-by-Step Setup

### 1. Clone the Repository

```bash
git clone https://github.com/YOUR_ORG/Cloud_Comp_Project_Group_6.git
cd Cloud_Comp_Project_Group_6
```

### 2. Create Python Virtual Environment

```bash
python -m venv venv
venv\Scripts\activate       # Windows
# source venv/bin/activate  # macOS/Linux
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Download the Dataset

Download the **E-commerce App Transactional Dataset** from Kaggle:
- URL: https://www.kaggle.com/datasets/bytadit/transactional-ecommerce
- Extract all 4 CSV files into `data/raw/`:

```
data/raw/
├── customer.csv           (~25 MB)
├── product.csv            (~4 MB)
├── transactions.csv       (~256 MB)
└── click_stream.csv       (~1.7 GB)
```

> **Note**: If you don't have the Kaggle dataset, the pipeline will automatically use generated sample data from `data/sample/`.

### 5. Update Java Path (if needed)

Edit the top of `src/utils.py` to match your Java 11 installation:

```python
os.environ["JAVA_HOME"] = r"C:\jdk11\jdk-11.0.25+9"  # ← Update this path
os.environ["HADOOP_HOME"] = r"C:\hadoop"               # ← Update this path
```

## Running the Pipeline

### Full Pipeline (All Steps)

```bash
bash run.sh
```

Or run individual steps:

### Step 1: Data Ingestion

```bash
python src/ingestion.py
```

Reads CSVs → validates schemas → parses JSON → saves Parquet to `data/processed/`.

### Step 2: EDA

```bash
python src/eda.py
```

Generates 5 visualization dashboards → saves PNGs to `outputs/eda/`.

### Step 3: Spark SQL Queries

```bash
python src/transformations.py
```

Executes 6 non-trivial SQL queries with window functions, CTEs, and joins.

### Step 4: Streaming Demo

```bash
python src/streaming.py
```

Simulates 500 click stream events → processes with Structured Streaming → outputs aggregations.

### Step 5: ML Pipeline

```bash
python src/ml_pipeline.py
```

Trains 3 classifiers (payment prediction) + KMeans segmentation → saves results to `outputs/ml/`.

### Running Tests

```bash
python -m pytest tests/ -v
```

Tests use generated sample data (auto-created if not present).

## Expected Outputs

After running the full pipeline:

```
outputs/
├── eda/
│   ├── 01_customer_demographics.png
│   ├── 02_product_analysis.png
│   ├── 03_transaction_analysis.png
│   ├── 04_clickstream_analysis.png
│   ├── 05_cross_table_insights.png
│   └── eda_summary.txt
├── sql_results/           (query output files)
├── streaming/             (streaming output Parquet)
└── ml/
    ├── classification_results/   (model comparison JSON)
    ├── k_evaluation/             (silhouette scores JSON)
    ├── cluster_assignments/      (customer-cluster Parquet)
    └── cluster_statistics/       (cluster summary JSON)
```

## Troubleshooting

### Java 17 Error
```
UnsupportedOperationException: Subject.getSubject()
```
**Fix**: Install Java 11 and update `JAVA_HOME` in `src/utils.py`.

### Missing winutils.exe
```
Could not locate executable null\bin\winutils.exe
```
**Fix**: Download Hadoop winutils.exe for Windows and place in `C:\hadoop\bin\`.

### Memory Errors
```
java.lang.OutOfMemoryError: Java heap space
```
**Fix**: Increase `spark.driver.memory` in `src/utils.py` (default is 4g).

### Slow Click Stream Processing
The click stream table is ~1.7 GB with 12.8M rows. Processing time depends on available RAM and CPU cores.
**Tip**: Use sample data first (`data/sample/`) for faster iteration, then switch to full data.
