# Week 7 Check-in: Data Ingestion & EDA Results

**Project:** Customer360: E-Commerce Transaction Analytics Pipeline
**Team:** Group 6
**Goal:** Demonstrate initial EDA results, summaries, sample queries, visualizations, and discuss progress via our Weekly Check-in Issue.

---

## 1. Initial Exploratory Data Analysis (EDA) Results

Our initial EDA focused on understanding the structure, quality, and distributions within our ~2GB Kaggle E-Commerce dataset. We utilized PySpark's DataFrame API (`src/eda.py`) to systematically analyze all four main tables.

### Data Summaries & Quality
*   **Customer Table (~100k rows):** We verified demographics, finding a diverse user base. We identified missing values in some non-critical fields and handled them during ingestion.
*   **Product Table (~44k rows):** We analyzed the catalog, ensuring pricing and categorization data were properly cast to numeric/string types.
*   **Transaction Table (~850k rows):** We focused heavily on the `payment_status` and `total_amount` fields, as these are the core of our business value analysis. We correctly parsed the nested JSON metadata in this table using Spark's CSV read options (`escape` and `quote`).
*   **Clickstream Table (~12.8M rows):** We validated the massive volume of event data, ensuring timestamps and event names (clicks, scrolls, searches) were properly formatted for downstream Structured Streaming.

### Sample Queries (PySpark DataFrame API)
To generate our insights, we used PySpark DataFrames instead of raw SQL for the initial EDA. Example queries included:

```python
# Sample: Calculating Revenue Distribution by Payment Status
revenue_dist = transactions_df.groupBy("payment_status") \
    .agg(
        count("*").alias("transaction_count"),
        sum("total_amount").alias("total_revenue")
    )

# Sample: Analyzing Customer Demographics (Gender & Device)
demo_summary = customers_df.groupBy("gender", "device_type") \
    .count() \
    .orderBy(desc("count"))
```

### Visualizations
Our automated EDA pipeline generated several key visualizations (available in `outputs/eda/`):
1.  **`01_customer_demographics.png`**: Illustrates the breakdown of our user base by age brackets and device types.
2.  **`03_transaction_analysis.png`**: Highlights the critical disparity in payment success vs. failure rates, which directly motivates our downstream Machine Learning classification task.
3.  **`05_cross_table_insights.png`**: Shows combined insights, linking customer demographics to their transactional behavior.

---

## 2. Weekly Check-In Issue Outline

*(We will present this from our GitHub Issues tab during the meeting)*

**Title:** Weekly Check-In: Data Ingestion & EDA Results

### 📈 Progress since the last meeting
*   **Data Ingestion Pipeline Built:** We fully implemented `src/ingestion.py` using PySpark Structured APIs. It successfully handles exactly 4 massive Kaggle datasets, enforcing schema types (`StructType`), parsing nested JSON fields dynamically, and compressing the output to Parquet format.
*   **EDA Visualizations Generated:** We completed the Exploratory Data Analysis phase, utilizing Spark aggregations to automatically generate specific visualizations in our `outputs/eda/` folder detailing customer demographics and transactional behaviors.
*   **CI/CD Foundation:** We safely excluded the 2GB raw dataset from Git tracking (`.gitignore`) and wrote an automated synthetic data generator (`generate_sample_data.py`) to allow our GitHub Actions to run the full pipeline in the cloud.

### 🚧 Current challenges/blockers
*   **Blocker:** Apache Spark crashed consistently on Windows environments due to a `java.lang.UnsatisfiedLinkError` when trying to natively write Parquet/CSV files (a known Hadoop NativeIO bug). Additionally, PySpark crashed with `JAVA_GATEWAY_EXITED` on Linux GitHub Action runners.
*   **Resolution:** We built dynamic OS-conditional logic into our wrapper (`src/utils.py`). To solve the Windows write crashes, we successfully bypassed the Hadoop JVM entirely by gathering aggregated data natively in memory and leveraging standard Pandas to safely write the output CSV files.

### 🎯 Plan for next week
*   **Develop Complex Queries:** Fully validate our Spark SQL queries in `src/transformations.py` (e.g., Customer Lifetime Value, Revenue Trending) and ensure they output cleanly to the analytics folders.
*   **Begin Structured Streaming:** Begin simulating real-time web traffic events to ingest the clickstream dataset using Spark Streaming window aggregations.
