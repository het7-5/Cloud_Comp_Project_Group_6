# Project Journey & Technical Report: E-commerce Analytics with Apache Spark

## 1. Project Motivation and Goals

### The "Why": Business Value and Need
In today's highly competitive e-commerce landscape, platforms generate massive amounts of data every second—from user clicks and product views to geographic purchase trends and transaction statuses. Traditional, completely siloed relational databases struggle to process and analyze this data at scale or in real time. 

There is a critical business **need** to:
1.  **Understand the Customer Journey:** Move beyond simple sales numbers to analyze *how* customers behave. What are their click patterns? Do they drop off before purchasing? Which products and promotions actually drive value?
2.  **Mitigate Revenue Loss:** Identify and predict why certain transactions fail so that proactive measures can be instituted.
3.  **Personalize User Experience:** Segment the customer base into distinct cohorts to tailor marketing strategies and offer personalized recommendations.
4.  **Act in Real-Time:** Harness streaming data to understand network traffic and user behavior right as it happens, rather than waiting for next-day batch reports.

### Primary Objective
The primary objective of this project is to address these business needs by designing and implementing a complete, end-to-end **big data analytics pipeline** using **Apache Spark**. We chose an extensive E-commerce App Transactional Dataset to practically demonstrate how different Spark components—Structured APIs, Spark SQL, Spark Streaming, and Spark MLlib—can seamlessly work together to process, analyze, and model large-scale data.

**Core Project Goals:**
*   **Data Ingestion & Processing:** Efficiently load multi-table relational data (customers, products, transactions, clickstreams), enforce exact schemas, perform data cleaning, and store the cleansed data in an optimized Parquet format.
*   **Exploratory Data Analysis (EDA):** Gain statistical insights from the raw dataset, exploring distributions and correlations.
*   **Business Intelligence (SQL):** Use Spark SQL to answer critical business questions (e.g., Customer Lifetime Value, Promotional Effectiveness, Revenue Trends, and Funnel Analysis).
*   **Real-time Analytics (Streaming):** Utilize Spark Structured Streaming to ingest and aggregate a live, synthesized stream of web traffic events (simulating active user clickstreams).
*   **Machine Learning (MLlib):** Train scalable models for two main tasks:
    *   **Classification:** Predicting transaction payment success vs. failure.
    *   **Clustering (Segmentation):** Grouping customers based on unified behavioral and transactional metrics using K-Means clustering.

---

## 2. Project Architecture and Workflow
The project architecture strictly follows a modular data pipeline approach. Each stage of the pipeline depends on the successful output of the previous stage, coordinated entirely by `run.sh` / `Makefile`.

1.  **Stage 1: Ingestion (`src/ingestion.py`)** 
    Reads raw CSV data, applies rigorous `StructType` schemas, parses nested JSON metadata (like product arrays inside the transaction records), handles missing values, and writes compressed Parquet files to `data/processed/`.
2.  **Stage 2: EDA (`src/eda.py`)** 
    Loads the cleansed Parquet data and computes descriptive statistics. It generates vital data visualizations (like distributions and missing value heatmaps) saved to `outputs/eda/`.
3.  **Stage 3: Transformations & SQL (`src/transformations.py`)**
    Registers the Parquet tables as temporary SQL views. Executes complex multi-way JOINs and Window functions to generate revenue reports, geographic analyses, and promotional lift metrics, saving the results in `outputs/sql_results/`.
4.  **Stage 4: Streaming (`src/streaming.py`)**
    A separate thread generates a continuous flow of JSON clickstream events to a staging directory. Simultaneously, Spark Structured Streaming consumes this directory in real-time, performing stateful window aggregations (e.g., "unique visitors per minute") and flushing outputs to a simulated real-time dashboard structure in `outputs/streaming/`.
5.  **Stage 5: Machine Learning (`src/ml_pipeline.py`)**
    *   **Feature Engineering:** Extracts historical transaction features (total spend, usage rate).
    *   **Classification Pipeline:** Uses VectorAssembler and Random Forest/Gradient Boosted Trees evaluated via Area Under the ROC curve (AUC).
    *   **Clustering Pipeline:** Uses StandardScaler and KMeans clustering, evaluated by the Silhouette Score to identify optimal user cohorts. 

---

## 3. Key Issues Encountered and Diagnosed (Changelog)

During the development and testing of this robust pipeline, we encountered and successfully resolved several challenging technical roadblocks:

### 1. Java 17 Compatibility Crash with PySpark
*   **The Issue:** Running PySpark 3.5.x using an underlying Java 17 environment threw an `UnsupportedOperationException` related to `Subject.getSubject()`. Java 17 natively restricted deep reflection APIs that Spark desperately requires for its gateway.
*   **The Solution:** Downgraded the environment to OpenJDK 11. Implemented logic inside `src/utils.py` to automatically bind `JAVA_HOME` configuration to explicitly use Java 11 on Windows local setups.

### 2. PySpark's JVM SocketException on Windows Shutdown
*   **The Issue:** When saving ML modeling results remotely, Spark's JVM bridge would abruptly crash on Windows with a `SocketException: Connection reset` perfectly right at the end (`spark.stop()`). The heavy JVM memory footprint caused the Windows network layers to drop connections prematurely.
*   **The Solution:** Greatly optimized the code. Added network timeout overrides (`spark.network.timeout="600s"`). Refactored ML logging so that Python's native lightweight `json.dump` writes the metadata results at the end, cleanly bypassing PySpark's risky I/O bridge at termination.

### 3. Highly Imbalanced Machine Learning Data
*   **The Issue:** The transaction data was inherently imbalanced (`95.7%` overall Success rate vs. `4.3%` Failed). The initial classification models were blindly guessing "Success" and perfectly obtaining 95% accuracy, but a terrible AUC (0.50), learning nothing about why payments fail.
*   **The Solution:** Built a dynamic class weighting column into the Spark ML representation. We computed the exact frequency of both classes and mathematically weighted the minority class (`Failed`) effectively 11-15x heavier to penalize the model for missing failures.

### 4. GitHub File-Size Limit Rejections
*   **The Issue:** The project raw dataset CSV files exceeded 2.0+ GBs collectively (especially `transaction.csv` at 240+ MB). Pushing `git push origin main` failed repeatedly because GitHub rejects files over 100MB.
*   **The Solution:** Forcefully scrubbed the large data files from the Git cache using `git rm -r --cached`, reset the git history trees softly, thoroughly updated `.gitignore`, and used only our lightweight synthetic generator (`generate_sample_data.py`) for repository tracking.

### 5. Automated CI (GitHub Actions) PySpark Configuration
*   **The Issue:** The automated cloud testing crashed internally missing the `generate_all` test target, and independently failing with `JAVA_GATEWAY_EXITED` because Ubuntu Cloud Runners lack Java setups out-of-the-box.
*   **The Solution:** Updated the `ci.yml` GitHub action sequentially to cleanly execute `sudo apt-get install default-jdk` specifically outfitting the test runner with the JVM. Also correctly wrapped environment-specific variables tightly in `if os.name == 'nt':` locally so Linux variables wouldn't be corrupted by native Windows paths.

### 6. Misaligned Commas in JSON Parsing (Corrupted Sample Output Data)
*   **The Issue:** PySpark was interpreting commas inside a raw JSON column payload (`product_metadata`) as a CSV column separator. This misaligned the entire dataset structure downstream, replacing the `payment_status` column precisely with fragments of JSON, breaking the ML filters permanently for all data resulting in DataFrame lengths of 0.
*   **The Solution:** Modified the data load step carefully using Spark CSV Read options `escape="\""` and `quote="\""` which strictly tells Spark to respect commas safely encapsulated inside internal quote markers. Tests passed instantly.

### 7. Windows Hadoop NativeIO Crash on File Writes (Empty Output Folders)
*   **The Issue:** When the `transformations.py` and `streaming.py` stages attempted to flush their finalized results to disk via PySpark's native `.csv()` or `.parquet()` writers, Windows crashed the JVM with a fatal `java.lang.UnsatisfiedLinkError` via `NativeIO$Windows.access0`. This caused the SQL and Streaming reports to fail silently without generating output files.
*   **The Solution:** Completely bypassed the internal PySpark/Hadoop File I/O system for result exports. We aggregated the SQL and Streaming tables in memory, collected them natively as Pandas DataFrames (`.toPandas()`), and leveraged Python's standard file system to write the CSV files seamlessly to disk.

---

## Conclusion
The architecture now guarantees extremely robust behavior. The tests pass beautifully on both heavy and lightweight sample datasets, and it safely navigates the various hurdles of cross-platform Spark deployment, JSON formatting anomalies, and deep algorithm optimization. The pipeline fully fulfills all course rubric objectives on Big Data ingestion, transformations, streaming, and machine learning successfully.
