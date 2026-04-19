# Final Progress Update: Real-Time ML & Advanced SQL

**Project:** Customer360: E-Commerce Transaction Analytics Pipeline
**Team:** Group 6
**Goal:** Demonstrate the completion of the advanced analytics pipeline, specifically highlighting our implementation of the professor's recent feedback regarding real-time machine learning predictions, complex SQL queries, and dataset relational mapping.

---

## 1. Advanced Analytics & Machine Learning Results

Since our EDA check-in, we transitioned from basic data cleaning to advanced, predictive analytics. We focused heavily on bridging the gap between historical batch processing and live streaming data.

### Real-Time Machine Learning Predictions
To answer the core question—*"What predictions can we make on the live stream?"*—we architected a two-phase Streaming ML module:
*   **Phase 1 (Batch Training):** We engineer session-level features (e.g., total events, cart additions, search usage) from our historical Clickstream and Transactions tables. We then train a **RandomForest Classifier** using Spark MLlib to predict session conversion (i.e., will this active session result in a successful checkout?).
*   **Phase 2 (Streaming Inference):** We consume live, simulated clickstream JSON events via Spark Structured Streaming. Using a `foreachBatch` micro-batch methodology, we evaluate every active user in real-time, assigning them a `HIGH`, `MEDIUM`, or `LOW` conversion risk label instantly.

### Complex SQL Insights
We moved beyond basic aggregations by implementing 6 complex business queries using advanced Spark SQL features:
*   **Customer Lifetime Value (CLV):** Utilized `DENSE_RANK()` and `SUM() OVER ()` window functions to rank our top 15 most valuable customers.
*   **Funnel Drop-off & Promo Effectiveness:** Used Common Table Expressions (`WITH`) and conditional `CASE WHEN` pivoting to calculate checkout success rates, proving that promo codes increase average order value despite not shifting the technical transaction success rate.

### Sample Code (Structured Streaming + MLlib)
To score live sessions without crashing the streaming engine, we utilized native Spark SQL functions to extract probabilities directly from MLlib's `DenseVector` arrays without relying on slow Python UDFs:

```python
# Sample: Extracting real-time P(convert=1) from MLlib's DenseVector
from pyspark.ml.functions import vector_to_array

result = (
    scored_micro_batch
    .withColumn("prob_array", vector_to_array(col("probability")))
    .withColumn("conversion_probability", col("prob_array")[1].cast("double"))
    .withColumn("risk_label",
        when(col("conversion_probability") >= 0.70, "HIGH_CONVERSION")
        .when(col("conversion_probability") >= 0.40, "MEDIUM_CONVERSION")
        .otherwise("LOW_CONVERSION")
    )
)
```

---

## 2. Check-In Presentation Outline

*(We will present this overview during our final class update)*

**Title:** Final Progress Update: Streaming ML & complex SQL

### 📈 Progress since the last meeting
*   **Implemented Professor's Feedback:** 
    1. **Real-time Predictions:** We successfully built the `streaming_predictions.py` module to classify live stream traffic dynamically.
    2. **Complex Queries:** We finalized `transformations.py`, proving we can extract deep insights (LTV, MoM Growth) using advanced window functions.
    3. **Dimension/Fact Relationships:** We clearly defined our data warehouse flow. The `Clickstream` (Fact) safely joins to `Transactions` (Fact) via `session_id`, which ties directly to the `Customers` and `Products` Dimension tables.
*   **Intelligent Data Auto-Detection:** We engineered our `utils.py` wrapper to automatically detect if the massive 2GB Kaggle dataset is present on the local machine. If it isn't (e.g., when the code is downloaded by a grader), the pipeline gracefully falls back to generating a small synthetic sample, proving portability.

### 🚧 Current challenges/blockers (and how we solved them)
*   **Blocker:** When attempting to save our trained MLlib `PipelineModel` to disk for the streaming process, Spark crashed repeatedly on Windows machines due to a known `Hadoop NativeIO` UnsatisfiedLinkError.
*   **Resolution:** We bypassed the Hadoop disk-write bug entirely. We re-architected the Python process to retain the heavy Machine Learning model purely **in-memory** after the batch training phase, passing it directly into the Structured Streaming `foreachBatch` closure.
*   **Blocker:** PySpark's Python UI workers crashed (EOFException) when executing Python `udf()` functions inside the live streaming micro-batches.
*   **Resolution:** We stripped out all Python UDFs and replaced them with PySpark 3.x native SQL array functions (`vector_to_array`), which run inside the much faster, crash-proof JVM space.

### 🎯 Plan for next week
*   **Final Code Clean-up:** Ensure all Python files adhere to PEP8 formatting and verify that our `.gitignore` properly handles the new dynamically generated Machine Learning model folders.
*   **Final Demo/Presentation:** Prepare for the final project submission and recording.

---

## 3. Technical Parameters & Engineering Configuration (Appendix)
*(To be used for technical Q&A during the presentation)*

### Machine Learning Model Parameters (Random Forest)
*   **`numTrees=50`**: Balances high accuracy without causing Out-of-Memory (OOM) errors during the live streaming phase.
*   **`maxDepth=6`**: Explicitly limited tree depth to prevent **overfitting**. E-commerce traffic is highly noisy; deeper trees memorize random clicks instead of generalized purchasing patterns.
*   **Data Split (`0.8, 0.2`)**: Standard 80/20 train/test split, anchored with a fixed seed (`seed=42`) to guarantee experimental reproducibility.

### Feature Engineering Logic
We engineered 8 session-level features using PySpark DataFrame aggregation APIs:
*   **Engagement Metrics (`total_events`, `unique_event_types`)**: Quantifies overall session depth.
*   **Intent Flags (`viewed_product`, `added_to_cart`, `searched`, `applied_promo`)**: Used conditional pivoting `max(when(col(...) == "event", 1).otherwise(0))` to convert thousands of messy row-based clicks into clean binary indicators of user intent.
*   **Categorical Encoding (`traffic_source_idx`)**: Utilized Spark's `StringIndexer` to convert devices (Mobile vs. Web) into numeric MLlib indices.

### Streaming Pipeline Configuration
*   **Backpressure (`maxFilesPerTrigger=5`)**: Limits the streaming engine to consume a maximum of 5 JSON files per micro-batch trigger. This acts as built-in safety backpressure, preventing JVM heap explosion if millions of events arrive simultaneously.
*   **Write Mode (`outputMode="append"`)**: Because we predict for new sessions continuously, this forces Spark to strictly append new prediction rows to output instead of recalculating the entire dataframe history.

### Business Logic Risk Thresholds
Translated the raw Decimal Machine Learning probability into actionable business buckets:
*   **`HIGH_CONVERSION` ($\ge$ 0.70)**: High confidence to buy. Actionable insight: Save promotional budget; they are converting naturally.
*   **`MEDIUM_CONVERSION` (0.40 to 0.69)**: Users distinctly "on the fence." Actionable insight: Prime target for real-time 10% discount push notifications to secure the sale.
*   **`LOW_CONVERSION` (< 0.40)**: Passive scrollers or bounced traffic.
