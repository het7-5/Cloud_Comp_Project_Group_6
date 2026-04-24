"""
One-shot script — pull small aggregates from the S3 Parquet tables into local
CSVs that the Streamlit dashboard can read instantly (no Spark at demo time).

Run:  python dashboard/prepare_data.py
"""

import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Java 11 + Spark driver memory must be set before the JVM starts
os.environ["JAVA_HOME"] = "/usr/local/sdkman/candidates/java/11.0.30-ms"
os.environ["PATH"] = (
    os.environ["JAVA_HOME"] + "/bin:"
    + ":".join(p for p in os.environ["PATH"].split(":")
               if "sdkman" not in p and "jvm" not in p)
)
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--driver-memory 3g "
    "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
    "pyspark-shell"
)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

OUT_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(OUT_DIR, exist_ok=True)

spark = (SparkSession.builder
         .appName("Group6-DashboardData")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", ""))
         .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
         .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                 "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

BUCKET = os.environ.get("S3_BUCKET_PATH", "")
PROC = f"{BUCKET}/processed" if BUCKET else "../data/processed"

customers    = spark.read.parquet(f"{PROC}/customers_clean")
products     = spark.read.parquet(f"{PROC}/products_clean")
transactions = spark.read.parquet(f"{PROC}/transactions_exploded")
clicks       = spark.read.parquet(f"{PROC}/clickstream_clean")
sessions     = spark.read.parquet(f"{PROC}/session_funnel")


def save(df, name, limit=None):
    d = df.limit(limit) if limit else df
    d.toPandas().to_csv(os.path.join(OUT_DIR, f"{name}.csv"), index=False)
    print(f"  saved {name}.csv")


print("\nExtracting snapshots...\n")

# 1. Scale / KPI
kpi = {
    "customers":    customers.count(),
    "products":     products.count(),
    "transactions": transactions.count(),
    "clickstream":  clicks.count(),
    "sessions":     sessions.count(),
}
print("  kpi:", kpi)
import json
json.dump(kpi, open(os.path.join(OUT_DIR, "kpi.json"), "w"), indent=2)

# 2. Device distribution
save(customers.groupBy("device_type").count().orderBy(F.desc("count")),
     "device_distribution")

# 3. Gender split
save(customers.groupBy("gender").count().orderBy(F.desc("count")), "gender_split")

# 4. Top home countries
save(customers.groupBy("home_country").count().orderBy(F.desc("count")).limit(10),
     "top_countries")

# 5. Payment outcomes
save(transactions.groupBy("payment_status").count().orderBy(F.desc("count")),
     "payment_outcomes")

# 6. Revenue by category
rev = (transactions
       .join(products.select("product_id", "masterCategory"), "product_id", "left")
       .withColumn("line_revenue", F.col("quantity") * F.col("item_price"))
       .groupBy("masterCategory")
       .agg(F.sum("line_revenue").alias("revenue"),
            F.count("*").alias("line_items"))
       .orderBy(F.desc("revenue")))
save(rev, "revenue_by_category")

# 7. Hourly click events
save(clicks.withColumn("hr", F.hour("event_time"))
           .groupBy("hr").count().orderBy("hr"),
     "hourly_clicks")

# 8. Monthly transaction trend
save(transactions.withColumn("month", F.date_format("created_at", "yyyy-MM"))
                 .groupBy("month")
                 .agg(F.count("*").alias("orders"),
                      F.sum(F.col("quantity") * F.col("item_price")).alias("revenue"))
                 .orderBy("month"),
     "monthly_trend")

# 9. Session funnel summary
funnel_row = sessions.agg(
    F.count("*").alias("sessions"),
    F.sum("visited_homepage").alias("homepage"),
    F.sum("did_search").alias("searched"),
    F.sum("added_to_cart").alias("add_to_cart"),
    F.sum("used_promo").alias("used_promo"),
    F.sum("converted").alias("converted"),
).toPandas().T.reset_index()
funnel_row.columns = ["stage", "count"]
funnel_row.to_csv(os.path.join(OUT_DIR, "session_funnel_summary.csv"), index=False)
print("  saved session_funnel_summary.csv")

# 10. Traffic source breakdown
save(sessions.groupBy("traffic_source")
             .agg(F.count("*").alias("sessions"),
                  F.sum("converted").alias("conversions"))
             .withColumn("conversion_rate",
                         F.round(F.col("conversions") / F.col("sessions") * 100, 2))
             .orderBy(F.desc("sessions")),
     "traffic_sources")

# 11. Top products by revenue (for recommender demo)
top_products = (transactions
                .filter(F.col("payment_status") == "Success")
                .join(products.select("product_id", "productDisplayName", "masterCategory"),
                      "product_id", "left")
                .withColumn("line_revenue", F.col("quantity") * F.col("item_price"))
                .groupBy("product_id", "productDisplayName", "masterCategory")
                .agg(F.sum("line_revenue").alias("revenue"),
                     F.sum("quantity").alias("units"))
                .orderBy(F.desc("revenue"))
                .limit(20))
save(top_products, "top_products")

# 12. Top customers by spend
top_cust = (transactions
            .filter(F.col("payment_status") == "Success")
            .groupBy("customer_id")
            .agg(F.sum(F.col("quantity") * F.col("item_price")).alias("lifetime_spend"),
                 F.countDistinct("booking_id").alias("bookings"))
            .orderBy(F.desc("lifetime_spend"))
            .limit(20))
save(top_cust, "top_customers")

# 13. RFM cluster profile (needs KMeans on a sample — quick version)
success = transactions.filter(F.col("payment_status") == "Success")
max_date = success.agg(F.max("created_at")).first()[0]
rfm = (success.groupBy("customer_id").agg(
        F.datediff(F.lit(max_date).cast("timestamp"),
                   F.max("created_at")).alias("recency"),
        F.countDistinct("booking_id").alias("frequency"),
        F.sum(F.col("quantity") * F.col("item_price")).alias("monetary"))
       .na.drop())

from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline

assembler = VectorAssembler(inputCols=["recency", "frequency", "monetary"],
                            outputCol="raw_f")
scaler = StandardScaler(inputCol="raw_f", outputCol="features",
                        withStd=True, withMean=True)
kmeans = KMeans(featuresCol="features", predictionCol="cluster",
                k=5, seed=42, maxIter=20)
pipe = Pipeline(stages=[assembler, scaler, kmeans])
pipe_model = pipe.fit(rfm)
preds = pipe_model.transform(rfm)

profile = (preds.groupBy("cluster").agg(
            F.count("*").alias("n_customers"),
            F.round(F.avg("recency"), 1).alias("avg_recency_days"),
            F.round(F.avg("frequency"), 2).alias("avg_frequency"),
            F.round(F.avg("monetary"), 0).alias("avg_monetary"))
           .orderBy("cluster"))
save(profile, "rfm_clusters")

# scatter for RFM viz: sample 5000 points with cluster label
save(preds.select("customer_id", "recency", "frequency", "monetary", "cluster")
          .sample(fraction=5000 / max(rfm.count(), 1), seed=42)
          .limit(5000),
     "rfm_sample_points")

# 14. Pre-computed ML metrics (from our notebook runs)
ml_metrics = {
    "random_forest": {
        "accuracy": 0.9423, "f1": 0.9288, "auc_roc": 0.7659,
        "feature_importances": {
            "added_to_cart": 0.6355, "session_duration_mins": 0.2204,
            "total_events": 0.1130, "used_promo": 0.0296,
            "num_keywords": 0.0008, "did_search": 0.0005,
            "traffic_vec": 0.0000, "visited_homepage": 0.0000,
        },
    },
    "kmeans": {"silhouette": 0.7129, "k": 5},
    "als": {
        "rmse": 4.36, "interactions": 1_889_569,
        "users": 50_705, "items": 44_446, "sparsity_pct": 99.92,
        "rank": 8, "maxIter": 5, "regParam": 0.1,
    },
}
json.dump(ml_metrics, open(os.path.join(OUT_DIR, "ml_metrics.json"), "w"), indent=2)
print("  saved ml_metrics.json")

print("\nDone. Snapshots in", OUT_DIR)
spark.stop()
