"""
ML Pipeline Module (aligned with ingestion.ipynb Parquet output)
================================================================
Three MLlib tasks on the *_clean Parquet tables written by the ingestion
notebook (session_funnel, transactions_exploded, cart_events, products_clean):

  1. Random Forest — Session conversion prediction (binary classification)
  2. KMeans       — Customer RFM segmentation (k=5)
  3. ALS          — Implicit-feedback collaborative filtering recommender

Reads from ``$S3_BUCKET_PATH/processed/*`` if set, else ``data/processed/*``.
"""

import os
import sys
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.utils import get_spark_session, ML_OUTPUT_DIR, ensure_dirs, PROJECT_ROOT

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.functions import (
    col, count, sum as _sum, avg, max as _max, countDistinct, datediff,
    when, lit, explode, round as _round, desc,
)
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    VectorAssembler, StringIndexer, OneHotEncoder, StandardScaler,
)
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.clustering import KMeans
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator, MulticlassClassificationEvaluator,
    ClusteringEvaluator, RegressionEvaluator,
)


# ─────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────

def _processed_dir():
    bucket = os.environ.get("S3_BUCKET_PATH", "")
    if bucket:
        return f"{bucket}/processed"
    return os.path.join(PROJECT_ROOT, "data", "processed")


def _load(spark, name):
    return spark.read.parquet(f"{_processed_dir()}/{name}")


def _save_json(data, filepath):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    print(f"   Saved: {filepath}")


# ─────────────────────────────────────────────────────────────
# Feature builders (reusable; also exercised by tests)
# ─────────────────────────────────────────────────────────────

def build_classification_features(spark) -> DataFrame:
    """Per-session features for session-conversion classification.

    Target column ``label`` = 1 if the session ended with a successful BOOKING.
    Features come from ``session_funnel`` and a derived ``num_keywords``.
    """
    sessions = _load(spark, "session_funnel")
    return (sessions
            .withColumn("num_keywords", F.size("keywords_searched"))
            .withColumn("label", col("converted").cast("double"))
            .fillna(0, subset=["session_duration_mins", "num_keywords"])
            .na.drop(subset=["traffic_source"]))


def build_clustering_features(spark):
    """Per-customer RFM + AOV features (successful payments only).

    Returns ``(DataFrame, feature_col_list)``.
    """
    txns = _load(spark, "transactions_exploded") \
        .filter(col("payment_status") == "Success")
    max_date = txns.agg(_max("created_at")).first()[0]

    df = (txns.groupBy("customer_id").agg(
            datediff(lit(max_date).cast("timestamp"), _max("created_at"))
                .alias("recency_days"),
            countDistinct("booking_id").alias("num_transactions"),
            _sum(col("quantity") * col("item_price")).alias("total_spend"),
        )
        .withColumn("avg_order_value",
                    col("total_spend") / col("num_transactions"))
        .na.drop())

    feature_cols = [
        "recency_days", "num_transactions", "total_spend", "avg_order_value",
    ]
    return df, feature_cols


# ─────────────────────────────────────────────────────────────
# TASK 1 — Random Forest: Session Conversion Prediction
# ─────────────────────────────────────────────────────────────

def run_classification(spark):
    print("\n" + "=" * 60)
    print("  TASK 1: RANDOM FOREST — SESSION CONVERSION PREDICTION")
    print("=" * 60)

    data = build_classification_features(spark)
    print(f"   Sessions: {data.count():,}")

    traffic_idx = StringIndexer(
        inputCol="traffic_source", outputCol="traffic_idx",
        handleInvalid="keep",
    )
    traffic_ohe = OneHotEncoder(inputCol="traffic_idx", outputCol="traffic_vec")
    numeric_cols = [
        "total_events", "session_duration_mins",
        "visited_homepage", "did_search", "added_to_cart",
        "used_promo", "num_keywords",
    ]
    assembler = VectorAssembler(
        inputCols=numeric_cols + ["traffic_vec"],
        outputCol="features", handleInvalid="skip",
    )
    rf = RandomForestClassifier(
        featuresCol="features", labelCol="label",
        numTrees=50, maxDepth=6, seed=42,
    )

    train, test = data.randomSplit([0.8, 0.2], seed=42)
    pipeline = Pipeline(stages=[traffic_idx, traffic_ohe, assembler, rf])
    model = pipeline.fit(train)
    preds = model.transform(test)

    acc = MulticlassClassificationEvaluator(
        labelCol="label", metricName="accuracy").evaluate(preds)
    f1 = MulticlassClassificationEvaluator(
        labelCol="label", metricName="f1").evaluate(preds)
    auc = BinaryClassificationEvaluator(
        labelCol="label", metricName="areaUnderROC").evaluate(preds)
    print(f"   accuracy={acc:.4f}  f1={f1:.4f}  auc={auc:.4f}")

    rf_model = model.stages[-1]
    importances = dict(sorted(
        zip(numeric_cols + ["traffic_vec"],
            [float(x) for x in rf_model.featureImportances.toArray()]),
        key=lambda x: -x[1],
    ))

    metrics = {
        "model": "RandomForest",
        "accuracy": round(acc, 4),
        "f1": round(f1, 4),
        "auc": round(auc, 4),
        "feature_importances": importances,
    }
    _save_json(
        metrics,
        os.path.join(ML_OUTPUT_DIR, "classification_results.json"),
    )
    return metrics


# ─────────────────────────────────────────────────────────────
# TASK 2 — KMeans Customer Segmentation
# ─────────────────────────────────────────────────────────────

def run_rfm_segmentation(spark, k=5):
    print("\n" + "=" * 60)
    print(f"  TASK 2: KMEANS CUSTOMER SEGMENTATION (k={k})")
    print("=" * 60)

    df, feature_cols = build_clustering_features(spark)
    print(f"   Customers with purchases: {df.count():,}")
    print(f"   Features: {feature_cols}")

    assembler = VectorAssembler(
        inputCols=feature_cols, outputCol="raw_features",
    )
    scaler = StandardScaler(
        inputCol="raw_features", outputCol="features",
        withStd=True, withMean=True,
    )
    kmeans = KMeans(
        featuresCol="features", predictionCol="cluster",
        k=k, seed=42, maxIter=20,
    )
    pipeline = Pipeline(stages=[assembler, scaler, kmeans])
    model = pipeline.fit(df)
    preds = model.transform(df)

    silhouette = ClusteringEvaluator(
        featuresCol="features", predictionCol="cluster",
        metricName="silhouette",
    ).evaluate(preds)
    print(f"   silhouette={silhouette:.4f}")

    cluster_stats = (preds.groupBy("cluster").agg(
        count("*").alias("n_customers"),
        _round(avg("recency_days"), 1).alias("avg_recency_days"),
        _round(avg("num_transactions"), 2).alias("avg_frequency"),
        _round(avg("total_spend"), 0).alias("avg_monetary"),
        _round(avg("avg_order_value"), 0).alias("avg_order_value"),
    ).orderBy("cluster"))
    cluster_stats.show(truncate=False)

    _save_json({
        "k": k,
        "silhouette": round(silhouette, 4),
        "feature_cols": feature_cols,
        "cluster_stats": [r.asDict() for r in cluster_stats.collect()],
    }, os.path.join(ML_OUTPUT_DIR, "rfm_clustering_results.json"))

    assignments_path = os.path.join(ML_OUTPUT_DIR, "rfm_cluster_assignments.csv")
    (preds.select("customer_id", "cluster",
                  "recency_days", "num_transactions", "total_spend",
                  "avg_order_value")
          .toPandas()
          .to_csv(assignments_path, index=False))
    print(f"   Saved: {assignments_path}")

    return {"k": k, "silhouette": round(silhouette, 4)}


# ─────────────────────────────────────────────────────────────
# TASK 3 — ALS Collaborative Filtering Recommender
# ─────────────────────────────────────────────────────────────

def run_als_recommendations(spark, top_n=5):
    print("\n" + "=" * 60)
    print("  TASK 3: ALS COLLABORATIVE FILTERING RECOMMENDER")
    print("=" * 60)

    txns = _load(spark, "transactions_exploded")
    cart_events = _load(spark, "cart_events")
    products = _load(spark, "products_clean")

    purchase_interactions = (txns
        .filter((col("payment_status") == "Success")
                & col("product_id").isNotNull())
        .select("customer_id", "product_id")
        .withColumn("rating", lit(5.0)))

    session_to_customer = txns.select("session_id", "customer_id").dropDuplicates()
    cart_interactions = (cart_events
        .join(session_to_customer, on="session_id", how="inner")
        .filter(col("product_id").isNotNull())
        .select("customer_id", "product_id")
        .withColumn("rating", lit(3.0)))

    interactions = (purchase_interactions.unionByName(cart_interactions)
        .groupBy("customer_id", "product_id")
        .agg(F.max("rating").alias("rating"))
        .withColumn("customer_id", col("customer_id").cast("int"))
        .withColumn("product_id", col("product_id").cast("int")))

    n_int = interactions.count()
    n_users = interactions.select("customer_id").distinct().count()
    n_items = interactions.select("product_id").distinct().count()
    sparsity = (1 - n_int / max(n_users * n_items, 1)) * 100
    print(f"   interactions={n_int:,}  users={n_users:,}  items={n_items:,}"
          f"  sparsity={sparsity:.2f}%")

    if n_int < 50:
        print("   ⚠ Too few interactions for ALS. Skipping.")
        return None

    train, test = interactions.randomSplit([0.8, 0.2], seed=42)
    als = ALS(
        userCol="customer_id", itemCol="product_id", ratingCol="rating",
        rank=10, maxIter=10, regParam=0.1,
        implicitPrefs=True, nonnegative=True,
        coldStartStrategy="drop", seed=42,
    )
    model = als.fit(train)
    rmse = RegressionEvaluator(
        labelCol="rating", predictionCol="prediction", metricName="rmse",
    ).evaluate(model.transform(test))
    print(f"   RMSE={rmse:.4f}")

    recs = (model.recommendForAllUsers(top_n)
        .select(col("customer_id"), explode("recommendations").alias("rec"))
        .select("customer_id",
                col("rec.product_id").alias("product_id"),
                _round(col("rec.rating"), 3).alias("score"))
        .join(products.select("product_id",
                              "productDisplayName", "masterCategory"),
              on="product_id", how="left")
        .orderBy("customer_id", desc("score")))

    recs_path = os.path.join(ML_OUTPUT_DIR, "als_recommendations.csv")
    os.makedirs(os.path.dirname(recs_path), exist_ok=True)
    recs.limit(10000).toPandas().to_csv(recs_path, index=False)
    print(f"   Saved: {recs_path}")

    metrics = {
        "model": "ALS",
        "rmse": round(rmse, 4),
        "interactions": n_int,
        "users": n_users,
        "items": n_items,
        "sparsity_pct": round(sparsity, 2),
        "rank": 10, "maxIter": 10, "regParam": 0.1,
    }
    _save_json(metrics, os.path.join(ML_OUTPUT_DIR, "als_results.json"))
    return metrics


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def run_ml_pipeline():
    ensure_dirs()
    spark = get_spark_session("Group6-ML")
    print("\n" + "=" * 60)
    print("  Group 6 ML PIPELINE")
    print("=" * 60)
    print(f"  Processed data: {_processed_dir()}")

    results = {}
    for name, fn in [
        ("classification", run_classification),
        ("clustering", run_rfm_segmentation),
        ("als", run_als_recommendations),
    ]:
        try:
            results[name] = fn(spark)
        except Exception as exc:
            print(f"   ✗ {name} failed: {exc}")
            import traceback
            traceback.print_exc()
            results[name] = {"error": str(exc)}

    print("\n" + "=" * 60)
    print("  PIPELINE COMPLETE")
    print("=" * 60)
    print(f"  Outputs → {ML_OUTPUT_DIR}")
    spark.stop()
    return results


if __name__ == "__main__":
    run_ml_pipeline()
