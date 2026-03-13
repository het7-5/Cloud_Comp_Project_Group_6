"""
ML Pipeline Module
==================
Implements two ML tasks using PySpark MLlib:

  1. Payment Status Classification
     - Predicts whether a transaction will succeed or fail
     - Handles class imbalance using class weights
     - Models: Logistic Regression, Random Forest, Gradient-Boosted Trees
     - Evaluation: AUC, F1, Precision, Recall

  2. Customer Segmentation (KMeans Clustering)
     - Segments customers by purchasing behavior
     - Evaluation: Silhouette Score

Spark Component: MLlib
"""

import os
import sys
import json
import traceback

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.utils import (
    get_spark_session, ML_OUTPUT_DIR, ensure_dirs, get_data_dir,
)
from src.ingestion import load_customers, load_transactions

from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, countDistinct, when, lit,
    round as spark_round, desc,
)
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer, VectorAssembler, StandardScaler,
)
from pyspark.ml.classification import (
    LogisticRegression, RandomForestClassifier, GBTClassifier,
)
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
    ClusteringEvaluator,
)


def _save_json(data, filepath):
    """Save a Python dict/list as plain JSON file (no Spark write needed)."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(f"   Saved: {filepath}")


# ===================================================================
# TASK 1: Payment Status Classification
# ===================================================================

def build_classification_features(spark):
    """
    Build a feature DataFrame for payment status classification.
    Joins Transaction and Customer tables, engineers features.
    """
    data_dir = get_data_dir()
    transactions_df = load_transactions(spark, data_dir)
    customers_df = load_customers(spark, data_dir)

    df = transactions_df.join(
        customers_df.select("customer_id", "device_type", "gender"),
        on="customer_id", how="inner",
    )
    df = (df
          .filter(col("payment_status").isin("Success", "Failed"))
          .filter(col("total_amount").isNotNull())
          .filter(col("payment_method").isNotNull())
          .fillna({"promo_amount": 0.0, "shipment_fee": 0.0})
          .withColumn("has_promo", when(col("promo_amount") > 0, 1.0).otherwise(0.0))
          .select("total_amount", "shipment_fee", "promo_amount", "has_promo",
                  "payment_method", "device_type", "gender", "payment_status"))
    print(f"\n   Classification feature dataset: {df.count():,} rows")
    df.groupBy("payment_status").count().show()
    return df


def run_classification(spark):
    """Train and evaluate payment status classifiers with class weighting."""
    print("\n" + "=" * 60)
    print("  TASK 1: PAYMENT STATUS CLASSIFICATION")
    print("=" * 60)

    df = build_classification_features(spark)

    total_rows = df.count()
    print(f"\n   Total features: {total_rows:,} rows")

    # Sample for local mode
    MAX_ROWS = 50_000
    if total_rows > MAX_ROWS:
        df = df.sample(False, MAX_ROWS / total_rows, seed=42)
        print(f"   Sampled to ~{df.count():,} rows for local mode")

    # --- Handle class imbalance ---
    # Count each class
    class_counts = df.groupBy("payment_status").count().collect()
    class_dict = {row.payment_status: row["count"] for row in class_counts}
    total = sum(class_dict.values())
    n_classes = len(class_dict)
    print(f"   Class distribution: {class_dict}")

    # Compute balanced class weights: total / (n_classes * count_per_class)
    # This gives higher weight to the minority class (Failed)
    weight_map = {}
    for cls, cnt in class_dict.items():
        weight_map[cls] = total / (n_classes * cnt)
    print(f"   Class weights: {weight_map}")

    # Add weight column
    weight_expr = when(col("payment_status") == "Success", weight_map.get("Success", 1.0))
    for cls, wt in weight_map.items():
        if cls != "Success":
            weight_expr = weight_expr.when(col("payment_status") == cls, wt)
    # Simpler: use when/otherwise
    df = df.withColumn(
        "classWeight",
        when(col("payment_status") == "Failed", weight_map.get("Failed", 1.0))
        .otherwise(weight_map.get("Success", 1.0))
    )

    df = df.repartition(2).cache()
    df.count()

    # Preprocessing pipeline
    cat_cols = ["payment_method", "device_type", "gender"]
    indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
                for c in cat_cols]
    label_indexer = StringIndexer(inputCol="payment_status", outputCol="label")
    assembler = VectorAssembler(
        inputCols=["total_amount", "shipment_fee", "promo_amount", "has_promo"] +
                  [f"{c}_idx" for c in cat_cols],
        outputCol="raw_features")
    scaler = StandardScaler(inputCol="raw_features", outputCol="features",
                            withStd=True, withMean=False)
    preprocessing = indexers + [label_indexer, assembler, scaler]

    # Train/test split
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    train_df = train_df.cache()
    test_df = test_df.cache()
    print(f"   Train: {train_df.count():,}  |  Test: {test_df.count():,}")
    df.groupBy("payment_status").count().show()

    # Evaluators
    auc_eval = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    f1_eval = MulticlassClassificationEvaluator(labelCol="label", metricName="f1")
    prec_eval = MulticlassClassificationEvaluator(labelCol="label", metricName="weightedPrecision")
    rec_eval = MulticlassClassificationEvaluator(labelCol="label", metricName="weightedRecall")
    acc_eval = MulticlassClassificationEvaluator(labelCol="label", metricName="accuracy")

    # Models -- use weightCol for class imbalance handling
    models = [
        ("Logistic Regression", LogisticRegression(
            featuresCol="features", labelCol="label", weightCol="classWeight",
            maxIter=20)),
        ("Random Forest", RandomForestClassifier(
            featuresCol="features", labelCol="label",
            numTrees=20, seed=42)),
        ("Gradient-Boosted Trees", GBTClassifier(
            featuresCol="features", labelCol="label", weightCol="classWeight",
            maxIter=10, seed=42)),
    ]

    all_results = []

    for name, model in models:
        print(f"\n   -- Training {name} --")
        try:
            pipe = Pipeline(stages=preprocessing + [model])
            pipe_model = pipe.fit(train_df)
            preds = pipe_model.transform(test_df)

            metrics = {
                "model": name,
                "AUC": round(auc_eval.evaluate(preds), 4),
                "F1": round(f1_eval.evaluate(preds), 4),
                "Precision": round(prec_eval.evaluate(preds), 4),
                "Recall": round(rec_eval.evaluate(preds), 4),
                "Accuracy": round(acc_eval.evaluate(preds), 4),
            }

            print(f"      AUC:       {metrics['AUC']}")
            print(f"      F1:        {metrics['F1']}")
            print(f"      Precision: {metrics['Precision']}")
            print(f"      Recall:    {metrics['Recall']}")
            print(f"      Accuracy:  {metrics['Accuracy']}")

            # Confusion matrix
            print(f"\n      Confusion Matrix ({name}):")
            cm_rows = preds.groupBy("payment_status", "prediction").count().collect()
            cm_data = [{"payment_status": r.payment_status,
                        "prediction": float(r.prediction),
                        "count": r["count"]} for r in cm_rows]
            for row in cm_data:
                print(f"        {row}")
            metrics["confusion_matrix"] = cm_data
            all_results.append(metrics)

            # Save this model's result immediately
            _save_json(metrics, os.path.join(
                ML_OUTPUT_DIR, f"classification_{name.replace(' ', '_').lower()}.json"))

        except Exception as exc:
            print(f"      ERROR: {exc}")
            traceback.print_exc()
            all_results.append({
                "model": name, "AUC": 0.0, "F1": 0.0,
                "Precision": 0.0, "Recall": 0.0, "Accuracy": 0.0,
                "error": str(exc),
            })

    # Save combined results
    _save_json(all_results, os.path.join(ML_OUTPUT_DIR, "classification_results.json"))

    # Print comparison table
    print("\n" + "-" * 60)
    print("  CLASSIFICATION RESULTS COMPARISON")
    print("-" * 60)
    print(f"  {'Model':<30} {'AUC':>7} {'F1':>7} {'Acc':>7}")
    print("  " + "-" * 56)
    for r in all_results:
        print(f"  {r['model']:<30} {r.get('AUC',0):>7.4f} {r.get('F1',0):>7.4f} {r.get('Accuracy',0):>7.4f}")

    train_df.unpersist()
    test_df.unpersist()
    df.unpersist()

    return all_results


# ===================================================================
# TASK 2: Customer Segmentation (KMeans Clustering)
# ===================================================================

def build_clustering_features(spark):
    """
    Build customer-level feature DataFrame for clustering.
    Features: total_spend, num_transactions, avg_order_value,
              num_sessions, promo_usage_rate, payment_methods_used
    """
    data_dir = get_data_dir()
    transactions_df = load_transactions(spark, data_dir)
    customers_df = load_customers(spark, data_dir)

    customer_metrics = (transactions_df
                        .filter(col("payment_status") == "Success")
                        .groupBy("customer_id")
                        .agg(
                            spark_sum("total_amount").alias("total_spend"),
                            count("*").alias("num_transactions"),
                            avg("total_amount").alias("avg_order_value"),
                            countDistinct("session_id").alias("num_sessions"),
                            spark_sum("promo_amount").alias("total_promo_used"),
                            countDistinct("payment_method").alias("payment_methods_used"),
                        ))

    customer_metrics = customer_metrics.withColumn(
        "promo_usage_rate",
        when(col("total_spend") > 0,
             col("total_promo_used") / col("total_spend")).otherwise(0.0))

    cluster_df = customer_metrics.join(
        customers_df.select("customer_id", "device_type", "gender"),
        on="customer_id", how="inner")

    numeric_feature_cols = [
        "total_spend", "num_transactions", "avg_order_value",
        "num_sessions", "promo_usage_rate", "payment_methods_used",
    ]
    cluster_df = cluster_df.dropna(subset=numeric_feature_cols)

    print(f"\n   Clustering feature dataset: {cluster_df.count():,} customers")
    cluster_df.describe().show()
    return cluster_df, numeric_feature_cols


def run_clustering(spark):
    """Perform KMeans clustering for customer segmentation."""
    print("\n" + "=" * 60)
    print("  TASK 2: CUSTOMER SEGMENTATION (K-MEANS CLUSTERING)")
    print("=" * 60)

    cluster_df, numeric_features = build_clustering_features(spark)
    cluster_df = cluster_df.repartition(2).cache()

    num_customers = cluster_df.count()
    print(f"\n   Clustering dataset: {num_customers:,} customers")

    # Build assembled+scaled DataFrame once (so silhouette eval works)
    assembler = VectorAssembler(inputCols=numeric_features, outputCol="raw_features")
    scaler = StandardScaler(inputCol="raw_features", outputCol="features",
                            withStd=True, withMean=False)

    prep_pipe = Pipeline(stages=[assembler, scaler])
    prep_model = prep_pipe.fit(cluster_df)
    scaled_df = prep_model.transform(cluster_df).cache()
    scaled_df.count()

    silhouette_eval = ClusteringEvaluator(
        featuresCol="features", metricName="silhouette",
        predictionCol="cluster")

    # Test different k values
    k_values = [3, 4, 5, 6, 8]
    best_k, best_score = 4, -1.0
    k_results = []

    print("\n   Evaluating k values ...")
    for k in k_values:
        try:
            km = KMeans(featuresCol="features", predictionCol="cluster",
                        k=k, seed=42, maxIter=20)
            km_model = km.fit(scaled_df)
            preds = km_model.transform(scaled_df)
            score = silhouette_eval.evaluate(preds)
            print(f"      k={k}  ->  Silhouette = {score:.4f}")
            k_results.append({"k": k, "silhouette_score": round(score, 4)})
            if score > best_score:
                best_score = score
                best_k = k
        except Exception as exc:
            print(f"      k={k}  ->  ERROR: {exc}")
            traceback.print_exc()

    print(f"\n   Best k = {best_k} (silhouette = {best_score:.4f})")

    # Final model with best k
    print(f"   Training final model with k={best_k} ...")
    km = KMeans(featuresCol="features", predictionCol="cluster",
                k=best_k, seed=42, maxIter=20)
    km_model = km.fit(scaled_df)
    predictions = km_model.transform(scaled_df)

    # Cluster statistics
    print("\n" + "-" * 60)
    print("  CLUSTER SUMMARY")
    print("-" * 60)

    clusters_raw = (predictions
                    .groupBy("cluster")
                    .agg(
                        count("*").alias("num_customers"),
                        spark_round(avg("total_spend"), 2).alias("avg_total_spend"),
                        spark_round(avg("num_transactions"), 2).alias("avg_transactions"),
                        spark_round(avg("avg_order_value"), 2).alias("avg_order_value"),
                        spark_round(avg("num_sessions"), 2).alias("avg_sessions"),
                        spark_round(avg("promo_usage_rate"), 4).alias("avg_promo_rate"),
                    )
                    .orderBy("cluster"))
    clusters_raw.show(truncate=False)

    # Device type distribution
    print("\n   Device Type by Cluster:")
    predictions.groupBy("cluster", "device_type").count().orderBy("cluster", desc("count")).show(20)

    # Gender distribution
    print("\n   Gender by Cluster:")
    predictions.groupBy("cluster", "gender").count().orderBy("cluster", desc("count")).show(20)

    # Save results as plain JSON
    cluster_stats = [row.asDict() for row in clusters_raw.collect()]
    for stat in cluster_stats:
        for key in stat:
            if hasattr(stat[key], 'item'):
                stat[key] = float(stat[key])

    _save_json({
        "best_k": best_k,
        "best_silhouette": round(best_score, 4),
        "k_evaluation": k_results,
        "cluster_statistics": cluster_stats,
    }, os.path.join(ML_OUTPUT_DIR, "clustering_results.json"))

    # Save cluster assignments as CSV
    assignments_path = os.path.join(ML_OUTPUT_DIR, "cluster_assignments.csv")
    assignments = predictions.select(
        "customer_id", "cluster", "total_spend", "num_transactions",
        "avg_order_value", "num_sessions", "promo_usage_rate",
        "device_type", "gender"
    ).toPandas()
    assignments.to_csv(assignments_path, index=False)
    print(f"   Saved: {assignments_path}")

    scaled_df.unpersist()
    cluster_df.unpersist()
    return k_results, best_k


# ===================================================================
# Main
# ===================================================================

def run_ml_pipeline():
    """Execute both ML tasks."""
    ensure_dirs()
    spark = get_spark_session("ECommerce-ML")

    print("\n" + "=" * 60)
    print("  RUNNING ML PIPELINE")
    print("=" * 60)

    # Task 1: Classification
    classification_results = run_classification(spark)

    # Task 2: Clustering
    clustering_results, best_k = run_clustering(spark)

    # Final summary
    print("\n" + "=" * 60)
    print("  ML PIPELINE COMPLETE")
    print("=" * 60)
    successful = [r for r in classification_results if r.get("F1", 0) > 0]
    if successful:
        best = max(successful, key=lambda x: x["F1"])
        print(f"\n   Best classifier: {best['model']} (F1={best['F1']:.4f}, AUC={best['AUC']:.4f})")
    print(f"   Customer segments: {best_k}")
    print(f"   Results saved to: {ML_OUTPUT_DIR}\n")

    spark.stop()


if __name__ == "__main__":
    run_ml_pipeline()
