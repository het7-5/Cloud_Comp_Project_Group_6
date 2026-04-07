"""
Advanced ML Pipeline Module
============================
Three ML tasks using PySpark MLlib:

  1. Clickstream-Enhanced Payment Classification
     - Enriches transaction features with browsing behavior from clickstream
     - Models: Logistic Regression, Random Forest, GBT (with class weights)

  2. ALS Collaborative Filtering Recommender
     - Builds customer×product interaction matrix from clickstream events
     - Generates top-N product recommendations per customer

  3. RFM-Based Dynamic Customer Segmentation
     - Computes Recency/Frequency/Monetary features
     - KMeans clustering on RFM scores vs behavioral features

Spark Components: MLlib (Pipeline, ALS, KMeans, RandomForest, GBT)
"""

import os
import sys
import json
import traceback

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.utils import (
    get_spark_session, ML_OUTPUT_DIR, ensure_dirs, get_data_dir,
)
from src.ingestion import load_customers, load_transactions, load_clickstream, load_products

from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, countDistinct, when, lit,
    round as spark_round, desc, max as spark_max, min as spark_min,
    datediff, explode,
)
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer, VectorAssembler, StandardScaler,
)
from pyspark.ml.classification import (
    LogisticRegression, RandomForestClassifier, GBTClassifier,
)
from pyspark.ml.clustering import KMeans
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
    ClusteringEvaluator,
    RegressionEvaluator,
)


def _save_json(data, filepath):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(f"   Saved: {filepath}")


# ===================================================================
# TASK 1: Clickstream-Enhanced Payment Classification
# ===================================================================

def build_enhanced_features(spark):
    """
    Build features that combine transaction data WITH clickstream behavior.
    This is the key difference from the original — browsing features are added.
    """
    data_dir = get_data_dir()
    transactions_df = load_transactions(spark, data_dir)
    customers_df = load_customers(spark, data_dir)
    clickstream_df = load_clickstream(spark, data_dir)

    transactions_df.createOrReplaceTempView("transactions")
    customers_df.createOrReplaceTempView("customers")
    clickstream_df.createOrReplaceTempView("clickstream")

    print("   Building clickstream-enhanced feature set...")

    df = spark.sql("""
        WITH session_behavior AS (
            SELECT
                cs.session_id,
                COUNT(*) AS session_events,
                COUNT(DISTINCT cs.event_name) AS unique_actions,
                ROUND(AVG(cs.parsed_event_meta.duration_sec), 1) AS avg_browse_duration,
                MAX(CASE WHEN cs.event_name = 'view_product' THEN 1 ELSE 0 END) AS viewed_product,
                MAX(CASE WHEN cs.event_name = 'add_to_cart' THEN 1 ELSE 0 END) AS added_to_cart,
                MAX(CASE WHEN cs.event_name = 'search' THEN 1 ELSE 0 END) AS searched,
                MAX(CASE WHEN cs.event_name = 'apply_promo' THEN 1 ELSE 0 END) AS applied_promo,
                COUNT(DISTINCT cs.parsed_event_meta.product_id) AS products_browsed
            FROM clickstream cs
            GROUP BY cs.session_id
        )
        SELECT
            t.total_amount,
            t.shipment_fee,
            t.promo_amount,
            CASE WHEN t.promo_amount > 0 THEN 1.0 ELSE 0.0 END AS has_promo,
            t.payment_method,
            c.device_type,
            c.gender,
            t.payment_status,
            COALESCE(sb.session_events, 0) AS session_events,
            COALESCE(sb.unique_actions, 0) AS unique_actions,
            COALESCE(sb.avg_browse_duration, 0) AS avg_browse_duration,
            COALESCE(sb.viewed_product, 0) AS viewed_product,
            COALESCE(sb.added_to_cart, 0) AS added_to_cart,
            COALESCE(sb.searched, 0) AS searched,
            COALESCE(sb.applied_promo, 0) AS applied_promo,
            COALESCE(sb.products_browsed, 0) AS products_browsed
        FROM transactions t
        JOIN customers c ON t.customer_id = c.customer_id
        LEFT JOIN session_behavior sb ON t.session_id = sb.session_id
        WHERE t.payment_status IN ('Success', 'Failed')
          AND t.total_amount IS NOT NULL
    """)

    df = df.fillna({
        "promo_amount": 0.0, "shipment_fee": 0.0,
        "session_events": 0, "unique_actions": 0,
        "avg_browse_duration": 0.0, "products_browsed": 0,
    })

    print(f"   Enhanced feature dataset: {df.count():,} rows")
    print(f"   Features: transaction (4) + browsing behavior (8) + demographics (3) = 15")
    df.groupBy("payment_status").count().show()
    return df


def run_classification(spark):
    """Train payment classifiers with clickstream-enhanced features."""
    print("\n" + "=" * 60)
    print("  TASK 1: CLICKSTREAM-ENHANCED PAYMENT CLASSIFICATION")
    print("=" * 60)

    df = build_enhanced_features(spark)

    # Class weights
    class_counts = df.groupBy("payment_status").count().collect()
    class_dict = {row.payment_status: row["count"] for row in class_counts}
    total = sum(class_dict.values())
    n_classes = len(class_dict)
    weight_map = {cls: total / (n_classes * cnt) for cls, cnt in class_dict.items()}
    print(f"   Class weights: {weight_map}")

    df = df.withColumn(
        "classWeight",
        when(col("payment_status") == "Failed", weight_map.get("Failed", 1.0))
        .otherwise(weight_map.get("Success", 1.0))
    )

    df = df.repartition(2).cache()
    df.count()

    cat_cols = ["payment_method", "device_type", "gender"]
    indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
                for c in cat_cols]
    label_indexer = StringIndexer(inputCol="payment_status", outputCol="label")

    # Enhanced feature columns: transaction + browsing + categorical
    numeric_features = [
        "total_amount", "shipment_fee", "promo_amount", "has_promo",
        "session_events", "unique_actions", "avg_browse_duration",
        "viewed_product", "added_to_cart", "searched", "applied_promo",
        "products_browsed",
    ]
    assembler = VectorAssembler(
        inputCols=numeric_features + [f"{c}_idx" for c in cat_cols],
        outputCol="raw_features", handleInvalid="keep")
    scaler = StandardScaler(inputCol="raw_features", outputCol="features",
                            withStd=True, withMean=False)
    preprocessing = indexers + [label_indexer, assembler, scaler]

    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    train_df = train_df.cache()
    test_df = test_df.cache()
    print(f"   Train: {train_df.count():,}  |  Test: {test_df.count():,}")

    auc_eval = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    f1_eval = MulticlassClassificationEvaluator(labelCol="label", metricName="f1")
    acc_eval = MulticlassClassificationEvaluator(labelCol="label", metricName="accuracy")

    models = [
        ("Logistic Regression", LogisticRegression(
            featuresCol="features", labelCol="label", weightCol="classWeight", maxIter=20)),
        ("Random Forest", RandomForestClassifier(
            featuresCol="features", labelCol="label", numTrees=30, maxDepth=8, seed=42)),
        ("Gradient-Boosted Trees", GBTClassifier(
            featuresCol="features", labelCol="label", weightCol="classWeight",
            maxIter=15, seed=42)),
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
                "Accuracy": round(acc_eval.evaluate(preds), 4),
                "features_used": numeric_features + [f"{c}_idx" for c in cat_cols],
            }
            print(f"      AUC: {metrics['AUC']}  |  F1: {metrics['F1']}  |  Acc: {metrics['Accuracy']}")
            all_results.append(metrics)
            _save_json(metrics, os.path.join(
                ML_OUTPUT_DIR, f"classification_{name.replace(' ', '_').lower()}.json"))
        except Exception as exc:
            print(f"      ERROR: {exc}")
            traceback.print_exc()
            all_results.append({"model": name, "error": str(exc)})

    _save_json(all_results, os.path.join(ML_OUTPUT_DIR, "classification_results.json"))

    print("\n" + "-" * 60)
    print("  CLASSIFICATION RESULTS (with clickstream features)")
    print("-" * 60)
    print(f"  {'Model':<30} {'AUC':>7} {'F1':>7} {'Acc':>7}")
    print("  " + "-" * 56)
    for r in all_results:
        if "AUC" in r:
            print(f"  {r['model']:<30} {r['AUC']:>7.4f} {r['F1']:>7.4f} {r['Accuracy']:>7.4f}")

    train_df.unpersist()
    test_df.unpersist()
    df.unpersist()
    return all_results


# ===================================================================
# TASK 2: ALS Collaborative Filtering Recommender
# ===================================================================

def run_als_recommendations(spark):
    """
    Build a collaborative filtering recommender using ALS on
    customer × product interaction matrix from clickstream events.
    """
    print("\n" + "=" * 60)
    print("  TASK 2: ALS COLLABORATIVE FILTERING RECOMMENDER")
    print("=" * 60)

    data_dir = get_data_dir()
    clickstream_df = load_clickstream(spark, data_dir)
    transactions_df = load_transactions(spark, data_dir)
    products_df = load_products(spark, data_dir)

    clickstream_df.createOrReplaceTempView("clickstream")
    transactions_df.createOrReplaceTempView("transactions")
    products_df.createOrReplaceTempView("products")

    # Build interaction matrix: customer × product with implicit rating
    print("\n   Building customer-product interaction matrix...")
    interactions = spark.sql("""
        SELECT
            t.customer_id,
            cs.parsed_event_meta.product_id AS product_id,
            MAX(CASE
                WHEN cs.event_name = 'checkout' THEN 5.0
                WHEN cs.event_name = 'add_to_cart' THEN 3.0
                WHEN cs.event_name = 'wishlist_add' THEN 2.0
                WHEN cs.event_name = 'view_product' THEN 1.0
                ELSE 0.5
            END) AS rating
        FROM clickstream cs
        JOIN transactions t ON cs.session_id = t.session_id
        WHERE cs.parsed_event_meta.product_id IS NOT NULL
        GROUP BY t.customer_id, cs.parsed_event_meta.product_id
    """)

    n_interactions = interactions.count()
    n_users = interactions.select("customer_id").distinct().count()
    n_items = interactions.select("product_id").distinct().count()
    print(f"   Interactions: {n_interactions:,}")
    print(f"   Unique customers: {n_users:,}")
    print(f"   Unique products: {n_items:,}")
    print(f"   Sparsity: {(1 - n_interactions / (n_users * n_items)) * 100:.1f}%")

    if n_interactions < 10:
        print("   ⚠ Too few interactions for ALS. Skipping.")
        return None

    # Index customer_id to integer for ALS
    user_indexer = StringIndexer(inputCol="customer_id", outputCol="user_id")
    user_model = user_indexer.fit(interactions)
    indexed = user_model.transform(interactions)
    indexed = indexed.withColumn("user_id", col("user_id").cast("int"))
    indexed = indexed.withColumn("product_id", col("product_id").cast("int"))

    # Store mapping for later
    user_mapping = indexed.select("customer_id", "user_id").distinct()

    # Train/test split
    train, test = indexed.randomSplit([0.8, 0.2], seed=42)
    train = train.cache()
    test = test.cache()
    print(f"   Train: {train.count():,}  |  Test: {test.count():,}")

    # Train ALS
    print("   Training ALS model...")
    als = ALS(
        userCol="user_id",
        itemCol="product_id",
        ratingCol="rating",
        maxIter=10,
        regParam=0.1,
        rank=10,
        coldStartStrategy="drop",
        seed=42
    )
    model = als.fit(train)

    # Evaluate
    predictions = model.transform(test)
    evaluator = RegressionEvaluator(
        predictionCol="prediction", labelCol="rating", metricName="rmse")
    rmse = round(evaluator.evaluate(predictions), 4)
    print(f"   RMSE on test set: {rmse}")

    # Generate top-5 recommendations for all users
    print("\n   Generating top-5 recommendations per customer...")
    user_recs = model.recommendForAllUsers(5)

    # Explode recommendations and join back to get customer_id + product names
    recs_exploded = user_recs.select(
        col("user_id"),
        explode(col("recommendations")).alias("rec")
    ).select(
        col("user_id"),
        col("rec.product_id").alias("rec_product_id"),
        spark_round(col("rec.rating"), 3).alias("predicted_score")
    )

    # Join with user mapping and product names
    named_recs = (recs_exploded
                  .join(user_mapping, "user_id")
                  .join(products_df.select(
                      col("product_id").alias("rec_product_id"),
                      "productDisplayName", "masterCategory", "subCategory"),
                      "rec_product_id")
                  .select("customer_id", "productDisplayName", "masterCategory",
                          "subCategory", "predicted_score")
                  .orderBy("customer_id", desc("predicted_score")))

    print("\n   Sample Recommendations:")
    named_recs.show(20, truncate=False)

    # Save results
    als_results = {
        "model": "ALS Collaborative Filtering",
        "interactions": n_interactions,
        "unique_users": n_users,
        "unique_items": n_items,
        "RMSE": rmse,
        "rank": 10,
        "regParam": 0.1,
        "maxIter": 10,
    }
    _save_json(als_results, os.path.join(ML_OUTPUT_DIR, "als_recommender_results.json"))

    recs_csv = os.path.join(ML_OUTPUT_DIR, "als_recommendations.csv")
    named_recs.toPandas().to_csv(recs_csv, index=False)
    print(f"   Saved: {recs_csv}")

    train.unpersist()
    test.unpersist()
    return als_results


# ===================================================================
# TASK 3: RFM-Based Dynamic Customer Segmentation
# ===================================================================

def run_rfm_segmentation(spark):
    """KMeans clustering on RFM features + browsing behavior."""
    print("\n" + "=" * 60)
    print("  TASK 3: RFM-BASED DYNAMIC CUSTOMER SEGMENTATION")
    print("=" * 60)

    data_dir = get_data_dir()
    transactions_df = load_transactions(spark, data_dir)
    customers_df = load_customers(spark, data_dir)
    clickstream_df = load_clickstream(spark, data_dir)

    transactions_df.createOrReplaceTempView("transactions")
    customers_df.createOrReplaceTempView("customers")
    clickstream_df.createOrReplaceTempView("clickstream")

    print("\n   Building RFM + behavioral features...")
    cluster_df = spark.sql("""
        WITH date_ref AS (
            SELECT MAX(created_at) AS max_date FROM transactions
        ),
        rfm AS (
            SELECT
                t.customer_id,
                DATEDIFF(dr.max_date, MAX(t.created_at)) AS recency_days,
                COUNT(DISTINCT t.booking_id) AS frequency,
                ROUND(SUM(t.total_amount), 2) AS monetary,
                ROUND(AVG(t.total_amount), 2) AS avg_order_value,
                COUNT(DISTINCT t.payment_method) AS payment_methods_used,
                ROUND(SUM(t.promo_amount), 2) AS total_promo_used
            FROM transactions t
            CROSS JOIN date_ref dr
            WHERE t.payment_status = 'Success'
            GROUP BY t.customer_id, dr.max_date
        ),
        browsing AS (
            SELECT
                t.customer_id,
                COUNT(DISTINCT cs.session_id) AS browse_sessions,
                COALESCE(ROUND(AVG(cs.parsed_event_meta.duration_sec), 1), 0) AS avg_browse_time,
                COUNT(DISTINCT cs.parsed_event_meta.product_id) AS products_browsed,
                COUNT(DISTINCT cs.event_name) AS unique_event_types
            FROM transactions t
            JOIN clickstream cs ON t.session_id = cs.session_id
            GROUP BY t.customer_id
        )
        SELECT
            r.customer_id,
            r.recency_days,
            r.frequency,
            r.monetary,
            r.avg_order_value,
            r.payment_methods_used,
            r.total_promo_used,
            COALESCE(b.browse_sessions, 0) AS browse_sessions,
            COALESCE(b.avg_browse_time, 0) AS avg_browse_time,
            COALESCE(b.products_browsed, 0) AS products_browsed,
            COALESCE(b.unique_event_types, 0) AS unique_event_types
        FROM rfm r
        LEFT JOIN browsing b ON r.customer_id = b.customer_id
    """)

    cluster_df = cluster_df.dropna()
    cluster_df = cluster_df.repartition(2).cache()
    num_customers = cluster_df.count()
    print(f"   RFM + behavioral feature dataset: {num_customers:,} customers")
    print(f"   Features: RFM (6) + browsing (4) = 10 features")

    # Feature assembly + scaling
    feature_cols = [
        "recency_days", "frequency", "monetary", "avg_order_value",
        "payment_methods_used", "total_promo_used",
        "browse_sessions", "avg_browse_time", "products_browsed", "unique_event_types",
    ]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features",
                                handleInvalid="keep")
    scaler = StandardScaler(inputCol="raw_features", outputCol="features",
                            withStd=True, withMean=False)

    prep_pipe = Pipeline(stages=[assembler, scaler])
    prep_model = prep_pipe.fit(cluster_df)
    scaled_df = prep_model.transform(cluster_df).cache()
    scaled_df.count()

    silhouette_eval = ClusteringEvaluator(
        featuresCol="features", metricName="silhouette", predictionCol="cluster")

    k_values = [3, 4, 5, 6, 8]
    best_k, best_score = 4, -1.0
    k_results = []

    print("\n   Evaluating k values (RFM + behavioral)...")
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

    print(f"\n   Best k = {best_k} (silhouette = {best_score:.4f})")

    # Final model
    km = KMeans(featuresCol="features", predictionCol="cluster",
                k=best_k, seed=42, maxIter=20)
    km_model = km.fit(scaled_df)
    predictions = km_model.transform(scaled_df)

    # Cluster stats
    print("\n" + "-" * 60)
    print("  RFM + BEHAVIORAL CLUSTER SUMMARY")
    print("-" * 60)

    cluster_stats = (predictions
                     .groupBy("cluster")
                     .agg(
                         count("*").alias("num_customers"),
                         spark_round(avg("recency_days"), 0).alias("avg_recency"),
                         spark_round(avg("frequency"), 1).alias("avg_frequency"),
                         spark_round(avg("monetary"), 2).alias("avg_monetary"),
                         spark_round(avg("avg_order_value"), 2).alias("avg_aov"),
                         spark_round(avg("browse_sessions"), 1).alias("avg_browse_sessions"),
                         spark_round(avg("products_browsed"), 1).alias("avg_products_browsed"),
                     )
                     .orderBy("cluster"))
    cluster_stats.show(truncate=False)

    # Save
    stats_data = [row.asDict() for row in cluster_stats.collect()]
    for stat in stats_data:
        for key in stat:
            if hasattr(stat[key], 'item'):
                stat[key] = float(stat[key])

    _save_json({
        "best_k": best_k,
        "best_silhouette": round(best_score, 4),
        "k_evaluation": k_results,
        "features_used": feature_cols,
        "cluster_statistics": stats_data,
    }, os.path.join(ML_OUTPUT_DIR, "rfm_clustering_results.json"))

    assignments_path = os.path.join(ML_OUTPUT_DIR, "rfm_cluster_assignments.csv")
    predictions.select(
        "customer_id", "cluster", "recency_days", "frequency", "monetary",
        "avg_order_value", "browse_sessions", "products_browsed"
    ).toPandas().to_csv(assignments_path, index=False)
    print(f"   Saved: {assignments_path}")

    scaled_df.unpersist()
    cluster_df.unpersist()
    return k_results, best_k


# ===================================================================
# Main
# ===================================================================

def run_ml_pipeline():
    """Execute all three advanced ML tasks."""
    ensure_dirs()
    spark = get_spark_session("ECommerce-AdvancedML")

    print("\n" + "=" * 60)
    print("  🤖 RUNNING ADVANCED ML PIPELINE")
    print("=" * 60)

    # Task 1: Classification with clickstream features
    classification_results = run_classification(spark)

    # Task 2: ALS Recommender
    als_results = run_als_recommendations(spark)

    # Task 3: RFM + Behavioral Segmentation
    clustering_results, best_k = run_rfm_segmentation(spark)

    # Final summary
    print("\n" + "=" * 60)
    print("  🤖 ADVANCED ML PIPELINE COMPLETE")
    print("=" * 60)
    successful = [r for r in classification_results if r.get("F1", 0) > 0]
    if successful:
        best = max(successful, key=lambda x: x["F1"])
        print(f"\n   Best classifier: {best['model']} (F1={best['F1']:.4f}, AUC={best['AUC']:.4f})")
    if als_results:
        print(f"   ALS Recommender RMSE: {als_results['RMSE']}")
    print(f"   RFM Customer segments: {best_k}")
    print(f"   Results saved to: {ML_OUTPUT_DIR}\n")

    spark.stop()


if __name__ == "__main__":
    run_ml_pipeline()
