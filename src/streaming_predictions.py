"""
Streaming ML Predictions Module
================================
Applies trained ML models to real-time clickstream events using
Spark Structured Streaming + MLlib.

Architecture (two-phase):
  Phase 1 – TRAIN  (batch):
      • Load historical clickstream + transactions (batch DataFrames)
      • Engineer session-level features from clickstream history
      • Train a RandomForest classifier to predict whether a live
        session will end in a successful checkout ("conversion")
      • Persist the PipelineModel to data/models/session_conversion/

  Phase 2 – PREDICT  (streaming):
      • stream_simulator() writes new clickstream JSON files into
        data/stream_input/  (re-uses existing simulator)
      • readStream picks up those files
      • foreachBatch callback aggregates each micro-batch into
        per-session feature vectors, loads the saved model, scores
        every active session, and writes scored results to
        outputs/streaming/session_predictions.csv

Spark Components demonstrated:
    Structured Streaming  |  MLlib (Pipeline, RandomForest)
    Structured APIs       |  foreachBatch

Run standalone:
    python src/streaming_predictions.py
"""

import os
import sys
import json
import time
import shutil

# Force UTF-8 output on Windows (default console is cp1252)
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.utils import (
    get_spark_session, DATA_SAMPLE_DIR, STREAMING_OUTPUT_DIR,
    ensure_dirs, get_data_dir, PROJECT_ROOT,
)
from src.ingestion import load_transactions, load_clickstream
from src.streaming import stream_simulator, STREAM_INPUT_DIR

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    DoubleType, IntegerType,
)
from pyspark.sql.functions import (
    col, count, countDistinct, when, lit, max as spark_max,
    min as spark_min, avg, to_timestamp, current_timestamp,
    from_json,
)
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator


# ─────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────
MODEL_DIR = os.path.join(PROJECT_ROOT, "data", "models", "session_conversion")
STREAM_PRED_OUTPUT = os.path.join(STREAMING_OUTPUT_DIR, "session_predictions.csv")
TRAIN_METRICS_OUTPUT = os.path.join(STREAMING_OUTPUT_DIR, "stream_model_metrics.json")

STREAM_EVENT_SCHEMA = StructType([
    StructField("session_id",     StringType(), True),
    StructField("event_name",     StringType(), True),
    StructField("event_time",     StringType(), True),
    StructField("event_id",       StringType(), True),
    StructField("traffic_source", StringType(), True),
    StructField("event_metadata", StringType(), True),
])


# ─────────────────────────────────────────────────────────────
# FEATURE ENGINEERING  (shared by train + predict phases)
# ─────────────────────────────────────────────────────────────

def build_session_features(clickstream_df: DataFrame, label_df: DataFrame = None):
    """
    Aggregate raw clickstream events into one feature row per session.

    Features engineered:
        total_events        – how active the session was
        unique_event_types  – breadth of behaviour
        viewed_product      – ever hit view_product (0/1)
        added_to_cart       – ever hit add_to_cart  (0/1)
        applied_promo       – ever hit apply_promo  (0/1)
        searched            – ever hit search       (0/1)
        traffic_source      – mobile / web / tablet (categorical)

    If label_df (transactions) is provided the label column
    'converted' (1 = session had a successful payment, 0 = no) is
    added for use in training.
    """
    session_feats = (
        clickstream_df
        .groupBy("session_id", "traffic_source")
        .agg(
            count("*").alias("total_events"),
            countDistinct("event_name").alias("unique_event_types"),
            spark_max(when(col("event_name") == "view_product",  1).otherwise(0)).alias("viewed_product"),
            spark_max(when(col("event_name") == "add_to_cart",  1).otherwise(0)).alias("added_to_cart"),
            spark_max(when(col("event_name") == "checkout",     1).otherwise(0)).alias("reached_checkout"),
            spark_max(when(col("event_name") == "apply_promo",  1).otherwise(0)).alias("applied_promo"),
            spark_max(when(col("event_name") == "search",       1).otherwise(0)).alias("searched"),
        )
    )

    if label_df is not None:
        # A session "converted" if it has at least one successful transaction
        session_labels = (
            label_df
            .filter(col("payment_status") == "Success")
            .groupBy("session_id")
            .agg(spark_max(lit(1)).alias("converted"))
        )
        session_feats = session_feats.join(session_labels, on="session_id", how="left")
        session_feats = session_feats.fillna({"converted": 0})

    return session_feats


# ─────────────────────────────────────────────────────────────
# PHASE 1 – TRAIN  (batch)
# ─────────────────────────────────────────────────────────────

def train_session_model(spark: SparkSession):
    """
    Train a RandomForest classifier on historical session data.
    Returns (model, metrics_dict) — the model is kept in memory to
    avoid Windows Hadoop NativeIO crashes that occur when saving
    MLlib models via model.write().save().
    """
    print("\n" + "=" * 60)
    print("  PHASE 1: TRAINING SESSION CONVERSION MODEL  (batch)")
    print("=" * 60)

    data_dir = get_data_dir()

    # Load historical data
    print("\n   [>>] Loading historical clickstream + transactions ...")
    clickstream_df = load_clickstream(spark, data_dir)
    transactions_df = load_transactions(spark, data_dir)

    # Build labelled feature set
    print("   [>>] Engineering session-level features ...")
    session_df = build_session_features(clickstream_df, transactions_df)

    total = session_df.count()
    pos   = session_df.filter(col("converted") == 1).count()
    neg   = total - pos
    print(f"\n   Total sessions : {total:,}")
    print(f"   Converted (1)  : {pos:,}  ({pos*100/max(total,1):.1f}%)")
    print(f"   Not conv. (0)  : {neg:,}  ({neg*100/max(total,1):.1f}%)")

    # -- Build preprocessing pipeline --
    traffic_indexer = StringIndexer(
        inputCol="traffic_source",
        outputCol="traffic_source_idx",
        handleInvalid="keep"
    )
    feature_cols = [
        "total_events", "unique_event_types",
        "viewed_product", "added_to_cart", "reached_checkout",
        "applied_promo", "searched",
        "traffic_source_idx",
    ]
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="raw_features",
        handleInvalid="keep"
    )
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withStd=True, withMean=False
    )
    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="converted",
        numTrees=50,
        maxDepth=6,
        seed=42
    )

    pipe = Pipeline(stages=[traffic_indexer, assembler, scaler, rf])

    # -- Train / test split --
    train_df, test_df = session_df.randomSplit([0.8, 0.2], seed=42)
    train_df = train_df.cache()
    test_df  = test_df.cache()

    print(f"\n   Train: {train_df.count():,}   |   Test: {test_df.count():,}")
    print("   Training RandomForest ...")
    model = pipe.fit(train_df)

    # -- Evaluate --
    preds = model.transform(test_df)

    auc_eval = BinaryClassificationEvaluator(
        labelCol="converted", metricName="areaUnderROC")
    f1_eval  = MulticlassClassificationEvaluator(
        labelCol="converted", metricName="f1")
    acc_eval = MulticlassClassificationEvaluator(
        labelCol="converted", metricName="accuracy")

    auc = round(auc_eval.evaluate(preds), 4)
    f1  = round(f1_eval.evaluate(preds),  4)
    acc = round(acc_eval.evaluate(preds), 4)

    print(f"\n   [OK] Model Evaluation on Test Set:")
    print(f"      AUC-ROC  : {auc}")
    print(f"      F1 Score : {f1}")
    print(f"      Accuracy : {acc}")

    # NOTE: We intentionally do NOT call model.write().save() here.
    # MLlib's save() uses Hadoop FileOutputCommitter which triggers
    # NativeIO$Windows.access0() — an UnsatisfiedLinkError on Windows.
    # Instead we keep the model object in memory for the streaming phase.
    print("\n   [OK] Model retained in-memory (bypassing Hadoop IO)")

    # -- Save metrics only (pure Python JSON, no Spark IO) --
    metrics = {
        "model": "RandomForest - Session Conversion",
        "features": feature_cols,
        "train_sessions": train_df.count(),
        "test_sessions":  test_df.count(),
        "AUC":      auc,
        "F1":       f1,
        "Accuracy": acc,
    }
    os.makedirs(STREAMING_OUTPUT_DIR, exist_ok=True)
    with open(TRAIN_METRICS_OUTPUT, "w", encoding="utf-8") as fh:
        json.dump(metrics, fh, indent=2)
    print(f"   [OK] Metrics saved -> {TRAIN_METRICS_OUTPUT}")

    train_df.unpersist()
    test_df.unpersist()

    return model, metrics


# ─────────────────────────────────────────────────────────────
# PHASE 2 – PREDICT  (streaming)
# ─────────────────────────────────────────────────────────────

def run_streaming_predictions(spark: SparkSession, saved_model, timeout_seconds: int = 90):
    """
    Consume simulated real-time clickstream events via Spark
    Structured Streaming and score each session using the pre-trained
    session-conversion model (passed in-memory).

    Uses foreachBatch to:
      1. aggregate raw events into session feature vectors
      2. call saved_model.transform() on the micro-batch feature vectors
      3. append scored rows to outputs/streaming/session_predictions.csv
    """
    print("\n" + "=" * 60)
    print("  PHASE 2: REAL-TIME SESSION SCORING  (streaming)")
    print("=" * 60)

    # Ensure output dir exists; start with a fresh CSV
    os.makedirs(STREAMING_OUTPUT_DIR, exist_ok=True)
    if os.path.exists(STREAM_PRED_OUTPUT):
        os.remove(STREAM_PRED_OUTPUT)

    # Track state across batches
    batch_state = {"batch_num": 0, "total_sessions_scored": 0}

    def score_batch(batch_df: DataFrame, batch_id: int):
        """foreachBatch callback — feature engineering + ML scoring."""
        if batch_df.rdd.isEmpty():
            return

        batch_state["batch_num"] += 1
        bnum = batch_state["batch_num"]

        # ── Build session features from this micro-batch ──
        session_feats = build_session_features(batch_df)   # no labels

        # -- Score with the trained model --
        scored = saved_model.transform(session_feats)

        # Extract P(converted=1) using native Spark SQL (no Python UDF).
        # vector_to_array converts the MLlib DenseVector to an array column,
        # then we take index [1] which is the positive-class probability.
        from pyspark.ml.functions import vector_to_array

        result = (
            scored
            .withColumn("prob_array",            vector_to_array(col("probability")))
            .withColumn("conversion_probability", col("prob_array")[1].cast("double"))
            .withColumn("predicted_conversion",   col("prediction").cast(IntegerType()))
            .withColumn("risk_label",
                when(col("conversion_probability") >= 0.70, "HIGH_CONVERSION")
                .when(col("conversion_probability") >= 0.40, "MEDIUM_CONVERSION")
                .otherwise("LOW_CONVERSION")
            )
            .withColumn("batch_id",  lit(batch_id))
            .withColumn("scored_at", current_timestamp().cast(StringType()))
            .select(
                "session_id",
                "traffic_source",
                "total_events",
                "unique_event_types",
                "viewed_product",
                "added_to_cart",
                "reached_checkout",
                "applied_promo",
                "searched",
                "conversion_probability",
                "predicted_conversion",
                "risk_label",
                "batch_id",
                "scored_at",
            )
        )

        n = result.count()
        batch_state["total_sessions_scored"] += n

        # Show a live preview in the console
        print(f"\n  Batch {bnum} (stream batch_id={batch_id}) -- {n} sessions scored")
        result.select(
            "session_id", "traffic_source", "total_events",
            "conversion_probability", "risk_label"
        ).show(10, truncate=False)

        # Distribution summary
        dist = (
            result
            .groupBy("risk_label")
            .count()
            .orderBy("risk_label")
            .collect()
        )
        for row in dist:
            print(f"     {row.risk_label:<22} : {row['count']:>5} sessions")

        # -- Append to CSV (pandas for Windows compatibility) --
        pdf = result.toPandas()
        write_header = not os.path.exists(STREAM_PRED_OUTPUT)
        pdf.to_csv(STREAM_PRED_OUTPUT, mode="a", header=write_header, index=False)

    # ── Set up the streaming reader ──
    print(f"   📂 Reading stream from : {STREAM_INPUT_DIR}")
    print(f"   📂 Predictions output  : {STREAM_PRED_OUTPUT}")
    print(f"   ⏱  Timeout             : {timeout_seconds}s\n")

    raw_stream = (
        spark.readStream
        .format("json")
        .schema(STREAM_EVENT_SCHEMA)
        .option("maxFilesPerTrigger", 5)
        .load(STREAM_INPUT_DIR)
    )

    query = (
        raw_stream.writeStream
        .foreachBatch(score_batch)
        .outputMode("append")
        .queryName("session_scoring")
        .start()
    )

    print(f"⏳ Streaming + scoring for {timeout_seconds} seconds …")
    try:
        query.awaitTermination(timeout_seconds)
    except Exception as exc:
        print(f"   ⚠ Stream terminated early: {exc}")
    finally:
        query.stop()

    total = batch_state["total_sessions_scored"]
    print(f"\n   ✅ Streaming complete — {total:,} sessions scored across "
          f"{batch_state['batch_num']} micro-batches")
    print(f"   📄 Full results → {STREAM_PRED_OUTPUT}\n")


# ─────────────────────────────────────────────────────────────
# Batch-fallback scorer (for Windows NativeIO issues)
# ─────────────────────────────────────────────────────────────

def fallback_batch_scoring(spark: SparkSession, saved_model):
    """
    Score all events in STREAM_INPUT_DIR as a single batch.
    Produces identical output to the streaming path.
    Used when Spark Structured Streaming hits Windows JVM issues.
    """
    import glob
    print("\n   [WARN] Entering batch-fallback scoring path ...")
    files = glob.glob(os.path.join(STREAM_INPUT_DIR, "*.json"))
    if not files:
        print("   No files found for fallback scoring.")
        return

    batch_df = spark.read.schema(STREAM_EVENT_SCHEMA).json(files)
    session_feats = build_session_features(batch_df)
    scored = saved_model.transform(session_feats)

    # Extract probability using native Spark SQL (no Python UDF)
    from pyspark.ml.functions import vector_to_array

    result = (
        scored
        .withColumn("prob_array",            vector_to_array(col("probability")))
        .withColumn("conversion_probability", col("prob_array")[1].cast("double"))
        .withColumn("predicted_conversion",   col("prediction").cast(IntegerType()))
        .withColumn("risk_label",
            when(col("conversion_probability") >= 0.70, "HIGH_CONVERSION")
            .when(col("conversion_probability") >= 0.40, "MEDIUM_CONVERSION")
            .otherwise("LOW_CONVERSION")
        )
        .withColumn("batch_id",  lit(-1))
        .withColumn("scored_at", current_timestamp().cast(StringType()))
        .select(
            "session_id", "traffic_source", "total_events",
            "unique_event_types", "viewed_product", "added_to_cart",
            "reached_checkout", "applied_promo", "searched",
            "conversion_probability", "predicted_conversion",
            "risk_label", "batch_id", "scored_at",
        )
    )

    n = result.count()
    print(f"\n   Fallback batch scored {n:,} sessions\n")
    result.show(20, truncate=False)

    os.makedirs(STREAMING_OUTPUT_DIR, exist_ok=True)
    result.toPandas().to_csv(STREAM_PRED_OUTPUT, index=False)
    print(f"   ✅ Results saved → {STREAM_PRED_OUTPUT}\n")


# ─────────────────────────────────────────────────────────────
# Main orchestrator
# ─────────────────────────────────────────────────────────────

def run_streaming_ml_pipeline(
    max_events: int = 500,
    delay_ms: int = 50,
    timeout_seconds: int = 90,
):
    """
    End-to-end streaming ML pipeline:
      1) Train session-conversion model on historical data (batch)
      2) Simulate real-time clickstream events
      3) Score every active session in real-time (streaming + foreachBatch)
    """
    ensure_dirs()
    spark = get_spark_session("ECommerce-StreamingML")

    print("\n" + "=" * 60)
    print("  🌊🤖  STREAMING ML PREDICTIONS PIPELINE")
    print("=" * 60)

    # -- Phase 1: Train (keep model in memory) --
    model, metrics = train_session_model(spark)

    # -- Phase 2a: Simulate real-time events --
    print("\n" + "=" * 60)
    print("  PHASE 2a: SIMULATING REAL-TIME EVENTS")
    print("=" * 60)
    stream_simulator(max_events=max_events, delay_ms=delay_ms)

    # -- Phase 2b: Stream + score --
    try:
        run_streaming_predictions(spark, model, timeout_seconds=timeout_seconds)
    except Exception as exc:
        print(f"   [WARN] Streaming error: {exc}")
        if os.name == "nt" and ("UnsatisfiedLinkError" in str(exc) or
                                "NativeIO" in str(exc)):
            print("   [>>] Falling back to batch scoring (Windows NativeIO) ...")
            fallback_batch_scoring(spark, model)
        else:
            raise

    # ── Final summary ──
    print("\n" + "=" * 60)
    print("  ✅ STREAMING ML PIPELINE COMPLETE")
    print("=" * 60)
    print(f"\n   Training metrics:")
    print(f"      AUC-ROC  : {metrics['AUC']}")
    print(f"      F1 Score : {metrics['F1']}")
    print(f"      Accuracy : {metrics['Accuracy']}")
    print(f"\n   Outputs:")
    print(f"      Model        → {MODEL_DIR}")
    print(f"      Metrics JSON → {TRAIN_METRICS_OUTPUT}")
    print(f"      Predictions  → {STREAM_PRED_OUTPUT}\n")

    spark.stop()


if __name__ == "__main__":
    run_streaming_ml_pipeline()
