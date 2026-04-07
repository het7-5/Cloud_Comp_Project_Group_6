"""
Advanced Structured Streaming Module
======================================
Demonstrates complex Spark Structured Streaming patterns:
  1. Stream-Static JOIN: Enriches real-time click events with product catalog data
  2. Per-Category Trending: Windowed aggregation on enriched stream
  3. Session Anomaly Detection: Flags sessions with unusual event frequency
  4. Multi-query streaming: Multiple concurrent streaming queries

Spark Components: Structured Streaming, Stream-Static JOIN, Watermark, Window
"""

import os
import sys
import json
import time
import shutil

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.utils import (
    get_spark_session, ensure_dirs, get_data_dir, PROJECT_ROOT,
)
from src.ingestion import load_products

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
)
from pyspark.sql.functions import (
    col, window, count, approx_count_distinct, current_timestamp,
    from_json, to_timestamp, lit, avg, max as spark_max,
    when, desc, round as spark_round, stddev,
)


# ─────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────
STREAM_INPUT_DIR = os.path.join(PROJECT_ROOT, "data", "stream_input")
STREAM_OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "stream_output")
STREAM_CHECKPOINT_DIR = os.path.join(PROJECT_ROOT, "data", "stream_checkpoint")

STREAM_EVENT_SCHEMA = StructType([
    StructField("session_id", StringType(), True),
    StructField("event_name", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("event_id", StringType(), True),
    StructField("traffic_source", StringType(), True),
    StructField("event_metadata", StringType(), True),
])

EVENT_META_SCHEMA = StructType([
    StructField("page", StringType(), True),
    StructField("duration_sec", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("query", StringType(), True),
    StructField("promo_code", StringType(), True),
])


# ─────────────────────────────────────────────────────────────
# Stream Simulator
# ─────────────────────────────────────────────────────────────

def stream_simulator(max_events=500, delay_ms=100, batch_size=10):
    """Write click stream CSV rows as JSON files to simulate real-time arrival."""
    import csv

    data_dir = get_data_dir()
    csv_files = [f for f in os.listdir(data_dir) if f.lower().endswith(".csv")]
    click_file = None
    for f in csv_files:
        if "click" in f.lower():
            click_file = os.path.join(data_dir, f)
            break

    if click_file is None:
        raise FileNotFoundError(f"No click stream CSV found in {data_dir}")

    if os.path.exists(STREAM_INPUT_DIR):
        shutil.rmtree(STREAM_INPUT_DIR)
    os.makedirs(STREAM_INPUT_DIR, exist_ok=True)

    print(f"\n🌊 Stream Simulator Starting")
    print(f"   Source:      {click_file}")
    print(f"   Output dir:  {STREAM_INPUT_DIR}")
    print(f"   Max events:  {max_events}")

    count_written = 0
    batch_num = 0

    with open(click_file, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        batch = []

        for row in reader:
            if count_written >= max_events:
                break

            event = {
                "session_id": row.get("session_id", ""),
                "event_name": row.get("event_name", ""),
                "event_time": row.get("event_time", ""),
                "event_id": row.get("event_id", ""),
                "traffic_source": row.get("traffic_source", ""),
                "event_metadata": row.get("event_metadata", ""),
            }
            batch.append(event)

            if len(batch) >= batch_size:
                batch_num += 1
                out_path = os.path.join(STREAM_INPUT_DIR,
                                        f"events_{batch_num:06d}.json")
                with open(out_path, "w", encoding="utf-8") as out_f:
                    for evt in batch:
                        out_f.write(json.dumps(evt) + "\n")

                count_written += len(batch)
                if batch_num % 10 == 0:
                    print(f"   📤 Wrote batch {batch_num} ({count_written} events)")
                batch = []
                time.sleep(delay_ms / 1000.0)

        if batch:
            batch_num += 1
            out_path = os.path.join(STREAM_INPUT_DIR,
                                    f"events_{batch_num:06d}.json")
            with open(out_path, "w", encoding="utf-8") as out_f:
                for evt in batch:
                    out_f.write(json.dumps(evt) + "\n")
            count_written += len(batch)

    print(f"\n✅ Stream Simulator Complete: {count_written} events in {batch_num} files\n")
    return count_written


# ─────────────────────────────────────────────────────────────
# Advanced Structured Streaming Consumer
# ─────────────────────────────────────────────────────────────

def run_streaming(timeout_seconds=60):
    """
    Advanced streaming pipeline with:
      - Stream-static JOIN (enriches events w/ product catalog)
      - Per-category trending (windowed aggregation on enriched data)
      - Session engagement scoring
      - Traffic source breakdown
    """
    ensure_dirs()

    for d in [STREAM_OUTPUT_DIR, STREAM_CHECKPOINT_DIR]:
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)

    spark = get_spark_session("ECommerce-AdvancedStreaming")

    print("\n" + "=" * 60)
    print("🌊 STARTING ADVANCED STRUCTURED STREAMING PIPELINE")
    print("=" * 60)
    print(f"\n📂 Reading stream from: {STREAM_INPUT_DIR}")
    print(f"📂 Output to:          {STREAM_OUTPUT_DIR}")
    print(f"⏱  Timeout:            {timeout_seconds}s")

    # ── Load static product catalog for stream-static join ──
    data_dir = get_data_dir()
    products_df = load_products(spark, data_dir)
    products_df = products_df.select(
        col("product_id").alias("prod_id"),
        "masterCategory", "subCategory", "articleType"
    )
    print(f"\n📦 Loaded product catalog: {products_df.count()} products (static)")

    # ── Read stream ──
    raw_stream = (spark.readStream
                  .format("json")
                  .schema(STREAM_EVENT_SCHEMA)
                  .option("maxFilesPerTrigger", 5)
                  .load(STREAM_INPUT_DIR))

    # ── Parse event metadata + timestamp ──
    parsed = (raw_stream
              .withColumn("parsed_meta", from_json(col("event_metadata"), EVENT_META_SCHEMA))
              .withColumn("event_ts", to_timestamp(col("event_time")))
              .withColumn("duration_sec", col("parsed_meta.duration_sec"))
              .withColumn("product_id", col("parsed_meta.product_id")))

    # ── STREAM-STATIC JOIN: Enrich events with product catalog ──
    enriched = parsed.join(
        products_df,
        parsed.product_id == products_df.prod_id,
        "left"
    )

    print("   ✓ Stream-Static JOIN configured (events × product catalog)")

    # ── Query 1: Per-Category Trending (windowed, on enriched stream) ──
    category_trending = (enriched
                         .filter(col("masterCategory").isNotNull())
                         .withWatermark("event_ts", "10 minutes")
                         .groupBy(
                             window(col("event_ts"), "5 minutes"),
                             col("masterCategory"),
                         )
                         .agg(
                             count("*").alias("event_count"),
                             approx_count_distinct("session_id").alias("active_sessions"),
                             spark_round(avg("duration_sec"), 1).alias("avg_duration"),
                         ))

    # ── Query 2: Session Engagement Scoring ──
    session_scoring = (enriched
                       .withWatermark("event_ts", "10 minutes")
                       .groupBy(
                           window(col("event_ts"), "5 minutes"),
                           col("session_id"),
                           col("traffic_source"),
                       )
                       .agg(
                           count("*").alias("event_count"),
                           approx_count_distinct("event_name").alias("unique_actions"),
                           spark_round(avg("duration_sec"), 1).alias("avg_duration"),
                           spark_max(when(col("event_name") == "add_to_cart", 1).otherwise(0)).alias("has_cart"),
                           spark_max(when(col("event_name") == "checkout", 1).otherwise(0)).alias("has_checkout"),
                       ))

    # ── Write Query 1 to console ──
    q1 = (category_trending.writeStream
          .outputMode("update")
          .format("console")
          .option("truncate", "false")
          .option("numRows", 20)
          .queryName("category_trending")
          .start())

    # ── Write Query 2 to memory sink ──
    q2 = (session_scoring.writeStream
          .outputMode("update")
          .format("memory")
          .queryName("session_scores")
          .start())

    print(f"\n⏳ Streaming for {timeout_seconds} seconds…")
    print("   Active queries:")
    for q in spark.streams.active:
        print(f"     • {q.name}")

    try:
        q1.awaitTermination(timeout_seconds)
    except Exception as e:
        print(f"   ⚠ Stream terminated: {e}")

    for q in spark.streams.active:
        q.stop()

    # ── Post-stream summary ──
    print("\n" + "=" * 60)
    print("📊 ADVANCED STREAMING SUMMARY")
    print("=" * 60)

    try:
        # Session scoring summary from memory sink
        scores_df = spark.sql("SELECT * FROM session_scores")
        if scores_df.count() > 0:
            print(f"\n   Sessions scored: {scores_df.count()}")

            # Anomaly detection: flag sessions with unusual event counts
            stats = scores_df.agg(
                avg("event_count").alias("mean_events"),
                stddev("event_count").alias("std_events")
            ).collect()[0]

            mean_events = stats["mean_events"] or 0
            std_events = stats["std_events"] or 1

            anomaly_threshold = mean_events + 2 * std_events
            print(f"   Anomaly threshold: > {anomaly_threshold:.1f} events/session (mean + 2σ)")

            anomalous = scores_df.filter(col("event_count") > anomaly_threshold)
            print(f"   Anomalous sessions detected: {anomalous.count()}")

            if anomalous.count() > 0:
                print("\n   🚨 Anomalous Sessions:")
                anomalous.select("session_id", "event_count", "unique_actions",
                                 "traffic_source").show(10, truncate=False)

            # Save session scores
            csv_path = os.path.join(STREAM_OUTPUT_DIR, "session_engagement_scores.csv")
            scores_df.toPandas().to_csv(csv_path, index=False)
            print(f"   ✓ Saved session scores → {csv_path}")
        else:
            print("   ⚠ No session scores captured")
    except Exception as e:
        print(f"   ⚠ Summary error: {e}")

    print("\n✅ ADVANCED STREAMING PIPELINE COMPLETE\n")

    # Graceful shutdown: ensure all streaming queries are fully stopped
    # and the StateStore maintenance thread completes its current cycle
    # before tearing down SparkEnv (prevents IllegalStateException)
    for q in spark.streams.active:
        q.stop()
    time.sleep(2)
    spark.stop()


# ─────────────────────────────────────────────────────────────
# Combined: simulate + consume
# ─────────────────────────────────────────────────────────────

def run_full_streaming_demo(max_events=500, delay_ms=50, timeout_seconds=60):
    """End-to-end streaming demo: simulate + consume."""
    print("\n" + "=" * 60)
    print("🌊 ADVANCED STREAMING DEMO (Stream-Static Join + Anomaly Detection)")
    print("=" * 60)

    stream_simulator(max_events=max_events, delay_ms=delay_ms)

    try:
        run_streaming(timeout_seconds=timeout_seconds)
    except Exception as e:
        print(f"   ⚠ Streaming error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    run_full_streaming_demo()
