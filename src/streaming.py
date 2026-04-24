"""
Structured Streaming Module (aligned with real clickstream schema)
==================================================================
Simulates a real-time clickstream by drip-feeding rows from the raw
``click_stream.csv`` as JSON batch files, then consumes the stream with
Spark Structured Streaming.

Pipeline:
  - Simulator → JSON files in data/stream_input/
  - readStream → parse event_metadata (single-quoted Python-dict → JSON)
  - Stream-static JOIN with products_clean (Parquet)
  - Query 1 (console sink):  category trending per 5-min window
  - Query 2 (memory sink):   per-session engagement for anomaly detection

Event vocabulary (uppercase, from real data):
  HOMEPAGE, SCROLL, SEARCH, ADD_TO_CART, ADD_PROMO, BOOKING
"""

import os
import sys
import csv
import json
import time
import shutil

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.utils import (
    get_spark_session, ensure_dirs, get_data_dir, PROJECT_ROOT,
)

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
)
from pyspark.sql.functions import (
    col, window, count, approx_count_distinct, from_json, to_timestamp,
    regexp_replace, avg, round as _round, when, max as _max, stddev,
)


# ─────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────

STREAM_INPUT_DIR      = os.path.join(PROJECT_ROOT, "data", "stream_input")
STREAM_OUTPUT_DIR     = os.path.join(PROJECT_ROOT, "data", "stream_output")
STREAM_CHECKPOINT_DIR = os.path.join(PROJECT_ROOT, "data", "stream_checkpoint")


def _processed_dir():
    bucket = os.environ.get("S3_BUCKET_PATH", "")
    if bucket:
        return f"{bucket}/processed"
    return os.path.join(PROJECT_ROOT, "data", "processed")


# ─────────────────────────────────────────────────────────────
# Schemas — match the real raw click_stream.csv
# ─────────────────────────────────────────────────────────────

STREAM_EVENT_SCHEMA = StructType([
    StructField("session_id",     StringType(), True),
    StructField("event_name",     StringType(), True),
    StructField("event_time",     StringType(), True),
    StructField("event_id",       StringType(), True),
    StructField("traffic_source", StringType(), True),
    StructField("event_metadata", StringType(), True),
])

# event_metadata is a single-quoted Python-dict string; the union of keys
# across all event types is captured here so from_json() surfaces the ones
# present for each row and leaves the rest null.
EVENT_META_SCHEMA = StructType([
    StructField("product_id",      IntegerType(), True),
    StructField("quantity",        IntegerType(), True),
    StructField("item_price",      DoubleType(),  True),
    StructField("search_keywords", StringType(),  True),
    StructField("promo_code",      StringType(),  True),
    StructField("promo_amount",    DoubleType(),  True),
    StructField("payment_status",  StringType(),  True),
])


# ─────────────────────────────────────────────────────────────
# Stream Simulator
# ─────────────────────────────────────────────────────────────

def _find_clickstream_csv():
    """Find the clickstream CSV in data/raw/ (preferred) or data/sample/."""
    data_dir = get_data_dir()
    for f in os.listdir(data_dir):
        if f.lower().endswith(".csv") and "click" in f.lower():
            return os.path.join(data_dir, f)
    raise FileNotFoundError(f"No click stream CSV found in {data_dir}")


def stream_simulator(max_events=500, delay_ms=100, batch_size=10):
    """Drip-feed rows from the raw clickstream CSV as JSON batches.

    Writes line-delimited JSON files into STREAM_INPUT_DIR so Spark's
    file-source streaming reader picks each batch up as a micro-batch.
    """
    click_file = _find_clickstream_csv()

    if os.path.exists(STREAM_INPUT_DIR):
        shutil.rmtree(STREAM_INPUT_DIR)
    os.makedirs(STREAM_INPUT_DIR, exist_ok=True)

    print("\n🌊 Stream Simulator Starting")
    print(f"   Source:      {click_file}")
    print(f"   Output dir:  {STREAM_INPUT_DIR}")
    print(f"   Max events:  {max_events}")

    fields = [f.name for f in STREAM_EVENT_SCHEMA.fields]
    written = 0
    batch_num = 0

    with open(click_file, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        batch = []
        for row in reader:
            if written >= max_events:
                break
            batch.append({k: row.get(k, "") for k in fields})
            if len(batch) >= batch_size:
                batch_num += 1
                out = os.path.join(STREAM_INPUT_DIR,
                                   f"events_{batch_num:06d}.json")
                with open(out, "w", encoding="utf-8") as o:
                    for evt in batch:
                        o.write(json.dumps(evt) + "\n")
                written += len(batch)
                if batch_num % 10 == 0:
                    print(f"   📤 Wrote batch {batch_num} ({written} events)")
                batch = []
                time.sleep(delay_ms / 1000.0)

        if batch:
            batch_num += 1
            out = os.path.join(STREAM_INPUT_DIR,
                               f"events_{batch_num:06d}.json")
            with open(out, "w", encoding="utf-8") as o:
                for evt in batch:
                    o.write(json.dumps(evt) + "\n")
            written += len(batch)

    print(f"\n✅ Simulator complete: {written} events in {batch_num} files\n")
    return written


# ─────────────────────────────────────────────────────────────
# Structured Streaming Consumer
# ─────────────────────────────────────────────────────────────

def run_streaming(timeout_seconds=60):
    """End-to-end streaming consumer with stream-static join + two queries."""
    ensure_dirs()
    for d in [STREAM_OUTPUT_DIR, STREAM_CHECKPOINT_DIR]:
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)

    spark = get_spark_session("Group6-Streaming")
    spark.conf.set("spark.sql.streaming.checkpointLocation",
                   STREAM_CHECKPOINT_DIR)

    print("\n" + "=" * 60)
    print("  🌊 STRUCTURED STREAMING PIPELINE")
    print("=" * 60)
    print(f"   Reading from: {STREAM_INPUT_DIR}")
    print(f"   Timeout:      {timeout_seconds}s")

    # Static side of the join: product catalog from the ingested Parquet
    products_path = f"{_processed_dir()}/products_clean"
    print(f"   Products:     {products_path}")
    products_static = (spark.read.parquet(products_path)
                       .select("product_id", "masterCategory",
                               "subCategory", "articleType"))

    # Streaming source
    raw_stream = (spark.readStream
                  .schema(STREAM_EVENT_SCHEMA)
                  .option("maxFilesPerTrigger", 5)
                  .json(STREAM_INPUT_DIR))

    # Parse single-quoted Python-dict event_metadata → JSON → struct
    parsed = (raw_stream
              .withColumn("meta_json",
                          regexp_replace("event_metadata", "'", '"'))
              .withColumn("meta",
                          from_json(col("meta_json"), EVENT_META_SCHEMA))
              .withColumn("event_ts", to_timestamp("event_time"))
              .withColumn("product_id", col("meta.product_id"))
              .withColumn("quantity",   col("meta.quantity"))
              .withColumn("item_price", col("meta.item_price")))

    # Stream-static JOIN for product enrichment
    enriched = parsed.join(products_static, on="product_id", how="left")

    # Query 1: category trending (5-min tumbling window, 10-min watermark)
    category_trending = (enriched
        .filter(col("masterCategory").isNotNull())
        .withWatermark("event_ts", "10 minutes")
        .groupBy(window(col("event_ts"), "5 minutes"), col("masterCategory"))
        .agg(
            count("*").alias("event_count"),
            approx_count_distinct("session_id").alias("active_sessions"),
        ))

    # Query 2: per-session engagement — inputs for the "about to bounce?" signal
    session_engagement = (enriched
        .withWatermark("event_ts", "10 minutes")
        .groupBy(window(col("event_ts"), "5 minutes"),
                 col("session_id"), col("traffic_source"))
        .agg(
            count("*").alias("event_count"),
            approx_count_distinct("event_name").alias("unique_actions"),
            _max(when(col("event_name") == "ADD_TO_CART", 1).otherwise(0))
                .alias("has_cart"),
            _max(when(col("event_name") == "BOOKING", 1).otherwise(0))
                .alias("has_booking"),
        ))

    q1 = (category_trending.writeStream
          .outputMode("update")
          .format("console")
          .option("truncate", "false")
          .option("numRows", 20)
          .queryName("category_trending")
          .start())

    q2 = (session_engagement.writeStream
          .outputMode("update")
          .format("memory")
          .queryName("session_scores")
          .start())

    print(f"\n   Active queries: {[q.name for q in spark.streams.active]}")
    print(f"   Streaming for {timeout_seconds}s…")

    try:
        q1.awaitTermination(timeout_seconds)
    except Exception as exc:
        print(f"   ⚠ Stream terminated: {exc}")
    finally:
        for q in spark.streams.active:
            q.stop()

    # Post-stream: high-intent + anomalous sessions from the memory sink
    print("\n" + "=" * 60)
    print("  📊 POST-STREAM ANALYSIS")
    print("=" * 60)
    try:
        scores = spark.sql("SELECT * FROM session_scores")
        n_rows = scores.count()
        print(f"   Session-score rows: {n_rows:,}")

        if n_rows > 0:
            high_intent = scores.filter(
                (col("has_cart") == 1) & (col("has_booking") == 0)
            )
            print(f"   High-intent sessions (cart but no booking): "
                  f"{high_intent.count():,}")
            high_intent.select("session_id", "traffic_source",
                               "event_count", "unique_actions") \
                       .show(10, truncate=False)

            stats = scores.agg(
                avg("event_count").alias("mean_events"),
                stddev("event_count").alias("std_events"),
            ).first()
            mean_events = stats["mean_events"] or 0
            std_events  = stats["std_events"]  or 1
            threshold = mean_events + 2 * std_events
            anomalies = scores.filter(col("event_count") > threshold)
            print(f"   Anomaly threshold (μ + 2σ): {threshold:.1f} events")
            print(f"   Anomalous sessions: {anomalies.count():,}")

            csv_path = os.path.join(STREAM_OUTPUT_DIR,
                                    "session_engagement_scores.csv")
            scores.toPandas().to_csv(csv_path, index=False)
            print(f"   Saved: {csv_path}")
    except Exception as exc:
        print(f"   ⚠ Post-stream analysis error: {exc}")

    # Graceful shutdown — let the StateStore maintenance thread finish
    for q in spark.streams.active:
        q.stop()
    time.sleep(2)
    spark.stop()


# ─────────────────────────────────────────────────────────────
# Combined entry point: simulate + consume
# ─────────────────────────────────────────────────────────────

def run_full_streaming_demo(max_events=500, delay_ms=50, timeout_seconds=60):
    print("\n" + "=" * 60)
    print("  🌊 Group 6 STREAMING DEMO")
    print("=" * 60)
    stream_simulator(max_events=max_events, delay_ms=delay_ms)
    try:
        run_streaming(timeout_seconds=timeout_seconds)
    except Exception as exc:
        print(f"   ⚠ Streaming error: {exc}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    run_full_streaming_demo()
