"""
Structured Streaming Module
============================
Simulates real-time click stream event processing using Spark Structured Streaming.

Two modes:
  1. stream_simulator()  – writes click stream CSV rows as individual JSON files
                           to data/stream_input/ with configurable delay
  2. run_streaming()     – reads the JSON stream, computes windowed aggregations
                           (events per window, active sessions, event-type distribution),
                           and writes results to console + data/stream_output/

Spark Component: Structured Streaming
"""

import os
import sys
import json
import time
import shutil
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.utils import (
    get_spark_session, DATA_RAW_DIR, DATA_SAMPLE_DIR, STREAMING_OUTPUT_DIR,
    ensure_dirs, get_data_dir, PROJECT_ROOT,
)

from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
)
from pyspark.sql.functions import (
    col, window, count, approx_count_distinct, current_timestamp,
    from_json, to_timestamp, lit, expr, sum as spark_sum,
    when, desc,
)


# ─────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────
STREAM_INPUT_DIR = os.path.join(PROJECT_ROOT, "data", "stream_input")
STREAM_OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "stream_output")
STREAM_CHECKPOINT_DIR = os.path.join(PROJECT_ROOT, "data", "stream_checkpoint")


# Schema for the JSON files produced by the simulator
STREAM_EVENT_SCHEMA = StructType([
    StructField("session_id", StringType(), True),
    StructField("event_name", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("event_id", StringType(), True),
    StructField("traffic_source", StringType(), True),
    StructField("event_metadata", StringType(), True),
])


# ─────────────────────────────────────────────────────────────
# Stream Simulator — writes JSON files that mimic real-time
# ─────────────────────────────────────────────────────────────

def stream_simulator(max_events=500, delay_ms=100, batch_size=10):
    """
    Read click stream CSV rows and write them as JSON files to
    STREAM_INPUT_DIR, simulating real-time event arrival.

    Args:
        max_events:  Total number of events to emit.
        delay_ms:    Delay between batches (milliseconds).
        batch_size:  Number of events per JSON file.
    """
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

    # Clean previous stream input
    if os.path.exists(STREAM_INPUT_DIR):
        shutil.rmtree(STREAM_INPUT_DIR)
    os.makedirs(STREAM_INPUT_DIR, exist_ok=True)

    print(f"\n🌊 Stream Simulator Starting")
    print(f"   Source:      {click_file}")
    print(f"   Output dir:  {STREAM_INPUT_DIR}")
    print(f"   Max events:  {max_events}")
    print(f"   Batch size:  {batch_size}")
    print(f"   Delay:       {delay_ms}ms\n")

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
                # Write batch as a JSON file (one JSON object per line)
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

        # Write remaining events
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
# Structured Streaming Consumer
# ─────────────────────────────────────────────────────────────

def run_streaming(timeout_seconds=60):
    """
    Run Spark Structured Streaming to consume simulated click stream events.

    Performs:
      - 5-minute sliding window aggregation of event counts
      - Active session counting per window
      - Event type distribution per window
      - Traffic source breakdown per window

    Results are written to console and to STREAM_OUTPUT_DIR as Parquet.

    Args:
        timeout_seconds:  How long to wait before stopping the stream.
    """
    ensure_dirs()

    # Clean previous outputs/checkpoints
    for d in [STREAM_OUTPUT_DIR, STREAM_CHECKPOINT_DIR]:
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)

    spark = get_spark_session("ECommerce-Streaming")

    print("\n" + "=" * 60)
    print("🌊 STARTING STRUCTURED STREAMING PIPELINE")
    print("=" * 60)
    print(f"\n📂 Reading stream from: {STREAM_INPUT_DIR}")
    print(f"📂 Output to:          {STREAM_OUTPUT_DIR}")
    print(f"⏱  Timeout:            {timeout_seconds}s\n")

    # ── Read stream ──
    raw_stream = (spark.readStream
                  .format("json")
                  .schema(STREAM_EVENT_SCHEMA)
                  .option("maxFilesPerTrigger", 5)
                  .load(STREAM_INPUT_DIR))

    # ── Parse event_time as timestamp ──
    events = raw_stream.withColumn(
        "event_ts",
        to_timestamp(col("event_time"))
    )

    # ── Query 1: Windowed event counts (5-min tumbling window) ──
    windowed_counts = (events
                       .withWatermark("event_ts", "10 minutes")
                       .groupBy(
                           window(col("event_ts"), "5 minutes"),
                           col("event_name"),
                       )
                       .agg(
                           count("*").alias("event_count"),
                           approx_count_distinct("session_id").alias("active_sessions"),
                       ))

    # ── Query 2: Traffic source breakdown ──
    traffic_breakdown = (events
                         .withWatermark("event_ts", "10 minutes")
                         .groupBy(
                             window(col("event_ts"), "5 minutes"),
                             col("traffic_source"),
                         )
                         .agg(
                             count("*").alias("event_count"),
                             approx_count_distinct("session_id").alias("unique_sessions"),
                         ))

    # ── Write Query 1 to console ──
    console_query = (windowed_counts.writeStream
                     .outputMode("update")
                     .format("console")
                     .option("truncate", "false")
                     .option("numRows", 30)
                     .queryName("windowed_event_counts")
                     .start())

    # ── Write Query 2 to Parquet sink ──
    parquet_checkpoint = os.path.join(STREAM_CHECKPOINT_DIR, "traffic")
    parquet_output = os.path.join(STREAM_OUTPUT_DIR, "traffic_breakdown")

    file_query = (traffic_breakdown.writeStream
                  .outputMode("append")
                  .format("parquet")
                  .option("path", parquet_output)
                  .option("checkpointLocation", parquet_checkpoint)
                  .queryName("traffic_breakdown_parquet")
                  .start())

    print(f"\n⏳ Streaming for {timeout_seconds} seconds…")
    print("   Active queries:")
    for q in spark.streams.active:
        print(f"     • {q.name} (status: {q.status})")

    # Wait for timeout
    try:
        console_query.awaitTermination(timeout_seconds)
    except Exception as e:
        print(f"   ⚠ Stream terminated: {e}")

    # Stop queries gracefully
    for q in spark.streams.active:
        q.stop()

    # ── Post-stream summary ──
    print("\n" + "=" * 60)
    print("📊 STREAMING SUMMARY")
    print("=" * 60)

    # Read the parquet output and show summary if it exists
    if os.path.exists(parquet_output):
        try:
            summary_df = spark.read.parquet(parquet_output)
            print(f"\n   Traffic breakdown records written: {summary_df.count()}")
            summary_df.show(20, truncate=False)
        except Exception:
            print("   ⚠ No streaming output to summarise (dataset may be too small)")
    else:
        print("   ⚠ No parquet output generated (stream may not have processed data)")

    print("\n✅ STREAMING PIPELINE COMPLETE\n")
    spark.stop()


# ─────────────────────────────────────────────────────────────
# Combined: simulate + consume
# ─────────────────────────────────────────────────────────────

def run_full_streaming_demo(max_events=500, delay_ms=50, timeout_seconds=60):
    """
    End-to-end streaming demo:
      1) Generate simulated events in a background thread
      2) Start Structured Streaming consumer
    """
    print("\n" + "=" * 60)
    print("🌊 FULL STREAMING DEMO (Simulate + Consume)")
    print("=" * 60)

    # Step 1: Generate the events first (synchronously for simplicity)
    stream_simulator(max_events=max_events, delay_ms=delay_ms)

    # Step 2: Run the streaming consumer
    run_streaming(timeout_seconds=timeout_seconds)


if __name__ == "__main__":
    run_full_streaming_demo()
