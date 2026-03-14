"""
Data Ingestion Module
=====================
Reads raw CSV files (Customer, Product, Transaction, Click Stream),
applies explicit schemas, parses JSON metadata columns, and writes
cleaned DataFrames to Parquet for downstream analysis.

Spark Component: Structured APIs (DataFrame API)
"""

import os
import sys
import glob
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    TimestampType, DateType
)
from pyspark.sql.functions import (
    col, from_json, explode, to_timestamp, to_date, trim,
    when, regexp_replace, lit
)

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.utils import get_spark_session, DATA_SAMPLE_DIR, DATA_PROCESSED_DIR, ensure_dirs, get_data_dir


# ─────────────────────────────────────────────────────────────
# Schema Definitions
# ─────────────────────────────────────────────────────────────

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("username", StringType(), True),
    StructField("email", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("birthdate", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("device_version", StringType(), True),
    StructField("home_location_lat", DoubleType(), True),
    StructField("home_location_long", DoubleType(), True),
    StructField("home_location", StringType(), True),
    StructField("home_country", StringType(), True),
    StructField("first_join_date", StringType(), True),
])

PRODUCT_SCHEMA = StructType([
    StructField("id", IntegerType(), False),
    StructField("gender", StringType(), True),
    StructField("masterCategory", StringType(), True),
    StructField("subCategory", StringType(), True),
    StructField("articleType", StringType(), True),
    StructField("baseColour", StringType(), True),
    StructField("season", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("usage", StringType(), True),
    StructField("productDisplayName", StringType(), True),
])

TRANSACTION_SCHEMA = StructType([
    StructField("created_at", StringType(), True),
    StructField("customer_id", StringType(), False),
    StructField("booking_id", StringType(), False),
    StructField("session_id", StringType(), True),
    StructField("product_metadata", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("promo_amount", DoubleType(), True),
    StructField("promo_code", StringType(), True),
    StructField("shipment_fee", DoubleType(), True),
    StructField("shipment_date_limit", StringType(), True),
    StructField("shipment_location_lat", DoubleType(), True),
    StructField("shipment_location_long", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
])

CLICKSTREAM_SCHEMA = StructType([
    StructField("session_id", StringType(), False),
    StructField("event_name", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("event_id", StringType(), False),
    StructField("traffic_source", StringType(), True),
    StructField("event_metadata", StringType(), True),
])


def find_csv(data_dir, keywords):
    """
    Find a CSV file in data_dir matching any of the given keywords.
    Returns the filepath or None.
    """
    csv_files = [f for f in os.listdir(data_dir) if f.endswith(".csv")]
    for keyword in keywords:
        for f in csv_files:
            if keyword.lower() in f.lower():
                return os.path.join(data_dir, f)
    return None


def load_customers(spark, data_dir=None):
    """Load and clean customer data."""
    if data_dir is None:
        data_dir = get_data_dir()

    filepath = find_csv(data_dir, ["customer"])
    if filepath is None:
        raise FileNotFoundError(f"No customer CSV found in {data_dir}")
    print(f"📥 Loading customers from: {filepath}")

    # Try with explicit schema first, fall back to inferSchema
    try:
        df = (spark.read
              .option("header", "true")
              .schema(CUSTOMER_SCHEMA)
              .csv(filepath))
    except Exception:
        print("   ⚠ Schema mismatch, using inferred schema")
        df = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(filepath))

    # Convert first_join_date to proper date type if column exists
    if "first_join_date" in df.columns:
        df = df.withColumn("first_join_date", to_date(col("first_join_date")))

    # Convert birthdate to date type if it exists
    if "birthdate" in df.columns:
        df = df.withColumn("birthdate", to_date(col("birthdate")))

    # Trim string columns that exist
    for c in ["first_name", "last_name", "username", "email", "gender",
              "device_type", "home_location", "home_country"]:
        if c in df.columns:
            df = df.withColumn(c, trim(col(c)))

    row_count = df.count()
    print(f"   → {row_count:,} customers loaded ({len(df.columns)} columns)")
    return df


def load_products(spark, data_dir=None):
    """Load and clean product data."""
    if data_dir is None:
        data_dir = get_data_dir()

    filepath = find_csv(data_dir, ["product"])
    if filepath is None:
        raise FileNotFoundError(f"No product CSV found in {data_dir}")
    print(f"📥 Loading products from: {filepath}")

    try:
        df = (spark.read
              .option("header", "true")
              .schema(PRODUCT_SCHEMA)
              .csv(filepath))
    except Exception:
        print("   ⚠ Schema mismatch, using inferred schema")
        df = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(filepath))

    # Rename 'id' to 'product_id' for consistency if column exists
    if "id" in df.columns:
        df = df.withColumnRenamed("id", "product_id")

    # Trim string columns that exist
    for c in ["gender", "masterCategory", "subCategory", "articleType",
              "baseColour", "season", "usage", "productDisplayName"]:
        if c in df.columns:
            df = df.withColumn(c, trim(col(c)))

    row_count = df.count()
    print(f"   → {row_count:,} products loaded ({len(df.columns)} columns)")
    return df


def load_transactions(spark, data_dir=None):
    """Load transactions and parse the product_metadata JSON column."""
    if data_dir is None:
        data_dir = get_data_dir()

    filepath = find_csv(data_dir, ["transaction", "transactions"])
    if filepath is None:
        raise FileNotFoundError(f"No transaction CSV found in {data_dir}")
    print(f"📥 Loading transactions from: {filepath}")

    try:
        df = (spark.read
              .option("header", "true")
              .option("escape", "\"")
              .option("quote", "\"")
              .schema(TRANSACTION_SCHEMA)
              .csv(filepath))
    except Exception:
        print("   ⚠ Schema mismatch, using inferred schema")
        df = (spark.read
              .option("header", "true")
              .option("escape", "\"")
              .option("quote", "\"")
              .option("inferSchema", "true")
              .csv(filepath))

    # Convert timestamp columns if they exist
    if "created_at" in df.columns:
        df = df.withColumn("created_at", to_timestamp(col("created_at")))
    if "shipment_date_limit" in df.columns:
        df = df.withColumn("shipment_date_limit", to_date(col("shipment_date_limit")))

    # Parse product_metadata JSON if column exists
    if "product_metadata" in df.columns:
        df = df.withColumn(
            "parsed_metadata",
            from_json(col("product_metadata"),
                      "struct<items:array<struct<product_id:int,quantity:int,price:double>>>")
        )

    # Fill nulls for promo_amount
    if "promo_amount" in df.columns:
        df = df.withColumn("promo_amount",
                           when(col("promo_amount").isNull(), lit(0.0)).otherwise(col("promo_amount")))

    row_count = df.count()
    print(f"   → {row_count:,} transactions loaded ({len(df.columns)} columns)")
    return df


def load_clickstream(spark, data_dir=None):
    """Load click stream events and parse event_metadata JSON."""
    if data_dir is None:
        data_dir = get_data_dir()

    filepath = find_csv(data_dir, ["click", "clickstream", "click_stream"])
    if filepath is None:
        raise FileNotFoundError(f"No click stream CSV found in {data_dir}")
    print(f"📥 Loading click stream from: {filepath}")

    try:
        df = (spark.read
              .option("header", "true")
              .option("escape", "\"")
              .option("quote", "\"")
              .schema(CLICKSTREAM_SCHEMA)
              .csv(filepath))
    except Exception:
        print("   ⚠ Schema mismatch, using inferred schema")
        df = (spark.read
              .option("header", "true")
              .option("escape", "\"")
              .option("quote", "\"")
              .option("inferSchema", "true")
              .csv(filepath))

    # Convert event_time to timestamp if column exists
    if "event_time" in df.columns:
        df = df.withColumn("event_time", to_timestamp(col("event_time")))

    # Parse event_metadata JSON if column exists
    if "event_metadata" in df.columns:
        event_meta_schema = "struct<page:string,duration_sec:int,product_id:int,query:string,promo_code:string>"
        df = df.withColumn("parsed_event_meta", from_json(col("event_metadata"), event_meta_schema))

    row_count = df.count()
    print(f"   → {row_count:,} click stream events loaded ({len(df.columns)} columns)")
    return df


def save_to_parquet(df, name, output_dir=DATA_PROCESSED_DIR):
    """Save DataFrame to Parquet with overwrite mode."""
    path = os.path.join(output_dir, name)
    df.write.mode("overwrite").parquet(path)
    print(f"💾 Saved {name} → {path}")


def run_ingestion():
    """Execute the full data ingestion pipeline."""
    ensure_dirs()
    spark = get_spark_session("ECommerce-Ingestion")

    print("\n" + "=" * 60)
    print("🚀 STARTING DATA INGESTION PIPELINE")
    print("=" * 60 + "\n")

    # Auto-detect data directory
    data_dir = get_data_dir()

    # Show available CSV files
    csv_files = [f for f in os.listdir(data_dir) if f.endswith(".csv")]
    print(f"\n📁 Found {len(csv_files)} CSV files: {csv_files}\n")

    # Load all tables
    customers_df = load_customers(spark, data_dir)
    products_df = load_products(spark, data_dir)
    transactions_df = load_transactions(spark, data_dir)
    clickstream_df = load_clickstream(spark, data_dir)

    # Show schemas
    print("\n📋 SCHEMAS:")
    print("\n--- Customer ---")
    customers_df.printSchema()
    print("\n--- Product ---")
    products_df.printSchema()
    print("\n--- Transaction ---")
    transactions_df.printSchema()
    print("\n--- Click Stream ---")
    clickstream_df.printSchema()

    # Show sample rows
    print("\n📄 SAMPLE DATA:")
    print("\n--- Customer (5 rows) ---")
    customers_df.show(5, truncate=30)
    print("\n--- Product (5 rows) ---")
    products_df.show(5, truncate=30)
    print("\n--- Transaction (5 rows) ---")
    transactions_df.show(5, truncate=30)
    print("\n--- Click Stream (5 rows) ---")
    clickstream_df.show(5, truncate=30)

    # Save to Parquet
    print("\n💾 SAVING TO PARQUET...")
    save_to_parquet(customers_df, "customers")
    save_to_parquet(products_df, "products")
    save_to_parquet(transactions_df, "transactions")
    save_to_parquet(clickstream_df, "clickstream")

    print("\n" + "=" * 60)
    print("✅ DATA INGESTION COMPLETE")
    print("=" * 60 + "\n")

    spark.stop()


if __name__ == "__main__":
    run_ingestion()
