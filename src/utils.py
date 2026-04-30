"""
Utility functions and shared configuration for the E-Commerce Spark Pipeline.
"""

import os
import sys


# ─────────────────────────────────────────────
# Java 11 setup
# PySpark 3.5.x is incompatible with Java 17/21 — pin to Java 11.
# ─────────────────────────────────────────────
if os.name == "nt":
    if not os.environ.get("JAVA_HOME") or "jdk11" not in os.environ.get("JAVA_HOME"):
        os.environ["JAVA_HOME"] = r"C:\jdk11\jdk-11.0.25+9"
        os.environ["PATH"] = os.path.join(os.environ["JAVA_HOME"], "bin") + os.pathsep + os.environ.get("PATH", "")
    if not os.environ.get("HADOOP_HOME"):
        os.environ["HADOOP_HOME"] = r"C:\hadoop"
        os.environ["PATH"] = r"C:\hadoop\bin" + os.pathsep + os.environ["PATH"]
else:
    # Linux / codespace — point at the SDKMAN-installed Java 11 if current
    # JAVA_HOME isn't already on Java 11.
    _java_home = os.environ.get("JAVA_HOME", "")
    if "11" not in _java_home:
        for cand in (
            "/usr/local/sdkman/candidates/java/11.0.30-ms",
            "/usr/lib/jvm/java-11-openjdk-amd64",
        ):
            if os.path.isdir(cand):
                os.environ["JAVA_HOME"] = cand
                # prepend Java 11 bin, drop any other sdkman/jvm paths
                _path_parts = [p for p in os.environ.get("PATH", "").split(":")
                               if "sdkman" not in p and "jvm" not in p]
                os.environ["PATH"] = f"{cand}/bin:" + ":".join(_path_parts)
                break

# Clean up conflicting Java variables that confuse Spark's gateway launch
for key in ["JAVA_TOOL_OPTIONS", "_JAVA_OPTIONS", "SPARK_SUBMIT_OPTS"]:
    os.environ.pop(key, None)

# Pull hadoop-aws + aws-java-sdk-bundle via --packages so spark.read.parquet
# can hit s3a:// paths. Mirrors the notebook setup. Set BEFORE the JVM starts.
_existing_args = os.environ.get("PYSPARK_SUBMIT_ARGS", "")
if "hadoop-aws" not in _existing_args:
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        "--packages org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262 "
        + ("pyspark-shell" if "pyspark-shell" not in _existing_args else _existing_args)
    )

from pyspark.sql import SparkSession


# ─────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
DATA_SAMPLE_DIR = os.path.join(PROJECT_ROOT, "data", "sample")
DATA_PROCESSED_DIR = os.path.join(PROJECT_ROOT, "data", "processed")
OUTPUTS_DIR = os.path.join(PROJECT_ROOT, "outputs")
EDA_OUTPUT_DIR = os.path.join(OUTPUTS_DIR, "eda")
SQL_OUTPUT_DIR = os.path.join(OUTPUTS_DIR, "sql_results")
ML_OUTPUT_DIR = os.path.join(OUTPUTS_DIR, "ml")
STREAMING_OUTPUT_DIR = os.path.join(OUTPUTS_DIR, "streaming")
MODEL_DIR = os.path.join(PROJECT_ROOT, "data", "models")


def get_data_dir():
    """
    Return the data directory to use.
    Prefers data/raw/ (real Kaggle dataset) if it exists and has CSV files,
    otherwise falls back to data/sample/ (generated data).
    """
    if os.path.isdir(DATA_RAW_DIR):
        csv_files = [f for f in os.listdir(DATA_RAW_DIR) if f.endswith(".csv")]
        if csv_files:
            print(f"📂 Using REAL dataset from: {DATA_RAW_DIR}")
            return DATA_RAW_DIR
    print(f"📂 Using SAMPLE dataset from: {DATA_SAMPLE_DIR}")
    return DATA_SAMPLE_DIR


def ensure_dirs():
    """Create all output directories if they don't exist."""
    for d in [DATA_PROCESSED_DIR, OUTPUTS_DIR, EDA_OUTPUT_DIR,
              SQL_OUTPUT_DIR, ML_OUTPUT_DIR, STREAMING_OUTPUT_DIR, MODEL_DIR]:
        os.makedirs(d, exist_ok=True)


def get_spark_session(app_name="ECommerceSparkPipeline"):
    """
    Create and return a SparkSession configured for local mode.
    Wires the s3a:// filesystem + AWS credentials when env vars are set.
    """
    builder = (SparkSession.builder
               .appName(app_name)
               .master("local[*]")
               .config("spark.sql.shuffle.partitions", "8")
               .config("spark.driver.memory", "3g")
               .config("spark.ui.showConsoleProgress", "false")
               .config("spark.network.timeout", "600s")
               .config("spark.executor.heartbeatInterval", "120s")
               .config("spark.hadoop.fs.s3a.impl",
                       "org.apache.hadoop.fs.s3a.S3AFileSystem"))
    if os.environ.get("AWS_ACCESS_KEY_ID"):
        builder = (builder
                   .config("spark.hadoop.fs.s3a.access.key",
                           os.environ["AWS_ACCESS_KEY_ID"])
                   .config("spark.hadoop.fs.s3a.secret.key",
                           os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
                   .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                           "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
