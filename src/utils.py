"""
Utility functions and shared configuration for the E-Commerce Spark Pipeline.
"""

import os
import sys


# ─────────────────────────────────────────────
# Java 11 + Hadoop setup for Windows
# PySpark 3.5.x is incompatible with Java 17.0.12+
# (Subject.getSubject() throws UnsupportedOperationException)
# ─────────────────────────────────────────────
if os.name == "nt":
    if not os.environ.get("JAVA_HOME") or "jdk11" not in os.environ.get("JAVA_HOME"):
        os.environ["JAVA_HOME"] = r"C:\jdk11\jdk-11.0.25+9"
        os.environ["PATH"] = os.path.join(os.environ["JAVA_HOME"], "bin") + os.pathsep + os.environ.get("PATH", "")
    
    if not os.environ.get("HADOOP_HOME"):
        os.environ["HADOOP_HOME"] = r"C:\hadoop"
        os.environ["PATH"] = r"C:\hadoop\bin" + os.pathsep + os.environ["PATH"]

# Clean up any conflicting Java variables
for key in ["JAVA_TOOL_OPTIONS", "_JAVA_OPTIONS", "SPARK_SUBMIT_OPTS"]:
    os.environ.pop(key, None)

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
              SQL_OUTPUT_DIR, ML_OUTPUT_DIR, STREAMING_OUTPUT_DIR]:
        os.makedirs(d, exist_ok=True)


def get_spark_session(app_name="ECommerceSparkPipeline"):
    """
    Create and return a SparkSession configured for local mode.
    """
    spark = (SparkSession.builder
             .appName(app_name)
             .master("local[*]")
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.driver.memory", "6g")
             .config("spark.ui.showConsoleProgress", "false")
             .config("spark.network.timeout", "600s")
             .config("spark.executor.heartbeatInterval", "120s")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark
