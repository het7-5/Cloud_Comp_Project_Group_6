#!/bin/bash
set -e

echo "=============================================="
echo "Group 6 · Spark Analytics Pipeline"
echo "=============================================="

# Step 0: Generate sample data if no raw data is present (fallback for tests)
if [ ! -f "data/sample/customer.csv" ] && [ ! -d "data/raw" ] && [ -z "$S3_BUCKET_PATH" ]; then
    echo ""
    echo "📦 Step 0: Generating sample data..."
    python src/generate_sample_data.py
fi

# Step 1: Data Ingestion (raw CSV → 8 Parquet tables)
echo ""
echo "📥 Step 1: Running Data Ingestion..."
python src/ingestion.py

# Step 2: ML Pipeline (RandomForest · KMeans · ALS)
echo ""
echo "🤖 Step 2: Running ML Pipeline..."
python src/ml_pipeline.py

# Step 3: Structured Streaming demo (simulator + consumer)
echo ""
echo "🌊 Step 3: Running Streaming Demo..."
python src/streaming.py

echo ""
echo "=============================================="
echo "✅ Pipeline Complete!"
echo ""
echo "Spark SQL queries live in notebooks/sql_queries.ipynb"
echo "EDA analysis lives in  notebooks/eda.ipynb"
echo "Start the dashboard with:  make dashboard"
echo "=============================================="
