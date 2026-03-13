#!/bin/bash
set -e

echo "=============================================="
echo "E-Commerce Spark Analytics Pipeline"
echo "=============================================="

# Step 0: Generate sample data if not present
if [ ! -f "data/sample/customer.csv" ]; then
    echo ""
    echo "📦 Step 0: Generating sample data..."
    python src/generate_sample_data.py
fi

# Step 1: Data Ingestion
echo ""
echo "📥 Step 1: Running Data Ingestion..."
python src/ingestion.py

# Step 2: Exploratory Data Analysis
echo ""
echo "🔬 Step 2: Running EDA..."
python src/eda.py

# Step 3: Spark SQL Queries
echo ""
echo "🔍 Step 3: Running Spark SQL Transformations..."
python src/transformations.py

# Step 4: Streaming
echo ""
echo "🌊 Step 4: Running Streaming Demo..."
python src/streaming.py

# Step 5: ML Pipeline
echo ""
echo "🤖 Step 5: Running ML Pipeline..."
python src/ml_pipeline.py

echo ""
echo "=============================================="
echo "✅ Pipeline Complete!"
echo "=============================================="
