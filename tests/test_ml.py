"""
Tests for the ML pipeline module.
Validates feature engineering and model training on sample data.
"""

import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.utils import get_spark_session, DATA_SAMPLE_DIR


@pytest.fixture(scope="module")
def spark():
    session = get_spark_session("TestML")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def ensure_sample_data():
    """Make sure sample data exists."""
    if not os.path.exists(DATA_SAMPLE_DIR):
        os.makedirs(DATA_SAMPLE_DIR, exist_ok=True)
    csv_files = [f for f in os.listdir(DATA_SAMPLE_DIR) if f.endswith(".csv")]
    if not csv_files:
        from src.generate_sample_data import generate_customers, generate_products, generate_transactions, generate_clickstream
        generate_customers()
        generate_products()
        generate_transactions()
        generate_clickstream()
    return DATA_SAMPLE_DIR


class TestClassificationFeatures:
    def test_features_build_successfully(self, spark, ensure_sample_data):
        from src.ml_pipeline import build_classification_features
        df = build_classification_features(spark)
        assert df.count() > 0

    def test_has_required_columns(self, spark, ensure_sample_data):
        from src.ml_pipeline import build_classification_features
        df = build_classification_features(spark)
        required = {"total_amount", "payment_method", "payment_status"}
        assert required.issubset(set(df.columns))

    def test_target_values(self, spark, ensure_sample_data):
        from src.ml_pipeline import build_classification_features
        df = build_classification_features(spark)
        statuses = [r.payment_status for r in df.select("payment_status").distinct().collect()]
        assert set(statuses).issubset({"Success", "Failed"})


class TestClusteringFeatures:
    def test_features_build_successfully(self, spark, ensure_sample_data):
        from src.ml_pipeline import build_clustering_features
        df, feature_cols = build_clustering_features(spark)
        assert df.count() > 0
        assert len(feature_cols) > 0

    def test_has_numeric_features(self, spark, ensure_sample_data):
        from src.ml_pipeline import build_clustering_features
        df, feature_cols = build_clustering_features(spark)
        required = {"total_spend", "num_transactions", "avg_order_value"}
        assert required.issubset(set(feature_cols))
