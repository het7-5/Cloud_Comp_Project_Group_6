"""
Tests for the Spark SQL transformations module.
Validates that advanced queries execute successfully and return expected structure.
Note: Some queries may return 0 rows on sample data due to small data sizes,
but they will execute SQL without syntax errors, which is what we're testing.
"""

import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.utils import get_spark_session, DATA_SAMPLE_DIR


@pytest.fixture(scope="module")
def spark():
    session = get_spark_session("TestSQL")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def registered_views(spark):
    """Load sample data and register temp views for SQL queries."""
    if not os.path.exists(DATA_SAMPLE_DIR):
        os.makedirs(DATA_SAMPLE_DIR, exist_ok=True)
    csv_files = [f for f in os.listdir(DATA_SAMPLE_DIR) if f.endswith(".csv")]
    if not csv_files:
        from src.generate_sample_data import generate_customers, generate_products
        from src.generate_sample_data import generate_transactions, generate_clickstream
        generate_customers()
        generate_products()
        generate_transactions()
        generate_clickstream()

    from src.ingestion import load_customers, load_products, load_transactions, load_clickstream
    from src.transformations import register_views

    customers = load_customers(spark, DATA_SAMPLE_DIR)
    products = load_products(spark, DATA_SAMPLE_DIR)
    transactions = load_transactions(spark, DATA_SAMPLE_DIR)
    clickstream = load_clickstream(spark, DATA_SAMPLE_DIR)

    register_views(spark, customers, products, transactions, clickstream)
    return True


class TestCustomerConversionFunnel:
    def test_query_executes(self, spark, registered_views):
        from src.transformations import query_customer_conversion_funnel
        result = query_customer_conversion_funnel(spark)
        assert result is not None

    def test_has_columns(self, spark, registered_views):
        from src.transformations import query_customer_conversion_funnel
        result = query_customer_conversion_funnel(spark)
        expected = {"customer_id", "total_product_events", "converted"}
        assert expected.issubset(set(result.columns))


class TestMarketBasket:
    def test_query_executes(self, spark, registered_views):
        from src.transformations import query_market_basket
        result = query_market_basket(spark)
        assert result is not None


class TestCohortRetention:
    def test_query_executes(self, spark, registered_views):
        from src.transformations import query_cohort_retention
        result = query_cohort_retention(spark)
        assert result is not None

    def test_has_columns(self, spark, registered_views):
        from src.transformations import query_cohort_retention
        result = query_cohort_retention(spark)
        assert "cohort_month" in result.columns


class TestRFMScoring:
    def test_query_executes(self, spark, registered_views):
        from src.transformations import query_rfm_scoring
        result = query_rfm_scoring(spark)
        assert result is not None

    def test_has_segments(self, spark, registered_views):
        from src.transformations import query_rfm_scoring
        result = query_rfm_scoring(spark)
        expected = {"customer_id", "rfm_segment"}
        assert expected.issubset(set(result.columns))


class TestPurchaseVelocity:
    def test_query_executes(self, spark, registered_views):
        from src.transformations import query_purchase_velocity
        result = query_purchase_velocity(spark)
        assert result is not None


class TestProductAffinity:
    def test_query_executes(self, spark, registered_views):
        from src.transformations import query_product_affinity
        result = query_product_affinity(spark)
        assert result is not None
