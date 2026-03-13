"""
Tests for the data ingestion module.
Uses sample data to validate schema, column presence, and data types.
"""

import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.utils import get_spark_session, DATA_SAMPLE_DIR


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for the entire test module."""
    session = get_spark_session("TestIngestion")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def data_dir():
    """Return the sample data directory, generating data if needed."""
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
    return DATA_SAMPLE_DIR


class TestLoadCustomers:
    def test_loads_successfully(self, spark, data_dir):
        from src.ingestion import load_customers
        df = load_customers(spark, data_dir)
        assert df.count() > 0

    def test_has_required_columns(self, spark, data_dir):
        from src.ingestion import load_customers
        df = load_customers(spark, data_dir)
        required = {"customer_id", "first_name", "last_name"}
        assert required.issubset(set(df.columns))


class TestLoadProducts:
    def test_loads_successfully(self, spark, data_dir):
        from src.ingestion import load_products
        df = load_products(spark, data_dir)
        assert df.count() > 0

    def test_has_required_columns(self, spark, data_dir):
        from src.ingestion import load_products
        df = load_products(spark, data_dir)
        required = {"product_id", "masterCategory", "subCategory", "baseColour", "season"}
        assert required.issubset(set(df.columns))


class TestLoadTransactions:
    def test_loads_successfully(self, spark, data_dir):
        from src.ingestion import load_transactions
        df = load_transactions(spark, data_dir)
        assert df.count() > 0

    def test_has_required_columns(self, spark, data_dir):
        from src.ingestion import load_transactions
        df = load_transactions(spark, data_dir)
        required = {"customer_id", "booking_id", "total_amount"}
        assert required.issubset(set(df.columns))

    def test_payment_status_column_exists(self, spark, data_dir):
        from src.ingestion import load_transactions
        df = load_transactions(spark, data_dir)
        assert "payment_status" in df.columns


class TestLoadClickstream:
    def test_loads_successfully(self, spark, data_dir):
        from src.ingestion import load_clickstream
        df = load_clickstream(spark, data_dir)
        assert df.count() > 0

    def test_has_required_columns(self, spark, data_dir):
        from src.ingestion import load_clickstream
        df = load_clickstream(spark, data_dir)
        required = {"session_id", "event_name", "event_time", "traffic_source"}
        assert required.issubset(set(df.columns))
