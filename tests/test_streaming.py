"""
Tests for the streaming module.
Validates that the stream simulator and schema definitions work correctly.
"""

import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.utils import DATA_SAMPLE_DIR


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


class TestStreamSimulator:
    def test_simulator_writes_files(self, ensure_sample_data):
        from src.streaming import stream_simulator, STREAM_INPUT_DIR
        num_events = stream_simulator(max_events=50, delay_ms=10, batch_size=10)
        assert num_events > 0
        assert os.path.exists(STREAM_INPUT_DIR)

        json_files = [f for f in os.listdir(STREAM_INPUT_DIR) if f.endswith(".json")]
        assert len(json_files) > 0

    def test_simulator_json_format(self, ensure_sample_data):
        import json
        from src.streaming import stream_simulator, STREAM_INPUT_DIR

        stream_simulator(max_events=20, delay_ms=10, batch_size=10)

        json_files = [f for f in os.listdir(STREAM_INPUT_DIR) if f.endswith(".json")]
        # Read first file and verify JSON structure
        with open(os.path.join(STREAM_INPUT_DIR, json_files[0]), "r") as f:
            for line in f:
                event = json.loads(line.strip())
                assert "session_id" in event
                assert "event_name" in event
                assert "event_time" in event


class TestStreamSchema:
    def test_schema_has_required_fields(self):
        from src.streaming import STREAM_EVENT_SCHEMA
        field_names = [f.name for f in STREAM_EVENT_SCHEMA.fields]
        required = {"session_id", "event_name", "event_time", "event_id", "traffic_source"}
        assert required.issubset(set(field_names))
