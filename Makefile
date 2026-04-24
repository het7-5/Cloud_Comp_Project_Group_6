.PHONY: run install test ingestion streaming ml sample dashboard clean

run:
	bash run.sh

install:
	pip install -r requirements.txt

test:
	python -m pytest tests/ -v

# ── Spark jobs (read from $S3_BUCKET_PATH/processed) ──────────
ingestion:
	python src/ingestion.py

streaming:
	python src/streaming.py

ml:
	python src/ml_pipeline.py

sample:
	python src/generate_sample_data.py

# ── Dashboard (local HTTP server + live feed) ─────────────────
dashboard:
	@echo "Starting live_feed + HTTP server on 8765…"
	@cd dashboard && (python live_feed.py &) && python -m http.server 8765 --bind 0.0.0.0

# ── Cleanup artefacts (Spark checkpoints, stream input, outputs) ─
clean:
	rm -rf data/processed/* data/stream_input/* data/stream_output/* data/stream_checkpoint/*
	rm -rf outputs/eda/* outputs/ml/* outputs/sql_results/* outputs/streaming/*
