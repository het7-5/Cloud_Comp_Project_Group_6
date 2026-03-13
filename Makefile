run:
	bash run.sh

install:
	pip install -r requirements.txt

test:
	python -m pytest tests/ -v

ingestion:
	python src/ingestion.py

eda:
	python src/eda.py

sql:
	python src/transformations.py

streaming:
	python src/streaming.py

ml:
	python src/ml_pipeline.py

sample:
	python src/generate_sample_data.py

clean:
	rm -rf data/processed/* data/stream_input/* data/stream_output/* data/stream_checkpoint/*
	rm -rf outputs/eda/* outputs/ml/* outputs/sql_results/* outputs/streaming/*
