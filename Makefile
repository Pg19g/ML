.PHONY: help install test clean ingest train backtest report all

help:
	@echo "Quant Equity Alpha Platform - Available Commands:"
	@echo ""
	@echo "  make install     - Install dependencies"
	@echo "  make test        - Run unit tests"
	@echo "  make clean       - Clean generated files"
	@echo "  make ingest      - Ingest data from EODHD"
	@echo "  make train       - Train ML model"
	@echo "  make backtest    - Run strategy backtest"
	@echo "  make report      - Generate HTML report"
	@echo "  make all         - Run full pipeline (ingest -> train -> backtest -> report)"
	@echo ""

install:
	pip install -r requirements.txt
	@echo "✓ Dependencies installed"

test:
	pytest tests/ -v --cov=src --cov-report=html
	@echo "✓ Tests complete. View coverage: open htmlcov/index.html"

clean:
	rm -rf data/cache/*
	rm -rf data/*.parquet
	rm -rf models/*
	rm -rf results/*
	rm -rf reports/*
	rm -rf __pycache__ src/__pycache__ tests/__pycache__
	rm -rf .pytest_cache
	rm -rf htmlcov
	@echo "✓ Cleaned generated files"

ingest:
	python scripts/ingest.py --config config/defaults.yaml
	@echo "✓ Data ingestion complete"

train:
	python scripts/train.py --config config/defaults.yaml --output models/model.pkl
	@echo "✓ Model training complete"

backtest:
	python scripts/backtest.py --config config/defaults.yaml --model models/model.pkl --output results/backtest
	@echo "✓ Backtest complete"

report:
	python scripts/report.py --backtest results/backtest/backtest_result.pkl --output reports/latest
	@echo "✓ Report generated: reports/latest/backtest_report.html"

all: ingest train backtest report
	@echo ""
	@echo "============================================"
	@echo "FULL PIPELINE COMPLETE"
	@echo "============================================"
	@echo "View report: open reports/latest/backtest_report.html"
