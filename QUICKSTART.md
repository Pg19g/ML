# Quick Start Guide

## 30-Second Setup

```bash
# 1. Install dependencies
make install

# 2. Set API key
cp .env.example .env
# Edit .env and add your EODHD_API_KEY

# 3. Run full pipeline
make all
```

That's it! Your report will be at `reports/latest/backtest_report.html`

## Individual Steps

```bash
# Run tests
make test

# Just ingest data
make ingest

# Just train model
make train

# Just backtest
make backtest

# Just generate report
make report

# Clean everything
make clean
```

## Custom Configuration

Create your own config file:

```bash
cp config/defaults.yaml config/my_strategy.yaml
# Edit config/my_strategy.yaml
python scripts/ingest.py --config config/my_strategy.yaml
python scripts/train.py --config config/my_strategy.yaml
python scripts/backtest.py --config config/my_strategy.yaml
```

## Test on Small Universe

```bash
# Test with just a few tickers
python scripts/ingest.py --tickers AAPL MSFT GOOGL AMZN --start-date 2022-01-01 --end-date 2023-12-31
python scripts/train.py --config config/defaults.yaml
python scripts/backtest.py --config config/defaults.yaml
python scripts/report.py
```

## Jupyter Notebook

```bash
jupyter notebook notebooks/quickstart.ipynb
```

## Troubleshooting

### "No module named 'src'"
```bash
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
```

### "EODHD API key not found"
```bash
# Make sure .env file exists with:
EODHD_API_KEY=your_key_here
```

### Tests failing
```bash
# Make sure data/test directory is writable
mkdir -p data/test
chmod 755 data/test
```

## What Gets Created

After running the pipeline:

```
data/
  ├── cache/              # Cached API responses
  ├── prices_daily.parquet
  └── fundamentals_pit.parquet

models/
  ├── model.pkl
  ├── cv_results.csv
  └── feature_importance.csv

results/backtest/
  ├── summary.csv
  ├── daily_stats.csv
  └── backtest_result.pkl

reports/latest/
  ├── backtest_report.html
  ├── summary.md
  └── *.png (charts)
```

## Next Steps

1. Review `reports/latest/backtest_report.html`
2. Check `models/cv_results.csv` for IC stability
3. Adjust `config/defaults.yaml` based on results
4. Read full documentation in `README_PLATFORM.md`

## Performance Targets

**Good Strategy Characteristics:**
- Mean Test IC > 0.02
- IC stable across folds (low std)
- Sharpe Ratio > 0.7 (before costs)
- Max Drawdown < 20%
- Weekly Turnover < 120%

If not achieving these, consider:
- Adjusting feature weights in config
- Tuning model hyperparameters
- Expanding universe size
- Lengthening training window
