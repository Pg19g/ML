# Quant Equity Alpha Platform

A production-grade cross-sectional equity alpha platform for building, backtesting, and deploying long/short equity strategies using EODHD end-of-day data.

## ✨ NEW: Zero-Leakage PIT Snapshot Architecture

**Revolutionary approach to fundamentals data** - eliminates information leakage through immutable snapshots with computed effective_date.

- 🎯 **Zero Information Leakage**: Each snapshot has explicit effective_date = earliest date market could know the numbers
- 📸 **Immutable Snapshots**: Each statement period saved separately, never overwritten
- 🔄 **On-Demand Ingestion**: Fetch once from API, work offline forever
- ✅ **PIT Integrity Validation**: Automatic detection of look-ahead bias
- 🌍 **All-Markets Support**: Universal design for US, EU, UK, PL, and any exchange

**→ See [ARCHITECTURE_PIT.md](ARCHITECTURE_PIT.md) for complete design documentation**

## Features

- **Point-in-Time Data Integrity**: NEW snapshot-based system prevents all forms of information leakage
- **Robust Factor Engineering**: Value, Quality, Momentum, and Short-term Reversion factors with sector neutralization
- **ML Model Training**: LightGBM/CatBoost with purged walk-forward cross-validation
- **Portfolio Optimization**: Sector-neutral long/short with cvxpy QP optimizer
- **Realistic Backtesting**: Transaction cost modeling, turnover control, and comprehensive metrics
- **Explainability**: SHAP analysis, feature importance, and diagnostic reports
- **Production-Ready**: CLI tools, comprehensive tests, deterministic execution

## Architecture

```
├── config/              # Configuration files (YAML)
├── data/                # Data storage (PIT store)
├── src/
│   ├── eodhd_client.py  # API client with caching
│   ├── pit_store.py     # Point-in-time data store
│   ├── features/        # Factor engineering
│   ├── models/          # ML training with CV
│   ├── portfolio/       # Portfolio optimization
│   ├── backtest/        # Backtest engine
│   ├── reporting/       # Report generation
│   └── utils/           # Utilities (logging, calendar)
├── scripts/             # CLI entry points
│   ├── ingest.py        # Data ingestion
│   ├── train.py         # Model training
│   ├── backtest.py      # Strategy backtest
│   └── report.py        # Report generation
├── tests/               # Unit tests
└── notebooks/           # Jupyter notebooks
```

## Quick Start

### 1. Installation

```bash
# Create virtual environment
python3.11 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configuration

Create `.env` file with your EODHD API key:

```bash
cp .env.example .env
# Edit .env and add your EODHD_API_KEY
```

### 3. Run End-to-End Pipeline

```bash
# 1. Ingest data
python scripts/ingest.py --config config/defaults.yaml

# 2. Train model
python scripts/train.py --config config/defaults.yaml

# 3. Backtest strategy
python scripts/backtest.py --config config/defaults.yaml

# 4. Generate report
python scripts/report.py
```

## Running Tests

```bash
pytest tests/ -v --cov=src
```

## Documentation

See README_PLATFORM.md for full documentation including:
- Configuration reference
- Factor descriptions
- Performance metrics
- Troubleshooting
- Extension guide

---

**⚠️ Disclaimer**: For educational purposes only. Not financial advice.
