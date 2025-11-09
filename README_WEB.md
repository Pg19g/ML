# Quantitative Trading Platform - Web Edition

**AI-powered backtesting platform for multi-exchange quantitative strategies**

## 🌟 Features

- 🌍 **Multi-Exchange Support** - US, LSE, WSE, XETRA, and 20+ exchanges via EODHD
- 📊 **Comprehensive Market Data** - EOD, intraday (1h), and fundamental data
- 🎯 **Strategy Framework** - Built-in strategies with easy customization
- 📈 **Interactive Dashboard** - Streamlit-based UI for all operations
- 🔒 **Point-in-Time Integrity** - Zero look-ahead bias protection
- 🚀 **Auto-Deploy** - Railway integration with GitHub push
- 🤖 **AI Assistant** - Coming in Phase 2 (Gemini integration)

## 🏗️ Architecture

```
┌─────────────────┐
│   Streamlit     │  Interactive Dashboard
│   Dashboard     │  (Port 8501)
└────────┬────────┘
         │ HTTP
┌────────▼────────┐
│   FastAPI       │  REST API
│   Backend       │  (Port 8000)
└────────┬────────┘
         │
    ┌────┴────┐
    │         │
┌───▼──┐  ┌──▼────────┐
│ EODHD│  │PostgreSQL │
│ API  │  │ Database  │
└──────┘  └───────────┘
```

## 📦 Tech Stack

- **Backend**: FastAPI + SQLAlchemy + PostgreSQL
- **Frontend**: Streamlit
- **Backtesting**: [backtesting.py](https://github.com/kernc/backtesting.py)
- **Data**: EODHD API
- **Deployment**: Railway (auto-deploy from GitHub)
- **AI** (Phase 2): Google Gemini API

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- EODHD API key ([get one here](https://eodhistoricaldata.com/))
- PostgreSQL database (or use Railway's managed PostgreSQL)

### 1. Installation

```bash
# Clone repository
git clone <your-repo-url>
cd ML

# Create virtual environment
python3.11 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configuration

Create `.env` file:

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

```env
EODHD_API_KEY=your_eodhd_api_key
DATABASE_URL=postgresql://user:password@localhost:5432/quant_platform
API_URL=http://localhost:8000
DEBUG=True
```

### 3. Initialize Database

```bash
python scripts/init_database.py
```

### 4. Run Locally

**Terminal 1 - Start API:**
```bash
uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
```

**Terminal 2 - Start Dashboard:**
```bash
streamlit run dashboard/app.py
```

**Access:**
- Dashboard: http://localhost:8501
- API Docs: http://localhost:8000/docs
- Health Check: http://localhost:8000/health

## 📖 Usage Guide

### 1. Data Explorer

**Fetch Market Data:**
1. Navigate to **📊 Data Explorer**
2. Select exchange (e.g., US, LSE)
3. Search or manually enter symbols
4. Choose date range and data type
5. Click **🚀 Fetch Data**

**Supported Data Types:**
- **EOD (End of Day)** - Daily OHLCV data
- **Intraday (1h)** - Hourly bars (Phase 1 supports 1h)
- **Fundamentals** - Coming in Phase 2

### 2. Run Backtest

**Launch a Backtest:**
1. Navigate to **⚡ Run Backtest**
2. Select strategy (e.g., Mean Reversion)
3. Configure strategy parameters
4. Enter symbols to test
5. Set date range and settings
6. Click **🚀 Run Backtest**

**Built-in Strategies:**
- **Mean Reversion** - Buy dips, sell at mean
- More strategies coming in Phase 2

### 3. View Results

**Analyze Performance:**
1. Navigate to **📈 Results**
2. Select backtest from dropdown
3. View:
   - Performance metrics (Sharpe, Return, Drawdown)
   - Equity curve chart
   - Trade-by-trade analysis
   - Download CSV export

## 🎯 Built-in Strategies

### Mean Reversion Strategy

**Logic:**
- **Entry**: Price drops X% below MA + RSI oversold
- **Exit**: Price returns to MA or max hold period

**Parameters:**
- `ma_period` - Moving average period (default: 20)
- `entry_pct` - % below MA to enter (default: 5%)
- `rsi_period` - RSI calculation period (default: 14)
- `rsi_oversold` - Oversold threshold (default: 30)
- `max_hold_days` - Maximum position hold (default: 10 days)

## 🔧 API Reference

### Data Endpoints

```bash
# List exchanges
GET /api/data/exchanges

# Get symbols for exchange
GET /api/data/symbols/{exchange}

# Fetch market data (background task)
POST /api/data/fetch
{
  "symbols": ["AAPL", "MSFT"],
  "exchange": "US",
  "start_date": "2023-01-01",
  "end_date": "2024-01-01",
  "timeframe": "1D",
  "data_type": "eod"
}

# Check fetch status
GET /api/data/fetch/status/{task_id}
```

### Strategy Endpoints

```bash
# List available strategies
GET /api/strategy/list

# Get strategy parameters
GET /api/strategy/{strategy_name}/params
```

### Backtest Endpoints

```bash
# Run backtest
POST /api/backtest/run
{
  "symbols": ["AAPL"],
  "exchange": "US",
  "start_date": "2023-01-01",
  "end_date": "2024-01-01",
  "strategy_name": "Mean Reversion",
  "strategy_params": {
    "ma_period": 20,
    "entry_pct": 5.0
  },
  "timeframe": "1D",
  "initial_cash": 10000,
  "commission": 0.001
}

# Get backtest results
GET /api/backtest/{backtest_id}

# Get trade details
GET /api/backtest/{backtest_id}/trades

# List recent backtests
GET /api/backtest/list
```

## 🎨 Creating Custom Strategies

Create a new file in `backend/strategies/`:

```python
from backend.strategies.base import BaseStrategy
from backend.strategies.registry import StrategyRegistry
from backtesting.lib import crossover
from backtesting.test import SMA

@StrategyRegistry.register
class MyStrategy(BaseStrategy):
    """My custom strategy."""

    name = "My Strategy"
    description = "Strategy description here"
    timeframe = "1D"

    # Parameters
    param1 = 10
    param2 = 20

    def init(self):
        """Initialize indicators."""
        self.sma1 = self.I(SMA, self.data.Close, self.param1)
        self.sma2 = self.I(SMA, self.data.Close, self.param2)

    def next(self):
        """Trading logic."""
        if crossover(self.sma1, self.sma2):
            self.buy()
        elif crossover(self.sma2, self.sma1):
            self.position.close()

    @classmethod
    def get_param_definitions(cls):
        return [
            {
                "name": "param1",
                "type": "int",
                "default": 10,
                "min": 5,
                "max": 50,
                "description": "Fast period"
            },
            {
                "name": "param2",
                "type": "int",
                "default": 20,
                "min": 10,
                "max": 100,
                "description": "Slow period"
            }
        ]
```

Strategy will automatically appear in the dashboard!

## 🚂 Railway Deployment

### Initial Setup

1. **Create Railway Account**: [railway.app](https://railway.app)

2. **Create PostgreSQL Database**:
   - Click "New Project" → "Provision PostgreSQL"
   - Copy `DATABASE_URL` from Variables tab

3. **Create API Service**:
   - Click "New" → "GitHub Repo"
   - Select your repository
   - Add environment variables:
     ```
     EODHD_API_KEY=your_key
     DATABASE_URL=${{Postgres.DATABASE_URL}}
     ```
   - Railway will use `railway.toml` for deployment

4. **Create Dashboard Service**:
   - Click "New" → "GitHub Repo" (same repo)
   - Add environment variables:
     ```
     API_URL=https://your-api-service.railway.app
     ```
   - Rename config file:
     ```bash
     mv streamlit_railway.toml railway.toml
     ```

### Auto-Deploy

Every push to `main` branch automatically deploys!

```bash
git add .
git commit -m "Update platform"
git push origin main
```

Railway will:
1. Build Docker container
2. Install dependencies
3. Initialize database
4. Start services
5. Run health checks

**Access your platform:**
- Dashboard: `https://your-dashboard.railway.app`
- API: `https://your-api.railway.app/docs`

## 🔒 Look-Ahead Bias Prevention

### Safeguards Implemented

1. **Data Alignment**
   - backtesting.py processes bars sequentially
   - No future data access (index ≤ current bar)

2. **Point-in-Time Fundamentals** (Phase 2)
   - Effective date tracking
   - Report date validation
   - Conservative lag buffers

3. **Testing**
   - Unit tests for look-ahead detection
   - Walk-forward validation

### Best Practices

```python
# ❌ BAD - Looks ahead
def next(self):
    future_price = self.data.Close[1]  # Don't do this!

# ✅ GOOD - Uses past data
def next(self):
    current_price = self.data.Close[0]
    previous_price = self.data.Close[-1]
```

## 📊 Database Schema

### MarketData
- OHLCV data with timeframe support
- Indexed by symbol, exchange, date
- Automatic de-duplication

### Backtests
- Configuration and results storage
- Status tracking (pending/running/completed/failed)
- JSON storage for full results

### FundamentalData (Phase 2)
- Point-in-time integrity
- Report date tracking
- JSON storage for flexibility

## 🐛 Troubleshooting

### API Won't Start

**Error**: `sqlalchemy.exc.OperationalError`

**Solution**: Check DATABASE_URL format
```bash
# Correct format
postgresql://user:password@host:port/database

# Railway uses postgres:// - auto-converted in code
```

### Dashboard Can't Connect

**Error**: Connection refused

**Solution**:
1. Check API is running: `curl http://localhost:8000/health`
2. Update `API_URL` in `.env`
3. Restart dashboard

### Backtest Fails

**Error**: `No data available`

**Solution**:
1. Fetch data first via Data Explorer
2. Check date range (weekends/holidays = no data)
3. Verify symbol exists on exchange

### Import Errors

**Error**: `ModuleNotFoundError`

**Solution**:
```bash
pip install -r requirements.txt --upgrade
```

## 📈 Roadmap

### Phase 1 (Current)
- ✅ FastAPI backend
- ✅ Streamlit dashboard
- ✅ EOD data support
- ✅ Single asset backtesting
- ✅ Mean Reversion strategy
- ✅ PostgreSQL integration
- ✅ Railway deployment

### Phase 2 (Next)
- [ ] Portfolio backtesting (multi-asset)
- [ ] Intraday strategies (1h, 5m)
- [ ] Fundamental data integration
- [ ] Additional strategies (momentum, breakout, quality)
- [ ] AI Research Assistant (Gemini)
- [ ] Automated backtest analysis

### Phase 3 (Future)
- [ ] Walk-forward optimization
- [ ] Monte Carlo simulation
- [ ] Risk management tools
- [ ] Live trading simulation
- [ ] Portfolio allocation optimizer
- [ ] Custom indicator builder

## 🤝 Contributing

This is currently a personal project. If you'd like to contribute:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## 📝 License

This project is for educational purposes. See LICENSE file for details.

## ⚠️ Disclaimer

**IMPORTANT**: This platform is for educational and research purposes only.

- Not financial advice
- Past performance ≠ future results
- Use at your own risk
- Always validate strategies on out-of-sample data
- Be aware of overfitting and data mining bias

## 📚 Resources

- [backtesting.py Documentation](https://kernc.github.io/backtesting.py/)
- [EODHD API Docs](https://eodhistoricaldata.com/financial-apis/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [Railway Documentation](https://docs.railway.app/)

## 💬 Support

For issues and questions:
- Open a GitHub issue
- Check existing documentation
- Review troubleshooting section

---

**Built with ❤️ for quantitative researchers and algorithmic traders**
