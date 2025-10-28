# Troubleshooting Guide

## Common Issues and Solutions

### Fundamentals Fetching Issues

#### Error: "Cannot write struct type 'Listings' with no child field to Parquet"

**Status:** ✅ FIXED (as of commit d8694bc)

**Symptoms:**
```
ERROR | Failed to fetch fundamentals for TICKER: Cannot write struct type
'Listings' with no child field to Parquet.
```

**Cause:**
EODHD fundamentals API returns complex nested JSON with empty objects (e.g., empty 'Listings' structures). Parquet format requires all struct types to have at least one child field and cannot serialize empty nested objects.

**Solution:**
The platform now uses JSON format for fundamentals caching instead of Parquet:
- **Prices**: Still use Parquet (flat tabular data)
- **Fundamentals**: Now use JSON (complex nested structures)

**Clean Up Old Cache (if you have old .parquet fundamentals files):**
```bash
python scripts/clean_cache.py
```

Or manually:
```bash
rm -f data/cache/fundamentals_*.parquet
```

---

### Data Ingestion Issues

#### No Data Returned from EODHD

**Check:**
1. Verify API key is set:
   ```bash
   cat .env | grep EODHD_API_KEY
   ```

2. Test API key directly:
   ```bash
   curl "https://eodhistoricaldata.com/api/eod/AAPL.US?api_token=YOUR_KEY&fmt=json" | head
   ```

3. Check rate limits - free tier has limits

**Solution:**
- Ensure `.env` file exists with valid `EODHD_API_KEY`
- Check EODHD account status and rate limits
- Use `--skip-fundamentals` flag to test prices first

---

#### Ticker Not Found

**Symptoms:**
```
WARNING | No data returned for TICKER.EXCHANGE
```

**Causes:**
- Incorrect exchange code (use "US" not "NYSE")
- Ticker delisted or doesn't exist
- Different ticker format on that exchange

**Solution:**
```bash
# List available tickers for an exchange
python scripts/ingest.py --config config/defaults.yaml --tickers TEST

# Check EODHD exchange codes documentation
# US exchanges: use .US (covers NYSE, NASDAQ, AMEX)
# Warsaw: use .WAR
# London: use .LSE
```

---

### Training Issues

#### Insufficient Training Data

**Symptoms:**
```
ERROR | Empty training data
ValueError: Insufficient data for CV
```

**Causes:**
- Not enough historical data
- Too many missing fundamentals
- Date range too short
- Universe too small after filters

**Solution:**
```yaml
# In config, adjust:
cv:
  train_period_days: 180  # Reduce from 252
  min_train_size: 200     # Reduce from 500

universe:
  min_price: 1.0          # Lower price filter
  min_median_dollar_vol: 500000  # Lower volume filter
```

---

#### Model Training Crashes

**Memory Issues:**
```bash
# Reduce data size
universe:
  max_tickers: 100  # Instead of 500

cv:
  train_period_days: 180  # Instead of 252
```

**LightGBM/CatBoost not installed:**
```bash
pip install lightgbm catboost
```

---

### Optimization Issues

#### CVXPY Optimization Fails

**Symptoms:**
```
WARNING | Optimization status: infeasible
```

**Causes:**
- Constraints too tight
- Too few tickers in universe
- Conflicting constraints

**Solution:**
```yaml
portfolio:
  sector_max_weight: 0.20    # Increase from 0.15
  single_name_max_weight: 0.08  # Increase from 0.05
  beta_tolerance: 0.10       # Increase from 0.05
```

Or use fallback:
```yaml
optimizer:
  method: "fallback"  # Uses simple ranking instead of QP
```

---

### Testing Issues

#### Tests Fail: "data/test not writable"

**Solution:**
```bash
mkdir -p data/test
chmod 755 data/test
```

#### Import Errors in Tests

**Solution:**
```bash
# Run from project root
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
pytest tests/ -v
```

---

## Performance Optimization

### Slow Data Ingestion

**Solutions:**
1. Enable caching (default)
2. Fetch specific tickers instead of full exchange:
   ```bash
   python scripts/ingest.py --tickers AAPL MSFT GOOGL
   ```
3. Use `--skip-fundamentals` for quick price-only tests

### Slow Backtesting

**Solutions:**
1. Reduce rebalance frequency:
   ```yaml
   rebalance_freq: "monthly"  # Instead of weekly
   ```
2. Limit universe size
3. Disable PCA risk model:
   ```yaml
   optimizer:
     risk_model:
       use_pca: false
   ```

---

## API Rate Limits

EODHD free tier has rate limits. If hitting limits:

1. **Use caching** (enabled by default)
2. **Batch requests** with delays
3. **Upgrade API tier** for higher limits
4. **Clear cache only when necessary**

---

## Getting Help

1. **Check logs:**
   ```bash
   tail -f logs/quant_platform.log
   ```

2. **Run with debug logging:**
   ```yaml
   logging:
     level: "DEBUG"
   ```

3. **Minimal reproducible example:**
   ```bash
   # Test with just 3 tickers over 1 year
   python scripts/ingest.py --tickers AAPL MSFT GOOGL --start-date 2022-01-01 --end-date 2023-01-01
   python scripts/train.py
   python scripts/backtest.py
   ```

4. **Check environment:**
   ```bash
   python --version  # Should be 3.11+
   pip list | grep -E "(pandas|numpy|lightgbm|cvxpy)"
   ```

---

## Known Limitations

1. **Historical Data**: EODHD may have limited history for some tickers
2. **Fundamentals Coverage**: Not all tickers have complete fundamentals
3. **Exchange Support**: Some exchanges have better data quality than others
4. **Point-in-Time**: We enforce conservative T+2 lag; real filing dates vary

---

## Best Practices

1. **Start Small**: Test with 10-20 tickers first
2. **Check Data Quality**: Inspect cached data before training
3. **Validate PIT**: Run `pit_store.validate_pit_integrity()`
4. **Monitor Costs**: Watch turnover and transaction costs in reports
5. **Version Control**: Keep config files in git, data files in .gitignore

---

## Support Resources

- **EODHD API Docs**: https://eodhistoricaldata.com/financial-apis/
- **Project Issues**: Check GitHub issues
- **Platform Docs**: See README_PLATFORM.md
- **Quick Start**: See QUICKSTART.md
