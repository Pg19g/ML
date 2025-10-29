# CRITICAL FIX: PIT Snapshot Information Leakage

**Date**: 2025-10-29
**Severity**: CRITICAL
**Status**: FIXED (Commit: ea1a78b)

## ⚠️ Problem Statement

The initial PIT snapshot implementation contained a **critical information leakage bug** that violated point-in-time integrity and created look-ahead bias.

### The Bug

Each snapshot saved the ENTIRE fundamentals payload, including future data that hadn't been published yet:

```python
# BROKEN CODE (before fix):
snapshot_payload = {
    "statement_type": statement_type,
    "period_data": period_data,
    "full_payload": full_payload,  # ❌ Contains ALL quarters including future ones!
}
```

### Impact

**Example of the leakage:**
- Q1 2024 snapshot (filed May 16, 2024) incorrectly contained Q2 2024 data (filed Aug 14, 2024)
- Q2 2024 snapshot (filed Aug 14, 2024) incorrectly contained Q3 2024 data (filed Oct 31, 2024)
- Q3 2024 snapshot (filed Oct 31, 2024) incorrectly contained future data not yet published

This means:
- **All backtests using old snapshots are INVALID** - they contained look-ahead bias
- Performance metrics (IC, Sharpe, returns) were artificially inflated
- Strategies trained on old data had access to future information
- Research conclusions drawn from old data are unreliable

## ✅ The Fix

Complete rewrite of snapshot materialization to enforce TRUE point-in-time integrity.

### Key Insight: filing_date Fields

The EODHD fundamentals JSON contains `filing_date` fields that indicate when each statement was ACTUALLY PUBLISHED to the market:

```json
{
  "Financials": {
    "Income_Statement": {
      "quarterly": {
        "2024-09-30": {
          "date": "2024-09-30",           // ← Period end date
          "filing_date": "2024-10-31",    // ← Actual publication date
          "totalRevenue": 300000000
        },
        "2024-06-30": {
          "date": "2024-06-30",
          "filing_date": "2024-08-14",    // ← Published Aug 14
          "totalRevenue": 280000000
        }
      }
    }
  }
}
```

### Solution Architecture

1. **Extract all periods with filing_dates**
   ```python
   all_periods = self._extract_all_periods(financials)
   ```

2. **Sort by filing_date (chronological order of publication)**
   ```python
   all_periods.sort(key=lambda x: x["filing_date"])
   ```

3. **Create cumulative snapshots, each filtered to its filing_date**
   ```python
   for period in all_periods:
       # Filter payload to ONLY include data published up to this filing_date
       filtered_payload = self._filter_payload_to_filing_date(
           fundamentals, period["filing_date"]
       )
       # Create snapshot with filtered PIT data
       self.pit_store.append_snapshot(...)
   ```

### New Method: _filter_payload_to_filing_date()

```python
def _filter_payload_to_filing_date(
    self, full_payload: Dict[str, Any], cutoff_filing_date: date
) -> Dict[str, Any]:
    """
    Filter payload to ONLY include data published on or before cutoff_filing_date.

    This is the CRITICAL method that prevents look-ahead bias.
    """
    # Create deep copy
    filtered = deepcopy(full_payload)

    # Filter each period in Financials section
    for period_end_str, period_data in periods.items():
        filing_date = pd.to_datetime(period_data.get("filing_date")).date()

        # CRITICAL CHECK: Only include if published on or before cutoff
        if filing_date <= cutoff_filing_date:
            filtered_periods[period_end_str] = period_data
        # else: Future data - exclude

    return filtered
```

## 📊 Verification

### Correct Behavior (After Fix)

Given example data:
- Q1 2024 (period: 2024-03-31, filed: 2024-05-16)
- Q2 2024 (period: 2024-06-30, filed: 2024-08-14)
- Q3 2024 (period: 2024-09-30, filed: 2024-10-31)
- Annual 2023 (period: 2023-12-31, filed: 2024-03-21)

**Q1 2024 snapshot (filed May 16)**:
- ✅ Contains: Annual 2023 (filed Mar 21), Q1 2024 (filed May 16)
- ❌ Does NOT contain: Q2 2024 (filed Aug 14), Q3 2024 (filed Oct 31)

**Q2 2024 snapshot (filed Aug 14)**:
- ✅ Contains: Annual 2023, Q1 2024, Q2 2024
- ❌ Does NOT contain: Q3 2024 (filed Oct 31)

**Q3 2024 snapshot (filed Oct 31)**:
- ✅ Contains: Annual 2023, Q1 2024, Q2 2024, Q3 2024
- ✅ All historical data preserved

### Test Coverage

Created comprehensive test suite (`tests/test_pit_leakage_fix.py`, 500+ lines):

- ✅ **TestNoFutureDataLeakage**: Verifies Q1 excludes Q2/Q3, Q2 excludes Q3
- ✅ **TestCumulativeHistory**: Verifies snapshots accumulate historical data correctly
- ✅ **TestFilingDateOrdering**: Verifies ordering by filing_date not period_end
- ✅ **TestFilterPayloadMethod**: Tests payload filtering directly
- ✅ **TestExtractAllPeriods**: Tests period extraction and sorting

## 🔄 Migration Guide

### Step 1: Understand the Impact

**⚠️ BREAKING CHANGE**: Existing snapshots created before commit `ea1a78b` contain information leakage and must be regenerated.

### Step 2: Delete Old Snapshots

```bash
# Backup old snapshots (optional)
mv data/pit data/pit_BACKUP_$(date +%Y%m%d)

# Or delete directly
rm -rf data/pit/
```

### Step 3: Re-ingest with Fixed Code

```bash
# Pull latest code
git fetch origin
git checkout claude/session-011CUZjGVsxM8MbHGo5CP5Dm

# Re-ingest fundamentals (will use fixed code)
python scripts/ingest_on_demand.py --config config/defaults.yaml --force-refresh

# Or for specific symbols
python scripts/ingest_on_demand.py --symbols AAPL.US MSFT.US --force-refresh
```

### Step 4: Verify Fix

```bash
# Run tests to verify no leakage
python -m pytest tests/test_pit_leakage_fix.py -v

# Check snapshots are cumulative
python scripts/ingest_on_demand.py --coverage-only
```

### Step 5: Re-run Backtests

All backtests must be re-run with new snapshots:

```bash
# Re-train models
python scripts/train.py --config config/defaults.yaml

# Re-run backtests
python scripts/backtest.py --config config/defaults.yaml

# Generate new reports
python scripts/report.py
```

## 📉 Expected Performance Changes

### What to Expect

After fixing the leakage, you will likely observe:

- **Lower IC (Information Coefficient)**: The model no longer has access to future data
- **Lower Sharpe Ratio**: Returns will be more realistic
- **Lower absolute returns**: Artificial look-ahead advantage removed
- **More realistic drawdowns**: True risk characteristics revealed

### This is GOOD!

These changes represent **true performance** of the strategy. The previous inflated metrics were due to look-ahead bias and would not be achievable in production.

## 🔍 How to Detect Old Leaky Snapshots

If you're unsure whether snapshots were created before or after the fix:

```python
import json
from pathlib import Path

snapshot_file = Path("data/pit/AAPL.US/2024-05-18__quarterly__2024-03-31.json")

with open(snapshot_file) as f:
    snapshot = json.load(f)

# Old (leaky) snapshots have this structure:
if "full_payload" in str(snapshot):
    print("⚠️ OLD LEAKY SNAPSHOT - needs regeneration")

# New (fixed) snapshots have filtered Financials:
financials = snapshot.get("Financials", {})
income = financials.get("Income_Statement", {}).get("quarterly", {})
if len(income) > 0:
    # Check if future data is excluded
    print(f"Snapshot contains periods: {list(income.keys())}")
```

## 📝 Code Changes Summary

### Files Modified

1. **src/ingest/fundamentals_on_demand.py** (COMPLETE REWRITE - 358 lines)
   - NEW: `_filter_payload_to_filing_date()` - Core filtering logic
   - NEW: `_extract_all_periods()` - Period extraction with filing_dates
   - REWRITTEN: `_materialize_snapshots()` - Cumulative snapshot creation
   - REMOVED: Old `_process_statement_block()` that saved full payload

2. **src/pit_snapshots.py** (ENHANCED - 40 lines changed)
   - FIXED: `_flatten_payload()` - Robust period extraction
   - NEW: `_get_most_recent_period()` - Helper method

3. **tests/test_pit_leakage_fix.py** (NEW - 500+ lines)
   - Comprehensive test suite for leakage prevention

### Lines Changed

- **584 lines added**
- **90 lines removed**
- **Net: +494 lines**

## ✅ Verification Checklist

Before considering migration complete:

- [ ] Old snapshots deleted or backed up
- [ ] New snapshots created with fixed code (commit ea1a78b or later)
- [ ] Tests pass: `pytest tests/test_pit_leakage_fix.py -v`
- [ ] Coverage report shows data for all symbols
- [ ] Models re-trained with new snapshots
- [ ] Backtests re-run with new snapshots
- [ ] Performance metrics documented (compare old vs new)
- [ ] Team notified of breaking change

## 🎓 Lessons Learned

1. **Always validate PIT integrity**: The `validate_pit_integrity()` method catches violations, but only if data structure exposes effective_date
2. **filing_date is critical**: Must be used to determine when data was actually available
3. **Cumulative snapshots**: Each snapshot should be a superset of previous ones
4. **Test with negative cases**: Deliberately create violations to verify detection
5. **Document breaking changes**: Clear migration path is essential

## 🔗 References

- **Commit**: `ea1a78b` - CRITICAL FIX: Eliminate information leakage in PIT snapshot system
- **Branch**: `claude/session-011CUZjGVsxM8MbHGo5CP5Dm`
- **Test Suite**: `tests/test_pit_leakage_fix.py`
- **Architecture Doc**: `ARCHITECTURE_PIT.md`

---

**This fix is ESSENTIAL for research validity. All previous results using old snapshots should be considered invalid and re-run with fixed code.**

🤖 Generated with [Claude Code](https://claude.com/claude-code)
