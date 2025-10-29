"""
Point-In-Time (PIT) Snapshot Store for Fundamentals.

Prevents information leakage by storing fundamentals as immutable snapshots
with computed effective_date (first business day when data became available).

Key features:
- Snapshots never overwritten (restatements get new effective_date)
- effective_date = earliest date when market could know the numbers
- Forward-fill only within validity windows [effective_date, next_effective_date)
- Validation ensures no leakage (no data used before its effective_date)
"""

import json
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict

import pandas as pd
import numpy as np
from loguru import logger


@dataclass
class Snapshot:
    """Single fundamentals snapshot with metadata."""
    symbol: str
    effective_date: date
    statement_kind: str  # quarterly, annual, ttm
    period_end: date
    payload: Dict[str, Any]
    reported_date: Optional[date] = None
    source: str = "unknown"  # which availability source was used


class PITStore:
    """
    Point-In-Time fundamentals snapshot store.

    Design:
    - Each snapshot is an immutable JSON file
    - Filename encodes: {EFFECTIVE_DATE}__{STATEMENT_KIND}__{PERIOD_END}.json
    - Directory structure: data/pit/{SYMBOL}/
    - Panel builder forward-fills within validity windows
    """

    def __init__(
        self,
        snapshot_dir: Path | str = "data/pit",
        extra_lag_trading_days: int = 2,
        conservative_lag_days: Dict[str, int] = None,
        availability_source_priority: List[str] = None,
    ):
        """
        Initialize PIT store.

        Args:
            snapshot_dir: Root directory for snapshots
            extra_lag_trading_days: Additional trading days after reported_date
            conservative_lag_days: Fallback lags by statement kind
            availability_source_priority: Order to try finding reported_date
        """
        self.snapshot_dir = Path(snapshot_dir)
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)

        self.extra_lag_trading_days = extra_lag_trading_days

        self.conservative_lag_days = conservative_lag_days or {
            "quarterly": 60,
            "annual": 90,
            "ttm": 60,
        }

        self.availability_source_priority = availability_source_priority or [
            "earnings_report_date",
            "payload_updated_at",
            "period_end_plus_lag",
        ]

        logger.info(
            f"Initialized PITStore at {self.snapshot_dir} "
            f"(extra_lag={extra_lag_trading_days}d)"
        )

    def _compute_effective_date(
        self,
        period_end: date,
        statement_kind: str,
        reported_date: Optional[date],
        payload: Dict[str, Any],
    ) -> tuple[date, str]:
        """
        Compute effective_date using priority fallback logic.

        Returns:
            (effective_date, source_used)
        """
        source_used = None
        availability_date = None

        # Try each source in priority order
        for source in self.availability_source_priority:
            if source == "earnings_report_date" and reported_date:
                availability_date = reported_date
                source_used = "earnings_report_date"
                break

            elif source == "payload_updated_at":
                # Look for updatedAt in payload
                updated_str = payload.get("updatedAt") or payload.get("updated_at")
                if updated_str:
                    try:
                        availability_date = pd.to_datetime(updated_str).date()
                        source_used = "payload_updated_at"
                        break
                    except:
                        pass

            elif source == "period_end_plus_lag":
                # Conservative fallback
                lag_days = self.conservative_lag_days.get(statement_kind, 60)
                availability_date = period_end + timedelta(days=lag_days)
                source_used = "period_end_plus_lag"
                break

        if not availability_date:
            # Ultimate fallback
            availability_date = period_end + timedelta(days=90)
            source_used = "default_90d_lag"
            logger.warning(
                f"No availability source found, using default 90d lag from {period_end}"
            )

        # Add extra trading days
        # Simple approximation: add extra_lag_trading_days * 1.4 calendar days
        # (accounts for weekends, ~5/7 ratio)
        calendar_extra = int(self.extra_lag_trading_days * 1.4)
        effective_date = availability_date + timedelta(days=calendar_extra)

        # Ensure effective_date >= period_end
        if effective_date < period_end:
            effective_date = period_end + timedelta(days=1)
            logger.warning(
                f"effective_date before period_end, adjusting to {effective_date}"
            )

        return effective_date, source_used

    def append_snapshot(
        self,
        symbol: str,
        payload: Dict[str, Any],
        period_end: date,
        statement_kind: str,
        reported_date: Optional[date] = None,
    ) -> Path:
        """
        Append a fundamentals snapshot (never overwrites).

        Args:
            symbol: Ticker symbol (e.g., "AAPL.US", "BPN.WAR")
            payload: Complete fundamentals dict for this period
            period_end: Period end date
            statement_kind: "quarterly", "annual", or "ttm"
            reported_date: Known reporting date (if available)

        Returns:
            Path to created snapshot file
        """
        # Compute effective_date
        effective_date, source_used = self._compute_effective_date(
            period_end, statement_kind, reported_date, payload
        )

        # Create symbol directory
        symbol_dir = self.snapshot_dir / symbol
        symbol_dir.mkdir(parents=True, exist_ok=True)

        # Filename format: YYYY-MM-DD__kind__YYYY-MM-DD.json
        filename = (
            f"{effective_date.isoformat()}__"
            f"{statement_kind}__"
            f"{period_end.isoformat()}.json"
        )

        snapshot_path = symbol_dir / filename

        # Check if already exists (idempotent)
        if snapshot_path.exists():
            logger.debug(f"Snapshot already exists: {snapshot_path}")
            return snapshot_path

        # Create snapshot object
        snapshot = Snapshot(
            symbol=symbol,
            effective_date=effective_date,
            statement_kind=statement_kind,
            period_end=period_end,
            payload=payload,
            reported_date=reported_date,
            source=source_used,
        )

        # Write to disk
        snapshot_dict = {
            "symbol": snapshot.symbol,
            "effective_date": snapshot.effective_date.isoformat(),
            "statement_kind": snapshot.statement_kind,
            "period_end": snapshot.period_end.isoformat(),
            "reported_date": snapshot.reported_date.isoformat() if snapshot.reported_date else None,
            "source": snapshot.source,
            "payload": snapshot.payload,
        }

        with open(snapshot_path, 'w') as f:
            json.dump(snapshot_dict, f, indent=2)

        logger.info(
            f"Created snapshot: {symbol} {statement_kind} {period_end} "
            f"(effective={effective_date}, source={source_used})"
        )

        return snapshot_path

    def load_snapshots(self, symbol: str) -> List[Snapshot]:
        """
        Load all snapshots for a symbol.

        Returns:
            List of Snapshot objects, sorted by effective_date
        """
        symbol_dir = self.snapshot_dir / symbol

        if not symbol_dir.exists():
            return []

        snapshots = []

        for snapshot_file in symbol_dir.glob("*.json"):
            # Skip manifest
            if snapshot_file.name == "manifest.json":
                continue

            try:
                with open(snapshot_file, 'r') as f:
                    data = json.load(f)

                snapshot = Snapshot(
                    symbol=data["symbol"],
                    effective_date=date.fromisoformat(data["effective_date"]),
                    statement_kind=data["statement_kind"],
                    period_end=date.fromisoformat(data["period_end"]),
                    payload=data["payload"],
                    reported_date=(
                        date.fromisoformat(data["reported_date"])
                        if data.get("reported_date")
                        else None
                    ),
                    source=data.get("source", "unknown"),
                )

                snapshots.append(snapshot)

            except Exception as e:
                logger.error(f"Failed to load snapshot {snapshot_file}: {e}")
                continue

        # Sort by effective_date
        snapshots.sort(key=lambda s: s.effective_date)

        logger.debug(f"Loaded {len(snapshots)} snapshots for {symbol}")

        return snapshots

    def build_panel(
        self,
        symbols: List[str],
        start: date,
        end: date,
    ) -> pd.DataFrame:
        """
        Build a point-in-time fundamentals panel.

        For each (symbol, date) pair, uses the most recent snapshot where:
        effective_date <= date

        Forward-fills within validity windows: [effective_date, next_effective_date)

        Args:
            symbols: List of symbols
            start: Panel start date
            end: Panel end date

        Returns:
            DataFrame with columns: date, symbol, effective_date, statement_kind,
            and all fundamental fields from payloads
        """
        # Generate date range (business days)
        dates = pd.bdate_range(start, end, freq='B')

        panel_records = []

        for symbol in symbols:
            snapshots = self.load_snapshots(symbol)

            if not snapshots:
                logger.warning(f"No snapshots for {symbol}")
                continue

            # For each date, find applicable snapshot
            for dt in dates:
                dt_date = dt.date()

                # Find most recent snapshot where effective_date <= dt_date
                applicable_snapshot = None

                for snapshot in reversed(snapshots):  # Most recent first
                    if snapshot.effective_date <= dt_date:
                        applicable_snapshot = snapshot
                        break

                if applicable_snapshot:
                    # Extract fundamental fields from payload
                    record = {
                        "date": dt_date,
                        "symbol": symbol,
                        "effective_date": applicable_snapshot.effective_date,
                        "statement_kind": applicable_snapshot.statement_kind,
                        "period_end": applicable_snapshot.period_end,
                        "pit_source": applicable_snapshot.source,
                    }

                    # Add payload fields (flatten nested structure)
                    record.update(self._flatten_payload(applicable_snapshot.payload))

                    panel_records.append(record)

        if not panel_records:
            logger.warning("Empty PIT panel")
            return pd.DataFrame()

        panel = pd.DataFrame(panel_records)

        logger.info(
            f"Built PIT panel: {len(panel)} rows, "
            f"{panel['symbol'].nunique()} symbols, "
            f"{len(dates)} dates"
        )

        return panel

    def _flatten_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Flatten nested payload structure into flat dict.

        Extracts commonly used fundamental fields.
        Now works with filtered PIT payloads (no future data leakage).
        """
        flat = {}

        # Try to extract key fields (adjust based on EODHD structure)

        # Market cap, shares
        if "SharesStats" in payload:
            shares = payload["SharesStats"]
            flat["shares_outstanding"] = shares.get("SharesOutstanding")

        # Income statement TTM
        if "Highlights" in payload:
            highlights = payload["Highlights"]
            flat["market_cap"] = highlights.get("MarketCapitalization")
            flat["ebitda_ttm"] = highlights.get("EBITDA")
            flat["pe_ratio"] = highlights.get("PERatio")
            flat["revenue_ttm"] = highlights.get("RevenueTTM")
            flat["gross_profit_ttm"] = highlights.get("GrossProfitTTM")

        # Valuation
        if "Valuation" in payload:
            valuation = payload["Valuation"]
            flat["enterprise_value"] = valuation.get("EnterpriseValue")
            flat["ev_to_ebitda"] = valuation.get("EnterpriseValueEbitda")

        # Financials (most recent period available in this PIT snapshot)
        if "Financials" in payload:
            financials = payload["Financials"]

            # Income statement
            if "Income_Statement" in financials:
                income = financials["Income_Statement"]
                if "quarterly" in income and income["quarterly"]:
                    # Get most recent quarter (highest date)
                    latest_q = self._get_most_recent_period(income["quarterly"])
                    if latest_q:
                        flat["net_income"] = latest_q.get("netIncome")
                        flat["total_revenue"] = latest_q.get("totalRevenue")
                        flat["operating_income"] = latest_q.get("operatingIncome")
                        flat["gross_profit"] = latest_q.get("grossProfit")

            # Balance sheet
            if "Balance_Sheet" in financials:
                balance = financials["Balance_Sheet"]
                if "quarterly" in balance and balance["quarterly"]:
                    latest_q = self._get_most_recent_period(balance["quarterly"])
                    if latest_q:
                        flat["total_assets"] = latest_q.get("totalAssets")
                        flat["total_liabilities"] = latest_q.get("totalLiab")
                        flat["total_stockholder_equity"] = latest_q.get("totalStockholderEquity")
                        flat["cash"] = latest_q.get("cash")

            # Cash flow
            if "Cash_Flow" in financials:
                cashflow = financials["Cash_Flow"]
                if "quarterly" in cashflow and cashflow["quarterly"]:
                    latest_q = self._get_most_recent_period(cashflow["quarterly"])
                    if latest_q:
                        flat["free_cash_flow"] = latest_q.get("freeCashFlow")
                        flat["operating_cash_flow"] = latest_q.get("totalCashFromOperatingActivities")

        return flat

    def _get_most_recent_period(self, periods: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Get the most recent period from a dict of periods.

        Args:
            periods: Dict with date strings as keys (e.g., {"2024-09-30": {...}, "2024-06-30": {...}})

        Returns:
            The period data for the most recent date, or None if empty
        """
        if not periods:
            return None

        # Sort by date (descending) and get the first (most recent)
        sorted_dates = sorted(periods.keys(), reverse=True)
        return periods[sorted_dates[0]]

    def validate_pit_integrity(self, panel: pd.DataFrame) -> None:
        """
        Validate that PIT panel has no information leakage.

        Checks that for every row, effective_date <= date.

        Raises:
            AssertionError if leakage detected
        """
        if panel.empty:
            logger.warning("Empty panel for PIT validation")
            return

        # Check effective_date <= date for all rows
        violations = panel[panel["effective_date"] > panel["date"]]

        if not violations.empty:
            logger.error(
                f"PIT INTEGRITY VIOLATION: {len(violations)} rows with "
                f"effective_date > date"
            )
            logger.error(f"Sample violations:\n{violations.head()}")
            raise AssertionError(
                f"PIT integrity violated: {len(violations)} rows have "
                f"effective_date > date (information leakage detected)"
            )

        logger.info(f"✓ PIT integrity validated: {len(panel)} rows, no leakage detected")

    def get_manifest(self, symbol: str) -> Dict[str, Any]:
        """
        Get snapshot manifest for a symbol.

        Returns:
            Dict with counts, date ranges, statement kinds
        """
        snapshots = self.load_snapshots(symbol)

        if not snapshots:
            return {
                "symbol": symbol,
                "count": 0,
                "has_data": False,
            }

        effective_dates = [s.effective_date for s in snapshots]
        period_ends = [s.period_end for s in snapshots]

        return {
            "symbol": symbol,
            "count": len(snapshots),
            "has_data": True,
            "min_effective_date": min(effective_dates).isoformat(),
            "max_effective_date": max(effective_dates).isoformat(),
            "min_period_end": min(period_ends).isoformat(),
            "max_period_end": max(period_ends).isoformat(),
            "statement_kinds": list(set(s.statement_kind for s in snapshots)),
            "sources_used": list(set(s.source for s in snapshots)),
        }

    def save_manifest(self, symbol: str) -> Path:
        """Save manifest.json for a symbol."""
        manifest = self.get_manifest(symbol)

        symbol_dir = self.snapshot_dir / symbol
        symbol_dir.mkdir(parents=True, exist_ok=True)

        manifest_path = symbol_dir / "manifest.json"

        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)

        logger.info(f"Saved manifest for {symbol}: {manifest['count']} snapshots")

        return manifest_path
