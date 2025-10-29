"""
Test PIT Snapshot Architecture - Zero Information Leakage.

Includes both positive tests (correct behavior) and negative tests
(deliberately create violations to prove they are caught).
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import date, timedelta
import tempfile
import shutil
import json

from src.pit_snapshots import PITStore, Snapshot


@pytest.fixture
def temp_snapshot_dir():
    """Create temporary directory for test snapshots."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def pit_store(temp_snapshot_dir):
    """Create PITStore with test configuration."""
    return PITStore(
        snapshot_dir=temp_snapshot_dir,
        extra_lag_trading_days=2,
        conservative_lag_days={
            "quarterly": 60,
            "annual": 90,
            "ttm": 60,
        },
        availability_source_priority=[
            "earnings_report_date",
            "payload_updated_at",
            "period_end_plus_lag",
        ],
    )


def create_sample_payload(market_cap: float, revenue: float) -> dict:
    """Create sample fundamentals payload."""
    return {
        "Highlights": {
            "MarketCapitalization": market_cap,
            "RevenueTTM": revenue,
            "EBITDA": revenue * 0.2,
            "PERatio": 15.0,
        },
        "SharesStats": {
            "SharesOutstanding": market_cap / 150.0,
        },
    }


class TestSnapshotCreation:
    """Test snapshot creation and storage."""

    def test_append_snapshot_basic(self, pit_store):
        """Test creating a single snapshot."""
        payload = create_sample_payload(1e12, 400e9)
        period_end = date(2023, 6, 30)
        reported_date = date(2023, 8, 3)

        snapshot_path = pit_store.append_snapshot(
            symbol="AAPL.US",
            payload=payload,
            period_end=period_end,
            statement_kind="quarterly",
            reported_date=reported_date,
        )

        assert snapshot_path.exists()
        assert "AAPL.US" in str(snapshot_path)
        assert "2023-08-" in str(snapshot_path)  # effective_date should be in August
        assert "quarterly" in str(snapshot_path)
        assert "2023-06-30" in str(snapshot_path)

    def test_append_snapshot_idempotent(self, pit_store):
        """Test that appending same snapshot twice doesn't duplicate."""
        payload = create_sample_payload(1e12, 400e9)
        period_end = date(2023, 6, 30)
        reported_date = date(2023, 8, 3)

        path1 = pit_store.append_snapshot(
            symbol="AAPL.US",
            payload=payload,
            period_end=period_end,
            statement_kind="quarterly",
            reported_date=reported_date,
        )

        path2 = pit_store.append_snapshot(
            symbol="AAPL.US",
            payload=payload,
            period_end=period_end,
            statement_kind="quarterly",
            reported_date=reported_date,
        )

        assert path1 == path2
        # Should only be one snapshot file
        snapshots = pit_store.load_snapshots("AAPL.US")
        assert len(snapshots) == 1

    def test_multiple_snapshots_per_symbol(self, pit_store):
        """Test creating multiple periods for same symbol."""
        # Q2 2023
        pit_store.append_snapshot(
            symbol="AAPL.US",
            payload=create_sample_payload(2.5e12, 100e9),
            period_end=date(2023, 6, 30),
            statement_kind="quarterly",
            reported_date=date(2023, 8, 3),
        )

        # Q3 2023
        pit_store.append_snapshot(
            symbol="AAPL.US",
            payload=create_sample_payload(2.6e12, 105e9),
            period_end=date(2023, 9, 30),
            statement_kind="quarterly",
            reported_date=date(2023, 11, 2),
        )

        snapshots = pit_store.load_snapshots("AAPL.US")
        assert len(snapshots) == 2
        assert snapshots[0].period_end == date(2023, 6, 30)
        assert snapshots[1].period_end == date(2023, 9, 30)


class TestEffectiveDateComputation:
    """Test effective_date computation logic."""

    def test_effective_date_from_reported_date(self, pit_store):
        """Test using earnings_report_date as source."""
        payload = create_sample_payload(1e12, 400e9)
        period_end = date(2023, 6, 30)
        reported_date = date(2023, 8, 3)

        pit_store.append_snapshot(
            symbol="TEST.US",
            payload=payload,
            period_end=period_end,
            statement_kind="quarterly",
            reported_date=reported_date,
        )

        snapshots = pit_store.load_snapshots("TEST.US")
        snapshot = snapshots[0]

        # effective_date should be reported_date + extra_lag_trading_days
        # extra_lag = 2 trading days ≈ 2-4 calendar days
        assert snapshot.effective_date > reported_date
        assert snapshot.effective_date >= period_end
        assert snapshot.source == "earnings_report_date"

    def test_effective_date_from_updated_at(self, pit_store):
        """Test using payload updatedAt as fallback."""
        payload = {
            "updatedAt": "2023-08-10",
            "Highlights": {
                "MarketCapitalization": 1e12,
            },
        }
        period_end = date(2023, 6, 30)

        pit_store.append_snapshot(
            symbol="TEST.US",
            payload=payload,
            period_end=period_end,
            statement_kind="quarterly",
            reported_date=None,  # No reported_date
        )

        snapshots = pit_store.load_snapshots("TEST.US")
        snapshot = snapshots[0]

        # Should use payload_updated_at
        assert snapshot.source == "payload_updated_at"
        assert snapshot.effective_date >= date(2023, 8, 10)

    def test_effective_date_conservative_fallback(self, pit_store):
        """Test conservative period_end + lag fallback."""
        payload = create_sample_payload(1e12, 400e9)
        # No updatedAt field
        period_end = date(2023, 6, 30)

        pit_store.append_snapshot(
            symbol="TEST.US",
            payload=payload,
            period_end=period_end,
            statement_kind="quarterly",
            reported_date=None,
        )

        snapshots = pit_store.load_snapshots("TEST.US")
        snapshot = snapshots[0]

        # Should use period_end_plus_lag
        assert snapshot.source == "period_end_plus_lag"
        # Quarterly lag = 60 days + extra_lag
        expected_min = period_end + timedelta(days=60)
        assert snapshot.effective_date >= expected_min


class TestPanelBuilding:
    """Test PIT panel construction with forward-fill."""

    def test_build_panel_single_snapshot(self, pit_store):
        """Test panel building with one snapshot."""
        # Create snapshot with effective_date = 2023-08-07
        pit_store.append_snapshot(
            symbol="AAPL.US",
            payload=create_sample_payload(2.5e12, 100e9),
            period_end=date(2023, 6, 30),
            statement_kind="quarterly",
            reported_date=date(2023, 8, 3),
        )

        snapshots = pit_store.load_snapshots("AAPL.US")
        effective_date = snapshots[0].effective_date

        # Build panel
        panel = pit_store.build_panel(
            symbols=["AAPL.US"],
            start=date(2023, 7, 1),
            end=date(2023, 8, 31),
        )

        assert not panel.empty

        # Before effective_date → no data
        before = panel[panel["date"] < effective_date]
        assert len(before) == 0 or before["market_cap"].isna().all()

        # After effective_date → data present
        after = panel[panel["date"] >= effective_date]
        assert not after.empty
        assert after["market_cap"].notna().all()
        assert (after["market_cap"] == 2.5e12).all()

    def test_build_panel_multiple_snapshots_forward_fill(self, pit_store):
        """Test forward-fill transitions between snapshots."""
        # Q2 2023 - reported Aug 3
        pit_store.append_snapshot(
            symbol="AAPL.US",
            payload=create_sample_payload(2.5e12, 100e9),
            period_end=date(2023, 6, 30),
            statement_kind="quarterly",
            reported_date=date(2023, 8, 3),
        )

        # Q3 2023 - reported Nov 2
        pit_store.append_snapshot(
            symbol="AAPL.US",
            payload=create_sample_payload(2.8e12, 110e9),
            period_end=date(2023, 9, 30),
            statement_kind="quarterly",
            reported_date=date(2023, 11, 2),
        )

        snapshots = pit_store.load_snapshots("AAPL.US")
        eff_date_1 = snapshots[0].effective_date
        eff_date_2 = snapshots[1].effective_date

        # Build panel
        panel = pit_store.build_panel(
            symbols=["AAPL.US"],
            start=date(2023, 8, 1),
            end=date(2023, 11, 30),
        )

        assert not panel.empty

        # Between eff_date_1 and eff_date_2 → use first snapshot
        between = panel[
            (panel["date"] >= eff_date_1) & (panel["date"] < eff_date_2)
        ]
        if not between.empty:
            assert (between["market_cap"] == 2.5e12).all()

        # After eff_date_2 → use second snapshot
        after = panel[panel["date"] >= eff_date_2]
        if not after.empty:
            assert (after["market_cap"] == 2.8e12).all()

    def test_build_panel_multiple_symbols(self, pit_store):
        """Test panel with multiple symbols."""
        # AAPL
        pit_store.append_snapshot(
            symbol="AAPL.US",
            payload=create_sample_payload(2.5e12, 100e9),
            period_end=date(2023, 6, 30),
            statement_kind="quarterly",
            reported_date=date(2023, 8, 3),
        )

        # MSFT
        pit_store.append_snapshot(
            symbol="MSFT.US",
            payload=create_sample_payload(2.3e12, 50e9),
            period_end=date(2023, 6, 30),
            statement_kind="quarterly",
            reported_date=date(2023, 7, 25),
        )

        panel = pit_store.build_panel(
            symbols=["AAPL.US", "MSFT.US"],
            start=date(2023, 8, 1),
            end=date(2023, 8, 31),
        )

        assert not panel.empty
        assert panel["symbol"].nunique() == 2

        # Check both symbols present
        aapl_data = panel[panel["symbol"] == "AAPL.US"]
        msft_data = panel[panel["symbol"] == "MSFT.US"]

        assert not aapl_data.empty
        assert not msft_data.empty


class TestPITIntegrityValidation:
    """Test PIT integrity validation (positive and NEGATIVE tests)."""

    def test_validate_integrity_passes(self, pit_store):
        """Test validation passes for correct PIT panel."""
        pit_store.append_snapshot(
            symbol="AAPL.US",
            payload=create_sample_payload(2.5e12, 100e9),
            period_end=date(2023, 6, 30),
            statement_kind="quarterly",
            reported_date=date(2023, 8, 3),
        )

        panel = pit_store.build_panel(
            symbols=["AAPL.US"],
            start=date(2023, 8, 1),
            end=date(2023, 8, 31),
        )

        # Should not raise
        pit_store.validate_pit_integrity(panel)

    def test_validate_integrity_negative_deliberate_violation(self, pit_store):
        """
        NEGATIVE TEST: Deliberately create leaky panel and verify it's caught.

        This demonstrates the system correctly detects information leakage.
        """
        # Create snapshot with normal effective_date
        pit_store.append_snapshot(
            symbol="AAPL.US",
            payload=create_sample_payload(2.5e12, 100e9),
            period_end=date(2023, 6, 30),
            statement_kind="quarterly",
            reported_date=date(2023, 8, 3),
        )

        # Build correct panel
        panel = pit_store.build_panel(
            symbols=["AAPL.US"],
            start=date(2023, 8, 1),
            end=date(2023, 8, 31),
        )

        # DELIBERATELY CREATE LEAKAGE: Manually modify panel to use future effective_date
        if not panel.empty:
            # Set effective_date to future (VIOLATION)
            panel.loc[panel.index[0], "effective_date"] = panel.loc[panel.index[0], "date"] + timedelta(days=10)

            # Validation MUST catch this
            with pytest.raises(AssertionError, match="information leakage"):
                pit_store.validate_pit_integrity(panel)

    def test_validate_integrity_negative_backdated_snapshot(self, temp_snapshot_dir):
        """
        NEGATIVE TEST: Create snapshot with impossible effective_date.

        Demonstrates that effective_date cannot be before period_end.
        """
        pit_store = PITStore(
            snapshot_dir=temp_snapshot_dir,
            extra_lag_trading_days=0,  # Minimal lag
            conservative_lag_days={"quarterly": 0},  # Force edge case
        )

        payload = create_sample_payload(1e12, 400e9)
        period_end = date(2023, 6, 30)

        # Even with zero lags, effective_date should be >= period_end
        pit_store.append_snapshot(
            symbol="TEST.US",
            payload=payload,
            period_end=period_end,
            statement_kind="quarterly",
            reported_date=date(2023, 6, 25),  # BEFORE period_end (impossible)
        )

        snapshots = pit_store.load_snapshots("TEST.US")
        snapshot = snapshots[0]

        # System should correct this
        assert snapshot.effective_date >= period_end, \
            "effective_date must be >= period_end"


class TestManifest:
    """Test manifest generation and metadata."""

    def test_get_manifest_no_data(self, pit_store):
        """Test manifest for symbol with no snapshots."""
        manifest = pit_store.get_manifest("MISSING.US")

        assert manifest["symbol"] == "MISSING.US"
        assert manifest["count"] == 0
        assert manifest["has_data"] is False

    def test_get_manifest_with_data(self, pit_store):
        """Test manifest for symbol with snapshots."""
        # Create multiple snapshots
        pit_store.append_snapshot(
            symbol="AAPL.US",
            payload=create_sample_payload(2.5e12, 100e9),
            period_end=date(2023, 6, 30),
            statement_kind="quarterly",
            reported_date=date(2023, 8, 3),
        )

        pit_store.append_snapshot(
            symbol="AAPL.US",
            payload=create_sample_payload(2.6e12, 105e9),
            period_end=date(2023, 9, 30),
            statement_kind="quarterly",
            reported_date=date(2023, 11, 2),
        )

        manifest = pit_store.get_manifest("AAPL.US")

        assert manifest["symbol"] == "AAPL.US"
        assert manifest["count"] == 2
        assert manifest["has_data"] is True
        assert "min_effective_date" in manifest
        assert "max_effective_date" in manifest
        assert "quarterly" in manifest["statement_kinds"]

    def test_save_manifest(self, pit_store):
        """Test saving manifest to disk."""
        pit_store.append_snapshot(
            symbol="AAPL.US",
            payload=create_sample_payload(2.5e12, 100e9),
            period_end=date(2023, 6, 30),
            statement_kind="quarterly",
            reported_date=date(2023, 8, 3),
        )

        manifest_path = pit_store.save_manifest("AAPL.US")

        assert manifest_path.exists()
        assert manifest_path.name == "manifest.json"

        # Load and verify
        with open(manifest_path, 'r') as f:
            data = json.load(f)

        assert data["symbol"] == "AAPL.US"
        assert data["count"] == 1


class TestRestatements:
    """Test handling of restatements (multiple snapshots for same period)."""

    def test_restatement_creates_new_snapshot(self, pit_store):
        """Test that restatements create new snapshot with later effective_date."""
        # Original Q2 2023 filing
        pit_store.append_snapshot(
            symbol="AAPL.US",
            payload=create_sample_payload(2.5e12, 100e9),
            period_end=date(2023, 6, 30),
            statement_kind="quarterly",
            reported_date=date(2023, 8, 3),
        )

        # Restatement filed later with corrected numbers
        pit_store.append_snapshot(
            symbol="AAPL.US",
            payload=create_sample_payload(2.52e12, 102e9),  # Corrected
            period_end=date(2023, 6, 30),  # Same period!
            statement_kind="quarterly",
            reported_date=date(2023, 9, 15),  # Later date
        )

        snapshots = pit_store.load_snapshots("AAPL.US")

        # Should have TWO snapshots for same period
        assert len(snapshots) == 2
        assert snapshots[0].period_end == date(2023, 6, 30)
        assert snapshots[1].period_end == date(2023, 6, 30)

        # Later snapshot has later effective_date
        assert snapshots[1].effective_date > snapshots[0].effective_date

    def test_restatement_pit_integrity(self, pit_store):
        """Test PIT integrity with restatements."""
        # Original
        pit_store.append_snapshot(
            symbol="AAPL.US",
            payload=create_sample_payload(2.5e12, 100e9),
            period_end=date(2023, 6, 30),
            statement_kind="quarterly",
            reported_date=date(2023, 8, 3),
        )

        # Restatement
        pit_store.append_snapshot(
            symbol="AAPL.US",
            payload=create_sample_payload(2.52e12, 102e9),
            period_end=date(2023, 6, 30),
            statement_kind="quarterly",
            reported_date=date(2023, 9, 15),
        )

        snapshots = pit_store.load_snapshots("AAPL.US")
        eff_date_1 = snapshots[0].effective_date
        eff_date_2 = snapshots[1].effective_date

        # Build panel
        panel = pit_store.build_panel(
            symbols=["AAPL.US"],
            start=date(2023, 8, 1),
            end=date(2023, 10, 1),
        )

        # Before restatement effective_date → old value
        before_restatement = panel[
            (panel["date"] >= eff_date_1) & (panel["date"] < eff_date_2)
        ]
        if not before_restatement.empty:
            # Should use original snapshot
            assert (before_restatement["market_cap"] == 2.5e12).all()

        # After restatement effective_date → new value
        after_restatement = panel[panel["date"] >= eff_date_2]
        if not after_restatement.empty:
            # Should use restated snapshot
            assert (after_restatement["market_cap"] == 2.52e12).all()

        # Validate integrity
        pit_store.validate_pit_integrity(panel)


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_empty_payload(self, pit_store):
        """Test handling of empty payload."""
        pit_store.append_snapshot(
            symbol="EMPTY.US",
            payload={},
            period_end=date(2023, 6, 30),
            statement_kind="quarterly",
            reported_date=date(2023, 8, 3),
        )

        snapshots = pit_store.load_snapshots("EMPTY.US")
        assert len(snapshots) == 1

        panel = pit_store.build_panel(
            symbols=["EMPTY.US"],
            start=date(2023, 8, 1),
            end=date(2023, 8, 31),
        )

        # Should have rows but with NaN values
        assert not panel.empty
        # Most fields should be NaN
        assert panel["market_cap"].isna().all()

    def test_symbol_with_no_snapshots(self, pit_store):
        """Test panel building with missing symbol."""
        panel = pit_store.build_panel(
            symbols=["MISSING.US"],
            start=date(2023, 1, 1),
            end=date(2023, 12, 31),
        )

        # Should return empty DataFrame
        assert panel.empty

    def test_date_range_before_all_snapshots(self, pit_store):
        """Test panel for date range before any data available."""
        pit_store.append_snapshot(
            symbol="AAPL.US",
            payload=create_sample_payload(2.5e12, 100e9),
            period_end=date(2023, 6, 30),
            statement_kind="quarterly",
            reported_date=date(2023, 8, 3),
        )

        # Query dates before snapshot effective_date
        panel = pit_store.build_panel(
            symbols=["AAPL.US"],
            start=date(2023, 1, 1),
            end=date(2023, 7, 31),
        )

        # Should be empty or all NaN (no applicable snapshot)
        if not panel.empty:
            assert panel["market_cap"].isna().all()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
