#!/usr/bin/env python3
"""Clean up old Parquet fundamentals cache files."""

from pathlib import Path

cache_dir = Path("data/cache")

if cache_dir.exists():
    # Remove old .parquet fundamentals cache files
    old_files = list(cache_dir.glob("fundamentals_*.parquet"))

    if old_files:
        print(f"Found {len(old_files)} old Parquet fundamentals cache files")
        for file in old_files:
            file.unlink()
            print(f"  Removed: {file.name}")
        print("✓ Cleanup complete")
    else:
        print("✓ No old cache files to clean up")
else:
    print("Cache directory doesn't exist yet")
