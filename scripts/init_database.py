"""Initialize database schema for the quant platform."""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from loguru import logger
from backend.database import engine, Base, init_db
from backend.models import MarketData, FundamentalData, Backtest, DataFetchTask


def main():
    """Initialize database tables."""
    logger.info("Initializing database schema...")

    try:
        # Create all tables
        Base.metadata.create_all(bind=engine)

        logger.info("✅ Database schema created successfully")
        logger.info("Tables created:")
        logger.info("  - market_data")
        logger.info("  - fundamental_data")
        logger.info("  - backtests")
        logger.info("  - data_fetch_tasks")

        # Test connection
        from backend.database import SessionLocal
        db = SessionLocal()
        db.execute("SELECT 1")
        db.close()

        logger.info("✅ Database connection test successful")

    except Exception as e:
        logger.error(f"❌ Failed to initialize database: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
