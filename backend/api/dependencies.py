"""FastAPI dependencies."""

from backend.database import get_db
from backend.data.eodhd_client import EODHDClient
from backend.data.data_manager import DataManager


def get_eodhd_client() -> EODHDClient:
    """Get EODHD client instance."""
    return EODHDClient()


def get_data_manager(db = None):
    """Get data manager instance."""
    if db is None:
        from backend.database import SessionLocal
        db = SessionLocal()
    return DataManager(db)
