"""Data API endpoints."""

from datetime import datetime
from typing import List
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.data.eodhd_client import EODHDClient
from backend.data.data_manager import DataManager
from backend.data.schemas import (
    FetchDataRequest,
    FetchDataResponse,
    ExchangeListResponse,
    SymbolListResponse,
    SymbolInfo,
)
from backend.models import DataFetchTask

router = APIRouter()


# Supported exchanges
SUPPORTED_EXCHANGES = [
    "US", "LSE", "XETRA", "WAR", "V", "TO", "HK", "SHG", "SHE",
    "JPX", "NSE", "BSE", "MCX", "ASX", "NZ", "KO", "STO", "CPH",
]


@router.get("/exchanges", response_model=ExchangeListResponse)
async def list_exchanges():
    """Get list of supported exchanges."""
    return {"exchanges": SUPPORTED_EXCHANGES}


@router.get("/symbols/{exchange}", response_model=SymbolListResponse)
async def list_symbols(exchange: str):
    """Get list of symbols for an exchange."""
    if exchange not in SUPPORTED_EXCHANGES:
        raise HTTPException(status_code=400, detail=f"Unsupported exchange: {exchange}")

    try:
        client = EODHDClient()
        symbols = await client.list_exchange_symbols(exchange)

        # Convert to SymbolInfo format
        symbol_list = []
        for s in symbols:
            symbol_list.append(
                SymbolInfo(
                    code=s.get("Code", ""),
                    name=s.get("Name"),
                    country=s.get("Country"),
                    exchange=exchange,
                    type=s.get("Type"),
                )
            )

        return {
            "exchange": exchange,
            "symbols": symbol_list,
            "count": len(symbol_list),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch symbols: {str(e)}")


@router.post("/fetch", response_model=FetchDataResponse)
async def fetch_data(
    request: FetchDataRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Fetch market data (runs in background).

    Returns task_id to check status.
    """
    # Create task record
    task = DataFetchTask(
        id=uuid4(),
        symbols=request.symbols,
        exchange=request.exchange,
        start_date=request.start_date,
        end_date=request.end_date,
        timeframe=request.timeframe,
        data_type=request.data_type,
        status="pending",
    )
    db.add(task)
    db.commit()

    # Run fetch in background
    background_tasks.add_task(
        _run_data_fetch,
        task.id,
        request.symbols,
        request.exchange,
        request.start_date,
        request.end_date,
        request.timeframe,
        request.data_type,
    )

    return {
        "task_id": str(task.id),
        "status": "pending",
        "message": f"Fetching data for {len(request.symbols)} symbols",
    }


@router.get("/fetch/status/{task_id}")
async def fetch_status(task_id: str, db: Session = Depends(get_db)):
    """Check status of data fetch task."""
    from uuid import UUID

    try:
        task_uuid = UUID(task_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid task ID")

    task = db.query(DataFetchTask).filter(DataFetchTask.id == task_uuid).first()

    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    return {
        "task_id": str(task.id),
        "status": task.status,
        "progress": task.progress,
        "symbols": task.symbols,
        "error": task.error_message,
        "started_at": task.started_at.isoformat() if task.started_at else None,
        "completed_at": task.completed_at.isoformat() if task.completed_at else None,
    }


async def _run_data_fetch(
    task_id,
    symbols: List[str],
    exchange: str,
    start_date,
    end_date,
    timeframe: str,
    data_type: str,
):
    """Background task to fetch data."""
    from backend.database import SessionLocal

    db = SessionLocal()

    try:
        # Update task status
        task = db.query(DataFetchTask).filter(DataFetchTask.id == task_id).first()
        task.status = "running"
        task.started_at = datetime.utcnow()
        db.commit()

        # Fetch data
        data_manager = DataManager(db)

        for i, symbol in enumerate(symbols):
            try:
                if data_type == "eod":
                    await data_manager.get_or_fetch_eod(
                        symbol, exchange, start_date, end_date
                    )
                elif data_type == "intraday":
                    await data_manager.get_or_fetch_intraday(
                        symbol, exchange, timeframe.lower(), start_date, end_date
                    )
                elif data_type == "fundamentals":
                    await data_manager.get_or_fetch_fundamentals(symbol, exchange)

                # Update progress
                task.progress = ((i + 1) / len(symbols)) * 100
                db.commit()

            except Exception as e:
                # Log error but continue with other symbols
                from loguru import logger
                logger.error(f"Error fetching {symbol}: {e}")

        # Mark as completed
        task.status = "completed"
        task.completed_at = datetime.utcnow()
        task.progress = 100.0
        db.commit()

    except Exception as e:
        # Mark as failed
        task.status = "failed"
        task.error_message = str(e)
        task.completed_at = datetime.utcnow()
        db.commit()

    finally:
        db.close()
