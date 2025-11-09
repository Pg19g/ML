"""Backtest API endpoints."""

from datetime import datetime
from typing import Dict, Any, Optional
from uuid import uuid4, UUID

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.data.data_manager import DataManager
from backend.backtesting.engine import BacktestEngine
from backend.strategies.registry import StrategyRegistry
from backend.models import Backtest

router = APIRouter()


class BacktestRequest(BaseModel):
    """Request to run a backtest."""
    symbols: list[str]
    exchange: str
    start_date: str  # YYYY-MM-DD
    end_date: str  # YYYY-MM-DD
    strategy_name: str
    strategy_params: Dict[str, Any] = {}
    timeframe: str = "1D"
    initial_cash: float = 10000.0
    commission: float = 0.001
    name: Optional[str] = None


class BacktestResponse(BaseModel):
    """Response from backtest request."""
    backtest_id: str
    status: str
    message: str


@router.post("/run", response_model=BacktestResponse)
async def run_backtest(
    request: BacktestRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Start a backtest job.

    Returns backtest_id to check status and results.
    """
    # Validate strategy exists
    try:
        StrategyRegistry.get(request.strategy_name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Create backtest record
    backtest = Backtest(
        id=uuid4(),
        name=request.name or f"{request.strategy_name} - {datetime.utcnow().strftime('%Y-%m-%d %H:%M')}",
        strategy_name=request.strategy_name,
        strategy_params=request.strategy_params,
        symbols=request.symbols,
        exchange=request.exchange,
        start_date=datetime.fromisoformat(request.start_date),
        end_date=datetime.fromisoformat(request.end_date),
        timeframe=request.timeframe,
        initial_cash=request.initial_cash,
        commission=request.commission,
        status="pending",
    )
    db.add(backtest)
    db.commit()

    # Run backtest in background
    background_tasks.add_task(
        _run_backtest_job,
        backtest.id,
    )

    return {
        "backtest_id": str(backtest.id),
        "status": "pending",
        "message": f"Backtest queued for {len(request.symbols)} symbols",
    }


@router.get("/{backtest_id}")
async def get_backtest_results(backtest_id: str, db: Session = Depends(get_db)):
    """Get backtest results."""
    try:
        bt_uuid = UUID(backtest_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid backtest ID")

    backtest = db.query(Backtest).filter(Backtest.id == bt_uuid).first()

    if not backtest:
        raise HTTPException(status_code=404, detail="Backtest not found")

    response = {
        "id": str(backtest.id),
        "name": backtest.name,
        "strategy_name": backtest.strategy_name,
        "strategy_params": backtest.strategy_params,
        "symbols": backtest.symbols,
        "exchange": backtest.exchange,
        "start_date": backtest.start_date.isoformat(),
        "end_date": backtest.end_date.isoformat(),
        "timeframe": backtest.timeframe,
        "status": backtest.status,
        "progress": backtest.progress,
        "error": backtest.error_message,
        "created_at": backtest.created_at.isoformat(),
        "completed_at": backtest.completed_at.isoformat() if backtest.completed_at else None,
    }

    # Add metrics if completed
    if backtest.status == "completed":
        response["metrics"] = {
            "sharpe_ratio": backtest.sharpe_ratio,
            "total_return": backtest.total_return,
            "max_drawdown": backtest.max_drawdown,
            "num_trades": backtest.num_trades,
            "win_rate": backtest.win_rate,
        }
        response["results"] = backtest.results_json

    return response


@router.get("/{backtest_id}/trades")
async def get_backtest_trades(backtest_id: str, db: Session = Depends(get_db)):
    """Get detailed trade list."""
    try:
        bt_uuid = UUID(backtest_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid backtest ID")

    backtest = db.query(Backtest).filter(Backtest.id == bt_uuid).first()

    if not backtest:
        raise HTTPException(status_code=404, detail="Backtest not found")

    if backtest.status != "completed":
        raise HTTPException(status_code=400, detail="Backtest not completed")

    trades = backtest.results_json.get("trades", []) if backtest.results_json else []

    return {
        "backtest_id": str(backtest.id),
        "trades": trades,
        "count": len(trades),
    }


@router.get("/list")
async def list_backtests(db: Session = Depends(get_db), limit: int = 50):
    """List recent backtests."""
    backtests = (
        db.query(Backtest)
        .order_by(Backtest.created_at.desc())
        .limit(limit)
        .all()
    )

    return [
        {
            "id": str(bt.id),
            "name": bt.name,
            "strategy_name": bt.strategy_name,
            "status": bt.status,
            "symbols": bt.symbols,
            "created_at": bt.created_at.isoformat(),
            "sharpe_ratio": bt.sharpe_ratio,
            "total_return": bt.total_return,
        }
        for bt in backtests
    ]


async def _run_backtest_job(backtest_id: UUID):
    """Background task to run backtest."""
    from backend.database import SessionLocal
    from loguru import logger

    db = SessionLocal()

    try:
        # Get backtest record
        backtest = db.query(Backtest).filter(Backtest.id == backtest_id).first()

        # Update status
        backtest.status = "running"
        backtest.started_at = datetime.utcnow()
        db.commit()

        logger.info(f"Starting backtest {backtest_id}")

        # Fetch data
        data_manager = DataManager(db)
        symbols_data = {}

        for i, symbol in enumerate(backtest.symbols):
            logger.info(f"Fetching data for {symbol}...")

            if backtest.timeframe == "1D":
                df = await data_manager.get_or_fetch_eod(
                    symbol,
                    backtest.exchange,
                    backtest.start_date.date(),
                    backtest.end_date.date(),
                )
            else:
                df = await data_manager.get_or_fetch_intraday(
                    symbol,
                    backtest.exchange,
                    backtest.timeframe.lower(),
                    backtest.start_date.date(),
                    backtest.end_date.date(),
                )

            if not df.empty:
                symbols_data[symbol] = df

            # Update progress (50% for data fetch)
            backtest.progress = ((i + 1) / len(backtest.symbols)) * 50
            db.commit()

        if not symbols_data:
            raise ValueError("No data available for any symbols")

        # Get strategy
        strategy_class = StrategyRegistry.get(backtest.strategy_name)

        # Run backtest
        logger.info(f"Running backtest with {len(symbols_data)} symbols...")
        engine = BacktestEngine()

        if len(symbols_data) == 1:
            # Single asset backtest
            symbol = list(symbols_data.keys())[0]
            result = engine.run_single_asset(
                symbols_data[symbol],
                strategy_class,
                backtest.strategy_params,
                cash=backtest.initial_cash,
                commission=backtest.commission,
            )
        else:
            # Portfolio backtest
            result = engine.run_portfolio(
                symbols_data,
                strategy_class,
                backtest.strategy_params,
                cash=backtest.initial_cash,
                commission=backtest.commission,
            )

        # Save results
        if result.get("success"):
            metrics = result.get("metrics") or result.get("portfolio_metrics", {})
            backtest.sharpe_ratio = metrics.get("sharpe_ratio", 0)
            backtest.total_return = metrics.get("total_return", 0)
            backtest.max_drawdown = metrics.get("max_drawdown", 0)
            backtest.num_trades = metrics.get("num_trades", 0)
            backtest.win_rate = metrics.get("win_rate", 0)
            backtest.results_json = result
            backtest.status = "completed"
        else:
            backtest.status = "failed"
            backtest.error_message = result.get("error", "Unknown error")

        backtest.progress = 100.0
        backtest.completed_at = datetime.utcnow()
        db.commit()

        logger.info(f"Backtest {backtest_id} completed")

    except Exception as e:
        logger.error(f"Backtest {backtest_id} failed: {e}")
        backtest.status = "failed"
        backtest.error_message = str(e)
        backtest.completed_at = datetime.utcnow()
        db.commit()

    finally:
        db.close()
