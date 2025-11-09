"""Strategy API endpoints."""

from typing import List, Dict, Any

from fastapi import APIRouter, HTTPException

from backend.strategies.registry import StrategyRegistry

# Import all strategies to register them
from backend.strategies import mean_reversion  # noqa

router = APIRouter()


@router.get("/list")
async def list_strategies() -> List[Dict[str, Any]]:
    """Get list of all available strategies."""
    return StrategyRegistry.list_strategies()


@router.get("/{strategy_name}/info")
async def get_strategy_info(strategy_name: str) -> Dict[str, Any]:
    """Get detailed information about a strategy."""
    try:
        strategy = StrategyRegistry.get(strategy_name)
        return strategy.get_info()
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/{strategy_name}/params")
async def get_strategy_params(strategy_name: str) -> List[Dict[str, Any]]:
    """Get parameter definitions for a strategy."""
    try:
        return StrategyRegistry.get_strategy_params(strategy_name)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
