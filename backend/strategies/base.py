"""Base strategy class for all trading strategies."""

from typing import Dict, Any, List
from backtesting import Strategy


class BaseStrategy(Strategy):
    """
    Base class for all trading strategies.

    All strategies must implement:
    - init(): Initialize indicators
    - next(): Define entry/exit logic
    - get_param_definitions(): Return parameter definitions for UI

    Attributes:
        name: Strategy display name
        description: Strategy description
        timeframe: Recommended timeframe (1D, 1H, etc.)
        requires_fundamentals: Whether strategy needs fundamental data
    """

    name: str = "Base Strategy"
    description: str = "Base strategy class"
    timeframe: str = "1D"
    requires_fundamentals: bool = False

    def init(self):
        """
        Initialize indicators.

        Called once before backtest starts.
        Use self.I() to wrap indicator functions.
        """
        raise NotImplementedError("Strategies must implement init()")

    def next(self):
        """
        Execute trading logic for current bar.

        Called for each new bar in the backtest.
        Use self.buy() and self.sell() to enter/exit positions.
        """
        raise NotImplementedError("Strategies must implement next()")

    @classmethod
    def get_param_definitions(cls) -> List[Dict[str, Any]]:
        """
        Return parameter definitions for UI.

        Returns:
            List of parameter definitions with format:
            [
                {
                    'name': 'param_name',
                    'type': 'int' | 'float' | 'bool' | 'str',
                    'default': value,
                    'min': min_value (for numeric),
                    'max': max_value (for numeric),
                    'description': 'Parameter description'
                },
                ...
            ]
        """
        return []

    @classmethod
    def get_info(cls) -> Dict[str, Any]:
        """Get strategy metadata."""
        return {
            "name": cls.name,
            "description": cls.description,
            "timeframe": cls.timeframe,
            "requires_fundamentals": cls.requires_fundamentals,
            "parameters": cls.get_param_definitions(),
        }
