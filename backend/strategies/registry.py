"""Strategy registry for managing available strategies."""

from typing import Dict, Type, List

from backend.strategies.base import BaseStrategy


class StrategyRegistry:
    """
    Central registry for all available strategies.

    Strategies auto-register via decorator.
    """

    _strategies: Dict[str, Type[BaseStrategy]] = {}

    @classmethod
    def register(cls, strategy_class: Type[BaseStrategy]):
        """
        Register a strategy class.

        Usage:
            @StrategyRegistry.register
            class MyStrategy(BaseStrategy):
                ...
        """
        cls._strategies[strategy_class.name] = strategy_class
        return strategy_class

    @classmethod
    def get_all(cls) -> Dict[str, Type[BaseStrategy]]:
        """Get all registered strategies."""
        return cls._strategies.copy()

    @classmethod
    def get(cls, name: str) -> Type[BaseStrategy]:
        """Get strategy by name."""
        if name not in cls._strategies:
            raise ValueError(f"Strategy '{name}' not found in registry")
        return cls._strategies[name]

    @classmethod
    def list_strategies(cls) -> List[Dict]:
        """Get list of strategy metadata."""
        return [
            strategy_class.get_info()
            for strategy_class in cls._strategies.values()
        ]

    @classmethod
    def get_strategy_params(cls, name: str) -> List[Dict]:
        """Get parameter definitions for a strategy."""
        strategy = cls.get(name)
        return strategy.get_param_definitions()
