"""Mean reversion strategy implementation."""

from backtesting.lib import crossover
from backtesting.test import SMA

from backend.strategies.base import BaseStrategy
from backend.strategies.registry import StrategyRegistry


@StrategyRegistry.register
class MeanReversionStrategy(BaseStrategy):
    """
    Mean Reversion Strategy.

    Entry Logic:
    - Price drops X% below moving average AND
    - RSI is oversold (< oversold_level)

    Exit Logic:
    - Price reaches moving average OR
    - Maximum hold period reached

    This strategy profits from temporary price deviations from the mean.
    """

    name = "Mean Reversion"
    description = (
        "Buys when price drops below moving average with oversold RSI. "
        "Exits when price returns to mean or after max hold period."
    )
    timeframe = "1D"
    requires_fundamentals = False

    # Strategy parameters (class attributes)
    ma_period = 20
    entry_pct = 5.0
    rsi_period = 14
    rsi_oversold = 30
    max_hold_days = 10

    def init(self):
        """Initialize indicators."""
        # Moving average
        self.ma = self.I(SMA, self.data.Close, self.ma_period)

        # RSI indicator
        self.rsi = self.I(self._rsi, self.data.Close, self.rsi_period)

        # Track entry bar for max hold period
        self.entry_bar = None

    def next(self):
        """Execute trading logic."""
        # Skip if not enough data
        if len(self.data) < self.ma_period:
            return

        # Calculate distance from MA
        price = self.data.Close[-1]
        ma_value = self.ma[-1]
        pct_below_ma = ((ma_value - price) / ma_value) * 100

        # Entry logic: Price significantly below MA + RSI oversold
        if not self.position:
            if pct_below_ma >= self.entry_pct and self.rsi[-1] < self.rsi_oversold:
                self.buy()
                self.entry_bar = len(self.data)

        # Exit logic
        else:
            # Exit if price crosses above MA
            if crossover(self.data.Close, self.ma):
                self.position.close()
                self.entry_bar = None

            # Exit if max hold period exceeded
            elif self.entry_bar and (len(self.data) - self.entry_bar) >= self.max_hold_days:
                self.position.close()
                self.entry_bar = None

    @staticmethod
    def _rsi(array, period):
        """Calculate RSI indicator."""
        import pandas as pd

        # Convert to pandas Series
        series = pd.Series(array)

        # Calculate price changes
        delta = series.diff()

        # Separate gains and losses
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()

        # Calculate RS and RSI
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))

        return rsi.values

    @classmethod
    def get_param_definitions(cls):
        """Return parameter definitions for UI."""
        return [
            {
                "name": "ma_period",
                "type": "int",
                "default": 20,
                "min": 5,
                "max": 200,
                "description": "Moving average period",
            },
            {
                "name": "entry_pct",
                "type": "float",
                "default": 5.0,
                "min": 1.0,
                "max": 20.0,
                "description": "Entry threshold: % below MA",
            },
            {
                "name": "rsi_period",
                "type": "int",
                "default": 14,
                "min": 5,
                "max": 50,
                "description": "RSI period",
            },
            {
                "name": "rsi_oversold",
                "type": "int",
                "default": 30,
                "min": 10,
                "max": 50,
                "description": "RSI oversold level",
            },
            {
                "name": "max_hold_days",
                "type": "int",
                "default": 10,
                "min": 1,
                "max": 100,
                "description": "Maximum hold period (days)",
            },
        ]
