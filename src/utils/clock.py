"""Trading calendar and date utilities."""

from datetime import datetime, timedelta
from typing import List, Optional

import pandas as pd
import numpy as np


class TradingCalendar:
    """
    Simple trading calendar implementation.

    For production, consider using pandas_market_calendars or exchange-specific calendars.
    This implementation excludes weekends and major US holidays.
    """

    # Major US holidays (approximate - does not handle shifting rules)
    US_HOLIDAYS = [
        "2018-01-01", "2018-01-15", "2018-02-19", "2018-03-30", "2018-05-28", "2018-07-04",
        "2018-09-03", "2018-11-22", "2018-12-25",
        "2019-01-01", "2019-01-21", "2019-02-18", "2019-04-19", "2019-05-27", "2019-07-04",
        "2019-09-02", "2019-11-28", "2019-12-25",
        "2020-01-01", "2020-01-20", "2020-02-17", "2020-04-10", "2020-05-25", "2020-07-03",
        "2020-09-07", "2020-11-26", "2020-12-25",
        "2021-01-01", "2021-01-18", "2021-02-15", "2021-04-02", "2021-05-31", "2021-07-05",
        "2021-09-06", "2021-11-25", "2021-12-24",
        "2022-01-17", "2022-02-21", "2022-04-15", "2022-05-30", "2022-07-04", "2022-09-05",
        "2022-11-24", "2022-12-26",
        "2023-01-02", "2023-01-16", "2023-02-20", "2023-04-07", "2023-05-29", "2023-07-04",
        "2023-09-04", "2023-11-23", "2023-12-25",
        "2024-01-01", "2024-01-15", "2024-02-19", "2024-03-29", "2024-05-27", "2024-07-04",
        "2024-09-02", "2024-11-28", "2024-12-25",
        "2025-01-01", "2025-01-20", "2025-02-17", "2025-04-18", "2025-05-26", "2025-07-04",
        "2025-09-01", "2025-11-27", "2025-12-25",
    ]

    def __init__(self, start_date: str, end_date: str, exchange: str = "US"):
        """
        Initialize trading calendar.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            exchange: Exchange identifier (default US)
        """
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.exchange = exchange

        # Generate trading days
        self._trading_days = self._generate_trading_days()

    def _generate_trading_days(self) -> pd.DatetimeIndex:
        """Generate list of valid trading days."""
        # Start with all calendar days
        all_days = pd.date_range(start=self.start_date, end=self.end_date, freq="D")

        # Filter weekends
        weekdays = all_days[all_days.dayofweek < 5]

        # Filter holidays (US specific)
        if self.exchange == "US":
            holidays = pd.to_datetime(self.US_HOLIDAYS)
            trading_days = weekdays[~weekdays.isin(holidays)]
        else:
            # For other exchanges, just exclude weekends for now
            trading_days = weekdays

        return trading_days

    @property
    def trading_days(self) -> pd.DatetimeIndex:
        """Get all trading days in the calendar."""
        return self._trading_days

    def is_trading_day(self, date: pd.Timestamp) -> bool:
        """Check if a date is a trading day."""
        return date in self._trading_days

    def next_trading_day(self, date: pd.Timestamp, n: int = 1) -> pd.Timestamp:
        """
        Get the nth trading day after the given date.

        Args:
            date: Reference date
            n: Number of trading days ahead (default 1)

        Returns:
            Next trading day
        """
        idx = self._trading_days.searchsorted(date)
        if idx + n < len(self._trading_days):
            return self._trading_days[idx + n]
        return self._trading_days[-1]

    def prev_trading_day(self, date: pd.Timestamp, n: int = 1) -> pd.Timestamp:
        """
        Get the nth trading day before the given date.

        Args:
            date: Reference date
            n: Number of trading days back (default 1)

        Returns:
            Previous trading day
        """
        idx = self._trading_days.searchsorted(date)
        if idx - n >= 0:
            return self._trading_days[idx - n]
        return self._trading_days[0]

    def offset_trading_days(self, date: pd.Timestamp, offset: int) -> pd.Timestamp:
        """
        Offset a date by N trading days.

        Args:
            date: Reference date
            offset: Number of trading days (positive or negative)

        Returns:
            Offset date
        """
        if offset >= 0:
            return self.next_trading_day(date, offset)
        else:
            return self.prev_trading_day(date, -offset)

    def get_trading_days_between(
        self, start: pd.Timestamp, end: pd.Timestamp
    ) -> pd.DatetimeIndex:
        """Get trading days between two dates (inclusive)."""
        mask = (self._trading_days >= start) & (self._trading_days <= end)
        return self._trading_days[mask]

    def count_trading_days(self, start: pd.Timestamp, end: pd.Timestamp) -> int:
        """Count trading days between two dates."""
        return len(self.get_trading_days_between(start, end))


def get_rebalance_dates(
    start_date: str,
    end_date: str,
    frequency: str = "weekly",
    calendar: Optional[TradingCalendar] = None,
) -> List[pd.Timestamp]:
    """
    Generate rebalance dates based on frequency.

    Args:
        start_date: Start date
        end_date: End date
        frequency: Rebalance frequency (weekly, biweekly, monthly)
        calendar: Trading calendar (optional)

    Returns:
        List of rebalance dates
    """
    if calendar is None:
        calendar = TradingCalendar(start_date, end_date)

    trading_days = calendar.trading_days

    if frequency == "weekly":
        # First trading day of each week
        rebalance_dates = []
        current_week = None
        for day in trading_days:
            week = day.isocalendar()[1]
            if week != current_week:
                rebalance_dates.append(day)
                current_week = week

    elif frequency == "biweekly":
        # Every other week
        rebalance_dates = []
        current_week = None
        count = 0
        for day in trading_days:
            week = day.isocalendar()[1]
            if week != current_week:
                if count % 2 == 0:
                    rebalance_dates.append(day)
                count += 1
                current_week = week

    elif frequency == "monthly":
        # First trading day of each month
        rebalance_dates = []
        current_month = None
        for day in trading_days:
            month = (day.year, day.month)
            if month != current_month:
                rebalance_dates.append(day)
                current_month = month

    else:
        raise ValueError(f"Unknown frequency: {frequency}")

    return rebalance_dates


def align_to_trading_day(date: pd.Timestamp, calendar: TradingCalendar) -> pd.Timestamp:
    """
    Align a date to the next trading day if it's not a trading day.

    Args:
        date: Input date
        calendar: Trading calendar

    Returns:
        Aligned trading day
    """
    if calendar.is_trading_day(date):
        return date
    return calendar.next_trading_day(date, n=0)
