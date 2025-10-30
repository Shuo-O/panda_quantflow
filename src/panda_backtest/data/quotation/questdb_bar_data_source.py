#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
QuestDB bar data source.

This module provides a drop-in replacement for the default Mongo-backed
``BarDataSource`` so the backtesting engine can fetch historical data
directly from QuestDB.  The implementation keeps the same caching semantics
as the Mongo variant while translating QuestDB JSON responses into the
existing ``DailyQuotationData`` / ``BarQuotationData`` structures.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Iterable, Optional

from common.config.config import get_config
from common.connector.questdb_client import QuestDBClient
from panda_backtest.backtest_common.data.quotation.back_test.base_bar_data_source import (
    BaseBarDataSource,
)
from panda_backtest.backtest_common.model.quotation.bar_quotation_data import (
    BarQuotationData,
)
from panda_backtest.backtest_common.model.quotation.daily_quotation_data import (
    DailyQuotationData,
)


class QuestDBBarDataSource(BaseBarDataSource):
    """
    Fetches bar data from QuestDB using the HTTP endpoint.

    The following QuestDB tables are used by default and can be overridden via
    environment variables (or ``common.config``):

    - ``QUESTDB_STOCK_DAILY_TABLE`` (default: ``stock_daily_bars``)
    - ``QUESTDB_STOCK_MINUTE_TABLE`` (default: ``stock_minute_bars``)
    - ``QUESTDB_FUTURE_DAILY_TABLE`` (default: ``future_daily_bars``)
    - ``QUESTDB_FUTURE_MINUTE_TABLE`` (default: ``future_minute_bars``)
    - ``QUESTDB_FUND_DAILY_TABLE`` (default: ``fund_daily_bars``)

    The tables are expected to expose columns that match the names consumed by
    the original Mongo pipelines (``open``, ``high``, ``low``, ``close``,
    ``volume`` ...).  Missing columns are tolerated and will default to zero.
    """

    def __init__(self):
        self._client = QuestDBClient.instance()
        if self._client is None:
            raise RuntimeError(
                "QuestDB is disabled. Set QUESTDB_ENABLE=true to use QuestDBBarDataSource."
            )

        cfg = get_config()
        self._tables = {
            "stock_daily": os.getenv("QUESTDB_STOCK_DAILY_TABLE", cfg.get("QUESTDB_STOCK_DAILY_TABLE", "stock_daily_bars")),
            "stock_minute": os.getenv("QUESTDB_STOCK_MINUTE_TABLE", cfg.get("QUESTDB_STOCK_MINUTE_TABLE", "stock_minute_bars")),
            "future_daily": os.getenv("QUESTDB_FUTURE_DAILY_TABLE", cfg.get("QUESTDB_FUTURE_DAILY_TABLE", "future_daily_bars")),
            "future_minute": os.getenv("QUESTDB_FUTURE_MINUTE_TABLE", cfg.get("QUESTDB_FUTURE_MINUTE_TABLE", "future_minute_bars")),
            "fund_daily": os.getenv("QUESTDB_FUND_DAILY_TABLE", cfg.get("QUESTDB_FUND_DAILY_TABLE", "fund_daily_bars")),
        }

        self._last_field = "close"

        self._stock_daily_cache: Dict[str, DailyQuotationData] = {}
        self._stock_minute_cache: Dict[str, Dict[int, BarQuotationData]] = {}
        self._future_daily_cache: Dict[str, DailyQuotationData] = {}
        self._future_minute_cache: Dict[str, Dict[int, BarQuotationData]] = {}
        self._fund_daily_cache: Dict[str, DailyQuotationData] = {}

    # ------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------
    def _run_query(self, sql: str, limit: int = 1) -> Optional[Dict[str, Any]]:
        result = self._client.query(sql, limit=limit)
        if not result or not result.get("dataset"):
            return None
        columns = [col["name"] for col in result.get("columns", [])]
        row = result["dataset"][0]
        return dict(zip(columns, row))

    @staticmethod
    def _float(value) -> float:
        if value is None:
            return 0.0
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _int(value) -> int:
        if value is None:
            return 0
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    def _build_daily_bar(self, symbol: str, data: Dict[str, Any]) -> DailyQuotationData:
        bar = DailyQuotationData()
        bar.symbol = symbol
        bar.trade_date = self._int(data.get("trade_date") or data.get("date"))
        bar.code = data.get("code", "")
        bar.open = self._float(data.get("open"))
        bar.high = self._float(data.get("high"))
        bar.low = self._float(data.get("low"))
        bar.close = self._float(data.get("close"))
        bar.volume = self._float(data.get("volume"))
        bar.turnover = self._float(data.get("turnover"))
        bar.vwap = self._float(data.get("vwap"))
        bar.settlement = self._float(data.get("settlement"))
        bar.oi = self._float(data.get("oi"))
        bar.trade_status = data.get("trade_status", "")
        bar.unit_nav = self._float(data.get("unit_nav"))
        bar.last = getattr(bar, self._last_field, bar.close)
        bar.price_limit_range = self._float(data.get("price_limit_range"))
        return bar

    def _build_minute_bar(
        self, symbol: str, data: Dict[str, Any]
    ) -> BarQuotationData:
        bar = BarQuotationData()
        bar.symbol = symbol
        bar.code = data.get("code", "")
        bar.trade_date = self._int(data.get("trade_date") or data.get("date"))
        bar.date = bar.trade_date
        bar.time = self._int(data.get("time") or data.get("ts") or data.get("timestamp"))
        bar.freq = self._int(data.get("freq") or data.get("interval") or 0)
        bar.open = self._float(data.get("open"))
        bar.high = self._float(data.get("high"))
        bar.low = self._float(data.get("low"))
        bar.close = self._float(data.get("close"))
        bar.volume = self._float(data.get("volume"))
        bar.turnover = self._float(data.get("turnover"))
        bar.vwap = self._float(data.get("vwap"))
        bar.oi = self._float(data.get("oi"))
        bar.settlement = self._float(data.get("settlement"))
        bar.preclose = self._float(data.get("preclose"))
        bar.limit_up = self._float(data.get("limit_up"))
        bar.limit_down = self._float(data.get("limit_down"))
        bar.last = getattr(bar, self._last_field, bar.close)
        return bar

    def _prefetch_daily(
        self, table_key: str, symbol_list: Iterable[str], trade_date: int, cache: Dict[str, DailyQuotationData]
    ) -> None:
        symbols = list(symbol_list)
        if not symbols:
            return
        table = self._tables[table_key]
        quoted = ",".join(f"'{sym}'" for sym in symbols)
        sql = (
            f"SELECT * FROM {table} "
            f"WHERE symbol IN ({quoted}) AND trade_date = {trade_date}"
        )
        result = self._client.query(sql, limit=len(symbols) * 5 or 1000)
        if not result or not result.get("dataset"):
            return
        columns = [col["name"] for col in result.get("columns", [])]
        for row in result["dataset"]:
            row_dict = dict(zip(columns, row))
            symbol = row_dict.get("symbol")
            if symbol:
                cache[symbol] = self._build_daily_bar(symbol, row_dict)

    def _prefetch_minute(
        self,
        table_key: str,
        symbol: str,
        trade_date: int,
        cache: Dict[str, Dict[int, BarQuotationData]],
    ) -> None:
        table = self._tables[table_key]
        sql = (
            f"SELECT * FROM {table} "
            f"WHERE symbol = '{symbol}' AND trade_date = {trade_date} "
            f"ORDER BY time"
        )
        result = self._client.query(sql, limit=100000)
        if not result or not result.get("dataset"):
            return
        columns = [col["name"] for col in result.get("columns", [])]
        symbol_cache: Dict[int, BarQuotationData] = {}
        for row in result["dataset"]:
            row_dict = dict(zip(columns, row))
            bar = self._build_minute_bar(symbol, row_dict)
            symbol_cache[bar.time] = bar
        cache[symbol] = symbol_cache

    # ------------------------------------------------------------------
    # BaseBarDataSource interface
    # ------------------------------------------------------------------
    def get_stock_daily_bar(self, symbol, date):
        if symbol in self._stock_daily_cache:
            bar = self._stock_daily_cache[symbol]
            bar.last = getattr(bar, self._last_field, bar.close)
            return bar
        table = self._tables["stock_daily"]
        sql = (
            f"SELECT * FROM {table} "
            f"WHERE symbol = '{symbol}' AND trade_date = {date} "
            "ORDER BY ts DESC LIMIT 1"
        )
        row = self._run_query(sql)
        if row:
            bar = self._build_daily_bar(symbol, row)
            self._stock_daily_cache[symbol] = bar
            return bar
        return DailyQuotationData()

    def get_stock_minute_bar(self, symbol, date, time_value):
        time_value = int(time_value)
        if symbol not in self._stock_minute_cache:
            self._prefetch_minute("stock_minute", symbol, int(date), self._stock_minute_cache)
        symbol_cache = self._stock_minute_cache.get(symbol, {})
        bar = symbol_cache.get(time_value)
        if bar:
            bar.last = getattr(bar, self._last_field, bar.close)
            return bar
        return BarQuotationData()

    def get_future_daily_bar(self, symbol, date):
        if symbol in self._future_daily_cache:
            bar = self._future_daily_cache[symbol]
            bar.last = getattr(bar, self._last_field, bar.close)
            return bar
        table = self._tables["future_daily"]
        sql = (
            f"SELECT * FROM {table} "
            f"WHERE symbol = '{symbol}' AND trade_date = {date} "
            "ORDER BY ts DESC LIMIT 1"
        )
        row = self._run_query(sql)
        if row:
            bar = self._build_daily_bar(symbol, row)
            self._future_daily_cache[symbol] = bar
            return bar
        return DailyQuotationData()

    def get_future_minute_bar(self, symbol, date, time_value):
        time_value = int(time_value)
        if symbol not in self._future_minute_cache:
            self._prefetch_minute("future_minute", symbol, int(date), self._future_minute_cache)
        symbol_cache = self._future_minute_cache.get(symbol, {})
        bar = symbol_cache.get(time_value)
        if bar:
            bar.last = getattr(bar, self._last_field, bar.close)
            return bar
        return BarQuotationData()

    def init_future_list_daily_quotation(self, position_list, date, freq="d"):
        if not position_list:
            return
        self._prefetch_daily("future_daily", position_list, int(date), self._future_daily_cache)

    def init_stock_list_daily_quotation(self, position_list, date, freq="d"):
        if not position_list:
            return
        self._prefetch_daily("stock_daily", position_list, int(date), self._stock_daily_cache)

    def clear_cache_data(self):
        self._stock_daily_cache.clear()
        self._stock_minute_cache.clear()
        self._future_daily_cache.clear()
        self._future_minute_cache.clear()
        self._fund_daily_cache.clear()

    def get_fund_daily_bar(self, symbol, trade_date):
        if symbol in self._fund_daily_cache:
            bar = self._fund_daily_cache[symbol]
            bar.last = getattr(bar, self._last_field, bar.close)
            return bar
        table = self._tables["fund_daily"]
        sql = (
            f"SELECT * FROM {table} "
            f"WHERE symbol = '{symbol}' AND trade_date = {trade_date} "
            "ORDER BY ts DESC LIMIT 1"
        )
        row = self._run_query(sql)
        if row:
            bar = self._build_daily_bar(symbol, row)
            self._fund_daily_cache[symbol] = bar
            return bar
        return DailyQuotationData()

    def change_last_field(self, field_type):
        self._last_field = "open" if int(field_type) == 0 else "close"
