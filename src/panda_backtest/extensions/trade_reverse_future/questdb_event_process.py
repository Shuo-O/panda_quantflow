#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
QuestDB-backed event process.

Reuses the existing reverse event workflow but swaps the bar data source so
the engine reads historical bars from QuestDB instead of Mongo.
"""

from panda_backtest.data.quotation.questdb_bar_data_source import QuestDBBarDataSource
from panda_backtest.backtest_common.data.quotation.back_test.bar_map import BarMap
from panda_backtest.backtest_common.data.quotation.quotation_data import QuotationData
from panda_backtest.extensions.trade_reverse_future.reverse_event_process import (
    ReverseEventProcess,
)


class QuestDBReverseEventProcess(ReverseEventProcess):
    """Event process that initialises QuestDB-backed quotation data."""

    def init_backtest_params(self, handle_message):
        strategy_context = self._context.strategy_context
        strategy_context.init_run_info(handle_message)
        strategy_context.init_trade_time_manager(self.trade_time_manager)

        bar_map = BarMap(QuestDBBarDataSource())
        QuotationData.get_instance().init_bar_dict(bar_map)
        self.init_data()
