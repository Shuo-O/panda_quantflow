#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time   : 18-3-27 下午4:47
# @Author : wlb
# @File   : __init__.py.py
# @desc   :

import os
from typing import Optional


def load_extension(data_source: Optional[str] = None):
    """
    Load the trading extension.

    Args:
        data_source: Optional name of the quotation backend. Supported values:
            ``"mongo"`` (default) and ``"questdb"``.

    The value falls back to the environment variable
    ``PANDA_BACKTEST_DATA_SOURCE`` when not provided.
    """
    from panda_backtest.extensions.trade_reverse_future.main import (
        FutureTradingExtension,
        QuestDBFutureTradingExtension,
    )

    source = (data_source or os.getenv("PANDA_BACKTEST_DATA_SOURCE", "mongo")).lower()

    if source in {"questdb", "quest"}:
        return QuestDBFutureTradingExtension()

    return FutureTradingExtension()
