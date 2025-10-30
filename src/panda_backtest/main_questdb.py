#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
QuestDB backtest runner.

Provides a convenient entry point to execute the existing backtesting
pipeline while forcing the quotation backend to use QuestDB instead of
MongoDB.  Usage mirrors ``Test.py`` but exposes a few command line flags so
engineers can swap strategy files and date ranges quickly.
"""

from __future__ import annotations

import argparse
import os
import time
import uuid

from panda_backtest.main_local import Run


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run backtest using QuestDB data source.")
    parser.add_argument("--file", default="strategy/future01.py", help="Strategy file path (relative to project root).")
    parser.add_argument("--start-date", default="20241122", help="Backtest start date (YYYYMMDD).")
    parser.add_argument("--end-date", default="20241201", help="Backtest end date (YYYYMMDD).")
    parser.add_argument("--start-capital", type=int, default=10_000_000, help="Initial stock capital.")
    parser.add_argument("--start-future-capital", type=int, default=10_000_000, help="Initial future capital.")
    parser.add_argument("--start-fund-capital", type=int, default=1_000_000, help="Initial fund capital.")
    parser.add_argument("--frequency", default="1d", choices=["1d", "1M"], help="Bar frequency.")
    parser.add_argument("--matching-type", type=int, default=1, choices=[0, 1], help="0=close match, 1=open match.")
    parser.add_argument("--mock-id", default="100", help="Mock id for logging differentiation.")
    parser.add_argument("--account-id", default="15032863", help="Primary stock account id.")
    parser.add_argument("--commission-rate", type=float, default=1.0, help="Commission multiplier.")
    parser.add_argument("--slippage", type=float, default=0.0, help="Slippage setting.")
    parser.add_argument("--standard-symbol", default="000001.SH", help="Benchmark symbol.")
    parser.add_argument("--back-test-id", default=None, help="Optional back test id (defaults to random).")
    return parser.parse_args()


def _build_handle_message(args: argparse.Namespace) -> dict:
    back_test_id = args.back_test_id or uuid.uuid4().hex[:24]
    return {
        "file": args.file,
        "run_params": "no_opz",
        "start_capital": args.start_capital,
        "start_future_capital": args.start_future_capital,
        "start_fund_capital": args.start_fund_capital,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "standard_symbol": args.standard_symbol,
        "commission_rate": args.commission_rate,
        "slippage": args.slippage,
        "frequency": args.frequency,
        "matching_type": args.matching_type,
        "run_type": 1,
        "back_test_id": back_test_id,
        "mock_id": args.mock_id,
        "account_id": args.account_id,
        "account_type": 0,
        "margin_rate": 1,
        "data_source": "questdb",
    }


def main():
    args = _parse_args()
    handle_message = _build_handle_message(args)
    print(f"QuestDB backtest starting (pid={os.getpid()}, back_test_id={handle_message['back_test_id']})")
    start_time = time.perf_counter()
    Run.start(handle_message)
    duration = time.perf_counter() - start_time
    print(f"QuestDB backtest finished in {duration:.2f}s")


if __name__ == "__main__":
    main()
