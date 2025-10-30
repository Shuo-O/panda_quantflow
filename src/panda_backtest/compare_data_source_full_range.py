#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Run full-range backtest comparison between Mongo and QuestDB data sources.

The script自动从 Mongo 指定集合中推算某个标的的最小/最大交易日，
并在该区间内分别运行 Mongo 和 QuestDB 回测，再比较关键指标。
"""

from __future__ import annotations

import argparse
import copy
import os
import time
from typing import Dict, Optional

from panda_backtest.main_local import Run

from common.config.config import get_config
from common.connector.mongodb_handler import DatabaseHandler

from compare_data_source_performance import (
    _compare_results,
    _run_once,
    DEFAULT_COMPARE_FIELDS,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Full-range Mongo vs QuestDB backtest comparison.")
    parser.add_argument("--file", default="strategy/future01.py", help="Strategy file path.")
    parser.add_argument("--collection", required=True, help="Mongo collection storing bars.")
    parser.add_argument("--symbol", required=True, help="Benchmark symbol (e.g. 000001.SH).")
    parser.add_argument("--symbol-field", default="symbol", help="Field name for symbol (default: symbol).")
    parser.add_argument("--date-field", default="trade_date", help="Date field name (default: trade_date).")
    parser.add_argument(
        "--date-type",
        choices=["int", "str"],
        default="int",
        help="Date field type: int (20240101) or str ('20240101').",
    )
    parser.add_argument("--frequency", default="1d", choices=["1d", "1M"], help="Bar frequency.")
    parser.add_argument("--matching-type", type=int, default=1, choices=[0, 1], help="Matching type.")
    parser.add_argument("--standard-symbol", default="000001.SH", help="Benchmark symbol used in backtest.")
    parser.add_argument("--start-capital", type=int, default=10_000_000, help="Initial stock capital.")
    parser.add_argument("--start-future-capital", type=int, default=10_000_000, help="Initial future capital.")
    parser.add_argument("--start-fund-capital", type=int, default=1_000_000, help="Initial fund capital.")
    parser.add_argument("--commission-rate", type=float, default=1.0, help="Commission multiplier.")
    parser.add_argument("--slippage", type=float, default=0.0, help="Slippage setting.")
    parser.add_argument("--silent", action="store_true", help="Suppress per-run logging.")
    parser.add_argument(
        "--compare-fields",
        nargs="*",
        default=DEFAULT_COMPARE_FIELDS,
        help="Fields to compare (default: %(default)s).",
    )
    parser.add_argument(
        "--tolerance",
        type=float,
        default=1e-6,
        help="Allowed absolute difference for numeric comparisons.",
    )
    parser.add_argument("--print-summary", action="store_true", help="Print Mongo/QuestDB result documents.")
    return parser.parse_args()


def fetch_range(
    db: DatabaseHandler,
    mongo_db: str,
    collection: str,
    symbol: str,
    symbol_field: str,
    date_field: str,
    date_type: str,
) -> Optional[Dict[str, int]]:
    coll = db.get_mongo_collection(mongo_db, collection)
    pipeline = [
        {"$match": {symbol_field: symbol}},
        {
            "$group": {
                "_id": None,
                "count": {"$sum": 1},
                "min_date": {"$min": f"${date_field}"},
                "max_date": {"$max": f"${date_field}"},
            }
        },
    ]
    result = list(coll.aggregate(pipeline, allowDiskUse=True))
    if not result:
        return None
    stats = result[0]
    count = int(stats.get("count", 0))
    min_raw = stats.get("min_date")
    max_raw = stats.get("max_date")
    if date_type == "int":
        min_date = int(min_raw)
        max_date = int(max_raw)
    else:
        min_date = int(str(min_raw))
        max_date = int(str(max_raw))
    return {"count": count, "min_date": min_date, "max_date": max_date}


def build_handle_message(args: argparse.Namespace, start_date: int, end_date: int) -> Dict[str, object]:
    return {
        "file": args.file,
        "run_params": "no_opz",
        "start_capital": args.start_capital,
        "start_future_capital": args.start_future_capital,
        "start_fund_capital": args.start_fund_capital,
        "start_date": str(start_date),
        "end_date": str(end_date),
        "standard_symbol": args.standard_symbol,
        "commission_rate": args.commission_rate,
        "slippage": args.slippage,
        "frequency": args.frequency,
        "matching_type": args.matching_type,
        "run_type": 1,
        "mock_id": "full-range",
        "account_id": "benchmark",
        "account_type": 0,
        "margin_rate": 1,
    }


def main():
    args = parse_args()
    config = get_config()
    mongo_db = config["MONGO_DB"]
    db_handler = DatabaseHandler(config)

    stats = fetch_range(
        db_handler,
        mongo_db,
        args.collection,
        args.symbol,
        args.symbol_field,
        args.date_field,
        args.date_type,
    )
    if stats is None:
        raise SystemExit(f"未在 {args.collection} 中找到 {args.symbol} 的记录。")

    start_date = stats["min_date"]
    end_date = stats["max_date"]
    count = stats["count"]
    print(f"[INFO] {args.symbol} 数据量: {count}, 日期范围 [{start_date}, {end_date}]")

    base_handle = build_handle_message(args, start_date, end_date)

    mongo_result = _run_once(base_handle, "mongo", args.silent)

    quest_handle = copy.deepcopy(base_handle)
    quest_result = _run_once(quest_handle, "questdb", args.silent)

    mongo_duration, mongo_run_id, mongo_doc = mongo_result
    quest_duration, quest_run_id, quest_doc = quest_result

    print("\n=== Performance summary ===")
    print(f"Mongo  : duration={mongo_duration:.2f}s run_id={mongo_run_id}")
    print(f"QuestDB: duration={quest_duration:.2f}s run_id={quest_run_id}")
    if quest_duration and mongo_duration:
        ratio = quest_duration / mongo_duration
        delta = quest_duration - mongo_duration
        word = "faster" if ratio < 1 else "slower"
        print(f"QuestDB is {abs(delta):.2f}s {word} (ratio={ratio:.2f}x).")

    differences = _compare_results(mongo_doc, quest_doc, args.compare_fields, args.tolerance)

    print("\n=== Result comparison ===")
    if args.print_summary:
        print("Mongo result:", mongo_doc)
        print("QuestDB result:", quest_doc)

    if differences:
        print("Differences detected:")
        for diff in differences:
            print(f" - {diff}")
    else:
        print("All selected fields match within tolerance.")


if __name__ == "__main__":
    main()
