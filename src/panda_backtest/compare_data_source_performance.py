#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Benchmark helper comparing Mongo and QuestDB quotation backends.

The script runs the same backtest twice—once with the default Mongo based
data source and once with the QuestDB implementation—measuring wall clock
time for both runs.  This helps quantify performance differences introduced
by switching historical data providers.
"""

from __future__ import annotations

import argparse
import copy
import os
import time
import uuid
from typing import Dict, List, Tuple

from panda_backtest.main_local import Run

from bson import ObjectId
from common.config.config import get_config
from common.connector.mongodb_handler import DatabaseHandler

# Fields to compare between data sources
DEFAULT_COMPARE_FIELDS = [
    "back_profit",
    "back_profit_year",
    "benchmark_profit",
    "max_drawdown",
    "sharpe",
    "volatility",
    "information_ratio",
]

_CONFIG = get_config()
_DB = DatabaseHandler(_CONFIG)
# Pre-cache DB name
_MONGO_DB = _CONFIG["MONGO_DB"]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare Mongo vs QuestDB backtest performance.")
    parser.add_argument("--file", default="strategy/future01.py", help="Strategy file path.")
    parser.add_argument("--start-date", default="20241122", help="Backtest start date (YYYYMMDD).")
    parser.add_argument("--end-date", default="20241201", help="Backtest end date (YYYYMMDD).")
    parser.add_argument("--frequency", default="1d", choices=["1d", "1M"], help="Bar frequency.")
    parser.add_argument("--matching-type", type=int, default=1, choices=[0, 1], help="Matching type.")
    parser.add_argument("--repeats", type=int, default=1, help="Number of repetitions per data source.")
    parser.add_argument("--standard-symbol", default="000001.SH", help="Benchmark symbol.")
    parser.add_argument("--start-capital", type=int, default=10_000_000, help="Initial stock capital.")
    parser.add_argument("--start-future-capital", type=int, default=10_000_000, help="Initial future capital.")
    parser.add_argument("--start-fund-capital", type=int, default=1_000_000, help="Initial fund capital.")
    parser.add_argument("--commission-rate", type=float, default=1.0, help="Commission multiplier.")
    parser.add_argument("--slippage", type=float, default=0.0, help="Slippage setting.")
    parser.add_argument("--silent", action="store_true", help="Suppress detailed per-run output.")
    parser.add_argument(
        "--compare-fields",
        nargs="*",
        default=DEFAULT_COMPARE_FIELDS,
        help="Numeric fields to compare between Mongo and QuestDB results (default: %(default)s).",
    )
    parser.add_argument(
        "--tolerance",
        type=float,
        default=1e-6,
        help="Allowed absolute difference when comparing result fields.",
    )
    parser.add_argument(
        "--print-summary",
        action="store_true",
        help="Print the result document for each run (debugging aid).",
    )
    return parser.parse_args()


def _base_handle_message(args: argparse.Namespace) -> Dict[str, object]:
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
        "mock_id": "perf",
        "account_id": "benchmark",
        "account_type": 0,
        "margin_rate": 1,
    }


def _random_back_test_id() -> str:
    """Generate an ObjectId-compatible hex string."""
    return uuid.uuid4().hex[:24]


def _fetch_result(run_id: str, retries: int = 5, wait: float = 0.5) -> Dict[str, object]:
    """Fetch summary result from Mongo, retrying if needed."""
    query = {"_id": ObjectId(run_id)}
    for attempt in range(retries):
        doc = _DB.mongo_find_one(_MONGO_DB, "panda_back_test", query)
        if doc:
            return doc
        time.sleep(wait)
    raise RuntimeError(f"Cannot find result for run_id={run_id} after {retries} retries.")


def _run_once(base: Dict[str, object], data_source: str, silent: bool) -> Tuple[float, str, Dict[str, object]]:
    handle_message = copy.deepcopy(base)
    handle_message["back_test_id"] = _random_back_test_id()
    if data_source != "mongo":
        handle_message["data_source"] = data_source

    if not silent:
        print(f"[{data_source}] start pid={os.getpid()}, back_test_id={handle_message['back_test_id']}")
    start = time.perf_counter()
    try:
        Run.start(handle_message)
    except Exception as exc:  # noqa: BLE001 - propagate with context
        if data_source == "questdb":
            raise RuntimeError(
                "QuestDB run failed. Ensure QUESTDB_ENABLE=true and tables are populated."
            ) from exc
        raise
    duration = time.perf_counter() - start
    result_doc = _fetch_result(handle_message["back_test_id"])
    if not silent:
        print(f"[{data_source}] finished in {duration:.2f}s")
    return duration, handle_message["back_test_id"], result_doc


def _extract_fields(doc: Dict[str, object], fields: List[str]) -> Dict[str, float]:
    extracted = {}
    for field in fields:
        value = doc.get(field)
        if value is None:
            extracted[field] = None
        else:
            try:
                extracted[field] = float(value)
            except (TypeError, ValueError):
                extracted[field] = value
    return extracted


def _compare_results(
    mongo_doc: Dict[str, object],
    quest_doc: Dict[str, object],
    fields: List[str],
    tolerance: float,
) -> List[str]:
    differences = []
    for field in fields:
        mongo_value = mongo_doc.get(field)
        quest_value = quest_doc.get(field)
        if mongo_value is None and quest_value is None:
            continue
        if mongo_value is None or quest_value is None:
            differences.append(f"{field}: mongo={mongo_value}, questdb={quest_value} (one side missing)")
            continue
        try:
            mongo_float = float(mongo_value)
            quest_float = float(quest_value)
            if abs(mongo_float - quest_float) > tolerance:
                differences.append(
                    f"{field}: mongo={mongo_float:.10f}, questdb={quest_float:.10f}, Δ={mongo_float - quest_float:.10f}"
                )
        except (TypeError, ValueError):
            if mongo_value != quest_value:
                differences.append(f"{field}: mongo={mongo_value}, questdb={quest_value} (non-numeric mismatch)")
    return differences


def main():
    args = _parse_args()
    base_handle = _base_handle_message(args)

    results: Dict[str, List[Tuple[float, str, Dict[str, object]]]] = {"mongo": [], "questdb": []}
    for data_source in ("mongo", "questdb"):
        for _ in range(args.repeats):
            results[data_source].append(_run_once(base_handle, data_source, args.silent))

    def _stats(values):
        if not values:
            return 0.0, 0.0
        durations = [item[0] for item in values]
        return min(durations), sum(durations) / len(durations)

    mongo_min, mongo_avg = _stats(results["mongo"])
    quest_min, quest_avg = _stats(results["questdb"])

    print("\n=== Performance summary ===")
    print(f"Mongo  : runs={len(results['mongo'])} min={mongo_min:.2f}s avg={mongo_avg:.2f}s")
    print(f"QuestDB: runs={len(results['questdb'])} min={quest_min:.2f}s avg={quest_avg:.2f}s")

    if quest_avg and mongo_avg:
        delta = quest_avg - mongo_avg
        ratio = quest_avg / mongo_avg
        word = "faster" if ratio < 1 else "slower"
        print(f"QuestDB is {abs(delta):.2f}s {word} on average (ratio={ratio:.2f}x).")

    # Compare backtest outputs using the first run from each data source
    if results["mongo"] and results["questdb"]:
        mongo_duration, mongo_run_id, mongo_doc = results["mongo"][0]
        quest_duration, quest_run_id, quest_doc = results["questdb"][0]
        differences = _compare_results(mongo_doc, quest_doc, args.compare_fields, args.tolerance)

        print("\n=== Result comparison ===")
        print(f"Mongo run_id:   {mongo_run_id}")
        print(f"QuestDB run_id: {quest_run_id}")
        if args.print_summary:
            print("\nMongo result document:")
            print(mongo_doc)
            print("\nQuestDB result document:")
            print(quest_doc)

        if differences:
            print("\nDifferences detected:")
            for diff in differences:
                print(f" - {diff}")
        else:
            print("All selected fields match within tolerance.")
    else:
        print("\nSkipping result comparison because one of the data sources did not run.")


if __name__ == "__main__":
    main()
