#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Inspect MongoDB bar collections: count documents and show date range.

Usage:
    python tools/mongo_bar_stats.py \
        --collection index_daily_price \
        --symbols 000001.SH 399001.SZ \
        --date-field date \
        --date-type str

Set ``--all-symbols`` to scan every symbol in the collection (may take time).
"""

from __future__ import annotations

import argparse
from typing import Iterable, List, Optional

from common.config.config import get_config
from common.connector.mongodb_handler import DatabaseHandler


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Mongo bar stats.")
    parser.add_argument("--collection", required=True, help="Mongo collection name.")
    parser.add_argument(
        "--symbols",
        nargs="+",
        help="Symbols to inspect (e.g. 000001.SH). Mutually exclusive with --all-symbols.",
    )
    parser.add_argument(
        "--all-symbols",
        action="store_true",
        help="Inspect every distinct symbol in the collection.",
    )
    parser.add_argument(
        "--symbol-field",
        default="symbol",
        help="Field that stores the symbol (default: symbol).",
    )
    parser.add_argument(
        "--date-field",
        default="trade_date",
        help="Field that stores the trade date (default: trade_date).",
    )
    parser.add_argument(
        "--date-type",
        choices=["int", "str"],
        default="int",
        help="Date field type in Mongo (int like 20240101 or str).",
    )
    return parser.parse_args()


def distinct_symbols(
    db: DatabaseHandler, mongo_db: str, collection: str, symbol_field: str
) -> List[str]:
    coll = db.get_mongo_collection(mongo_db, collection)
    symbols = coll.distinct(symbol_field)
    symbols = sorted(sym for sym in symbols if sym)
    print(f"[INFO] Found {len(symbols)} distinct symbols in {collection}.")
    return symbols


def inspect_symbol(
    db: DatabaseHandler,
    mongo_db: str,
    collection: str,
    symbol: str,
    symbol_field: str,
    date_field: str,
    date_type: str,
) -> Optional[dict]:
    coll = db.get_mongo_collection(mongo_db, collection)
    match_cond = {symbol_field: symbol}
    pipeline = [
        {"$match": match_cond},
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
        print(f"[WARN] No data for {symbol}.")
        return None
    stats = result[0]
    count = stats.get("count", 0)
    min_date = stats.get("min_date")
    max_date = stats.get("max_date")
    if date_type == "int":
        min_date = int(min_date) if min_date is not None else None
        max_date = int(max_date) if max_date is not None else None
    else:
        min_date = str(min_date) if min_date is not None else None
        max_date = str(max_date) if max_date is not None else None
    return {
        "symbol": symbol,
        "count": count,
        "min_date": min_date,
        "max_date": max_date,
    }


def main():
    args = parse_args()
    if not args.all_symbols and not args.symbols:
        raise SystemExit("必须指定 --symbols 或 --all-symbols 之一。")

    config = get_config()
    mongo_db = config["MONGO_DB"]
    db_handler = DatabaseHandler(config)

    if args.all_symbols:
        symbols = distinct_symbols(db_handler, mongo_db, args.collection, args.symbol_field)
    else:
        symbols = args.symbols

    grand_total = 0
    min_date_global = None
    max_date_global = None

    for symbol in symbols:
        stats = inspect_symbol(
            db_handler,
            mongo_db,
            args.collection,
            symbol,
            args.symbol_field,
            args.date_field,
            args.date_type,
        )
        if stats is None:
            continue
        grand_total += stats["count"]
        if stats["min_date"] is not None:
            min_date_global = (
                stats["min_date"]
                if min_date_global is None
                else min(min_date_global, stats["min_date"])
            )
        if stats["max_date"] is not None:
            max_date_global = (
                stats["max_date"]
                if max_date_global is None
                else max(max_date_global, stats["max_date"])
            )
        print(
            f"{stats['symbol']}: count={stats['count']} "
            f"range=[{stats['min_date']}, {stats['max_date']}]"
        )

    print("\n=== Summary ===")
    print(f"Symbols analysed: {len(symbols)}")
    print(f"Total documents: {grand_total}")
    print(f"Date range: [{min_date_global}, {max_date_global}]")


if __name__ == "__main__":
    main()
