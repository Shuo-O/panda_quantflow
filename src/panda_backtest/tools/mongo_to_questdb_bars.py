#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Export daily bar data from MongoDB to QuestDB.

Usage example (补齐上证指数):

    python tools/mongo_to_questdb_bars.py \
        --collection stock_market \
        --quest-table stock_daily_bars \
        --symbols 000001.SH \
        --start-date 20240101 \
        --end-date 20241231 \
        --date-field date \
        --date-type str

The script依赖仓库里的 common.config 配置，自动读取 Mongo 和 QuestDB 连接参数。
"""

from __future__ import annotations

import argparse
import datetime as dt
import math
import time
from typing import Dict, Iterable, List, Optional

from common.config.config import get_config
from common.connector.mongodb_handler import DatabaseHandler
from common.connector.questdb_client import QuestDBClient

FLOAT_FIELDS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "turnover",
    "vwap",
    "settlement",
    "oi",
    "unit_nav",
    "preclose",
    "limit_up",
    "limit_down",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync bar data from MongoDB to QuestDB.")
    parser.add_argument("--collection", required=True, help="MongoDB collection name (e.g. stock_market).")
    parser.add_argument(
        "--symbols",
        nargs="+",
        required=True,
        help="Symbols to export (e.g. 000001.SH). Multiple symbols separated by space.",
    )
    parser.add_argument("--start-date", required=True, help="Start date YYYYMMDD (inclusive).")
    parser.add_argument("--end-date", required=True, help="End date YYYYMMDD (inclusive).")
    parser.add_argument(
        "--date-field",
        default="trade_date",
        help="Field name storing the trade date in Mongo (default: trade_date).",
    )
    parser.add_argument(
        "--date-type",
        choices=["int", "str"],
        default="int",
        help="Mongo date field type: 'int' for 20240101, 'str' for '20240101'.",
    )
    parser.add_argument(
        "--quest-table",
        default="stock_daily_bars",
        help="QuestDB table to write into (default: stock_daily_bars).",
    )
    parser.add_argument(
        "--symbol-field",
        default="symbol",
        help="Mongo field containing the instrument symbol (default: symbol).",
    )
    parser.add_argument(
        "--max-documents",
        type=int,
        default=0,
        help="Optional cap on documents per symbol for testing (0 = no cap).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be written without sending to QuestDB.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.0,
        help="Sleep seconds between QuestDB writes (useful to throttle).",
    )
    return parser.parse_args()


def _parse_trade_date(value, date_type: str) -> dt.datetime:
    if date_type == "int":
        value_int = int(value)
        value_str = f"{value_int:08d}"
    else:
        value_str = str(value).strip()
    return dt.datetime.strptime(value_str, "%Y%m%d")


def _to_timestamp_ns(trade_dt: dt.datetime) -> int:
    # 设定为当日 00:00 UTC
    return int(trade_dt.replace(tzinfo=dt.timezone.utc).timestamp() * 1_000_000_000)


def _safe_float(value) -> Optional[float]:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    if isinstance(value, (int, float)):
        if math.isnan(value):
            return None
        return float(value)
    try:
        parsed = float(value)
        if math.isnan(parsed):
            return None
        return parsed
    except (TypeError, ValueError):
        return None


def _build_ilp_line(table: str, symbol: str, fields: Dict[str, Optional[float]], timestamp_ns: int) -> str:
    field_parts: List[str] = []
    for key, value in fields.items():
        if value is None:
            continue
        if isinstance(value, bool):
            field_parts.append(f"{key}={'t' if value else 'f'}")
        elif isinstance(value, int) and not isinstance(value, bool):
            field_parts.append(f"{key}={value}i")
        else:
            field_parts.append(f"{key}={value}")
    if not field_parts:
        raise ValueError("No valid fields to write.")
    fields_str = ",".join(field_parts)
    return f"{table},symbol={symbol} {fields_str} {timestamp_ns}\n"


def fetch_documents(
    db: DatabaseHandler,
    mongo_db: str,
    collection: str,
    symbol: str,
    symbol_field: str,
    date_field: str,
    start_value,
    end_value,
    max_docs: int = 0,
) -> List[Dict]:
    query: Dict[str, object] = {symbol_field: symbol}
    query[date_field] = {"$gte": start_value, "$lte": end_value}
    sort = [(date_field, 1)]
    docs = db.mongo_find(mongo_db, collection, query, sort=sort)
    if max_docs and len(docs) > max_docs:
        return docs[:max_docs]
    return docs


def main():
    args = parse_args()
    config = get_config()
    mongo_db_name = config["MONGO_DB"]
    db_handler = DatabaseHandler(config)
    quest_client = QuestDBClient.instance()
    if quest_client is None and not args.dry_run:
        raise RuntimeError("QuestDBClient 未启用，请设置环境变量 QUESTDB_ENABLE=true 并配置连接信息。")

    start_value = int(args.start_date) if args.date_type == "int" else args.start_date
    end_value = int(args.end_date) if args.date_type == "int" else args.end_date

    total_written = 0
    for symbol in args.symbols:
        docs = fetch_documents(
            db_handler,
            mongo_db_name,
            args.collection,
            symbol,
            args.symbol_field,
            args.date_field,
            start_value,
            end_value,
            max_docs=args.max_documents,
        )
        if not docs:
            print(f"[WARN] 未找到 {symbol} 在 {args.collection} 中的数据.")
            continue

        print(f"[INFO] 准备写入 {symbol} 数据，共 {len(docs)} 条.")
        for doc in docs:
            trade_raw = doc.get(args.date_field)
            if trade_raw is None:
                print(f"[WARN] 文档缺少 {args.date_field}: {doc}")
                continue
            trade_dt = _parse_trade_date(trade_raw, args.date_type)
            trade_int = int(trade_dt.strftime("%Y%m%d"))
            timestamp_ns = _to_timestamp_ns(trade_dt)

            fields: Dict[str, Optional[float]] = {"trade_date": trade_int}
            for field in FLOAT_FIELDS:
                fields[field] = _safe_float(doc.get(field))

            try:
                line = _build_ilp_line(args.quest_table, symbol, fields, timestamp_ns)
            except ValueError as exc:
                print(f"[WARN] 跳过 {symbol} {trade_raw}: {exc}")
                continue

            if args.dry_run:
                print(line)
            else:
                quest_client.write_line(line)
                total_written += 1
                if args.sleep:
                    time.sleep(args.sleep)

    print(f"[DONE] 共处理写入 {total_written} 条记录（dry-run={args.dry_run}).")


if __name__ == "__main__":
    main()
