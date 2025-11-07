#!/usr/bin/env python3
"""
Benchmark JoinQuant Level2 读取/写入性能。

示例：
    python tools/benchmark_level2_io.py \
        --date 20241107 \
        --tick-file 000001.SZ.tick \
        --order-file 000001.SZ.order \
        --trade-file 000001.SZ.trade \
        --write-test         # 可选，触发实际 QuestDB 写入
"""

from __future__ import annotations

import argparse
import datetime as dt
import pathlib
import sys
import time
from dataclasses import dataclass
from typing import Callable, Optional

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
for candidate in (ROOT_DIR, SRC_DIR):
    path_str = str(candidate)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from common.connector.questdb_client import QuestDBClient  # noqa: E402
import tools.import_level2_to_questdb as importer  # noqa: E402
from tools.import_level2_to_questdb import QuestDBBatchWriter  # noqa: E402


@dataclass
class BenchmarkResult:
    label: str
    phase: str
    record_count: int
    elapsed: float
    bytes_processed: int

    @property
    def records_per_second(self) -> float:
        return self.record_count / self.elapsed if self.elapsed > 0 else 0.0

    @property
    def mb_per_second(self) -> float:
        return (self.bytes_processed / 1_000_000) / self.elapsed if self.elapsed > 0 else 0.0


def _benchmark(label: str, phase: str, bytes_processed: int, fn: Callable[[], int]) -> BenchmarkResult:
    start = time.perf_counter()
    count = fn()
    elapsed = time.perf_counter() - start
    return BenchmarkResult(label, phase, count, elapsed, bytes_processed)


def _collect_symbol_hint(args, *paths: Optional[pathlib.Path]) -> Optional[str]:
    if args.symbol:
        return args.symbol
    for path in paths:
        hint = importer._guess_symbol_from_path(path)  # type: ignore[attr-defined]
        if hint:
            return hint
    return None


def _run_read_phase(
    label: str,
    ingest_fn: Callable[..., int],
    file_path: pathlib.Path,
    trade_date: dt.date,
    symbol: Optional[str],
    repeats: int,
) -> BenchmarkResult:
    repeats = max(1, repeats)
    bytes_processed = file_path.stat().st_size * repeats

    def runner() -> int:
        total = 0
        for _ in range(repeats):
            total += ingest_fn(None, "benchmark", file_path, trade_date, symbol, True, None)
        return total

    return _benchmark(label=label, phase="read", bytes_processed=bytes_processed, fn=runner)


def _run_write_phase(
    label: str,
    ingest_fn: Callable[..., int],
    client: QuestDBClient,
    table: str,
    file_path: pathlib.Path,
    trade_date: dt.date,
    symbol: Optional[str],
    repeats: int,
    batch_size: int,
) -> BenchmarkResult:
    repeats = max(1, repeats)
    bytes_processed = file_path.stat().st_size * repeats

    def runner() -> int:
        total = 0
        writer = QuestDBBatchWriter(client, batch_size=batch_size)
        for _ in range(repeats):
            total += ingest_fn(client, table, file_path, trade_date, symbol, False, writer)
        writer.flush()
        return total

    return _benchmark(label=label, phase="write", bytes_processed=bytes_processed, fn=runner)


def _print_result(result: BenchmarkResult) -> None:
    print(
        f"[{result.phase}] {result.label}: "
        f"{result.record_count}条, "
        f"{result.elapsed:.3f}s, "
        f"{result.records_per_second:,.0f} rec/s, "
        f"{result.mb_per_second:,.2f} MB/s"
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark Level2 文件的读/写性能。")
    parser.add_argument("--date", required=True, help="交易日 YYYYMMDD")
    parser.add_argument("--symbol", help="覆盖文件名推断的合约代码")
    parser.add_argument("--tick-file", type=pathlib.Path)
    parser.add_argument("--order-file", type=pathlib.Path)
    parser.add_argument("--trade-file", type=pathlib.Path)
    parser.add_argument("--tick-table", help="写入 QuestDB 的 tick 表名（默认读取配置）")
    parser.add_argument("--order-table", default="level2_orders")
    parser.add_argument("--trade-table", default="level2_trades")
    parser.add_argument("--write-test", action="store_true", help="执行实际写入以测试写入性能")
    parser.add_argument("--repeat", type=int, default=1, help="重复读取同一份文件的次数，用于模拟大批量数据")
    parser.add_argument("--batch-size", type=int, default=1000, help="QuestDB 写入批大小，默认 1000")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    if not any([args.tick_file, args.order_file, args.trade_file]):
        raise SystemExit("至少指定一个 Level2 文件")

    trade_date = dt.datetime.strptime(args.date, "%Y%m%d").date()
    symbol_hint = _collect_symbol_hint(args, args.tick_file, args.order_file, args.trade_file)

    results: list[BenchmarkResult] = []

    if args.tick_file:
        results.append(
            _run_read_phase(
                "tick",
                importer._ingest_ticks,  # type: ignore[attr-defined]
                args.tick_file,
                trade_date,
                symbol_hint,
                args.repeat,
            )
        )
    if args.order_file:
        results.append(
            _run_read_phase(
                "order",
                importer._ingest_orders,  # type: ignore[attr-defined]
                args.order_file,
                trade_date,
                symbol_hint,
                args.repeat,
            )
        )
    if args.trade_file:
        results.append(
            _run_read_phase(
                "trade",
                importer._ingest_trades,  # type: ignore[attr-defined]
                args.trade_file,
                trade_date,
                symbol_hint,
                args.repeat,
            )
        )

    client: Optional[QuestDBClient] = None
    if args.write_test:
        client = QuestDBClient.instance()
        if client is None:
            raise SystemExit("QuestDB 未启用，无法执行写入测试。请设置 QUESTDB_ENABLE=true 或移除 --write-test。")

        tick_table = args.tick_table or client.settings.tick_table
        if args.tick_file:
            results.append(
                _run_write_phase(
                    "tick",
                    importer._ingest_ticks,  # type: ignore[attr-defined]
                    client,
                    tick_table,
                    args.tick_file,
                    trade_date,
                    symbol_hint,
                    args.repeat,
                    args.batch_size,
                )
            )
        if args.order_file:
            results.append(
                _run_write_phase(
                    "order",
                    importer._ingest_orders,  # type: ignore[attr-defined]
                    client,
                    args.order_table,
                    args.order_file,
                    trade_date,
                    symbol_hint,
                    args.repeat,
                    args.batch_size,
                )
            )
        if args.trade_file:
            results.append(
                _run_write_phase(
                    "trade",
                    importer._ingest_trades,  # type: ignore[attr-defined]
                    client,
                    args.trade_table,
                    args.trade_file,
                    trade_date,
                    symbol_hint,
                    args.repeat,
                    args.batch_size,
                )
            )

    for result in results:
        _print_result(result)


if __name__ == "__main__":
    main()
