"""Benchmark helper comparing publish latency (write + read) across legacy and optimized flows."""

import argparse
import json
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Callable, DefaultDict, Dict, List

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
for path in (ROOT_DIR, SRC_DIR):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from publish_sample_tick import send_clickhouse_tick, send_kafka_tick, send_questdb_tick
from common.connector.clickhouse_client import ClickHouseClient
from common.connector.questdb_client import QuestDBClient
from common.connector.redis_client import RedisClient

SCRIPT_PATH = Path(__file__).resolve().parent / "publish_sample_tick.py"
LEGACY_HASH_KEY = "tushare_future_tick_quotation"
READ_RETRIES = 3
READ_RETRY_DELAY = 0.05
_redis_unavailable_logged = False


def _measure(func: Callable[[], object]) -> float | None:
    for attempt in range(READ_RETRIES):
        start = time.perf_counter()
        result = func()
        duration = time.perf_counter() - start
        if result is not None:
            return duration
        if attempt < READ_RETRIES - 1:
            time.sleep(READ_RETRY_DELAY)
    return None


def measure_read_latency(symbol: str) -> Dict[str, float]:
    latencies: Dict[str, float] = {}

    questdb_client = QuestDBClient.instance()
    if questdb_client:
        sql = (
            f"select * from {questdb_client.settings.tick_table} "
            f"where symbol = '{symbol}' order by timestamp desc limit 1"
        )
        latency = _measure(lambda: questdb_client.query(sql))
        if latency is not None:
            latencies["questdb"] = latency

    clickhouse_client = ClickHouseClient.instance()
    if clickhouse_client:
        sql = (
            f"SELECT * FROM {clickhouse_client.settings.database}."
            f"{clickhouse_client.settings.tick_table} "
            f"WHERE symbol = '{symbol}' ORDER BY timestamp DESC LIMIT 1"
        )
        latency = _measure(lambda: clickhouse_client.query(sql))
        if latency is not None:
            latencies["clickhouse"] = latency

    return latencies


def run_legacy(symbol: str, price: float) -> float:
    global _redis_unavailable_logged
    client = RedisClient()
    payload = {
        "symbol": symbol,
        "price": price,
        "timestamp": time.time(),
    }
    start = time.perf_counter()
    try:
        result = client.setHashRedis(LEGACY_HASH_KEY, symbol, json.dumps(payload))
        if result is None:
            raise RuntimeError("Redis write failed")
    except Exception as exc:
        if not _redis_unavailable_logged:
            print(f"[legacy] skip Redis write: {exc}")
            _redis_unavailable_logged = True
        return None
    return time.perf_counter() - start


def measure_legacy_read(symbol: str) -> float | None:
    global _redis_unavailable_logged
    client = RedisClient()

    def _fetch():
        try:
            return client.getHashRedis(LEGACY_HASH_KEY, symbol)
        except Exception as exc:
            if not _redis_unavailable_logged:
                print(f"[legacy] skip Redis read: {exc}")
                _redis_unavailable_logged = True
            return None

    return _measure(_fetch)


def run_subprocess(symbol: str, price: float) -> float:
    start = time.perf_counter()
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--symbol",
            symbol,
            "--price",
            str(price),
            "--summary-only",
            "--quiet",
        ],
        check=False,
    )
    duration = time.perf_counter() - start
    if result.returncode != 0:
        raise RuntimeError(f"publish_sample_tick.py failed with code {result.returncode}")
    return duration


def run_inprocess(symbol: str, price: float) -> float:
    start = time.perf_counter()
    send_kafka_tick(symbol, price, verbose=False)
    send_questdb_tick(symbol, price, verbose=False)
    send_clickhouse_tick(symbol, price, verbose=False)
    return time.perf_counter() - start


def benchmark(symbol: str, price: float, count: int, warmup: int) -> None:
    if not SCRIPT_PATH.exists():
        raise SystemExit(f"publish_sample_tick.py not found at {SCRIPT_PATH}")

    for _ in range(max(warmup, 0)):
        run_legacy(symbol, price)
        run_inprocess(symbol, price)

    legacy_times = []
    baseline_times = []
    batched_times = []
    legacy_reads: List[float] = []
    baseline_reads: DefaultDict[str, List[float]] = defaultdict(list)
    batched_reads: DefaultDict[str, List[float]] = defaultdict(list)

    for _ in range(count):
        legacy_time = run_legacy(symbol, price)
        if legacy_time is not None:
            legacy_times.append(legacy_time)
            legacy_read = measure_legacy_read(symbol)
            if legacy_read is not None:
                legacy_reads.append(legacy_read)
        baseline_times.append(run_subprocess(symbol, price))
        reads = measure_read_latency(symbol)
        for backend, value in reads.items():
            baseline_reads[backend].append(value)
    for _ in range(count):
        batched_times.append(run_inprocess(symbol, price))
        reads = measure_read_latency(symbol)
        for backend, value in reads.items():
            batched_reads[backend].append(value)

    def summarize(label: str, durations: List[float]) -> None:
        if not durations:
            print(f"{label}: no samples")
            return
        total = sum(durations)
        avg = total / len(durations)
        print(
            f"{label:>12} | runs={len(durations):3d} | total={total:.3f}s | avg={avg*1000:.1f}ms"
        )

    print("Legacy pipeline (Redis only):")
    summarize("legacy", legacy_times)
    if legacy_reads:
        summarize("legacy-read", legacy_reads)

    print("\nOptimized pipeline (Kafka + QuestDB + ClickHouse):")
    summarize("baseline", baseline_times)
    summarize("batched", batched_times)

    if baseline_reads or batched_reads:
        print("\nRead latency (write immediately followed by query):")
        for backend, samples in baseline_reads.items():
            summarize(f"base-{backend}", samples)
        for backend, samples in batched_reads.items():
            summarize(f"batch-{backend}", samples)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare subprocess vs in-process sample tick publishing"
    )
    parser.add_argument("--symbol", default="TEST.FUT", help="合约代码")
    parser.add_argument("--price", type=float, default=101.5, help="价格")
    parser.add_argument("--count", type=int, default=100, help="每种模式写入次数")
    parser.add_argument("--warmup", type=int, default=5, help="预热次数，仅用于批量模式")
    args = parser.parse_args()

    if args.count <= 0:
        parser.error("--count must be positive")

    benchmark(args.symbol, args.price, args.count, args.warmup)


if __name__ == "__main__":
    main()
