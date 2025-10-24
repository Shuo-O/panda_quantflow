"""Benchmark helper comparing single-run subprocess publishes vs in-process batching."""

import argparse
import subprocess
import sys
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
for path in (ROOT_DIR, SRC_DIR):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from publish_sample_tick import send_clickhouse_tick, send_kafka_tick, send_questdb_tick

SCRIPT_PATH = Path(__file__).resolve().parent / "publish_sample_tick.py"


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
        run_inprocess(symbol, price)

    baseline_times = []
    batched_times = []

    for _ in range(count):
        baseline_times.append(run_subprocess(symbol, price))
    for _ in range(count):
        batched_times.append(run_inprocess(symbol, price))

    def summarize(label: str, durations: list[float]) -> None:
        if not durations:
            print(f"{label}: no samples")
            return
        total = sum(durations)
        avg = total / len(durations)
        print(
            f"{label:>12} | runs={len(durations):3d} | total={total:.3f}s | avg={avg*1000:.1f}ms"
        )

    print("Benchmark results (lower is better):")
    summarize("baseline", baseline_times)
    summarize("batched", batched_times)


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
