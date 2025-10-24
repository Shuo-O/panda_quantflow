"""Utility to repeatedly run publish_sample_tick with configurable interval."""

import argparse
import subprocess
import sys
import time
from pathlib import Path


PUBLISH_SCRIPT = Path(__file__).resolve().parent / "publish_sample_tick.py"


def run_once(symbol: str, price: float) -> int:
    cmd = [
        sys.executable,
        str(PUBLISH_SCRIPT),
        "--symbol",
        symbol,
        "--price",
        str(price),
    ]
    result = subprocess.run(cmd, check=False)
    return result.returncode


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Loop runner for tools/publish_sample_tick.py"
    )
    parser.add_argument("--symbol", default="TEST.FUT", help="Contract code to publish")
    parser.add_argument(
        "--price", type=float, default=101.5, help="Last price used in payload"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=5.0,
        help="Seconds to wait between two runs",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=0,
        help="Number of runs (0 means run until interrupted)",
    )
    parser.add_argument(
        "--sleep-first",
        type=float,
        default=0.0,
        help="Optional initial delay before the first run",
    )
    args = parser.parse_args()

    if not PUBLISH_SCRIPT.exists():
        parser.error(f"publish_sample_tick.py not found at: {PUBLISH_SCRIPT}")

    if args.sleep_first > 0:
        time.sleep(args.sleep_first)

    iteration = 0
    try:
        while True:
            iteration += 1
            print(f"[auto-publish] iteration {iteration} start", flush=True)
            return_code = run_once(args.symbol, args.price)
            if return_code != 0:
                print(
                    f"[auto-publish] iteration {iteration} failed with code {return_code}",
                    flush=True,
                )
            if args.count and iteration >= args.count:
                break
            time.sleep(max(args.interval, 0.0))
    except KeyboardInterrupt:
        print("[auto-publish] interrupted by user", flush=True)


if __name__ == "__main__":
    main()
