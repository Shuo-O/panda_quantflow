import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
for path in (ROOT_DIR, SRC_DIR):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from common.config.config import get_config
from common.connector.kafka_client import KafkaClientFactory, KafkaSettings
from common.connector.questdb_client import QuestDBClient
from common.connector.clickhouse_client import ClickHouseClient


def send_kafka_tick(symbol: str, price: float, verbose: bool = True) -> bool:
    settings = KafkaSettings.from_env()
    if not settings.is_configured():
        if verbose:
            print("Kafka 未配置或禁用，跳过发送")
        return False
    factory = KafkaClientFactory(settings)
    producer = factory.get_producer()
    cfg = get_config()
    topic = cfg.get("KAFKA_FUTURE_TICK_TOPIC", "market.future.tick")
    payload = {
        "symbol": symbol,
        "time": int(time.time() * 1000),
        "data": "0",
        "last": price,
    }
    producer.send(topic, json.dumps(payload).encode("utf-8"))
    producer.flush()
    if verbose:
        print(f"[Kafka] 已向 {topic} 写入测试 Tick: {payload}")
    return True


def send_questdb_tick(symbol: str, price: float, verbose: bool = True) -> bool:
    client = QuestDBClient.instance()
    if not client:
        if verbose:
            print("QuestDB 未启用，跳过写入")
        return False
    timestamp_ns = time.time_ns()
    fields = {
        "last": price,
        "open": price,
        "high": price,
        "low": price,
        "close": price,
        "volume": 0.0,
    }
    client.write_tick(symbol, fields, timestamp_ns)
    if verbose:
        print(
            f"[QuestDB] 已写入 {symbol} 测试 Tick @ {datetime.fromtimestamp(timestamp_ns / 1e9)}"
        )
    return True


def send_clickhouse_tick(symbol: str, price: float, verbose: bool = True) -> bool:
    client = ClickHouseClient.instance()
    if not client:
        if verbose:
            print("ClickHouse 未启用，跳过写入")
        return False
    row = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f"),
        "symbol": symbol,
        "last": price,
        "open": price,
        "high": price,
        "low": price,
        "close": price,
        "volume": 0.0,
    }
    client.write_tick(row)
    if verbose:
        print(f"[ClickHouse] 已写入 {symbol} 测试 Tick @ {row['timestamp']}")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="向 Kafka / QuestDB / ClickHouse 写入测试 Tick")
    parser.add_argument("--symbol", default="TEST.FUT", help="合约代码")
    parser.add_argument("--price", type=float, default=100.0, help="成交价")
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="仅输出最终写入条数，不显示每次写入详情",
    )
    parser.add_argument(
        "--quiet", action="store_true", help="完全静默模式，不输出任何日志"
    )
    args = parser.parse_args()

    verbose = not args.summary_only and not args.quiet
    total_written = 0
    total_written += int(send_kafka_tick(args.symbol, args.price, verbose))
    total_written += int(send_questdb_tick(args.symbol, args.price, verbose))
    total_written += int(send_clickhouse_tick(args.symbol, args.price, verbose))

    if args.quiet:
        return

    if args.summary_only:
        print(f"总共写入 {total_written} 条")
    else:
        print("完成测试数据写入，请通过 Kafka / QuestDB / ClickHouse 查询确认")


if __name__ == "__main__":
    main()
