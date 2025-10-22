import argparse
import json
import time
from datetime import datetime

from common.config.config import get_config
from common.connector.kafka_client import KafkaClientFactory, KafkaSettings
from common.connector.questdb_client import QuestDBClient
from common.connector.clickhouse_client import ClickHouseClient


def send_kafka_tick(symbol: str, price: float):
    settings = KafkaSettings.from_env()
    if not settings.is_configured():
        print("Kafka 未配置或禁用，跳过发送")
        return
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
    print(f"[Kafka] 已向 {topic} 写入测试 Tick: {payload}")


def send_questdb_tick(symbol: str, price: float):
    client = QuestDBClient.instance()
    if not client:
        print("QuestDB 未启用，跳过写入")
        return
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
    print(f"[QuestDB] 已写入 {symbol} 测试 Tick @ {datetime.fromtimestamp(timestamp_ns / 1e9)}")


def send_clickhouse_tick(symbol: str, price: float):
    client = ClickHouseClient.instance()
    if not client:
        print("ClickHouse 未启用，跳过写入")
        return
    row = {
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"),
        "symbol": symbol,
        "last": price,
        "open": price,
        "high": price,
        "low": price,
        "close": price,
        "volume": 0.0,
    }
    client.write_tick(row)
    print(f"[ClickHouse] 已写入 {symbol} 测试 Tick @ {row['timestamp']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="向 Kafka / QuestDB / ClickHouse 写入测试 Tick")
    parser.add_argument("--symbol", default="TEST.FUT", help="合约代码")
    parser.add_argument("--price", type=float, default=100.0, help="成交价")
    args = parser.parse_args()

    send_kafka_tick(args.symbol, args.price)
    send_questdb_tick(args.symbol, args.price)
    send_clickhouse_tick(args.symbol, args.price)
    print("完成测试数据写入，请通过 Kafka / QuestDB / ClickHouse 查询确认")
