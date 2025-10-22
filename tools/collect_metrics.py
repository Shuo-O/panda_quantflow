import argparse
import json
import time
from typing import List, Optional

from common.config.config import get_config
from common.connector.kafka_client import KafkaClientFactory, KafkaSettings
from common.connector.questdb_client import QuestDBClient
from common.connector.clickhouse_client import ClickHouseClient


def percentile(data: List[float], pct: float) -> float:
    if not data:
        return 0.0
    data_sorted = sorted(data)
    k = (len(data_sorted) - 1) * pct
    f = int(k)
    c = min(f + 1, len(data_sorted) - 1)
    if f == c:
        return data_sorted[int(k)]
    d0 = data_sorted[f] * (c - k)
    d1 = data_sorted[c] * (k - f)
    return d0 + d1


def measure_kafka_latency(message_count: int) -> Optional[dict]:
    settings = KafkaSettings.from_env()
    if not settings.is_configured():
        print("Kafka 未配置，跳过 Kafka 延迟测试")
        return None

    factory = KafkaClientFactory(settings)
    cfg = get_config()
    topic = cfg.get("KAFKA_FUTURE_TICK_TOPIC", "market.future.tick")
    print(f"[Kafka] 订阅 {topic}，预计读取 {message_count} 条消息 …")

    consumer = factory.create_consumer(
        topics=[topic],
        group_id=f"latency-metrics-{int(time.time())}",
        value_deserializer=lambda v: v.decode("utf-8"),
        enable_auto_commit=False,
        auto_offset_reset="latest",
    )

    latencies = []
    start = time.perf_counter()
    count = 0
    for msg in consumer:
        now_ms = time.time() * 1000
        try:
            payload = json.loads(msg.value)
            produce_ts = float(payload.get("time", now_ms))
        except json.JSONDecodeError:
            produce_ts = now_ms

        latency_ms = max(0.0, now_ms - produce_ts)
        latencies.append(latency_ms)
        count += 1
        if count >= message_count:
            break
    duration = time.perf_counter() - start
    consumer.close()

    if not latencies:
        print("[Kafka] 未读取到消息，无法计算延迟。")
        return None

    throughput = count / duration if duration > 0 else 0
    result = {
        "count": count,
        "duration_sec": duration,
        "throughput_per_sec": throughput,
        "avg_ms": sum(latencies) / len(latencies),
        "p50_ms": percentile(latencies, 0.5),
        "p95_ms": percentile(latencies, 0.95),
        "p99_ms": percentile(latencies, 0.99),
        "max_ms": max(latencies),
    }
    return result


def measure_query_time(name: str, func) -> Optional[float]:
    try:
        start = time.perf_counter()
        func()
        duration = time.perf_counter() - start
        print(f"[{name}] 查询耗时 {duration * 1000:.2f} ms")
        return duration
    except Exception as exc:
        print(f"[{name}] 查询失败: {exc}")
        return None


def questdb_probe():
    client = QuestDBClient.instance()
    if not client:
        print("QuestDB 未启用，跳过查询测试")
        return
    table = client.settings.tick_table
    sql = f"select count(*) from {table} where timestamp > now() - 3600000000000 limit 1"
    measure_query_time("QuestDB", lambda: client.query(sql))


def clickhouse_probe():
    client = ClickHouseClient.instance()
    if not client:
        print("ClickHouse 未启用，跳过查询测试")
        return
    table = client.settings.tick_table
    sql = f"select count(*) from {table} where timestamp > now() - INTERVAL 1 HOUR"
    measure_query_time("ClickHouse", lambda: client.query(sql))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="收集行情链路性能指标")
    parser.add_argument("--messages", type=int, default=200, help="Kafka 延迟采样条数")
    parser.add_argument("--skip-questdb", action="store_true", help="跳过 QuestDB 查询测试")
    parser.add_argument("--skip-clickhouse", action="store_true", help="跳过 ClickHouse 查询测试")
    args = parser.parse_args()

    kafka_stats = measure_kafka_latency(args.messages)
    if kafka_stats:
        print("[Kafka] 延迟统计：")
        for key, value in kafka_stats.items():
            if isinstance(value, float):
                print(f"  {key}: {value:.2f}")
            else:
                print(f"  {key}: {value}")

    if not args.skip_questdb:
        questdb_probe()

    if not args.skip_clickhouse:
        clickhouse_probe()

    print("性能采集完成。")
