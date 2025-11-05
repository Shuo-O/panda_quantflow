"""
简单的行情异步写入压测脚本，用于验证 Redis 快照写入与异步队列耗时。
运行方式：
    python -m panda_trading.scripts.benchmark_tick_async --ticks 50000 --queue 5000
"""

import argparse
import logging
import sys
import threading
import time
from contextlib import contextmanager
from types import ModuleType, SimpleNamespace

from queue import Queue


def _install_redis_stub() -> None:
    """为缺失的 redis 库注入轻量 stub，避免导入失败。"""

    if "redis" in sys.modules:
        return

    class _DummyConnectionPool:
        def __init__(self, *args, **kwargs):
            pass

        def disconnect(self):
            pass

    class _DummyStrictRedis:
        def __init__(self, *args, **kwargs):
            self._store = {}

        # 字符串 API
        def set(self, key, value):
            self._store[key] = value

        def setex(self, key, time, value):
            self._store[key] = value

        def get(self, key):
            return self._store.get(key)

        def delete(self, key):
            return self._store.pop(key, None)

        def exists(self, key):
            return key in self._store

        def incr(self, key, amount):
            self._store[key] = self._store.get(key, 0) + amount
            return self._store[key]

        # Hash API
        def hset(self, name, key, value):
            self._store.setdefault(name, {})[key] = value

        def hget(self, name, key):
            return self._store.get(name, {}).get(key)

        def hgetall(self, name):
            return self._store.get(name, {}).copy()

        def hdel(self, name, *keys):
            if name not in self._store:
                return 0
            for key in keys:
                self._store[name].pop(key, None)
            if not keys:
                self._store.pop(name, None)

    redis_stub = ModuleType("redis")
    redis_stub.Redis = _DummyStrictRedis
    redis_stub.StrictRedis = _DummyStrictRedis
    redis_stub.ConnectionPool = _DummyConnectionPool
    redis_stub.exceptions = ModuleType("redis.exceptions")

    class _DummyRedisError(Exception):
        pass

    redis_stub.exceptions.RedisError = _DummyRedisError
    sys.modules["redis"] = redis_stub
    sys.modules["redis.exceptions"] = redis_stub.exceptions


def _install_ctp_stub() -> None:
    """为缺失的 ctp 模块注入最小实现。"""

    if "ctp" in sys.modules:
        return

    ctp_stub = ModuleType("ctp")

    class _DummyMdSpi:
        def __init__(self, *args, **kwargs):
            pass

    class _DummyMdApi:
        @staticmethod
        def CreateFtdcMdApi(*args, **kwargs):
            return SimpleNamespace(
                RegisterSpi=lambda *_: None,
                RegisterFront=lambda *_: None,
                Init=lambda: None,
                Join=lambda: None,
                Release=lambda: None,
                ReqUserLogin=lambda *a, **k: None,
                SubscribeMarketData=lambda *a, **k: None,
            )

    ctp_stub.CThostFtdcMdSpi = _DummyMdSpi
    ctp_stub.CThostFtdcMdApi = _DummyMdApi
    sys.modules["ctp"] = ctp_stub


_install_redis_stub()
_install_ctp_stub()


@contextmanager
def _silence_print():
    import builtins

    original_print = builtins.print

    def no_op(*args, **kwargs):
        pass

    builtins.print = no_op
    try:
        yield
    finally:
        builtins.print = original_print

with _silence_print():
    from panda_backtest.backtest_common.model.quotation.bar_quotation_data import (
        BarQuotationData,
    )
    from panda_trading.trading.quotation.ctp.ctp_mdu import MdSpi


class FakeRedis:
    """简单的内存 Redis，统计写入耗时。"""

    def __init__(self):
        self.store = {}
        self.calls = 0
        self.total_time = 0.0

    def setHashRedis(self, key, field, value):
        start = time.perf_counter()
        self.store[(key, field)] = value
        self.calls += 1
        self.total_time += time.perf_counter() - start


def build_spi(queue_size: int) -> MdSpi:
    spi = object.__new__(MdSpi)
    spi.logger = logging.getLogger("benchmark.spi")
    spi.logger.setLevel(logging.INFO)
    spi._MdSpi__redis_client = FakeRedis()
    spi._async_queue = Queue(maxsize=queue_size)
    spi._async_stop_event = threading.Event()
    spi._async_queue_warned = False
    spi._questdb_client = None
    spi._clickhouse_client = None
    spi._kafka_error_logged = False
    spi._kafka_producer = None
    spi._kafka_factory = None

    # 替换慢操作为轻量统计
    async_stats = {"count": 0}

    def fake_publish_kafka(payload):
        if payload:
            async_stats["count"] += 1

    spi._publish_kafka_tick = fake_publish_kafka
    spi._publish_questdb_tick = lambda bar: None

    # 不依赖 Mongo/FutureInfoMap，直接返回输入
    spi.depth_market_dat_to_symbol = lambda tick: tick

    spi.api = SimpleNamespace(RegisterSpi=lambda _: None, Release=lambda: None)

    spi._async_worker = threading.Thread(
        target=spi._consume_async_tasks,
        name="BenchmarkAsyncWorker",
        daemon=True,
    )
    spi._async_worker.start()
    spi._async_stats = async_stats
    return spi


def generate_bar(seed: int) -> BarQuotationData:
    bar = BarQuotationData()
    bar.symbol = f"TEST{seed % 20}"
    bar.date = "20250101"
    bar.time = "120000"
    bar.trade_date = "20250101"
    bar.open = bar.high = bar.low = bar.close = float(seed % 1000)
    bar.volume = bar.turnover = bar.oi = seed
    bar.settle = bar.last = float(seed)
    bar.preclose = bar.limit_up = bar.limit_down = float(seed)
    bar.askprice1 = bar.bidprice1 = float(seed)
    bar.askvolume1 = bar.bidvolume1 = seed
    return bar


def run_benchmark(tick_count: int, queue_size: int):
    spi = build_spi(queue_size)
    start = time.perf_counter()
    for i in range(tick_count):
        bar = generate_bar(i)
        spi.save_data_task(bar)
    # 等待异步队列清空
    spi._async_queue.join()
    total = time.perf_counter() - start

    redis_client: FakeRedis = spi._MdSpi__redis_client

    # 关闭后台线程
    spi._async_stop_event.set()
    spi._async_worker.join(timeout=1.0)

    print(f"Ticks processed     : {tick_count}")
    print(f"Queue max size      : {queue_size}")
    print(f"Total elapsed       : {total:.4f}s")
    print(f"Throughput          : {tick_count / total:.2f} ticks/s")
    print(f"Redis writes        : {redis_client.calls}")
    print(
        f"Redis avg write time: {(redis_client.total_time / max(redis_client.calls, 1)) * 1e6:.2f} µs"
    )
    print(f"Async processed     : {spi._async_stats['count']}")
    print(f"Queue backlog       : {spi._async_queue.qsize()}")


def main():
    parser = argparse.ArgumentParser(description="Benchmark tick async pipeline")
    parser.add_argument("--ticks", type=int, default=50000, help="模拟 tick 数量")
    parser.add_argument("--queue", type=int, default=5000, help="异步队列大小")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    run_benchmark(args.ticks, args.queue)


if __name__ == "__main__":
    main()
