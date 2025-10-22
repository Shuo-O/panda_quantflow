import json
import os
import threading
import traceback
import logging
from typing import Any, Dict, Optional

from common.config.config import get_config
from common.connector.kafka_client import KafkaClientFactory, KafkaSettings
from common.connector.questdb_client import QuestDBClient
from panda_backtest.backtest_common.model.quotation.bar_quotation_data import BarQuotationData


def _parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _config_or_env(key: str, default: str) -> str:
    cfg = get_config()
    return os.getenv(key, cfg.get(key, default))


class TushareFutureTickQuotation(object):
    """
    Real-time quotation adapter.

    兼容 Redis Hash 与 Kafka 双通道。Kafka 缓存优先，Redis 作为回退。
    """

    def __init__(self, redis_client):
        self.logger = logging.getLogger(__name__)
        self.my_bar_dict = dict()
        self.redis_client = redis_client
        self.tushare_quotation_key = "tushare_future_tick_quotation"

        self._kafka_settings = KafkaSettings.from_env()
        self._kafka_topic = _config_or_env("KAFKA_FUTURE_TICK_TOPIC", "market.future.tick")
        self._kafka_group_id = _config_or_env(
            "KAFKA_FUTURE_TICK_GROUP_ID", "panda-future-tick-consumer"
        )
        self._kafka_offset_reset = _config_or_env("KAFKA_FUTURE_TICK_OFFSET_RESET", "latest")
        self._kafka_enabled = _parse_bool(
            _config_or_env("KAFKA_ENABLE_FUTURE_TICK", "false")
        ) and self._kafka_settings.is_configured()
        self._kafka_factory: Optional[KafkaClientFactory] = None
        self._kafka_consumer = None
        self._kafka_thread: Optional[threading.Thread] = None
        self._kafka_cache: Dict[str, BarQuotationData] = {}
        self._kafka_cache_lock = threading.Lock()
        self._kafka_error_logged = False
        self._kafka_ready_logged = False
        self._questdb_client = QuestDBClient.instance()

        if self._kafka_enabled:
            self._start_kafka_consumer()

    def __getitem__(self, item):
        try:
            kafka_bar = self._get_kafka_bar(str(item))
            if kafka_bar:
                return kafka_bar

            bar_data_json = self.redis_client.getHashRedis(
                self.tushare_quotation_key, str.encode(item)
            )
            if bar_data_json:
                bar_data = json.loads(bar_data_json)
                return self._dict_to_bar(bar_data) or BarQuotationData()

            return BarQuotationData()
        except Exception:
            self.logger.error(
                "获取期货实时行情失败,合约：%s,原因：%s", str(item), traceback.format_exc()
            )
            return BarQuotationData()

    def __setitem__(self, key, value):
        self.my_bar_dict[key] = value

    def keys(self):
        return self.my_bar_dict.keys()

    def _start_kafka_consumer(self) -> None:
        try:
            self._kafka_factory = KafkaClientFactory(self._kafka_settings)
            self._kafka_consumer = self._kafka_factory.create_consumer(
                topic=self._kafka_topic,
                group_id=self._kafka_group_id,
                value_deserializer=lambda value: json.loads(value.decode("utf-8")),
                enable_auto_commit=True,
                auto_offset_reset=self._kafka_offset_reset,
            )
            if not self._kafka_ready_logged:
                self.logger.info(
                    "[Kafka] Future tick consumer listening, topic=%s, group=%s",
                    self._kafka_topic,
                    self._kafka_group_id,
                )
                self._kafka_ready_logged = True
        except Exception as exc:
            if not self._kafka_error_logged:
                self.logger.error("[Kafka] 初始化期货行情消费者失败: %s", exc)
                self._kafka_error_logged = True
            self._kafka_enabled = False
            self._kafka_consumer = None
            return

        self._kafka_thread = threading.Thread(
            target=self._consume_loop,
            name="KafkaFutureTickConsumer",
            daemon=True,
        )
        self._kafka_thread.start()

    def _consume_loop(self) -> None:
        if not self._kafka_consumer:
            return

        for message in self._kafka_consumer:
            payload = message.value
            if not payload:
                continue

            bar = self._dict_to_bar(payload)
            if bar and bar.symbol:
                with self._kafka_cache_lock:
                    self._kafka_cache[bar.symbol] = bar

    def _dict_to_bar(self, data: Dict[str, Any]) -> Optional[BarQuotationData]:
        try:
            bar = BarQuotationData()
            bar.symbol = data.get("symbol")
            bar.date = str(data.get("date"))
            bar.time = str(data.get("time"))
            bar.trade_date = str(data.get("trade_date"))
            bar.open = data.get("open")
            bar.high = data.get("high")
            bar.low = data.get("low")
            bar.close = data.get("close")
            bar.volume = data.get("volume")
            bar.turnover = data.get("turnover")
            bar.oi = data.get("oi")
            bar.settle = data.get("settle")
            bar.last = data.get("last")
            bar.preclose = data.get("preclose")
            bar.limit_up = data.get("limit_up")
            bar.limit_down = data.get("limit_down")
            bar.askprice1 = data.get("askprice1")
            bar.bidprice1 = data.get("bidprice1")
            bar.askvolume1 = data.get("askvolume1")
            bar.bidvolume1 = data.get("bidvolume1")
            return bar
        except Exception as exc:
            if not self._kafka_error_logged:
                self.logger.error("[Kafka] 解析行情消息失败: %s", exc)
                self._kafka_error_logged = True
            return None

    def _get_kafka_bar(self, symbol: str) -> Optional[BarQuotationData]:
        if not self._kafka_enabled:
            return self._get_last_from_questdb(symbol)
        with self._kafka_cache_lock:
            bar = self._kafka_cache.get(symbol)
        if bar is None:
            return self._get_last_from_questdb(symbol)
        return bar

    def _get_last_from_questdb(self, symbol: str) -> Optional[BarQuotationData]:
        if not self._questdb_client:
            return None
        sql = (
            f"select * from {self._questdb_client.settings.tick_table} "
            f"where symbol='{symbol}' order by timestamp desc limit 1"
        )
        data = self._questdb_client.query(sql, limit=1)
        try:
            if not data or "dataset" not in data or not data["dataset"]:
                return None
            columns = data.get("columns", [])
            values = data["dataset"][0]
            row = {col["name"]: values[idx] for idx, col in enumerate(columns)}
            bar = BarQuotationData()
            bar.symbol = symbol
            bar.last = row.get("last")
            bar.open = row.get("open")
            bar.high = row.get("high")
            bar.low = row.get("low")
            bar.close = row.get("close")
            bar.volume = row.get("volume")
            bar.turnover = row.get("turnover")
            bar.oi = row.get("oi")
            bar.askprice1 = row.get("askprice1")
            bar.bidprice1 = row.get("bidprice1")
            bar.askvolume1 = row.get("askvolume1")
            bar.bidvolume1 = row.get("bidvolume1")
            bar.settle = row.get("settle")
            return bar
        except Exception:
            return None
