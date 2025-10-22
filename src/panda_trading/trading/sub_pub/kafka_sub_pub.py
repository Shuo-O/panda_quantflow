import json
import threading
import time
from typing import Optional

from common.config.config import get_config
from common.connector.kafka_client import KafkaClientFactory, KafkaSettings
from utils.log.log_factory import LogFactory
from utils.thread.thread_util import ThreadUtil


class KafkaSubPub:
    def __init__(self):
        self.logger = LogFactory.get_logger()
        self._settings = KafkaSettings.from_env()
        self._config = get_config()
        self._topic_prefix = self._config.get("KAFKA_TRADE_SIGNAL_TOPIC_PREFIX", "trade.signal")
        self._group_prefix = self._config.get("KAFKA_TRADE_SIGNAL_GROUP_PREFIX", "trade-signal-consumer")
        self._factory: Optional[KafkaClientFactory] = None
        self._producer = None
        self.trade_signal_thread: Optional[threading.Thread] = None
        self.qry_account_thread: Optional[threading.Thread] = None
        self.check_status_time = None

        self._enabled = self._settings.is_configured()
        if self._enabled:
            try:
                self._factory = KafkaClientFactory(self._settings)
                self._producer = self._factory.get_producer()
            except Exception as exc:
                self.logger.error("KafkaSubPub 初始化失败: %s", exc, exc_info=True)
                self._enabled = False

    def is_enabled(self) -> bool:
        return self._enabled

    def _topic(self, key: str) -> str:
        return f"{self._topic_prefix}.{key}"

    def pub_data(self, sub_key: str, json_data: str):
        if not self._enabled or not self._producer:
            return
        topic = self._topic(sub_key)
        try:
            payload = json_data if isinstance(json_data, str) else json.dumps(json_data)
            self._producer.send(topic, payload.encode("utf-8"))
        except Exception as exc:
            self.logger.warning("KafkaSubPub 发布失败 topic=%s: %s", topic, exc)

    def init_sub_trade_signal(self, run_id: str, account_type: int, call_back):
        if not self._enabled or not self._factory:
            raise RuntimeError("Kafka 未配置，无法订阅交易信号")

        routing_key = "stock" if account_type == 0 else "future"
        topics = [self._topic(routing_key), self._topic(f"risk_reload_{run_id}")]
        group_id = f"{self._group_prefix}.{routing_key}.{run_id}"

        def _consume():
            while True:
                try:
                    consumer = self._factory.create_consumer(
                        topics,
                        group_id=group_id,
                        value_deserializer=lambda v: v.decode("utf-8"),
                        enable_auto_commit=True,
                        auto_offset_reset="latest",
                    )
                    for msg in consumer:
                        try:
                            body = msg.value
                            body_dict = json.loads(body)
                            timestamp = body_dict.get("time", int(time.time() * 1000))
                            data = body_dict.get("data")
                            call_back(None, None, timestamp, data)
                        except Exception as cb_exc:
                            self.logger.error("KafkaSubPub 回调异常: %s", cb_exc, exc_info=True)
                except Exception as exc:
                    self.logger.error("KafkaSubPub 消费异常: %s", exc, exc_info=True)
                    time.sleep(5)

        self.trade_signal_thread = threading.Thread(
            target=_consume, name="KafkaTradeSignalThread", daemon=True
        )
        self.trade_signal_thread.start()

    def init_qry_account(self, call_back):
        self.qry_account_thread = ThreadUtil.hand_cycle_thread(call_back, (), 0.5)

    def init_check_thread(self):
        ThreadUtil.hand_cycle_thread(self.check_thread_status, (), 600)

    def check_thread_status(self):
        now = time.time()
        if (
            self.check_status_time is None
            or now - self.check_status_time > 60
        ):
            self.check_status_time = now
            alive_signal = self.trade_signal_thread.is_alive() if self.trade_signal_thread else False
            alive_qry = self.qry_account_thread.is_alive() if self.qry_account_thread else False
            self.logger.info("Kafka 交易信号线程：%s", alive_signal)
            self.logger.info("Kafka 账户查询线程：%s", alive_qry)
