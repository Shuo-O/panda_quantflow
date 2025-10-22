import json
import threading
import time
import traceback
from typing import Callable, Iterable, Optional

from common.connector.kafka_client import KafkaClientFactory, KafkaSettings
from utils.log.log_factory import LogFactory


class KafkaSubPub:
    def __init__(self):
        self.logger = LogFactory.get_logger()
        self._settings = KafkaSettings.from_env()
        self._factory = None
        self._producer = None
        self._consumer_threads = []
        self._enabled = self._settings.is_configured()
        if self._enabled:
            try:
                self._factory = KafkaClientFactory(self._settings)
                self._producer = self._factory.get_producer()
            except Exception as exc:
                self.logger.error("KafkaSubPub 初始化失败: %s", exc)
                self._enabled = False

    def is_enabled(self) -> bool:
        return self._enabled

    def pub_data(self, topic: str, data: dict):
        if not self._enabled or not self._producer:
            return
        try:
            payload = json.dumps(data).encode("utf-8")
            self._producer.send(topic, payload)
        except Exception as exc:
            self.logger.warning("KafkaSubPub 发布失败 topic=%s: %s", topic, exc)

    def init_subscribe(
        self,
        topics: Iterable[str],
        group_id: str,
        callback: Callable[[str, dict], None],
        *,
        name: Optional[str] = None,
    ):
        if not self._enabled or not self._factory:
            return

        def _consume():
            while True:
                try:
                    consumer = self._factory.create_consumer(
                        topic=list(topics),
                        group_id=group_id,
                        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                        enable_auto_commit=True,
                        auto_offset_reset="latest",
                    )
                    for msg in consumer:
                        try:
                            callback(msg.topic, msg.value)
                        except Exception as cb_exc:
                            self.logger.error("KafkaSubPub 回调异常: %s", cb_exc)
                except Exception as exc:
                    self.logger.error("KafkaSubPub 消费异常: %s", exc)
                    time.sleep(5)

        thread = threading.Thread(target=_consume, name=name or "KafkaSubThread", daemon=True)
        thread.start()
        self._consumer_threads.append(thread)
