from common.config.config import get_config
from .redis_sub_pub import RedisSubPub
from .kafka_sub_pub import KafkaSubPub


def get_trade_signal_bus():
    cfg = get_config()
    use_kafka = str(cfg.get("KAFKA_ENABLE_TRADE_SIGNAL", "false")).lower() in {"1", "true", "yes", "on"}
    if use_kafka:
        kafka_bus = KafkaSubPub()
        if kafka_bus.is_enabled():
            return kafka_bus
    return RedisSubPub()
