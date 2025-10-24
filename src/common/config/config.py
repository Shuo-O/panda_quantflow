"""
配置模块，用于加载和管理配置信息
支持从配置文件和环境变量导入，环境变量优先级更高
"""

import os
import logging

logger = logging.getLogger(__name__)

# 初始化配置变量
config = None


def load_config():
    """加载配置文件，并从环境变量更新配置"""
    global config
    config = {}
    # MongoDB
    config["MONGO_USER"] = os.getenv("MONGO_USER", "panda")
    config["MONGO_PASSWORD"] = os.getenv("MONGO_PASSWORD", "panda")
    config["MONGO_URI"] = os.getenv("MONGO_URI", "127.0.0.1:27017")
    config["MONGO_AUTH_DB"] = os.getenv("MONGO_AUTH_DB", "admin")
    config["MONGO_DB"] = os.getenv("MONGO_DB", "panda")
    config["MONGO_TYPE"] = os.getenv("MONGO_TYPE", "replica_set")
    config["MONGO_REPLICA_SET"] = os.getenv("MONGO_REPLICA_SET", "rs0")

    # 日志配置 Logging
    config["LOG_LEVEL"] = os.getenv("LOG_LEVEL", "DEBUG")
    config["log_file"] = os.getenv("LOG_FILE", "logs/data_cleaner.log")
    config["log_rotation"] = os.getenv("LOG_ROTATION", "1 MB")
    config["LOG_PATH"] = os.getenv("LOG_PATH", "logs")

    # Redis
    config["REDIS_HOST"] = os.getenv("REDIS_HOST", "localhost")
    config["REDIS_PORT"] = int(os.getenv("REDIS_PORT", 6379))
    config["REDIS_DB"] = int(os.getenv("REDIS_DB", 0))
    config["REDIS_PASSWORD"] = os.getenv("REDIS_PASSWORD", "123456")
    config["REDIS_MAX_CONNECTIONS"] = int(os.getenv("REDIS_MAX_CONNECTIONS", 10))
    config["REDIS_SOCKET_TIMEOUT"] = int(os.getenv("REDIS_SOCKET_TIMEOUT", 5))
    config["REDIS_CONNECT_TIMEOUT"] = int(os.getenv("REDIS_CONNECT_TIMEOUT", 5))

    # MySQL
    config["MYSQL_HOST"] = os.getenv("MYSQL_HOST", "localhost")
    config["MYSQL_PORT"] = int(os.getenv("MYSQL_PORT", 3306))
    config["MYSQL_USER"] = os.getenv("MYSQL_USER", "root")
    config["MYSQL_PASSWORD"] = os.getenv("MYSQL_PASSWORD", "qweqwe")
    config["MYSQL_DATABASE"] = os.getenv("MYSQL_DATABASE", "pandaai_test")

    # Kafka
    config["KAFKA_BOOTSTRAP_SERVERS"] = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
    )
    config["KAFKA_SECURITY_PROTOCOL"] = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    config["KAFKA_SASL_MECHANISM"] = os.getenv("KAFKA_SASL_MECHANISM", "")
    config["KAFKA_SASL_USERNAME"] = os.getenv("KAFKA_SASL_USERNAME", "")
    config["KAFKA_SASL_PASSWORD"] = os.getenv("KAFKA_SASL_PASSWORD", "")
    config["KAFKA_CLIENT_ID"] = os.getenv("KAFKA_CLIENT_ID", "panda-quantflow")
    config["KAFKA_ENABLE_IDEMPOTENCE"] = os.getenv("KAFKA_ENABLE_IDEMPOTENCE", "false")
    config["KAFKA_REQUEST_TIMEOUT_MS"] = int(
        os.getenv("KAFKA_REQUEST_TIMEOUT_MS", "60000")
    )
    config["KAFKA_SESSION_TIMEOUT_MS"] = int(
        os.getenv("KAFKA_SESSION_TIMEOUT_MS", "45000")
    )
    config["KAFKA_FUTURE_TICK_TOPIC"] = os.getenv(
        "KAFKA_FUTURE_TICK_TOPIC", "market.future.tick"
    )
    # temp！！！！！！
    # config["KAFKA_ENABLE_FUTURE_TICK"] = os.getenv(
    #     "KAFKA_ENABLE_FUTURE_TICK", "false"
    #     "true"
    # )
    config["KAFKA_ENABLE_FUTURE_TICK"] = os.getenv(
        # "KAFKA_ENABLE_FUTURE_TICK", "false"
        "true"
    )
    # temp！！！！！！
    config["KAFKA_FUTURE_TICK_GROUP_ID"] = os.getenv(
        "KAFKA_FUTURE_TICK_GROUP_ID", "panda-future-tick-consumer"
    )
    config["KAFKA_FUTURE_TICK_OFFSET_RESET"] = os.getenv(
        "KAFKA_FUTURE_TICK_OFFSET_RESET", "latest"
    )
    config["KAFKA_ENABLE_TRADE_SIGNAL"] = os.getenv(
        "KAFKA_ENABLE_TRADE_SIGNAL", "false"
    )
    config["KAFKA_TRADE_SIGNAL_TOPIC_PREFIX"] = os.getenv(
        "KAFKA_TRADE_SIGNAL_TOPIC_PREFIX", "trade.signal"
    )
    config["KAFKA_TRADE_SIGNAL_GROUP_PREFIX"] = os.getenv(
        "KAFKA_TRADE_SIGNAL_GROUP_PREFIX", "trade-signal-consumer"
    )

    # QuestDB
    config["QUESTDB_ENABLE"] = os.getenv("QUESTDB_ENABLE", "true")
    config["QUESTDB_HOST"] = os.getenv("QUESTDB_HOST", "localhost")
    config["QUESTDB_ILP_PORT"] = int(os.getenv("QUESTDB_ILP_PORT", "9009"))
    config["QUESTDB_HTTP_PORT"] = int(os.getenv("QUESTDB_HTTP_PORT", "9000"))
    config["QUESTDB_USERNAME"] = os.getenv("QUESTDB_USERNAME", "")
    config["QUESTDB_PASSWORD"] = os.getenv("QUESTDB_PASSWORD", "")
    config["QUESTDB_TICK_TABLE"] = os.getenv("QUESTDB_TICK_TABLE", "future_ticks")

    # ClickHouse
    # config["CLICKHOUSE_ENABLE"] = os.getenv("CLICKHOUSE_ENABLE", "false")
    config["CLICKHOUSE_ENABLE"] = "true"
    config["CLICKHOUSE_HOST"] = os.getenv("CLICKHOUSE_HOST", "localhost")
    config["CLICKHOUSE_HTTP_PORT"] = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
    config["CLICKHOUSE_DATABASE"] = os.getenv("CLICKHOUSE_DATABASE", "default")
    config["CLICKHOUSE_USERNAME"] = os.getenv("CLICKHOUSE_USERNAME", "")
    config["CLICKHOUSE_PASSWORD"] = os.getenv("CLICKHOUSE_PASSWORD", "")
    config["CLICKHOUSE_TICK_TABLE"] = os.getenv("CLICKHOUSE_TICK_TABLE", "future_ticks")
    config["CLICKHOUSE_TIMEOUT"] = float(os.getenv("CLICKHOUSE_TIMEOUT", "2.0"))

    return config


def get_config():
    """
    获取配置对象，如果配置未加载则先加载配置

    Returns:
        dict: 配置信息字典
    """
    global config
    if config is None:
        config = load_config()
    return config


# 初始加载配置
try:
    config = load_config()
    logger.info(f"初始化配置成功: {config}")
except Exception as e:
    logger.error(f"初始化配置失败: {str(e)}")
    # 不在初始化时抛出异常，留到实际使用时再处理
