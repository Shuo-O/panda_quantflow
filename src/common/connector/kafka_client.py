"""
Kafka client utilities shared by Panda QuantFlow services.

This module provides a thin wrapper around ``kafka-python`` so components
can lazily create producers / consumers while keeping configuration in one
place.  It intentionally keeps the footprint small so we can gradually
introduce Kafka without breaking existing Redis / RabbitMQ integrations.
"""

from __future__ import annotations

import os
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Mapping, Optional

try:
    from kafka import KafkaConsumer, KafkaProducer  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    KafkaConsumer = None  # type: ignore
    KafkaProducer = None  # type: ignore


def _parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class KafkaSettings:
    """Aggregated Kafka configuration."""

    bootstrap_servers: str = ""
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    client_id: str = "panda-quantflow"
    enable_idempotence: bool = False
    request_timeout_ms: int = 30_000
    session_timeout_ms: int = 45_000
    extra_producer_config: Dict[str, Any] = field(default_factory=dict)
    extra_consumer_config: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None) -> "KafkaSettings":
        env = env or os.environ
        settings = cls(
            bootstrap_servers=env.get("KAFKA_BOOTSTRAP_SERVERS", ""),
            security_protocol=env.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
            sasl_mechanism=env.get("KAFKA_SASL_MECHANISM") or None,
            sasl_username=env.get("KAFKA_SASL_USERNAME") or None,
            sasl_password=env.get("KAFKA_SASL_PASSWORD") or None,
            client_id=env.get("KAFKA_CLIENT_ID", "panda-quantflow"),
            enable_idempotence=_parse_bool(env.get("KAFKA_ENABLE_IDEMPOTENCE", "false")),
            request_timeout_ms=int(env.get("KAFKA_REQUEST_TIMEOUT_MS", "30000")),
            session_timeout_ms=int(env.get("KAFKA_SESSION_TIMEOUT_MS", "45000")),
        )
        if not settings.is_configured():
            try:
                from common.config.config import get_config  # local import to avoid cycles

                cfg = get_config()
                settings.bootstrap_servers = cfg.get("KAFKA_BOOTSTRAP_SERVERS", "")
                settings.security_protocol = cfg.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
                settings.sasl_mechanism = cfg.get("KAFKA_SASL_MECHANISM") or None
                settings.sasl_username = cfg.get("KAFKA_SASL_USERNAME") or None
                settings.sasl_password = cfg.get("KAFKA_SASL_PASSWORD") or None
                settings.client_id = cfg.get("KAFKA_CLIENT_ID", settings.client_id)
                settings.enable_idempotence = _parse_bool(
                    cfg.get("KAFKA_ENABLE_IDEMPOTENCE", str(settings.enable_idempotence))
                )
                settings.request_timeout_ms = int(
                    cfg.get("KAFKA_REQUEST_TIMEOUT_MS", settings.request_timeout_ms)
                )
                settings.session_timeout_ms = int(
                    cfg.get("KAFKA_SESSION_TIMEOUT_MS", settings.session_timeout_ms)
                )
            except Exception:
                # Fall back to original env-based values if config module is unavailable
                pass
        return settings

    def is_configured(self) -> bool:
        return bool(self.bootstrap_servers.strip())

    def bootstrap_list(self) -> Iterable[str]:
        return [server.strip() for server in self.bootstrap_servers.split(",") if server.strip()]


class KafkaClientFactory:
    """Lazy singleton factory for Kafka producers and consumers."""

    def __init__(self, settings: Optional[KafkaSettings] = None):
        self.settings = settings or KafkaSettings.from_env()
        self._producer_lock = threading.Lock()
        self._producer: Optional[KafkaProducer] = None

    def _ensure_dependency(self) -> None:
        if KafkaProducer is None or KafkaConsumer is None:
            raise RuntimeError(
                "kafka-python is not installed. Please install the extra dependency first."
            )

    def _build_common_config(self) -> Dict[str, Any]:
        cfg: Dict[str, Any] = {
            "bootstrap_servers": list(self.settings.bootstrap_list()),
            "security_protocol": self.settings.security_protocol,
            "request_timeout_ms": self.settings.request_timeout_ms,
            "api_version_auto_timeout_ms": self.settings.request_timeout_ms,
        }
        if self.settings.security_protocol.upper().startswith("SASL"):
            cfg["sasl_mechanism"] = self.settings.sasl_mechanism
            cfg["sasl_plain_username"] = self.settings.sasl_username
            cfg["sasl_plain_password"] = self.settings.sasl_password
        return cfg

    def get_producer(self, **overrides: Any) -> KafkaProducer:
        self._ensure_dependency()
        if not self.settings.is_configured():
            raise RuntimeError("Kafka bootstrap servers are not configured")

        with self._producer_lock:
            if self._producer is None:
                cfg = self._build_common_config()
                cfg.update(
                    {
                        "client_id": self.settings.client_id,
                        "acks": "all",
                        "retries": 5,
                        "linger_ms": 5,
                        "enable_idempotence": self.settings.enable_idempotence,
                    }
                )
                cfg.update(self.settings.extra_producer_config)
                cfg.update(overrides)
                self._producer = KafkaProducer(**cfg)
            return self._producer

    def create_consumer(
        self,
        topic: str,
        group_id: str,
        **overrides: Any,
    ) -> KafkaConsumer:
        self._ensure_dependency()
        if not self.settings.is_configured():
            raise RuntimeError("Kafka bootstrap servers are not configured")

        cfg = self._build_common_config()
        cfg.update(
            {
                "client_id": self.settings.client_id,
                "group_id": group_id,
                "enable_auto_commit": False,
                "session_timeout_ms": self.settings.session_timeout_ms,
                "auto_offset_reset": "latest",
            }
        )
        cfg.update(self.settings.extra_consumer_config)
        cfg.update(overrides)
        return KafkaConsumer(topic, **cfg)

    def close(self) -> None:
        with self._producer_lock:
            if self._producer:
                self._producer.flush()
                self._producer.close()
                self._producer = None
