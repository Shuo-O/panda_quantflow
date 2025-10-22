"""
QuestDB client utilities.

Provides minimal write (ILP) and query (HTTP) helpers so services can
optionally persist high-frequency data in QuestDB without adding heavy
dependencies.
"""

from __future__ import annotations

import os
import socket
import threading
from dataclasses import dataclass
from typing import Dict, Optional

import requests

from common.config.config import get_config


def _config_or_env(key: str, default: str) -> str:
    cfg = get_config()
    return os.getenv(key, cfg.get(key, default))


def _parse_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class QuestDBSettings:
    enabled: bool
    host: str
    ilp_port: int
    http_port: int
    username: Optional[str]
    password: Optional[str]
    tick_table: str

    @classmethod
    def load(cls) -> "QuestDBSettings":
        return cls(
            enabled=_parse_bool(_config_or_env("QUESTDB_ENABLE", "false")),
            host=_config_or_env("QUESTDB_HOST", "localhost"),
            ilp_port=int(_config_or_env("QUESTDB_ILP_PORT", "9009")),
            http_port=int(_config_or_env("QUESTDB_HTTP_PORT", "9000")),
            username=_config_or_env("QUESTDB_USERNAME", "") or None,
            password=_config_or_env("QUESTDB_PASSWORD", "") or None,
            tick_table=_config_or_env("QUESTDB_TICK_TABLE", "future_ticks"),
        )


class QuestDBClient:
    """Minimal QuestDB helper supporting ILP writes and REST queries."""

    _instance_lock = threading.Lock()
    _instance: Optional["QuestDBClient"] = None

    def __init__(self, settings: QuestDBSettings):
        self.settings = settings
        self._socket: Optional[socket.socket] = None
        self._socket_lock = threading.Lock()

    @classmethod
    def instance(cls) -> Optional["QuestDBClient"]:
        settings = QuestDBSettings.load()
        if not settings.enabled:
            return None
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = QuestDBClient(settings)
            return cls._instance

    def _ensure_socket(self) -> socket.socket:
        if self._socket is None:
            self._socket = socket.create_connection((self.settings.host, self.settings.ilp_port), timeout=1.0)
        return self._socket

    def write_line(self, line: str) -> None:
        """Write raw ILP line."""
        if not self.settings.enabled:
            return
        try:
            with self._socket_lock:
                sock = self._ensure_socket()
                sock.sendall(line.encode("utf-8"))
        except Exception:
            # reset socket and ignore to avoid breaking main flow
            self.close()

    def write_tick(self, symbol: str, data: Dict[str, Optional[float]], timestamp_ns: int) -> None:
        """
        Write tick data using line protocol.
        Fields are optional; absent values are skipped.
        """
        if not self.settings.enabled:
            return

        fields = []
        for key, value in data.items():
            if value is None:
                continue
            if isinstance(value, (int, float)):
                fields.append(f"{key}={value}")
            else:
                fields.append(f'{key}="{value}"')

        if not fields:
            return

        # Symbol is tag to allow grouping by instrument
        line = f"{self.settings.tick_table},symbol={symbol} " + ",".join(fields) + f" {timestamp_ns}\n"
        self.write_line(line)

    def query(self, sql: str, limit: int = 1000):
        """Execute SQL via REST API. Returns JSON on success, None otherwise."""
        if not self.settings.enabled:
            return None
        url = f"http://{self.settings.host}:{self.settings.http_port}/exec"
        auth = None
        if self.settings.username and self.settings.password:
            auth = (self.settings.username, self.settings.password)
        try:
            response = requests.get(
                url,
                params={"query": sql, "limit": limit},
                auth=auth,
                timeout=3,
            )
            response.raise_for_status()
            return response.json()
        except Exception:
            return None

    def close(self):
        with self._socket_lock:
            if self._socket:
                try:
                    self._socket.close()
                except Exception:
                    pass
                self._socket = None
