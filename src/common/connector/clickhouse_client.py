"""
ClickHouse client utilities using HTTP JSONEachRow interface.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from typing import Optional

import requests

from common.config.config import get_config


def _parse_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class ClickHouseSettings:
    enabled: bool
    host: str
    http_port: int
    database: str
    username: Optional[str]
    password: Optional[str]
    tick_table: str
    timeout: float

    @classmethod
    def load(cls) -> "ClickHouseSettings":
        cfg = get_config()
        return cls(
            enabled=_parse_bool(cfg.get("CLICKHOUSE_ENABLE", "false")),
            host=cfg.get("CLICKHOUSE_HOST", "localhost"),
            http_port=int(cfg.get("CLICKHOUSE_HTTP_PORT", 8123)),
            database=cfg.get("CLICKHOUSE_DATABASE", "default"),
            username=cfg.get("CLICKHOUSE_USERNAME") or None,
            password=cfg.get("CLICKHOUSE_PASSWORD") or None,
            tick_table=cfg.get("CLICKHOUSE_TICK_TABLE", "future_ticks"),
            timeout=float(cfg.get("CLICKHOUSE_TIMEOUT", 2.0)),
        )


class ClickHouseClient:
    _instance_lock = threading.Lock()
    _instance: Optional["ClickHouseClient"] = None

    def __init__(self, settings: ClickHouseSettings):
        self.settings = settings
        self._base_url = f"http://{settings.host}:{settings.http_port}"
        self._auth = None
        if settings.username and settings.password:
            self._auth = (settings.username, settings.password)

    @classmethod
    def instance(cls) -> Optional["ClickHouseClient"]:
        settings = ClickHouseSettings.load()
        if not settings.enabled:
            return None
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = ClickHouseClient(settings)
            return cls._instance

    def _post(self, query: str, data: str):
        try:
            resp = requests.post(
                f"{self._base_url}/",
                params={"query": query},
                data=data,
                auth=self._auth,
                timeout=self.settings.timeout,
            )
            resp.raise_for_status()
        except Exception:
            # 保持主流程不中断
            pass

    def write_tick(self, row: dict) -> None:
        """
        Insert tick data. `row` should already contain the fields to insert.
        """
        if not self.settings.enabled:
            return
        payload = json.dumps(row, ensure_ascii=False) + "\n"
        query = f"INSERT INTO {self.settings.database}.{self.settings.tick_table} FORMAT JSONEachRow"
        self._post(query, payload)

    def query(self, sql: str):
        if not self.settings.enabled:
            return None
        try:
            resp = requests.get(
                f"{self._base_url}/",
                params={"query": sql, "database": self.settings.database, "default_format": "JSON"},
                auth=self._auth,
                timeout=self.settings.timeout,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return None
