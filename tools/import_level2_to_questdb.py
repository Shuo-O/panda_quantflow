#!/usr/bin/env python3
"""
Parse JoinQuant Level2 `.tick` / `.order` / `.trade` binary dumps and ingest
them into QuestDB so the rest of the pipeline can treat them as realtime ticks.

Usage example
-------------
python tools/import_level2_to_questdb.py \
    --date 20241107 \
    --tick-file 000001.SZ.tick \
    --order-file 000001.SZ.order \
    --trade-file 000001.SZ.trade

The script extracts the symbol from the filename automatically if the
`--symbol` flag is not provided.  Pass `--dry-run` to verify parsing without
touching QuestDB (useful when the database is not running locally).
"""

from __future__ import annotations

import argparse
import datetime as dt
import math
import pathlib
import struct
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
import sys

for candidate in (ROOT_DIR, SRC_DIR):
    path_str = str(candidate)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from common.connector.questdb_client import QuestDBClient

try:
    from tools import level2_cparser

    _HAS_C_PARSER = True
except Exception:  # pragma: no cover - optional dependency
    level2_cparser = None
    _HAS_C_PARSER = False


class QuestDBBatchWriter:
    """Aggregate ILP lines and flush in batches to reduce socket overhead."""

    def __init__(self, client: QuestDBClient, batch_size: int = 1000) -> None:
        self.client = client
        self.batch_size = max(1, batch_size)
        self._buffer: List[str] = []

    def write(self, line: str) -> None:
        self._buffer.append(line)
        if len(self._buffer) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return
        payload = "".join(self._buffer)
        self.client.write_line(payload)
        self._buffer.clear()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sanitize_tag_value(value: str) -> str:
    """
    Escape characters QuestDB (Influx line protocol) treats as separators.
    """
    return (
        value.replace("\\", "\\\\")
        .replace(" ", "\\ ")
        .replace(",", "\\,")
        .replace("=", "\\=")
    )


def _iter_chunks(path: pathlib.Path, size: int) -> Iterator[bytes]:
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(size)
            if not chunk:
                break
            if len(chunk) != size:
                raise ValueError(f"{path} has partial record (expected {size} bytes)")
            yield chunk


def _ms_to_timestamp_ns(trade_date: dt.date, ms_since_midnight: int) -> int:
    base = dt.datetime.combine(trade_date, dt.time())
    ts = base + dt.timedelta(milliseconds=ms_since_midnight)
    return int(ts.timestamp() * 1_000_000_000)


def _write_line(
    client: QuestDBClient,
    table: str,
    symbol: str,
    tags: Dict[str, str],
    fields: Dict[str, Optional[float]],
    timestamp_ns: int,
    batch_writer: Optional[QuestDBBatchWriter] = None,
) -> None:
    field_parts = []
    for key, value in fields.items():
        if value is None or (isinstance(value, float) and math.isnan(value)):
            continue
        if isinstance(value, (int, float)):
            field_parts.append(f"{key}={value}")
        else:
            field_parts.append(f'{key}="{value}"')

    if not field_parts:
        return

    tag_parts = [f"symbol={_sanitize_tag_value(symbol)}"]
    for key, value in tags.items():
        tag_parts.append(f"{key}={_sanitize_tag_value(value)}")

    line = f"{table},{','.join(tag_parts)} {','.join(field_parts)} {timestamp_ns}\n"
    if batch_writer is not None:
        batch_writer.write(line)
    else:
        client.write_line(line)


def _decode_symbol(raw: bytes, fallback: Optional[str]) -> str:
    symbol = raw.split(b"\x00", 1)[0].decode("ascii", errors="ignore")
    return symbol or (fallback or "")


def _get_symbol_from_file(path: pathlib.Path, fallback: Optional[str]) -> str:
    if fallback:
        return fallback
    with path.open("rb") as handle:
        raw = handle.read(10)
    return raw.split(b"\x00", 1)[0].decode("ascii", errors="ignore")


# ---------------------------------------------------------------------------
# Parser dataclasses
# ---------------------------------------------------------------------------


@dataclass
class Level2Tick:
    symbol: str
    time_ms: int
    volume: float
    last_price: float
    high_price: float
    low_price: float
    ask_prices: List[float]
    bid_prices: List[float]
    ask_volumes: List[int]
    bid_volumes: List[int]
    preclose: float
    limit_up: float
    limit_down: float

    RECORD_SIZE = 360

    @classmethod
    def from_bytes(cls, chunk: bytes, fallback_symbol: Optional[str]) -> "Level2Tick":
        mv = memoryview(chunk)
        symbol = _decode_symbol(mv[:10].tobytes(), fallback_symbol)
        time_ms = struct.unpack_from("<I", mv, 12)[0]
        volume = struct.unpack_from("<d", mv, 24)[0]
        last_price = struct.unpack_from("<d", mv, 32)[0]
        high_price = struct.unpack_from("<d", mv, 48)[0]
        low_price = struct.unpack_from("<d", mv, 56)[0]

        ask_prices = [struct.unpack_from("<d", mv, 80 + i * 8)[0] for i in range(10)]
        bid_prices = [struct.unpack_from("<d", mv, 160 + i * 8)[0] for i in range(10)]
        ask_volumes = [struct.unpack_from("<I", mv, 240 + i * 4)[0] for i in range(10)]
        bid_volumes = [struct.unpack_from("<I", mv, 280 + i * 4)[0] for i in range(10)]

        preclose = struct.unpack_from("<d", mv, 336)[0]
        limit_up = struct.unpack_from("<d", mv, 344)[0]
        limit_down = struct.unpack_from("<d", mv, 352)[0]

        return cls(
            symbol=symbol,
            time_ms=time_ms,
            volume=volume,
            last_price=last_price,
            high_price=high_price,
            low_price=low_price,
            ask_prices=ask_prices,
            bid_prices=bid_prices,
            ask_volumes=ask_volumes,
            bid_volumes=bid_volumes,
            preclose=preclose,
            limit_up=limit_up,
            limit_down=limit_down,
        )


@dataclass
class Level2Order:
    symbol: str
    time_ms: int
    order_id: int
    price: float
    volume: int
    side: str
    seq: int
    channel: int
    function_code: int

    RECORD_SIZE = 72

    @classmethod
    def from_bytes(cls, chunk: bytes, fallback_symbol: Optional[str]) -> "Level2Order":
        mv = memoryview(chunk)
        symbol = _decode_symbol(mv[:10].tobytes(), fallback_symbol)
        time_ms = struct.unpack_from("<I", mv, 12)[0]
        order_id = struct.unpack_from("<Q", mv, 24)[0]
        price = struct.unpack_from("<d", mv, 32)[0]
        volume = struct.unpack_from("<I", mv, 40)[0]
        side_flag = struct.unpack_from("<I", mv, 48)[0]
        seq = struct.unpack_from("<I", mv, 52)[0]
        channel = struct.unpack_from("<I", mv, 56)[0]
        function_code = struct.unpack_from("<I", mv, 60)[0]
        side = "buy" if side_flag == 0 else "sell"

        return cls(
            symbol=symbol,
            time_ms=time_ms,
            order_id=order_id,
            price=price,
            volume=volume,
            side=side,
            seq=seq,
            channel=channel,
            function_code=function_code,
        )


@dataclass
class Level2Trade:
    symbol: str
    time_ms: int
    trade_id: int
    price: float
    volume: int
    buy_seq: int
    sell_seq: int
    function_code: int
    channel: int

    RECORD_SIZE = 72

    @classmethod
    def from_bytes(cls, chunk: bytes, fallback_symbol: Optional[str]) -> "Level2Trade":
        mv = memoryview(chunk)
        symbol = _decode_symbol(mv[:10].tobytes(), fallback_symbol)
        time_ms = struct.unpack_from("<I", mv, 12)[0]
        trade_id = struct.unpack_from("<Q", mv, 24)[0]
        price = struct.unpack_from("<d", mv, 32)[0]
        volume = struct.unpack_from("<Q", mv, 40)[0]
        buy_seq = struct.unpack_from("<I", mv, 48)[0]
        sell_seq = struct.unpack_from("<I", mv, 52)[0]
        function_code = struct.unpack_from("<I", mv, 56)[0]
        channel = struct.unpack_from("<I", mv, 64)[0]

        return cls(
            symbol=symbol,
            time_ms=time_ms,
            trade_id=trade_id,
            price=price,
            volume=volume,
            buy_seq=buy_seq,
            sell_seq=sell_seq,
            function_code=function_code,
            channel=channel,
        )


# ---------------------------------------------------------------------------
# Ingest helpers
# ---------------------------------------------------------------------------


def _ingest_ticks(
    client: Optional[QuestDBClient],
    table: str,
    file_path: pathlib.Path,
    trade_date: dt.date,
    fallback_symbol: Optional[str],
    dry_run: bool,
    batch_writer: Optional[QuestDBBatchWriter],
) -> int:
    count = 0
    if _HAS_C_PARSER:
        parsed = level2_cparser.parse_ticks(file_path)  # type: ignore[union-attr]
        symbol_value = _get_symbol_from_file(file_path, fallback_symbol)
        for idx in range(parsed.count):
            last_price = float(parsed.last[idx])
            if not symbol_value or last_price == 0.0:
                continue
            ts_ns = _ms_to_timestamp_ns(trade_date, int(parsed.time_ms[idx]))
            if dry_run:
                count += 1
                continue
            fields: Dict[str, Optional[float]] = {
                "last": last_price,
                "cum_volume": float(parsed.volume[idx]),
                "high": float(parsed.high[idx]),
                "low": float(parsed.low[idx]),
                "preclose": float(parsed.preclose[idx]),
                "limit_up": float(parsed.limit_up[idx]),
                "limit_down": float(parsed.limit_down[idx]),
            }
            ask_prices = parsed.ask_prices[idx]
            bid_prices = parsed.bid_prices[idx]
            ask_volumes = parsed.ask_volumes[idx]
            bid_volumes = parsed.bid_volumes[idx]
            for level in range(10):
                fields[f"ask_price{level + 1}"] = float(ask_prices[level])
                fields[f"bid_price{level + 1}"] = float(bid_prices[level])
                fields[f"ask_volume{level + 1}"] = int(ask_volumes[level])
                fields[f"bid_volume{level + 1}"] = int(bid_volumes[level])

            _write_line(
                client=client,
                table=table,
                symbol=symbol_value,
                tags={},
                fields=fields,
                timestamp_ns=ts_ns,
                batch_writer=batch_writer,
            )
            count += 1
        return count

    for chunk in _iter_chunks(file_path, Level2Tick.RECORD_SIZE):
        record = Level2Tick.from_bytes(chunk, fallback_symbol)
        if not record.symbol or record.last_price == 0.0:
            continue

        ts_ns = _ms_to_timestamp_ns(trade_date, record.time_ms)
        if dry_run:
            count += 1
            continue

        fields = {
            "last": record.last_price,
            "cum_volume": record.volume,
            "high": record.high_price,
            "low": record.low_price,
            "preclose": record.preclose,
            "limit_up": record.limit_up,
            "limit_down": record.limit_down,
        }

        for idx in range(10):
            fields[f"ask_price{idx + 1}"] = record.ask_prices[idx]
            fields[f"bid_price{idx + 1}"] = record.bid_prices[idx]
            fields[f"ask_volume{idx + 1}"] = record.ask_volumes[idx]
            fields[f"bid_volume{idx + 1}"] = record.bid_volumes[idx]

        _write_line(
            client=client,
            table=table,
            symbol=record.symbol,
            tags={},
            fields=fields,
            timestamp_ns=ts_ns,
            batch_writer=batch_writer,
        )
        count += 1

    return count


def _ingest_orders(
    client: Optional[QuestDBClient],
    table: str,
    file_path: pathlib.Path,
    trade_date: dt.date,
    fallback_symbol: Optional[str],
    dry_run: bool,
    batch_writer: Optional[QuestDBBatchWriter],
) -> int:
    count = 0

    if _HAS_C_PARSER:
        parsed = level2_cparser.parse_orders(file_path)  # type: ignore[union-attr]
        symbol_value = _get_symbol_from_file(file_path, fallback_symbol)
        for idx in range(parsed.count):
            price = float(parsed.price[idx])
            volume = int(parsed.volume[idx])
            if not symbol_value or price == 0.0 or volume == 0:
                continue
            ts_ns = _ms_to_timestamp_ns(trade_date, int(parsed.time_ms[idx]))
            if dry_run:
                count += 1
                continue
            fields = {
                "price": price,
                "volume": volume,
                "seq": int(parsed.seq[idx]),
                "channel": int(parsed.channel[idx]),
                "function_code": int(parsed.function_code[idx]),
            }
            side = "buy" if int(parsed.side_flag[idx]) == 0 else "sell"
            _write_line(
                client=client,
                table=table,
                symbol=symbol_value,
                tags={"side": side},
                fields=fields,
                timestamp_ns=ts_ns,
                batch_writer=batch_writer,
            )
            count += 1
        return count

    for chunk in _iter_chunks(file_path, Level2Order.RECORD_SIZE):
        record = Level2Order.from_bytes(chunk, fallback_symbol)
        if not record.symbol or record.price == 0.0 or record.volume == 0:
            continue
        ts_ns = _ms_to_timestamp_ns(trade_date, record.time_ms)
        if dry_run:
            count += 1
            continue

        fields = {
            "price": record.price,
            "volume": record.volume,
            "seq": record.seq,
            "channel": record.channel,
            "function_code": record.function_code,
        }
        _write_line(
            client=client,
            table=table,
            symbol=record.symbol,
            tags={"side": record.side},
            fields=fields,
            timestamp_ns=ts_ns,
            batch_writer=batch_writer,
        )
        count += 1
    return count


def _ingest_trades(
    client: Optional[QuestDBClient],
    table: str,
    file_path: pathlib.Path,
    trade_date: dt.date,
    fallback_symbol: Optional[str],
    dry_run: bool,
    batch_writer: Optional[QuestDBBatchWriter],
) -> int:
    count = 0

    if _HAS_C_PARSER:
        parsed = level2_cparser.parse_trades(file_path)  # type: ignore[union-attr]
        symbol_value = _get_symbol_from_file(file_path, fallback_symbol)
        for idx in range(parsed.count):
            price = float(parsed.price[idx])
            volume = float(parsed.volume[idx])
            if not symbol_value or price == 0.0 or volume == 0.0:
                continue
            ts_ns = _ms_to_timestamp_ns(trade_date, int(parsed.time_ms[idx]))
            if dry_run:
                count += 1
                continue
            fields = {
                "price": price,
                "volume": volume,
                "buy_seq": int(parsed.buy_seq[idx]),
                "sell_seq": int(parsed.sell_seq[idx]),
                "channel": int(parsed.channel[idx]),
                "function_code": int(parsed.function_code[idx]),
            }
            _write_line(
                client=client,
                table=table,
                symbol=symbol_value,
                tags={},
                fields=fields,
                timestamp_ns=ts_ns,
                batch_writer=batch_writer,
            )
            count += 1
        return count

    for chunk in _iter_chunks(file_path, Level2Trade.RECORD_SIZE):
        record = Level2Trade.from_bytes(chunk, fallback_symbol)
        if not record.symbol or record.price == 0.0 or record.volume == 0:
            continue
        ts_ns = _ms_to_timestamp_ns(trade_date, record.time_ms)
        if dry_run:
            count += 1
            continue

        fields = {
            "price": record.price,
            "volume": record.volume,
            "buy_seq": record.buy_seq,
            "sell_seq": record.sell_seq,
            "channel": record.channel,
            "function_code": record.function_code,
        }
        _write_line(
            client=client,
            table=table,
            symbol=record.symbol,
            tags={},
            fields=fields,
            timestamp_ns=ts_ns,
            batch_writer=batch_writer,
        )
        count += 1
    return count


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _guess_symbol_from_path(path: Optional[pathlib.Path]) -> Optional[str]:
    if not path:
        return None
    name = path.name
    if "." not in name:
        return None
    return name.split(".")[0]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import Level2 binary files into QuestDB.")
    parser.add_argument("--date", required=True, help="Trading date in YYYYMMDD format.")
    parser.add_argument("--symbol", help="Override symbol inferred from filenames.")
    parser.add_argument("--tick-file", type=pathlib.Path, help="Path to .tick file.")
    parser.add_argument("--order-file", type=pathlib.Path, help="Path to .order file.")
    parser.add_argument("--trade-file", type=pathlib.Path, help="Path to .trade file.")
    parser.add_argument("--tick-table", help="QuestDB table for ticks (default: QUESTDB_TICK_TABLE).")
    parser.add_argument("--order-table", default="level2_orders", help="QuestDB table for order flow.")
    parser.add_argument("--trade-table", default="level2_trades", help="QuestDB table for trades.")
    parser.add_argument("--dry-run", action="store_true", help="Parse files without writing to QuestDB.")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of ILP lines to buffer before flushing to QuestDB (default: 1000).",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    trade_date = dt.datetime.strptime(args.date, "%Y%m%d").date()

    symbol_hint = args.symbol
    for candidate in (_guess_symbol_from_path(args.tick_file), _guess_symbol_from_path(args.order_file), _guess_symbol_from_path(args.trade_file)):
        if candidate:
            symbol_hint = symbol_hint or candidate

    client = QuestDBClient.instance()
    if not args.dry_run and client is None:
        raise SystemExit("QuestDB client is not configured. Set QUESTDB_ENABLE=true or use --dry-run.")

    tick_table = args.tick_table or (client.settings.tick_table if client else "future_ticks")
    batch_writer = None
    if not args.dry_run and client is not None:
        batch_writer = QuestDBBatchWriter(client, batch_size=args.batch_size)

    if not any([args.tick_file, args.order_file, args.trade_file]):
        raise SystemExit("At least one of --tick-file/--order-file/--trade-file must be provided.")

    if args.tick_file:
        count = _ingest_ticks(
            client,
            tick_table,
            args.tick_file,
            trade_date,
            symbol_hint,
            args.dry_run,
            batch_writer,
        )
        print(f"[tick] processed {count} records from {args.tick_file}")

    if args.order_file:
        count = _ingest_orders(
            client,
            args.order_table,
            args.order_file,
            trade_date,
            symbol_hint,
            args.dry_run,
            batch_writer,
        )
        print(f"[order] processed {count} records from {args.order_file}")

    if args.trade_file:
        count = _ingest_trades(
            client,
            args.trade_table,
            args.trade_file,
            trade_date,
            symbol_hint,
            args.dry_run,
            batch_writer,
        )
        print(f"[trade] processed {count} records from {args.trade_file}")

    if batch_writer:
        batch_writer.flush()


if __name__ == "__main__":
    main()
