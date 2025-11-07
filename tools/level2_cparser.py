"""
C-backed parsers for JoinQuant Level2 binary files.

Uses cffi to JIT-compile small helper functions that decode fixed-length
records into NumPy arrays with minimal Python overhead.
"""

from __future__ import annotations

import mmap
import pathlib
from dataclasses import dataclass
from typing import Optional

try:
    import numpy as np
    from cffi import FFI
except ImportError as exc:  # pragma: no cover - optional dependency
    raise RuntimeError(
        "NumPy and cffi are required for the high-performance Level2 parser. "
        "Please install them via `pip install numpy cffi`."
    ) from exc

ffi = FFI()
ffi.cdef(
    """
    size_t parse_ticks(
        const unsigned char* data,
        size_t total_size,
        uint32_t* time_ms,
        double* volume,
        double* last,
        double* high,
        double* low,
        double* preclose,
        double* limit_up,
        double* limit_down,
        double* ask_prices,
        double* bid_prices,
        uint32_t* ask_volumes,
        uint32_t* bid_volumes
    );

    size_t parse_orders(
        const unsigned char* data,
        size_t total_size,
        uint32_t* time_ms,
        double* price,
        uint32_t* volume,
        uint32_t* seq,
        uint32_t* channel,
        uint32_t* function_code,
        uint32_t* side_flag
    );

    size_t parse_trades(
        const unsigned char* data,
        size_t total_size,
        uint32_t* time_ms,
        double* price,
        double* volume,
        uint32_t* buy_seq,
        uint32_t* sell_seq,
        uint32_t* channel,
        uint32_t* function_code
    );
    """
)

C_SOURCE = r"""
    #include <stdint.h>
    #include <stddef.h>
    #include <string.h>

    size_t parse_ticks(
        const unsigned char* data,
        size_t total_size,
        uint32_t* time_ms,
        double* volume,
        double* last,
        double* high,
        double* low,
        double* preclose,
        double* limit_up,
        double* limit_down,
        double* ask_prices,
        double* bid_prices,
        uint32_t* ask_volumes,
        uint32_t* bid_volumes
    ) {
        const size_t RECORD_SIZE = 360;
        size_t count = total_size / RECORD_SIZE;
        for (size_t i = 0; i < count; ++i) {
            const unsigned char* rec = data + i * RECORD_SIZE;
            memcpy(&time_ms[i], rec + 12, sizeof(uint32_t));
            memcpy(&volume[i],  rec + 24, sizeof(double));
            memcpy(&last[i],    rec + 32, sizeof(double));
            memcpy(&high[i],    rec + 48, sizeof(double));
            memcpy(&low[i],     rec + 56, sizeof(double));
            memcpy(&preclose[i],    rec + 336, sizeof(double));
            memcpy(&limit_up[i],    rec + 344, sizeof(double));
            memcpy(&limit_down[i],  rec + 352, sizeof(double));

            for (size_t j = 0; j < 10; ++j) {
                memcpy(&ask_prices[i * 10 + j], rec + 80  + j * 8, sizeof(double));
                memcpy(&bid_prices[i * 10 + j], rec + 160 + j * 8, sizeof(double));
                memcpy(&ask_volumes[i * 10 + j], rec + 240 + j * 4, sizeof(uint32_t));
                memcpy(&bid_volumes[i * 10 + j], rec + 280 + j * 4, sizeof(uint32_t));
            }
        }
        return count;
    }

    size_t parse_orders(
        const unsigned char* data,
        size_t total_size,
        uint32_t* time_ms,
        double* price,
        uint32_t* volume,
        uint32_t* seq,
        uint32_t* channel,
        uint32_t* function_code,
        uint32_t* side_flag
    ) {
        const size_t RECORD_SIZE = 72;
        size_t count = total_size / RECORD_SIZE;
        for (size_t i = 0; i < count; ++i) {
            const unsigned char* rec = data + i * RECORD_SIZE;
            memcpy(&time_ms[i], rec + 12, sizeof(uint32_t));
            memcpy(&price[i],   rec + 32, sizeof(double));
            memcpy(&volume[i],  rec + 40, sizeof(uint32_t));
            memcpy(&side_flag[i], rec + 48, sizeof(uint32_t));
            memcpy(&seq[i],     rec + 52, sizeof(uint32_t));
            memcpy(&channel[i], rec + 56, sizeof(uint32_t));
            memcpy(&function_code[i], rec + 60, sizeof(uint32_t));
        }
        return count;
    }

    size_t parse_trades(
        const unsigned char* data,
        size_t total_size,
        uint32_t* time_ms,
        double* price,
        double* volume,
        uint32_t* buy_seq,
        uint32_t* sell_seq,
        uint32_t* channel,
        uint32_t* function_code
    ) {
        const size_t RECORD_SIZE = 72;
        size_t count = total_size / RECORD_SIZE;
        for (size_t i = 0; i < count; ++i) {
            const unsigned char* rec = data + i * RECORD_SIZE;
            uint64_t raw_volume = 0;
            memcpy(&time_ms[i], rec + 12, sizeof(uint32_t));
            memcpy(&price[i],   rec + 32, sizeof(double));
            memcpy(&raw_volume, rec + 40, sizeof(uint64_t));
            volume[i] = (double) raw_volume;
            memcpy(&buy_seq[i],  rec + 48, sizeof(uint32_t));
            memcpy(&sell_seq[i], rec + 52, sizeof(uint32_t));
            memcpy(&function_code[i], rec + 56, sizeof(uint32_t));
            memcpy(&channel[i], rec + 64, sizeof(uint32_t));
        }
        return count;
    }
"""

C = ffi.verify(
    C_SOURCE,
    extra_compile_args=["-O3"],
)


def _mmap_file(path: pathlib.Path) -> tuple[mmap.mmap, object]:
    file_obj = path.open("rb")
    mm = mmap.mmap(file_obj.fileno(), 0, access=mmap.ACCESS_READ)
    return mm, file_obj


@dataclass
class TickParseResult:
    count: int
    time_ms: np.ndarray
    volume: np.ndarray
    last: np.ndarray
    high: np.ndarray
    low: np.ndarray
    preclose: np.ndarray
    limit_up: np.ndarray
    limit_down: np.ndarray
    ask_prices: np.ndarray  # shape (count, 10)
    bid_prices: np.ndarray
    ask_volumes: np.ndarray  # uint32
    bid_volumes: np.ndarray


@dataclass
class OrderParseResult:
    count: int
    time_ms: np.ndarray
    price: np.ndarray
    volume: np.ndarray
    seq: np.ndarray
    channel: np.ndarray
    function_code: np.ndarray
    side_flag: np.ndarray


@dataclass
class TradeParseResult:
    count: int
    time_ms: np.ndarray
    price: np.ndarray
    volume: np.ndarray
    buy_seq: np.ndarray
    sell_seq: np.ndarray
    channel: np.ndarray
    function_code: np.ndarray


def _np_uint32_array(size: int) -> np.ndarray:
    return np.empty(size, dtype=np.uint32)


def _np_float_array(size: int) -> np.ndarray:
    return np.empty(size, dtype=np.float64)


def parse_ticks(path: pathlib.Path) -> TickParseResult:
    mm, file_obj = _mmap_file(path)
    total_size = len(mm)
    record_size = 360
    count = total_size // record_size
    if count == 0:
        mm.close()
        file_obj.close()
        empty_float = np.empty(0, dtype=np.float64)
        empty_uint = np.empty(0, dtype=np.uint32)
        empty_float_matrix = np.empty((0, 10), dtype=np.float64)
        empty_uint_matrix = np.empty((0, 10), dtype=np.uint32)
        return TickParseResult(
            count=0,
            time_ms=empty_uint,
            volume=empty_float,
            last=empty_float,
            high=empty_float,
            low=empty_float,
            preclose=empty_float,
            limit_up=empty_float,
            limit_down=empty_float,
            ask_prices=empty_float_matrix,
            bid_prices=empty_float_matrix,
            ask_volumes=empty_uint_matrix,
            bid_volumes=empty_uint_matrix,
        )

    time_ms = _np_uint32_array(count)
    volume = _np_float_array(count)
    last = _np_float_array(count)
    high = _np_float_array(count)
    low = _np_float_array(count)
    preclose = _np_float_array(count)
    limit_up = _np_float_array(count)
    limit_down = _np_float_array(count)
    ask_prices = np.empty((count, 10), dtype=np.float64)
    bid_prices = np.empty_like(ask_prices)
    ask_volumes = np.empty((count, 10), dtype=np.uint32)
    bid_volumes = np.empty_like(ask_volumes)

    parsed = C.parse_ticks(
        ffi.from_buffer("unsigned char[]", mm),
        total_size,
        ffi.from_buffer("uint32_t[]", time_ms),
        ffi.from_buffer("double[]", volume),
        ffi.from_buffer("double[]", last),
        ffi.from_buffer("double[]", high),
        ffi.from_buffer("double[]", low),
        ffi.from_buffer("double[]", preclose),
        ffi.from_buffer("double[]", limit_up),
        ffi.from_buffer("double[]", limit_down),
        ffi.from_buffer("double[]", ask_prices.reshape(-1)),
        ffi.from_buffer("double[]", bid_prices.reshape(-1)),
        ffi.from_buffer("uint32_t[]", ask_volumes.reshape(-1)),
        ffi.from_buffer("uint32_t[]", bid_volumes.reshape(-1)),
    )

    mm.close()
    file_obj.close()

    return TickParseResult(
        count=int(parsed),
        time_ms=time_ms,
        volume=volume,
        last=last,
        high=high,
        low=low,
        preclose=preclose,
        limit_up=limit_up,
        limit_down=limit_down,
        ask_prices=ask_prices,
        bid_prices=bid_prices,
        ask_volumes=ask_volumes,
        bid_volumes=bid_volumes,
    )


def parse_orders(path: pathlib.Path) -> OrderParseResult:
    mm, file_obj = _mmap_file(path)
    total_size = len(mm)
    record_size = 72
    count = total_size // record_size
    if count == 0:
        mm.close()
        file_obj.close()
        empty_uint = np.empty(0, dtype=np.uint32)
        empty_float = np.empty(0, dtype=np.float64)
        return OrderParseResult(
            count=0,
            time_ms=empty_uint,
            price=empty_float,
            volume=empty_uint,
            seq=empty_uint,
            channel=empty_uint,
            function_code=empty_uint,
            side_flag=empty_uint,
        )

    time_ms = _np_uint32_array(count)
    price = _np_float_array(count)
    volume = _np_uint32_array(count)
    seq = _np_uint32_array(count)
    channel = _np_uint32_array(count)
    function_code = _np_uint32_array(count)
    side_flag = _np_uint32_array(count)

    parsed = C.parse_orders(
        ffi.from_buffer("unsigned char[]", mm),
        total_size,
        ffi.from_buffer("uint32_t[]", time_ms),
        ffi.from_buffer("double[]", price),
        ffi.from_buffer("uint32_t[]", volume),
        ffi.from_buffer("uint32_t[]", seq),
        ffi.from_buffer("uint32_t[]", channel),
        ffi.from_buffer("uint32_t[]", function_code),
        ffi.from_buffer("uint32_t[]", side_flag),
    )

    mm.close()
    file_obj.close()

    return OrderParseResult(
        count=int(parsed),
        time_ms=time_ms,
        price=price,
        volume=volume,
        seq=seq,
        channel=channel,
        function_code=function_code,
        side_flag=side_flag,
    )


def parse_trades(path: pathlib.Path) -> TradeParseResult:
    mm, file_obj = _mmap_file(path)
    total_size = len(mm)
    record_size = 72
    count = total_size // record_size
    if count == 0:
        mm.close()
        file_obj.close()
        empty_uint = np.empty(0, dtype=np.uint32)
        empty_float = np.empty(0, dtype=np.float64)
        return TradeParseResult(
            count=0,
            time_ms=empty_uint,
            price=empty_float,
            volume=empty_float,
            buy_seq=empty_uint,
            sell_seq=empty_uint,
            channel=empty_uint,
            function_code=empty_uint,
        )

    time_ms = _np_uint32_array(count)
    price = _np_float_array(count)
    volume = _np_float_array(count)
    buy_seq = _np_uint32_array(count)
    sell_seq = _np_uint32_array(count)
    channel = _np_uint32_array(count)
    function_code = _np_uint32_array(count)

    parsed = C.parse_trades(
        ffi.from_buffer("unsigned char[]", mm),
        total_size,
        ffi.from_buffer("uint32_t[]", time_ms),
        ffi.from_buffer("double[]", price),
        ffi.from_buffer("double[]", volume),
        ffi.from_buffer("uint32_t[]", buy_seq),
        ffi.from_buffer("uint32_t[]", sell_seq),
        ffi.from_buffer("uint32_t[]", channel),
        ffi.from_buffer("uint32_t[]", function_code),
    )

    mm.close()
    file_obj.close()

    return TradeParseResult(
        count=int(parsed),
        time_ms=time_ms,
        price=price,
        volume=volume,
        buy_seq=buy_seq,
        sell_seq=sell_seq,
        channel=channel,
        function_code=function_code,
    )
