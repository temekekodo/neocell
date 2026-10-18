"""Little-endian float32 codec, stdlib-only.

Payloads are raw little-endian float32 arrays so that the wire format
stays trivially decodable from Java, C, or anything else. ``struct``
does the conversion without pulling in numpy, which keeps an SPE
dependency-free and makes the Java port a straight reimplementation.
"""

from __future__ import annotations

import struct
import sys
from array import array
from collections.abc import Iterable

_SCALAR = struct.Struct("<f")
_HOST_BIG_ENDIAN = sys.byteorder == "big"


def pack_f32(values: Iterable[float]) -> bytes:
    arr = array("f", values)
    if _HOST_BIG_ENDIAN:
        arr.byteswap()
    return arr.tobytes()


def unpack_f32(payload: bytes) -> list[float]:
    if len(payload) % 4:
        raise ValueError(f"float32 payload must be 4-byte aligned, got {len(payload)}")
    arr = array("f")
    arr.frombytes(payload)
    if _HOST_BIG_ENDIAN:
        arr.byteswap()
    return list(arr)


def pack_f32_into(dst: memoryview, values: Iterable[float]) -> int:
    """Write float32 values into an existing writable memoryview."""
    data = pack_f32(values)
    if len(data) > len(dst):
        raise ValueError("destination too small for values")
    dst[: len(data)] = data
    return len(data)


def unpack_f32_from(view: memoryview) -> list[float]:
    return unpack_f32(bytes(view))


def pack_f32_scalar(value: float) -> bytes:
    return _SCALAR.pack(value)


def unpack_f32_scalar(payload: bytes) -> float:
    if len(payload) != 4:
        raise ValueError("expected exactly one float32")
    return _SCALAR.unpack(payload)[0]


def f32_nbytes(count: int) -> int:
    return count * 4
