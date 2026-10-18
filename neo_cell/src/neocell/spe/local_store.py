"""A bounded local store, the emulated equivalent of an SPE's LS.

On real hardware an SPE has a fixed, small, fast scratch memory and must
stage every input into it through DMA before it can compute. The same
constraint is reproduced here: the workspace is a fixed-size buffer, a
staged buffer is returned as a ``memoryview`` so ops can read it without
copying, and an oversized stage raises rather than silently growing.
"""

from __future__ import annotations

from collections.abc import Sequence


class LocalStoreOverflow(Exception):
    """A stage or allocation exceeded the local store capacity."""


class LocalStore:
    """Fixed-capacity scratch memory addressed with explicit staging."""

    __slots__ = ("capacity", "_buf", "_used", "high_water")

    def __init__(self, capacity_bytes: int) -> None:
        if capacity_bytes <= 0:
            raise ValueError("capacity_bytes must be positive")
        self.capacity = capacity_bytes
        self._buf = bytearray(capacity_bytes)
        self._used = 0
        self.high_water = 0

    @property
    def used(self) -> int:
        return self._used

    @property
    def free(self) -> int:
        return self.capacity - self._used

    def alloc(self, nbytes: int) -> memoryview:
        if nbytes > self.free:
            raise LocalStoreOverflow(
                f"requested {nbytes} bytes with {self.free} free "
                f"of {self.capacity}"
            )
        start = self._used
        self._used += nbytes
        self.high_water = max(self.high_water, self._used)
        return memoryview(self._buf)[start : start + nbytes]

    def stage(self, src: bytes | bytearray | memoryview) -> memoryview:
        """Copy raw bytes into the local store: the modelled DMA-in."""
        dst = self.alloc(len(src))
        dst[:] = src
        return dst

    def stage_f32(self, values: Sequence[float]) -> memoryview:
        """Stage float32 values: the DMA-in for numeric op input.

        Values are written as little-endian float32 regardless of host
        byte order, so an op sees identical bytes on every machine.
        """
        from neocell.codec import pack_f32

        data = pack_f32(values)
        return self.stage(data)

    def reset(self) -> None:
        """Release every staged buffer between tasks, as an SPE would."""
        self._used = 0

    def __repr__(self) -> str:
        return (
            f"LocalStore(capacity={self.capacity}, high_water={self.high_water})"
        )