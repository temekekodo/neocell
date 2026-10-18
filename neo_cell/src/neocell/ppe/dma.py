"""The transfer layer: the conceptual DMA path, made visible.

On the Cell, moving data is a first-class cost that competes with
compute. Here every byte crossing the PPE/SPE boundary is counted, and
optionally charged a simulated latency proportional to size. That is
what lets the demo show why task size and data locality matter instead
of only proving that threads run in parallel.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass


@dataclass(slots=True)
class TransferCounters:
    """Byte and time accounting for one logical direction."""

    bytes_moved: int = 0
    transfers: int = 0
    seconds: float = 0.0

    def add(self, nbytes: int, seconds: float) -> None:
        self.bytes_moved += nbytes
        self.transfers += 1
        self.seconds += seconds

    def as_dict(self) -> dict[str, float | int]:
        return {
            "bytes_moved": self.bytes_moved,
            "transfers": self.transfers,
            "seconds": round(self.seconds, 9),
        }


class DmaModel:
    """Simulated DMA cost: fixed latency plus size / bandwidth.

    With the default zero latency and infinite bandwidth the model is
    pure bookkeeping, which is what version 0.1 wants; setting a
    bandwidth makes transfer volume matter to the schedule.
    """

    def __init__(
        self,
        bytes_per_second: float | None = None,
        fixed_latency_s: float = 0.0,
    ) -> None:
        self.bytes_per_second = bytes_per_second
        self.fixed_latency_s = fixed_latency_s
        self.to_worker = TransferCounters()
        self.from_worker = TransferCounters()
        self.enabled = bytes_per_second is not None or fixed_latency_s > 0

    def charge(self, nbytes: int) -> float:
        """Return the simulated seconds for moving ``nbytes``."""
        if not self.enabled:
            return 0.0
        seconds = self.fixed_latency_s
        if self.bytes_per_second:
            seconds += nbytes / self.bytes_per_second
        return seconds

    async def move(self, nbytes: int, to_worker: bool) -> float:
        """Account and, when configured, sleep for the modelled cost.

        The modelled seconds are returned so the caller can attribute
        them to a specific task; the running totals alone cannot, since
        several tasks overlap in flight.
        """
        seconds = self.charge(nbytes)
        if to_worker:
            self.to_worker.add(nbytes, seconds)
        else:
            self.from_worker.add(nbytes, seconds)
        if seconds > 0:
            await asyncio.sleep(seconds)
        return seconds

    @property
    def total_seconds(self) -> float:
        return self.to_worker.seconds + self.from_worker.seconds

    def totals(self) -> dict[str, dict[str, float | int]]:
        return {
            "to_worker": self.to_worker.as_dict(),
            "from_worker": self.from_worker.as_dict(),
        }

    @property
    def total_bytes(self) -> int:
        return self.to_worker.bytes_moved + self.from_worker.bytes_moved
