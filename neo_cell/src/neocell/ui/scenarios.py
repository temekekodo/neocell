"""Demo scenarios for the UI.

Each one isolates a behaviour that is hard to see otherwise, so the UI is
a teaching tool rather than a dashboard. They map to the three properties
the architecture is meant to demonstrate: coordination and load spread,
explicit data movement, and the bounded local store.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from neocell.buffers import DEFAULT_INLINE_LIMIT


@dataclass(slots=True)
class Scenario:
    """One demo: a starting configuration plus why it is interesting."""

    key: str
    title: str
    summary: str
    teaches: str
    params: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "title": self.title,
            "summary": self.summary,
            "teaches": self.teaches,
            "params": self.params,
        }


SCENARIOS: list[Scenario] = [
    Scenario(
        key="fanout",
        title="Balanced fan-out",
        summary=(
            "256 small chunks across 4 SPEs. Watch the queue drain, the "
            "per-SPE task counts even out, and results come back out of order."
        ),
        teaches="Central coordination: the PPE owns the queue, the SPEs only compute.",
        params={
            "workload": "reductions",
            "spe": 4,
            "elements": 262144,
            "chunk": 1024,
            "store_bytes": 131072,
            "dma_bw": None,
        },
    ),
    Scenario(
        key="arrow",
        title="Transfer-bound, Arrow data plane",
        summary=(
            "4 chunks of 1.05 MB each. Every task is just over the 1 MiB "
            "inline limit, so each one travels as a reference and the bytes "
            "land in an Arrow IPC file the worker memory-maps."
        ),
        teaches=(
            "Explicit data movement: the control message carries a DataRef, "
            "the payload travels beside it."
        ),
        params={
            "workload": "scale",
            "spe": 4,
            "elements": 1100000,
            "chunk": 275000,
            "store_bytes": 4194304,
            "dma_bw": 40000000.0,
        },
    ),
    Scenario(
        key="overflow",
        title="Local store overflow",
        summary=(
            "Chunks are four times the local store, so staging fails on "
            "every task and the PPE retries before giving up."
        ),
        teaches=(
            "Bounded private storage: an SPE cannot hold the whole input, "
            "and the PPE reports per-task failures rather than hanging."
        ),
        params={
            "workload": "scale",
            "spe": 2,
            "elements": 131072,
            "chunk": 32768,
            "store_bytes": 8192,
            "dma_bw": None,
        },
    ),
]


def by_key(key: str) -> Scenario:
    for scenario in SCENARIOS:
        if scenario.key == key:
            return scenario
    raise KeyError(f"unknown scenario '{key}'")


def as_dicts() -> list[dict[str, Any]]:
    return [s.as_dict() for s in SCENARIOS]


# -- limits the UI enforces, kept here so CLI and UI agree -------------
MIN_ELEMENTS = 256
MAX_ELEMENTS = 64_000_000
MIN_CHUNK = 32
MAX_CHUNK = 8_000_000
MIN_SPE = 1
MAX_SPE = 32
MIN_STORE = 1024
MAX_STORE = 1 << 30
INLINE_LIMIT = DEFAULT_INLINE_LIMIT


@dataclass(slots=True)
class RunRequest:
    """Validated run parameters coming from the UI."""

    workload: str = "reductions"
    spe: int = 4
    elements: int = 262144
    chunk: int = 1024
    store_bytes: int = 131072
    dma_bw: float | None = None
    inline_limit: int = INLINE_LIMIT

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> RunRequest:
        from neocell.workloads import WORKLOADS

        workload = str(raw.get("workload", "reductions"))
        if workload not in WORKLOADS:
            raise ValueError(f"unknown workload '{workload}'")
        spe = _clamp(int(raw.get("spe", 4)), MIN_SPE, MAX_SPE, "spe")
        elements = _clamp(
            int(raw.get("elements", 262144)), MIN_ELEMENTS, MAX_ELEMENTS, "elements"
        )
        chunk = _clamp(int(raw.get("chunk", 1024)), MIN_CHUNK, MAX_CHUNK, "chunk")
        store_bytes = _clamp(
            int(raw.get("store_bytes", 131072)), MIN_STORE, MAX_STORE, "store_bytes"
        )
        bw = raw.get("dma_bw")
        return cls(
            workload=workload,
            spe=spe,
            elements=elements,
            chunk=chunk,
            store_bytes=store_bytes,
            dma_bw=float(bw) if bw else None,
            inline_limit=INLINE_LIMIT,
        )


def _clamp(value: int, low: int, high: int, name: str) -> int:
    if not low <= value <= high:
        raise ValueError(f"{name} must be between {low} and {high}, got {value}")
    return value


def describe_limits() -> dict[str, int]:
    return {
        "min_elements": MIN_ELEMENTS,
        "max_elements": MAX_ELEMENTS,
        "min_chunk": MIN_CHUNK,
        "max_chunk": MAX_CHUNK,
        "min_spe": MIN_SPE,
        "max_spe": MAX_SPE,
        "min_store_bytes": MIN_STORE,
        "max_store_bytes": MAX_STORE,
        "inline_limit": INLINE_LIMIT,
    }