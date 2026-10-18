"""Workloads for the demo, chosen to make the architecture visible.

Each builder returns a :class:`~neocell.ppe.job.Job` the PPE can split.
They are deterministic so a run is reproducible, and every one of them
has independent chunks, which is what lets the scheduler overlap
transfer with compute.
"""

from __future__ import annotations

import math
import random

from neocell.codec import pack_f32
from neocell.ppe.job import Job

DEFAULT_CHUNK = 4096


def ramp(elements: int) -> bytes:
    """A smooth, compressible-by-eye signal."""
    return pack_f32(
        math.sin(i / 64.0) * 100.0 + (i % 97) for i in range(elements)
    )


def noisy_wave(elements: int, seed: int = 7) -> bytes:
    """A signal with per-element noise, closer to real sensor data."""
    rng = random.Random(seed)
    return pack_f32(
        math.sin(i / 32.0) * 50.0 + rng.uniform(-5.0, 5.0)
        for i in range(elements)
    )


def reductions(elements: int, chunk_elements: int = DEFAULT_CHUNK) -> Job:
    """Chunked sum: many tiny outputs, so transfer volume stays visible."""
    return Job(
        name="reductions",
        op="sum",
        chunk_elements=chunk_elements,
        inputs=[noisy_wave(elements)],
        reduce="sum",
    )


def scale(elements: int, chunk_elements: int = DEFAULT_CHUNK,
          factor: float = 2.5) -> Job:
    """Elementwise scale: one output per input, the bandwidth case."""
    return Job(
        name="scale",
        op="scale",
        params={"factor": factor},
        chunk_elements=chunk_elements,
        inputs=[ramp(elements)],
        reduce="elementwise",
    )


def axpy(elements: int, chunk_elements: int = DEFAULT_CHUNK) -> Job:
    """Two-buffer op: proves multi-input staging and larger chunks."""
    return Job(
        name="axpy",
        op="axpy",
        params={"a": 2.0, "b": -1.0},
        chunk_elements=chunk_elements,
        inputs=[ramp(elements), noisy_wave(elements, seed=11)],
        reduce="elementwise",
    )


def compute_heavy(elements: int, chunk_elements: int = DEFAULT_CHUNK,
                  rounds: int = 24) -> Job:
    """Arithmetic-heavy reduction: compute dominates, transfers do not."""
    return Job(
        name="compute_heavy",
        op="mix_work",
        params={"factor": 1.000001, "rounds": rounds},
        chunk_elements=chunk_elements,
        inputs=[ramp(elements)],
        reduce="sum",
    )


def stats(elements: int, chunk_elements: int = DEFAULT_CHUNK) -> Job:
    """Per-chunk statistics: small output, exercise the four-value reply."""
    return Job(
        name="stats",
        op="stats",
        chunk_elements=chunk_elements,
        inputs=[noisy_wave(elements, seed=3)],
        reduce="stats",
    )


WORKLOADS = {
    "reductions": reductions,
    "scale": scale,
    "axpy": axpy,
    "compute_heavy": compute_heavy,
    "stats": stats,
}


def build(name: str, elements: int, chunk_elements: int = DEFAULT_CHUNK) -> Job:
    try:
        factory = WORKLOADS[name]
    except KeyError:
        raise KeyError(
            f"unknown workload '{name}'; have {sorted(WORKLOADS)}"
        ) from None
    return factory(elements, chunk_elements)