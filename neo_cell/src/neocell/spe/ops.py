"""Data-parallel operations available to every SPE worker.

An op receives only what it needs from the local store plus its
parameters, and returns raw bytes. Ops are deliberately simple: they
exist to exercise partitioning, staging and scheduling, not to be fast.
Adding an op is the main way to grow the worker capability set, and the
PPE discovers it through the handshake, so new ops never require PPE
changes.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from neocell.codec import pack_f32, pack_f32_scalar, unpack_f32_from
from neocell.spe.local_store import LocalStore

OpFn = Callable[..., bytes]
_REGISTRY: dict[str, dict[str, Any]] = {}


def op(name: str, *, n_inputs: int, out_elements: str | int, needs_workspace: int = 0):
    """Register an SPE operation.

    ``n_inputs`` buffers are staged in order, ``out_elements`` is
    ``"same"``, an integer count, or ``"scalar"``, and
    ``needs_workspace`` reserves extra local-store bytes so a worker can
    reject a task before doing any work.
    """

    def decorate(fn: OpFn) -> OpFn:
        _REGISTRY[name] = {
            "fn": fn,
            "n_inputs": n_inputs,
            "out_elements": out_elements,
            "needs_workspace": needs_workspace,
        }
        return fn

    return decorate


def registry() -> dict[str, dict[str, Any]]:
    return dict(_REGISTRY)


def supported_ops() -> list[str]:
    return sorted(_REGISTRY)


def describe(name: str) -> dict[str, Any]:
    try:
        return dict(_REGISTRY[name])
    except KeyError:
        raise KeyError(f"unknown op '{name}'") from None


def out_length(name: str, in_elements: int, params: dict[str, Any]) -> int:
    """Output element count an op produces for the given input length."""
    spec = _REGISTRY[name]
    out = spec["out_elements"]
    if out == "same":
        return in_elements
    if out == "scalar":
        return 1
    if isinstance(out, int):
        return out
    return 0


@op("sum", n_inputs=1, out_elements="scalar")
def _sum(ls: LocalStore, buffers: list[memoryview], params: dict[str, Any]) -> bytes:
    values = unpack_f32_from(buffers[0])
    return pack_f32_scalar(float(sum(values)))


@op("scale", n_inputs=1, out_elements="same")
def _scale(
    ls: LocalStore, buffers: list[memoryview], params: dict[str, Any]
) -> bytes:
    factor = float(params.get("factor", 1.0))
    return pack_f32(v * factor for v in unpack_f32_from(buffers[0]))


@op("axpy", n_inputs=2, out_elements="same")
def _axpy(
    ls: LocalStore, buffers: list[memoryview], params: dict[str, Any]
) -> bytes:
    a = float(params.get("a", 1.0))
    b = float(params.get("b", 0.0))
    x = unpack_f32_from(buffers[0])
    y = unpack_f32_from(buffers[1])
    return pack_f32(a * xi + b * yi for xi, yi in zip(x, y, strict=False))


@op("stats", n_inputs=1, out_elements=4)
def _stats(
    ls: LocalStore, buffers: list[memoryview], params: dict[str, Any]
) -> bytes:
    values = unpack_f32_from(buffers[0])
    n = len(values)
    total = float(sum(values))
    mean = total / n if n else 0.0
    lo = min(values) if values else 0.0
    hi = max(values) if values else 0.0
    midrange = (lo + hi) / 2.0
    return pack_f32([n, total, mean, midrange])


@op("mix_work", n_inputs=1, out_elements="scalar")
def _mix_work(
    ls: LocalStore, buffers: list[memoryview], params: dict[str, Any]
) -> bytes:
    factor = float(params.get("factor", 2.0))
    rounds = int(params.get("rounds", 8))
    values = unpack_f32_from(buffers[0])
    acc = 0.0
    for _ in range(rounds):
        acc += sum(v * factor for v in values)
    return pack_f32_scalar(acc)


def run_op(
    ls: LocalStore, name: str, buffers: list[memoryview], params: dict[str, Any]
) -> tuple[bytes, float]:
    """Run one op and return its output bytes plus execution seconds."""
    spec = _REGISTRY.get(name)
    if spec is None:
        raise KeyError(f"unknown op '{name}'")
    started = time.perf_counter()
    payload = spec["fn"](ls, buffers, params)
    return payload, time.perf_counter() - started


def required_store_bytes(
    name: str, input_sizes: list[int], params: dict[str, Any]
) -> int:
    """Bytes an op needs resident, so the PPE can size a task sanely."""
    spec = _REGISTRY[name]
    in_elements = input_sizes[0] // 4 if input_sizes else 0
    out_n = out_length(name, in_elements, params)
    return sum(input_sizes) + out_n * 4 + spec["needs_workspace"]
