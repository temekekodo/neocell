"""Measure telemetry overhead, so the docs can state a real number.

Compares wall time with telemetry off against telemetry on, using the
median of many runs because single runs on a shared machine are too noisy
to conclude anything. Runs the job in-process rather than through the CLI
so process startup, which telemetry cannot affect, is excluded from the
comparison.

    PYTHONPATH=src python3 -m neocell.bench --runs 15
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
import time

from neocell import workloads
from neocell.ppe import DmaModel, Ppe, PpeConfig
from neocell.telemetry import Telemetry, TelemetryConfig


async def _once(workload: str, elements: int, chunk: int, spe: int,
                telemetry: Telemetry) -> float:
    coordinator = Ppe(
        PpeConfig(spe_count=spe, local_store_bytes=131072),
        dma=DmaModel(),
        telemetry=telemetry,
    )
    await coordinator.start()
    try:
        job = workloads.build(workload, elements, chunk)
        started = time.perf_counter()
        await coordinator.submit(job)
        return time.perf_counter() - started
    finally:
        await coordinator.stop()


async def _series(workload: str, elements: int, chunk: int, spe: int,
                  runs: int, telemetry: Telemetry) -> list[float]:
    timings = []
    for _ in range(runs):
        timings.append(await _once(workload, elements, chunk, spe, telemetry))
        await asyncio.sleep(0.02)
    return timings


def _report(label: str, timings: list[float], baseline: float | None) -> tuple[float, float]:
    median = statistics.median(timings)
    best = min(timings)
    line = (
        f"{label:<14} median {median * 1000:8.2f} ms   "
        f"min {best * 1000:8.2f} ms"
    )
    if baseline:
        line += (
            f"   median {(median / baseline[0] - 1) * 100:+6.1f}%"
            f"   min {(best / baseline[1] - 1) * 100:+6.1f}%"
        )
    print(line)
    return median, best


async def main_async(args: argparse.Namespace) -> int:
    print(
        f"workload={args.workload} elements={args.elements} "
        f"chunk={args.chunk} spe={args.spe} runs={args.runs}"
    )
    print("warming up (excluded from results)")
    await _series(args.workload, args.elements, args.chunk, args.spe, 3,
                  Telemetry(TelemetryConfig(enabled=False)))

    off = Telemetry(TelemetryConfig(enabled=False))
    on = Telemetry(TelemetryConfig(enabled=True))
    try:
        print()
        base = _report("telemetry off", await _series(
            args.workload, args.elements, args.chunk, args.spe, args.runs, off), None)
        _report("telemetry on", await _series(
            args.workload, args.elements, args.chunk, args.spe, args.runs, on), base)
    finally:
        off.shutdown()
        on.shutdown()
    print()
    print("Note: 'on' records to a discarding exporter. Console export adds I/O")
    print("and is not representative of a deployed setup.")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="neocell-bench", description=__doc__)
    ap.add_argument("--workload", default="compute_heavy")
    ap.add_argument("--elements", type=int, default=65536)
    ap.add_argument("--chunk", type=int, default=2048)
    ap.add_argument("--spe", type=int, default=4)
    ap.add_argument("--runs", type=int, default=15)
    args = ap.parse_args(argv)
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())