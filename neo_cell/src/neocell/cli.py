"""Command line entry point: bring up a PPE with a pool of SPEs and run.

Examples::

    python -m neocell.cli run --spe 4 --workload reductions --elements 65536
    python -m neocell.cli sweep --workload scale --elements 65536
    python -m neocell.cli ops
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any

from neocell import workloads
from neocell.ppe import DmaModel, Ppe, PpeConfig, Trace
from neocell.spe.ops import registry
from neocell.telemetry import (
    OTEL_AVAILABLE,
    SERVICE_PPE,
    Telemetry,
    TelemetryConfig,
    config_from_env,
)
from neocell.version import VERSION


def _add_common(ap: argparse.ArgumentParser) -> None:
    ap.add_argument("--spe", type=int, default=4, help="number of SPE workers")
    ap.add_argument(
        "--store-bytes",
        type=int,
        default=131072,
        help="per-SPE local store capacity in bytes",
    )
    ap.add_argument("--workload", default="reductions", choices=sorted(workloads.WORKLOADS))
    ap.add_argument("--elements", type=int, default=65536, help="input elements")
    ap.add_argument(
        "--chunk", type=int, default=4096, help="elements staged per task"
    )
    ap.add_argument(
        "--dma-bw",
        type=float,
        default=None,
        help="simulated DMA bandwidth in bytes/s; omit for free transfers",
    )
    ap.add_argument(
        "--dma-latency",
        type=float,
        default=0.0,
        help="fixed simulated DMA latency per transfer in seconds",
    )
    ap.add_argument("--trace", type=Path, default=None, help="write a JSONL trace here")
    ap.add_argument("--trace-stdout", action="store_true")
    ap.add_argument("--json", action="store_true", help="emit metrics as JSON")
    ap.add_argument("--quiet", action="store_true")
    _add_telemetry_args(ap)


def _add_telemetry_args(ap: argparse.ArgumentParser) -> None:
    group = ap.add_argument_group("telemetry")
    group.add_argument(
        "--otel",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="enable OpenTelemetry traces and metrics (default: on)",
    )
    group.add_argument(
        "--otel-console",
        action="store_true",
        help="print spans and metrics to stderr instead of a file",
    )
    group.add_argument(
        "--otel-jsonl",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="append spans and metrics to JSONL files (default: on)",
    )
    group.add_argument(
        "--otel-jsonl-path",
        default=None,
        help="JSONL output path; one file per process is written alongside it",
    )
    group.add_argument(
        "--otel-interval-ms",
        type=int,
        default=10_000,
        help="metric export interval in milliseconds",
    )


def _build_telemetry(args: argparse.Namespace, service: str, worker_id: str | None = None):
    """Build Telemetry from CLI flags and the standard OTEL_ env vars."""
    cfg = TelemetryConfig(
        service_name=service,
        enabled=bool(getattr(args, "otel", True)),
        console_export=bool(getattr(args, "otel_console", False)),
        jsonl=bool(getattr(args, "otel_jsonl", True)),
        jsonl_path=getattr(args, "otel_jsonl_path", None),
        export_interval_ms=args.otel_interval_ms,
        worker_id=worker_id,
    )
    cfg = config_from_env(cfg)
    telemetry = Telemetry(cfg)
    if cfg.enabled and not OTEL_AVAILABLE:
        print(
            "warning: OpenTelemetry requested but the SDK is not installed; "
            "install with 'pip install neocell[telemetry]'",
            file=sys.stderr,
        )
    return telemetry


def _build_ppe(args: argparse.Namespace) -> tuple[Ppe, DmaModel, Trace | None, Any]:
    dma = DmaModel(bytes_per_second=args.dma_bw, fixed_latency_s=args.dma_latency)
    trace = None
    if args.trace or args.trace_stdout:
        trace = Trace(
            sink=sys.stdout if args.trace_stdout else None, path=args.trace
        )
    telemetry = _build_telemetry(args, SERVICE_PPE)
    config = PpeConfig(spe_count=args.spe, local_store_bytes=args.store_bytes)
    return Ppe(config, dma=dma, trace=trace, telemetry=telemetry), dma, trace, telemetry


def _report(result, dma: DmaModel, args: argparse.Namespace) -> dict:
    summary = result.metrics.summary() if result.metrics else {}
    summary["dma"] = dma.totals()
    summary["workload"] = result.name
    summary["op"] = result.op
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
        return summary

    if not args.quiet:
        print(f"job        : {result.name} (op={result.op})")
        print(f"failures   : {len(result.failures)}")
        if result.failures:
            for idx, msg in list(result.failures.items())[:5]:
                print(f"  task {idx}: {msg}")
        scalars = result.scalars()
        if scalars:
            shown = scalars[:8]
            extra = f" ... (+{len(scalars) - len(shown)} more)" if len(scalars) > len(shown) else ""
            print(f"output[0:{len(shown)}]  : {['%.6g' % v for v in shown]}{extra}")
        print()
        print(result.metrics.report())
        d = dma.totals()
        print(
            f"dma        : -> worker {d['to_worker']['bytes_moved']} B in "
            f"{d['to_worker']['transfers']} transfers; <- worker "
            f"{d['from_worker']['bytes_moved']} B in {d['from_worker']['transfers']} transfers"
        )
        exec_s = summary.get("sum_exec_s", 0.0)
        xfer_s = summary.get("sum_transfer_s", 0.0)
        if exec_s + xfer_s > 0:
            print(f"share      : compute {exec_s / (exec_s + xfer_s) * 100:.1f}% "
                  f"vs transfer {xfer_s / (exec_s + xfer_s) * 100:.1f}%")
    return summary


async def _run(args: argparse.Namespace) -> int:
    ppe, dma, trace, telemetry = _build_ppe(args)
    job = workloads.build(args.workload, args.elements, args.chunk)
    try:
        infos = await ppe.start()
        if not args.quiet and not args.json:
            print(f"PPE up with {len(infos)} SPEs:")
            for info in infos:
                print(
                    f"  {info.worker_id} pid={info.pid} "
                    f"store={info.local_store_bytes}B ops={','.join(info.ops)}"
                )
            print()
        result = await ppe.submit(job)
        _report(result, dma, args)
        return 0 if result.ok else 1
    finally:
        await ppe.stop()
        if trace:
            trace.close()
        telemetry.shutdown()


async def _sweep(args: argparse.Namespace) -> int:
    """Same job across SPE counts: the demo of scheduling value."""
    rows = []
    for count in args.sweep:
        ns = argparse.Namespace(**vars(args))
        ns.spe = count
        ppe, dma, trace, telemetry = _build_ppe(ns)
        job = workloads.build(args.workload, args.elements, args.chunk)
        try:
            await ppe.start()
            result = await ppe.submit(job)
            s = result.metrics.summary()
            rows.append(
                {
                    "spe": count,
                    "wall_ms": round(s["wall_s"] * 1000, 3),
                    "ok": result.ok,
                    "bytes": s["total_bytes"],
                }
            )
        finally:
            await ppe.stop()
            if trace:
                trace.close()
            telemetry.shutdown()
    if args.json:
        print(json.dumps(rows, indent=2))
    else:
        base = rows[0]["wall_ms"] if rows else 0.0
        print(f"{'SPE':>4} {'wall ms':>10} {'speedup':>8} {'bytes':>12}  ok")
        for r in rows:
            sp = base / r["wall_ms"] if r["wall_ms"] else 0.0
            print(f"{r['spe']:>4} {r['wall_ms']:>10.3f} {sp:>7.2f}x {r['bytes']:>12}  {r['ok']}")
    return 0


def _ui(args: argparse.Namespace) -> int:
    from neocell.ui.server import serve

    host = args.host
    if host not in ("127.0.0.1", "localhost", "::1"):
        print(
            f"refusing to bind {host}: the test UI is local-only by design",
            file=sys.stderr,
        )
        return 2
    serve(host=host, port=args.port)
    return 0


def _ops(args: argparse.Namespace) -> int:
    if args.json:
        print(json.dumps(registry(), indent=2, default=str))
        return 0
    print(f"{'op':<12} {'inputs':>6} {'out':>8}  workspace")
    for name, spec in sorted(registry().items()):
        print(
            f"{name:<12} {spec['n_inputs']:>6} {str(spec['out_elements']):>8}  "
            f"{spec['needs_workspace']} B"
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="neocell", description="Cell-style PPE/SPE compute model"
    )
    ap.add_argument("--version", action="version", version=f"neocell {VERSION}")
    sub = ap.add_subparsers(dest="command", required=True)

    run_ap = sub.add_parser("run", help="run one job on a pool of SPEs")
    _add_common(run_ap)
    run_ap.set_defaults(func=_run)

    sweep_ap = sub.add_parser("sweep", help="run one job across several SPE counts")
    _add_common(sweep_ap)
    sweep_ap.add_argument(
        "--sweep", type=int, nargs="+", default=[1, 2, 4], help="SPE counts to try"
    )
    sweep_ap.set_defaults(func=_sweep)

    ui_ap = sub.add_parser(
        "ui", help="serve the local test console (127.0.0.1 only)"
    )
    ui_ap.add_argument("--host", default="127.0.0.1")
    ui_ap.add_argument("--port", type=int, default=8765)
    ui_ap.set_defaults(func=_ui)

    ops_ap = sub.add_parser("ops", help="list the ops workers support")
    ops_ap.add_argument("--json", action="store_true")
    ops_ap.set_defaults(func=_ops)

    args = ap.parse_args(argv)
    if args.command == "ops":
        return _ops(args)
    if args.command == "ui":
        return _ui(args)
    return asyncio.run(_run(args) if args.command == "run" else _sweep(args))


if __name__ == "__main__":
    raise SystemExit(main())