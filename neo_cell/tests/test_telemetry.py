"""Telemetry tests.

Two things matter most here: that telemetry never corrupts the protocol,
and that a trace actually links the PPE to its SPE processes. The first
was a real bug -- the OTel console exporters default to stdout, which is
the worker's protocol channel -- so it is pinned by
``test_worker_stdout_stays_protocol_clean_with_console_export``.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from neocell import protocol as proto
from neocell import workloads
from neocell.codec import pack_f32, unpack_f32, unpack_f32_scalar
from neocell.ppe import DmaModel, Ppe, PpeConfig
from neocell.telemetry import (
    M_TASK_EXEC_SECONDS,
    M_TASK_QUEUE_SECONDS,
    M_TASKS,
    OTEL_AVAILABLE,
    Telemetry,
    TelemetryConfig,
)

SRC = str(Path(__file__).resolve().parents[1] / "src")
pytestmark = pytest.mark.skipif(
    not OTEL_AVAILABLE, reason="OpenTelemetry SDK not installed"
)


def _exporter(telemetry: Telemetry):
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )

    exporter = InMemorySpanExporter()
    telemetry.provider.add_span_processor(SimpleSpanProcessor(exporter))
    return exporter


def _span_names(exporter) -> list[str]:
    return sorted(s.name for s in exporter.get_finished_spans())


def _find(exporter, name: str):
    return [s for s in exporter.get_finished_spans() if s.name == name]


# -- unit: disabled telemetry -----------------------------------------
def test_disabled_telemetry_is_a_noop():
    telemetry = Telemetry(TelemetryConfig(enabled=False))
    assert telemetry.enabled is False
    with telemetry.span("x") as span:
        span.set_attribute("a", 1)
        span.add_event("e")
    telemetry.record(M_TASK_QUEUE_SECONDS, 1.0)
    telemetry.add(M_TASKS, 1)
    assert telemetry.inject_context({}) == {}
    assert telemetry.extract_context({"traceparent": "x"}) is None
    telemetry.shutdown()


def test_inject_context_yields_traceparent():
    telemetry = Telemetry(TelemetryConfig(enabled=True, jsonl=False))
    try:
        with telemetry.span("root"):
            carrier = telemetry.inject_context({})
        assert "traceparent" in carrier
    finally:
        telemetry.shutdown()


def test_extract_context_roundtrips():
    telemetry = Telemetry(TelemetryConfig(enabled=True, jsonl=False))
    try:
        with telemetry.span("root"):
            carrier = telemetry.inject_context({})
        context = telemetry.extract_context(carrier)
        assert context is not None
    finally:
        telemetry.shutdown()


def test_missing_trace_context_is_tolerated():
    telemetry = Telemetry(TelemetryConfig(enabled=True, jsonl=False))
    try:
        assert telemetry.extract_context(None) is None
        assert telemetry.extract_context({}) is None
    finally:
        telemetry.shutdown()


def test_shutdown_is_idempotent():
    telemetry = Telemetry(TelemetryConfig(enabled=True, jsonl=False))
    telemetry.shutdown()
    telemetry.shutdown()
    assert telemetry.enabled is False


def test_unfiltered_attributes_are_recorded():
    telemetry = Telemetry(TelemetryConfig(enabled=True, jsonl=False))
    exporter = _exporter(telemetry)
    try:
        with telemetry.span("clean", {"a": 1, "b": None}):
            pass
    finally:
        telemetry.shutdown()
    attrs = _find(exporter, "clean")[0].attributes
    assert attrs["a"] == 1
    assert "b" not in attrs


# -- integration: PPE spans and metrics -------------------------------
@pytest.mark.asyncio
async def test_ppe_emits_job_and_dispatch_spans():
    telemetry = Telemetry(TelemetryConfig(enabled=True, jsonl=False))
    exporter = _exporter(telemetry)
    coordinator = Ppe(
        PpeConfig(spe_count=2, local_store_bytes=8192),
        dma=DmaModel(),
        telemetry=telemetry,
    )
    await coordinator.start()
    try:
        result = await coordinator.submit(
            workloads.build("reductions", 2048, chunk_elements=512)
        )
        assert result.ok
    finally:
        await coordinator.stop()
        telemetry.shutdown()

    names = _span_names(exporter)
    assert "neocell.job" in names
    assert names.count("neocell.spe.execute") == 4

    job_span = _find(exporter, "neocell.job")[0]
    assert job_span.attributes["neocell.task.count"] == 4
    assert job_span.attributes["neocell.spe_count"] == 2
    assert job_span.attributes["neocell.op"] == "sum"


@pytest.mark.asyncio
async def test_dispatch_spans_are_children_of_the_job_span():
    telemetry = Telemetry(TelemetryConfig(enabled=True, jsonl=False))
    exporter = _exporter(telemetry)
    coordinator = Ppe(
        PpeConfig(spe_count=2, local_store_bytes=8192),
        dma=DmaModel(),
        telemetry=telemetry,
    )
    await coordinator.start()
    try:
        await coordinator.submit(workloads.build("reductions", 2048, 512))
    finally:
        await coordinator.stop()
        telemetry.shutdown()

    spans = {s.context.span_id: s for s in exporter.get_finished_spans()}
    job = _find(exporter, "neocell.job")[0]
    for dispatch in _find(exporter, "neocell.spe.execute"):
        assert dispatch.parent is not None
        assert dispatch.parent.span_id == job.context.span_id
    assert spans


@pytest.mark.asyncio
async def test_failure_is_recorded_with_a_low_cardinality_reason():
    telemetry = Telemetry(TelemetryConfig(enabled=True, jsonl=False))
    coordinator = Ppe(
        PpeConfig(spe_count=1, local_store_bytes=8192, max_attempts=1),
        dma=DmaModel(),
        telemetry=telemetry,
    )
    await coordinator.start()
    try:
        result = await coordinator.submit(
            workloads.build("scale", 32768, chunk_elements=16384)
        )
        assert not result.ok
    finally:
        await coordinator.stop()
        telemetry.shutdown()


@pytest.mark.asyncio
async def test_metrics_are_recorded_for_a_successful_task():
    telemetry = Telemetry(TelemetryConfig(enabled=True, jsonl=False))
    coordinator = Ppe(
        PpeConfig(spe_count=1, local_store_bytes=8192),
        dma=DmaModel(),
        telemetry=telemetry,
    )
    await coordinator.start()
    try:
        result = await coordinator.submit(
            workloads.build("reductions", 2048, chunk_elements=512)
        )
        assert result.ok
    finally:
        await coordinator.stop()
        telemetry.shutdown()

    for name in (M_TASK_QUEUE_SECONDS, M_TASK_EXEC_SECONDS, M_TASKS):
        assert telemetry._instruments[name] is not None


@pytest.mark.asyncio
async def test_work_still_completes_with_telemetry_enabled():
    telemetry = Telemetry(TelemetryConfig(enabled=True, jsonl=False))
    coordinator = Ppe(
        PpeConfig(spe_count=2, local_store_bytes=8192),
        dma=DmaModel(),
        telemetry=telemetry,
    )
    await coordinator.start()
    try:
        job = workloads.build("reductions", 4096, chunk_elements=512)
        result = await coordinator.submit(job)
        assert result.ok
        # sum is linear, so summing the per-chunk outputs must reproduce
        # the serial sum of the whole input.
        reference = sum(unpack_f32(job.inputs[0]))
        assert sum(result.scalars()) == pytest.approx(reference, rel=1e-4)
    finally:
        await coordinator.stop()
        telemetry.shutdown()


# -- regression: console export must not touch the protocol -----------
def test_worker_stdout_stays_protocol_clean_with_console_export():
    """Console telemetry on stdout would desynchronise the frame stream."""
    env = dict(os.environ)
    env["PYTHONPATH"] = SRC + os.pathsep + env.get("PYTHONPATH", "")
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "neocell.spe.worker",
            "--id",
            "SPE0",
            "--local-store-bytes",
            "65536",
            "--otel",
            "--otel-console",
            "--otel-interval-ms",
            "500",
        ],
        input=(
            proto.encode(
                proto.hello_frame("SPE0", 65536)
            )
            + proto.encode(
                proto.exec_frame("t1", "sum", None, [12], pack_f32([1.0, 2.0, 3.0]))
            )
        ),
        capture_output=True,
        timeout=60,
        env=env,
    )
    assert proc.returncode == 0, proc.stderr.decode()

    decoder = proto.FrameDecoder()
    decoder.feed(proc.stdout)
    frames = list(decoder)
    assert [f.type for f in frames] == [proto.HELLO_ACK, proto.RESULT]
    assert frames[1].envelope.result.task_id == "t1"
    assert unpack_f32_scalar(frames[1].payload) == pytest.approx(6.0)

    stderr = proc.stderr.decode()
    assert "neocell.spe.exec" in stderr
    assert len(proc.stdout) < 2000, "stdout carries far more than protocol data"


def test_exec_frame_carries_trace_context_only_when_present():
    plain = proto.exec_frame("t", "sum", None, [4], b"\x00\x00\x00\x00")
    assert not plain.envelope.exec.trace
    traced = proto.exec_frame(
        "t", "sum", None, [4], b"\x00\x00\x00\x00", trace={"traceparent": "00-x-y-01"}
    )
    assert traced.envelope.exec.trace["traceparent"] == "00-x-y-01"


def test_worker_reports_status_on_its_span():
    """An overflow must be visible as an error, not a silent failure."""
    env = dict(os.environ)
    env["PYTHONPATH"] = SRC + os.pathsep + env.get("PYTHONPATH", "")
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "neocell.spe.worker",
            "--id",
            "SPE0",
            "--otel",
            "--otel-console",
        ],
        input=(
            proto.encode(
                proto.hello_frame("SPE0", 64)
            )
            + proto.encode(
                proto.exec_frame("t1", "scale", None, [4096], b"\x00" * 4096)
            )
        ),
        capture_output=True,
        timeout=60,
        env=env,
    )
    decoder = proto.FrameDecoder()
    decoder.feed(proc.stdout)
    frames = list(decoder)
    assert frames[-1].type == proto.ERROR
    assert "overflow" in frames[-1].envelope.error.message