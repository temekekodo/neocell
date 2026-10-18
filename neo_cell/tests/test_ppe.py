"""PPE tests: splitting, scheduling, correctness and failure handling."""

from __future__ import annotations


import pytest

from neocell import protocol as proto
from neocell import workloads
from neocell.codec import pack_f32, unpack_f32
from neocell.ppe import DmaModel, Job, Ppe, PpeConfig, SchedulerError, SpeAdapter


def test_split_produces_independent_ordered_chunks():
    job = Job(
        name="j",
        op="scale",
        chunk_elements=4,
        inputs=[pack_f32([float(i) for i in range(10)])],
    )
    tasks = job.split()
    assert [t.index for t in tasks] == [0, 1, 2]
    assert [t.input_elements for t in tasks] == [4, 4, 2]
    assert tasks[0].chunk_count == 3
    flat = b"".join(t.packed() for t in tasks)
    assert flat == job.inputs[0]


def test_split_keeps_buffers_in_lockstep_for_multi_input():
    x = pack_f32([float(i) for i in range(8)])
    y = pack_f32([float(i) * 10 for i in range(8)])
    job = Job(name="j", op="axpy", chunk_elements=4, inputs=[x, y])
    tasks = job.split()
    assert len(tasks) == 2
    assert all(len(t.inputs) == 2 for t in tasks)
    assert unpack_f32(tasks[0].inputs[1]) == [0.0, 10.0, 20.0, 30.0]


def test_split_of_empty_job_is_empty():
    assert Job(name="j", op="sum", inputs=[]).split() == []


@pytest.mark.asyncio
async def test_ppe_starts_requested_spe_count(ppe: Ppe):
    assert len(ppe.slots) == 4
    ids = sorted(s.info.worker_id for s in ppe.slots)
    assert ids == ["SPE0", "SPE1", "SPE2", "SPE3"]
    for slot in ppe.slots:
        assert slot.info.pid and slot.info.pid > 0
        assert "sum" in slot.info.ops


@pytest.mark.asyncio
async def test_submit_before_start_raises():
    coordinator = Ppe(PpeConfig(spe_count=1))
    with pytest.raises(SchedulerError):
        await coordinator.submit(Job(name="j", op="sum", inputs=[pack_f32([1.0])]))


@pytest.mark.asyncio
async def test_sum_reduction_matches_serial_reference(ppe: Ppe):
    elements = 8192
    job = workloads.build("reductions", elements, chunk_elements=1024)
    result = await ppe.submit(job)

    assert result.ok, result.failures
    reference = sum(unpack_f32(job.inputs[0]))
    got = sum(result.scalars())
    assert got == pytest.approx(reference, rel=1e-4)
    assert len(result.outputs) == elements // 1024


@pytest.mark.asyncio
async def test_scale_output_matches_serial_reference(ppe: Ppe):
    elements = 4096
    job = workloads.build("scale", elements, chunk_elements=512)
    result = await ppe.submit(job)

    assert result.ok, result.failures
    got = result.scalars()
    expected = [v * job.params["factor"] for v in unpack_f32(job.inputs[0])]
    assert got == pytest.approx(expected, rel=1e-4)
    assert len(got) == elements


@pytest.mark.asyncio
async def test_axpy_multi_input_matches_reference(ppe: Ppe):
    elements = 2048
    job = workloads.build("axpy", elements, chunk_elements=256)
    result = await ppe.submit(job)

    assert result.ok, result.failures
    x = unpack_f32(job.inputs[0])
    y = unpack_f32(job.inputs[1])
    expected = [2.0 * xi - yi for xi, yi in zip(x, y, strict=False)]
    assert result.scalars() == pytest.approx(expected, rel=1e-4)


@pytest.mark.asyncio
async def test_results_are_reassembled_in_chunk_order(ppe: Ppe):
    job = workloads.build("scale", 4096, chunk_elements=256)
    result = await ppe.submit(job)
    assert sorted(result.outputs) == list(range(16))
    flat = result.scalars()
    expected = [v * 2.5 for v in unpack_f32(job.inputs[0])]
    assert flat == pytest.approx(expected, rel=1e-4)


@pytest.mark.asyncio
async def test_every_task_is_measured(ppe: Ppe):
    job = workloads.build("reductions", 4096, chunk_elements=1024)
    result = await ppe.submit(job)
    summary = result.metrics.summary()
    assert summary["tasks"] == 4
    assert summary["ok"] == 4
    assert summary["total_bytes"] == 4096 * 4 + 4 * 4
    assert summary["max_store_high_water"] > 0
    assert sum(w["tasks"] for w in summary["workers"].values()) == 4


@pytest.mark.asyncio
async def test_work_is_spread_across_workers(ppe: Ppe):
    job = workloads.build("reductions", 65536, chunk_elements=1024)
    result = await ppe.submit(job)
    used = {s.info.worker_id for s in ppe.slots if s.tasks_completed > 0}
    assert len(used) == 4


@pytest.mark.asyncio
async def test_unsupported_op_is_rejected_before_dispatch(ppe: Ppe):
    job = Job(name="bad", op="does_not_exist", inputs=[pack_f32([1.0])])
    with pytest.raises(SchedulerError, match="unsupported"):
        await ppe.submit(job)


@pytest.mark.asyncio
async def test_job_with_no_healthy_workers_is_rejected(ppe: Ppe):
    for slot in ppe.slots:
        slot.info.healthy = False
    job = workloads.build("reductions", 1024, chunk_elements=256)
    with pytest.raises(SchedulerError, match="no healthy workers"):
        await ppe.submit(job)


@pytest.mark.asyncio
async def test_local_store_overflow_is_reported_per_task(ppe_config: PpeConfig):
    coordinator = Ppe(ppe_config, dma=DmaModel())
    await coordinator.start()
    try:
        job = workloads.build("scale", 65536, chunk_elements=32768)
        result = await coordinator.submit(job)
        assert not result.ok
        assert all("overflow" in msg for msg in result.failures.values())
        assert result.metrics.summary()["failed"] == 2
    finally:
        await coordinator.stop()


@pytest.mark.asyncio
async def test_fail_fast_raises_on_failure(ppe_config: PpeConfig):
    config = PpeConfig(
        spe_count=ppe_config.spe_count,
        local_store_bytes=ppe_config.local_store_bytes,
        fail_fast=True,
    )
    coordinator = Ppe(config, dma=DmaModel())
    await coordinator.start()
    try:
        job = workloads.build("scale", 65536, chunk_elements=32768)
        with pytest.raises(SchedulerError):
            await coordinator.submit(job)
    finally:
        await coordinator.stop()


@pytest.mark.asyncio
async def test_worker_loss_marks_tasks_failed_not_hung():
    """A dead worker fails its tasks instead of hanging the scheduler."""

    class BrokenAdapter(SpeAdapter):
        def __init__(self, worker_id: str):
            from neocell.ppe.adapter import WorkerInfo

            self.info = WorkerInfo(
                worker_id=worker_id, healthy=True, ops=["sum"]
            )

        async def start(self):
            return self.info

        async def execute(self, task):
            raise ConnectionResetError("worker died")

        async def cancel(self, task_id: str) -> None:
            pass

        async def stop(self) -> None:
            pass

    from neocell.ppe.coordinator import WorkerSlot

    coordinator = Ppe(PpeConfig(spe_count=0, max_attempts=1))
    coordinator._started = True
    for i in range(2):
        coordinator.slots.append(WorkerSlot(adapter=BrokenAdapter(f"S{i}")))
    job = workloads.build("reductions", 1024, chunk_elements=256)
    result = await coordinator.submit(job)
    assert not result.ok
    assert len(result.failures) == 4
    assert all("ConnectionResetError" in m for m in result.failures.values())


@pytest.mark.asyncio
async def test_dma_model_accounts_bytes_and_time():
    dma = DmaModel(bytes_per_second=1000.0, fixed_latency_s=0.001)
    await dma.move(100, to_worker=True)
    await dma.move(50, to_worker=False)
    assert dma.to_worker.bytes_moved == 100
    assert dma.from_worker.bytes_moved == 50
    assert dma.total_bytes == 150
    assert dma.to_worker.seconds == pytest.approx(0.001 + 0.1)
    assert dma.to_worker.transfers == 1


@pytest.mark.asyncio
async def test_free_dma_is_pure_bookkeeping():
    dma = DmaModel()
    await dma.move(1 << 20, to_worker=True)
    assert dma.to_worker.seconds == 0.0
    assert dma.to_worker.bytes_moved == 1 << 20


@pytest.mark.asyncio
async def test_dma_cost_serializes_ppe_when_configured():
    config = PpeConfig(spe_count=4, local_store_bytes=8192)
    dma = DmaModel(bytes_per_second=1e9, fixed_latency_s=0.0005)
    coordinator = Ppe(config, dma=dma)
    await coordinator.start()
    try:
        job = workloads.build("reductions", 2048, chunk_elements=512)
        result = await coordinator.submit(job)
        assert result.ok
        assert dma.to_worker.transfers == 4
        assert dma.to_worker.seconds > 0
    finally:
        await coordinator.stop()


@pytest.mark.asyncio
async def test_transfer_seconds_are_attributed_per_task():
    """Regression: per-task transfer time must not stay zero when charged."""
    dma = DmaModel(bytes_per_second=1e6)
    coordinator = Ppe(PpeConfig(spe_count=2, local_store_bytes=8192), dma=dma)
    await coordinator.start()
    try:
        result = await coordinator.submit(
            workloads.build("reductions", 2048, chunk_elements=512)
        )
        assert result.ok
        for task in result.metrics.tasks:
            assert task.to_worker_s > 0
            assert task.to_worker_s == pytest.approx(512 * 4 / 1e6)
            assert task.from_worker_s > 0
        summary = result.metrics.summary()
        assert summary["sum_transfer_s"] > 0
        assert summary["sum_transfer_s"] == pytest.approx(dma.total_seconds, rel=1e-6)
    finally:
        await coordinator.stop()


@pytest.mark.asyncio
async def test_unpaid_transfer_reports_zero_seconds():
    """Free DMA still moves bytes but costs no modelled time."""
    dma = DmaModel()
    coordinator = Ppe(PpeConfig(spe_count=2, local_store_bytes=8192), dma=dma)
    await coordinator.start()
    try:
        result = await coordinator.submit(
            workloads.build("reductions", 2048, chunk_elements=512)
        )
        assert result.ok
        assert all(t.to_worker_s == 0.0 for t in result.metrics.tasks)
        assert result.metrics.summary()["total_bytes"] > 0
    finally:
        await coordinator.stop()


@pytest.mark.asyncio
async def test_transfer_time_dominates_at_low_bandwidth():
    """The headline claim: bandwidth limits change the compute/transfer mix."""
    dma = DmaModel(bytes_per_second=200000.0)
    coordinator = Ppe(PpeConfig(spe_count=2, local_store_bytes=8192), dma=dma)
    await coordinator.start()
    try:
        result = await coordinator.submit(
            workloads.build("reductions", 4096, chunk_elements=256)
        )
        assert result.ok
        summary = result.metrics.summary()
        assert summary["sum_transfer_s"] > summary["sum_exec_s"]
    finally:
        await coordinator.stop()


@pytest.mark.asyncio
async def test_stop_is_idempotent():
    coordinator = Ppe(PpeConfig(spe_count=2))
    await coordinator.start()
    await coordinator.stop()
    await coordinator.stop()
    assert coordinator.slots == []


@pytest.mark.asyncio
async def test_single_spe_still_completes_job(ppe_config: PpeConfig):
    coordinator = Ppe(PpeConfig(spe_count=1, local_store_bytes=8192))
    await coordinator.start()
    try:
        result = await coordinator.submit(workloads.build("reductions", 2048, 512))
        assert result.ok
        assert len(result.outputs) == 4
    finally:
        await coordinator.stop()


@pytest.mark.asyncio
async def test_trace_records_dispatch_and_collect(tmp_path):
    from neocell.ppe import Trace

    trace_file = tmp_path / "trace.jsonl"
    trace = Trace(path=trace_file)
    coordinator = Ppe(PpeConfig(spe_count=2, local_store_bytes=8192), trace=trace)
    await coordinator.start()
    try:
        result = await coordinator.submit(workloads.build("reductions", 2048, 512))
        assert result.ok
    finally:
        await coordinator.stop()
        trace.close()

    import json

    events = [json.loads(line) for line in trace_file.read_text().splitlines()]
    kinds = {e["event"] for e in events}
    assert {"worker_ready", "job_submit", "dispatch", "collect", "job_done"} <= kinds


@pytest.mark.asyncio
async def test_spe_workers_are_separate_processes(ppe: Ppe):
    pids = {s.info.pid for s in ppe.slots}
    assert len(pids) == 4
    import os

    assert os.getpid() not in pids


@pytest.mark.asyncio
async def test_ppe_protocol_version_is_negotiated(ppe: Ppe):
    from neocell.version import PROTOCOL_VERSION

    slot = ppe.slots[0]
    assert slot.info.healthy
    assert slot.info.supports("sum")
    assert not slot.info.supports("nope")
    reply = proto.hello_frame("SPE0", 1024)
    assert reply.envelope.hello.protocol == PROTOCOL_VERSION

# -- data plane: inline versus Arrow reference ------------------------
@pytest.mark.asyncio
async def test_arrow_transport_produces_identical_results():
    """The same job must give the same answer on both data planes."""
    from neocell.buffers import DEFAULT_INLINE_LIMIT

    elements = 300_000
    inline = await _run_once("scale", elements, 4096, 131072, DEFAULT_INLINE_LIMIT)
    arrow = await _run_once("scale", elements, 4096, 131072, 16)

    assert inline.ok and arrow.ok
    assert inline.metrics.summary()["by_transport"] == {"inline": len(inline.outputs)}
    assert arrow.metrics.summary()["by_transport"] == {"arrow_ref": len(arrow.outputs)}
    assert inline.scalars() == pytest.approx(arrow.scalars(), rel=1e-4)


@pytest.mark.asyncio
async def test_arrow_references_are_released_after_the_run():
    from neocell.buffers import BufferManager

    # 300k float32 in, 300k out, so the store must hold 2.4 MB.
    coordinator = Ppe(
        PpeConfig(spe_count=2, local_store_bytes=3 << 20), dma=DmaModel()
    )
    await coordinator.start()
    try:
        result = await coordinator.submit(
            workloads.build("scale", 600_000, chunk_elements=300_000)
        )
        assert result.ok
        assert result.metrics.summary()["by_transport"] == {"arrow_ref": 2}
        for slot in coordinator.slots:
            assert isinstance(slot.adapter.buffers, BufferManager)
            assert slot.adapter.buffers.stats.live_refs == 0
    finally:
        await coordinator.stop()


async def _run_once(workload, elements, chunk, store_bytes, inline_limit):
    coordinator = Ppe(
        PpeConfig(spe_count=2, local_store_bytes=store_bytes), dma=DmaModel()
    )
    await coordinator.start()
    try:
        for slot in coordinator.slots:
            slot.adapter.buffers.inline_limit = inline_limit
        return await coordinator.submit(workloads.build(workload, elements, chunk))
    finally:
        await coordinator.stop()
