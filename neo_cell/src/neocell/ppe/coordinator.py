"""The PPE: coordinator, scheduler and result assembler.

The PPE accepts a job, splits it into tasks, hands each task to an idle
worker, pays the transfer cost, collects results and reassembles them in
order. It performs no bulk arithmetic itself; that separation is the
architectural point, not an accident of the implementation.

Scheduling is intentionally simple and inspectable: tasks are dispatched
in index order to whichever worker is free, with retries on failure and
no speculation. A fancier policy can replace ``_run_tasks`` without
touching job splitting, the adapter interface, or the workers.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass

from neocell import protocol as proto
from neocell.ppe.adapter import Execution, ProcessSpeAdapter, SpeAdapter, WorkerInfo
from neocell.ppe.dma import DmaModel
from neocell.ppe.job import Job, Task
from neocell.ppe.metrics import JobResult, MetricsCollector, TaskMetric, Trace
from neocell.spe.ops import describe
from neocell.telemetry import (
    ATTR_CHUNK_ELEMENTS,
    ATTR_OP,
    ATTR_REASON,
    ATTR_SPE_COUNT,
    ATTR_STATUS,
    ATTR_TOTAL_ELEMENTS,
    ATTR_WORKER,
    M_EXCEPTIONS,
    M_JOB_WALL_SECONDS,
    M_RETRIES,
    M_STORE_BYTES,
    M_STORE_HIGH_WATER_BYTES,
    M_TASK_EXEC_SECONDS,
    M_TASK_FAILURES,
    M_TASK_INPUT_BYTES,
    M_TASK_OUTPUT_BYTES,
    M_TASK_QUEUE_SECONDS,
    M_TASK_STAGE_SECONDS,
    M_TASK_TOTAL_SECONDS,
    M_TASK_TRANSFER_SECONDS,
    M_TASKS,
    M_WORKER_BUSY_SECONDS,
    Telemetry,
    disabled,
)


class SchedulerError(Exception):
    """Raised when a job cannot be completed as specified."""


@dataclass(slots=True)
class PpeConfig:
    """Everything tunable about the PPE for one run."""

    spe_count: int = 4
    local_store_bytes: int = 131072
    max_attempts: int = 2
    request_timeout_s: float = 120.0
    fail_fast: bool = False
    worker_command: list[str] | None = None


@dataclass(slots=True)
class WorkerSlot:
    """A worker plus its live bookkeeping."""

    adapter: SpeAdapter
    busy_task: str | None = None
    tasks_completed: int = 0
    bytes_in: int = 0
    bytes_out: int = 0

    @property
    def info(self) -> WorkerInfo:
        return self.adapter.info

    @property
    def free(self) -> bool:
        return self.busy_task is None and self.adapter.healthy


class Ppe:
    """The PowerPC Processing Element stand-in: orchestrate, never crunch."""

    def __init__(
        self,
        config: PpeConfig | None = None,
        dma: DmaModel | None = None,
        trace: Trace | None = None,
        telemetry: Telemetry | None = None,
    ) -> None:
        self.config = config or PpeConfig()
        self.dma = dma or DmaModel()
        self.trace = trace
        self.telemetry = telemetry or disabled()
        self.slots: list[WorkerSlot] = []
        self._started = False

    async def start(self) -> list[WorkerInfo]:
        """Bring up the SPE pool and verify they can serve our ops."""
        if self._started:
            return [s.info for s in self.slots]
        if self.config.spe_count < 1:
            raise ValueError("spe_count must be at least 1")

        adapters: list[SpeAdapter] = []
        for i in range(self.config.spe_count):
            adapter = ProcessSpeAdapter(
                worker_id=f"SPE{i}",
                local_store_bytes=self.config.local_store_bytes,
                dma=self.dma,
                trace=self.trace,
                command=self.config.worker_command,
                request_timeout_s=self.config.request_timeout_s,
                telemetry=self.telemetry,
            )
            adapters.append(adapter)

        if self.trace:
            self.trace.emit("ppe_start", spe_count=self.config.spe_count)

        results = await asyncio.gather(
            *(a.start() for a in adapters), return_exceptions=True
        )
        for adapter, res in zip(adapters, results, strict=True):
            if isinstance(res, BaseException):
                await asyncio.gather(
                    *(a.stop() for a in adapters), return_exceptions=True
                )
                self.telemetry.add(
                    M_EXCEPTIONS, 1, {"neocell.reason": "worker_start_failed"}
                )
                raise SchedulerError(f"worker failed to start: {res}") from res
            self.slots.append(WorkerSlot(adapter=adapter))
        self._started = True
        self.telemetry.record(
            M_STORE_BYTES,
            self.config.local_store_bytes,
            {ATTR_SPE_COUNT: self.config.spe_count},
        )
        return [s.info for s in self.slots]

    async def stop(self) -> None:
        await asyncio.gather(
            *(s.adapter.stop() for s in self.slots), return_exceptions=True
        )
        self.slots.clear()
        self._started = False

    async def __aenter__(self) -> Ppe:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    @property
    def available_ops(self) -> set[str]:
        ops: set[str] = set()
        for slot in self.slots:
            if slot.info.healthy:
                ops.update(slot.info.ops)
        return ops

    def _validate(self, job: Job, tasks: list[Task]) -> None:
        if not tasks:
            raise SchedulerError(f"job '{job.name}' produced no tasks")
        ops = self.available_ops
        if not ops:
            raise SchedulerError("no healthy workers available")
        if job.op not in ops:
            raise SchedulerError(
                f"job op '{job.op}' unsupported; workers offer {sorted(ops)}"
            )
        try:
            n_inputs = describe(job.op)["n_inputs"]
        except KeyError:
            raise SchedulerError(f"unknown op '{job.op}'") from None
        for t in tasks:
            if len(t.inputs) != n_inputs:
                raise SchedulerError(
                    f"task {t.task_id} has {len(t.inputs)} input buffers but op "
                    f"'{job.op}' needs {n_inputs}"
                )

    async def submit(self, job: Job) -> JobResult:
        """Split, schedule and assemble one job."""
        if not self._started:
            raise SchedulerError("PPE not started; call start() first")
        tasks = job.split()
        self._validate(job, tasks)

        job_attrs = {
            "neocell.job.name": job.name,
            ATTR_OP: job.op,
            "neocell.job.id": job.job_id,
            "neocell.task.count": len(tasks),
            ATTR_CHUNK_ELEMENTS: job.chunk_elements,
            ATTR_TOTAL_ELEMENTS: job.total_elements,
            ATTR_SPE_COUNT: len(self.slots),
        }
        with self.telemetry.span("neocell.job", job_attrs) as job_span:
            metrics = MetricsCollector()
            started = time.perf_counter()
            if self.trace:
                self.trace.emit(
                    "job_submit", job=job.name, op=job.op, tasks=len(tasks),
                    chunk_elements=job.chunk_elements,
                )

            queued: dict[str, TaskMetric] = {}
            attempts: dict[str, int] = {t.task_id: 0 for t in tasks}

            for task in tasks:
                queued[task.task_id] = TaskMetric(
                    task_id=task.task_id,
                    index=task.index,
                    worker_id="",
                    queued_at=time.perf_counter(),
                    input_bytes=task.input_bytes,
                )

            try:
                results, failures = await self._run_tasks(
                    list(tasks), queued, attempts
                )
            finally:
                metrics.wall_s = time.perf_counter() - started
                for m in queued.values():
                    if m.status == "pending":
                        m.status = "cancelled"
                        m.error = m.error or "not dispatched"
                    metrics.add(m)

            job_span.set_attribute(
                "neocell.job.failed_tasks", len(failures)
            )
            self.telemetry.record(
                M_JOB_WALL_SECONDS,
                metrics.wall_s,
                {ATTR_OP: job.op, "neocell.job.name": job.name},
            )

            if failures and self.config.fail_fast:
                raise SchedulerError(f"job '{job.name}' failed: {failures}")

            if self.trace:
                self.trace.emit("job_done", job=job.name, **metrics.summary())

        return JobResult(
            job_id=job.job_id,
            name=job.name,
            op=job.op,
            outputs=results,
            failures=failures,
            metrics=metrics,
            wall_s=metrics.wall_s,
        )

    async def _run_tasks(
        self,
        tasks: list[Task],
        queued: dict[str, TaskMetric],
        attempts: dict[str, int],
    ) -> tuple[dict[int, bytes], dict[int, str]]:
        """Dispatch loop: hand tasks to free workers until none remain."""
        results: dict[int, bytes] = {}
        ready: deque[Task] = deque(tasks)
        inflight: dict[asyncio.Task, tuple[WorkerSlot, Task]] = {}

        async def run(
            slot: WorkerSlot, task: Task
        ) -> tuple[WorkerSlot, Task, Execution | BaseException]:
            metric = queued[task.task_id]
            slot.busy_task = task.task_id
            metric.worker_id = slot.info.worker_id
            metric.dispatched_at = time.perf_counter()
            metric.attempts = attempts[task.task_id]
            if self.trace:
                self.trace.emit(
                    "dispatch",
                    task=task.task_id,
                    worker=slot.info.worker_id,
                    index=task.index,
                    input_bytes=task.input_bytes,
                    queue_s=round(metric.queue_s, 6),
                )
            self.telemetry.record(
                M_TASK_QUEUE_SECONDS,
                metric.queue_s,
                {ATTR_OP: task.op, ATTR_WORKER: slot.info.worker_id},
            )
            self.telemetry.record(
                M_TASK_INPUT_BYTES,
                task.input_bytes,
                {ATTR_OP: task.op, ATTR_WORKER: slot.info.worker_id},
            )
            try:
                return slot, task, await slot.adapter.execute(task)
            except Exception as exc:
                self.telemetry.add(
                    M_EXCEPTIONS, 1, {"neocell.exception.type": type(exc).__name__}
                )
                return slot, task, exc

        while ready or inflight:
            for slot in self.slots:
                if not ready:
                    break
                if not slot.free:
                    continue
                task = ready.popleft()
                attempts[task.task_id] += 1
                fut = asyncio.create_task(run(slot, task))
                inflight[fut] = (slot, task)

            if not inflight:
                if not any(s.adapter.healthy for s in self.slots):
                    break
                await asyncio.sleep(0.001)
                continue

            done, _ = await asyncio.wait(inflight, return_when=asyncio.FIRST_COMPLETED)
            for fut in done:
                slot, task, outcome = fut.result()
                inflight.pop(fut, None)
                slot.busy_task = None
                metric = queued[task.task_id]
                metric.completed_at = time.perf_counter()

                if isinstance(outcome, BaseException):
                    reason = f"{type(outcome).__name__}: {outcome}"
                    self._requeue_or_fail(task, metric, ready, attempts, reason)
                    continue

                frame: proto.Frame = outcome.frame
                metric.to_worker_s = outcome.to_worker_s
                metric.from_worker_s = outcome.from_worker_s
                if frame.type == proto.ERROR:
                    message = frame.envelope.error.message or "worker error"
                    if self.trace:
                        self.trace.emit(
                            "task_error", task=task.task_id,
                            worker=metric.worker_id, message=message,
                        )
                    self._requeue_or_fail(task, metric, ready, attempts, message)
                    continue

                result = frame.envelope.result
                stats = result.stats
                metric.status = "ok"
                metric.stage_s = float(stats.stage_s)
                metric.exec_s = float(stats.exec_s)
                metric.store_high_water = int(stats.store_high_water)
                metric.store_bytes = int(stats.store_bytes)
                metric.output_bytes = outcome.output_bytes
                metric.transport = outcome.transport
                slot.tasks_completed += 1
                slot.bytes_in += metric.input_bytes
                slot.bytes_out += metric.output_bytes
                results[task.index] = outcome.output
                self._record_success(task, metric)
                if self.trace:
                    self.trace.emit(
                        "collect",
                        task=task.task_id,
                        worker=metric.worker_id,
                        index=task.index,
                        output_bytes=metric.output_bytes,
                        exec_s=round(metric.exec_s, 6),
                        total_s=round(metric.total_s, 6),
                    )

        failures: dict[int, str] = {}
        for task in tasks:
            metric = queued[task.task_id]
            if metric.status not in ("ok", "error"):
                metric.status = "error"
                metric.error = metric.error or "task never completed"
            if metric.status != "ok":
                failures[task.index] = metric.error or "failed"
                results.pop(task.index, None)
        return results, failures

    def _record_success(self, task: Task, metric: TaskMetric) -> None:
        """Publish one successful task to metrics, keyed for slicing."""
        attrs = {ATTR_OP: task.op, ATTR_WORKER: metric.worker_id}
        self.telemetry.add(M_TASKS, 1, {**attrs, ATTR_STATUS: "ok"})
        self.telemetry.record(M_TASK_EXEC_SECONDS, metric.exec_s, attrs)
        self.telemetry.record(M_TASK_STAGE_SECONDS, metric.stage_s, attrs)
        self.telemetry.record(M_TASK_OUTPUT_BYTES, metric.output_bytes, attrs)
        self.telemetry.record(
            M_TASK_TRANSFER_SECONDS, metric.to_worker_s + metric.from_worker_s, attrs
        )
        self.telemetry.record(M_TASK_TOTAL_SECONDS, metric.total_s, attrs)
        self.telemetry.record(M_STORE_HIGH_WATER_BYTES, metric.store_high_water, attrs)
        self.telemetry.add(
            M_WORKER_BUSY_SECONDS, metric.exec_s + metric.stage_s, attrs
        )

    def _requeue_or_fail(
        self,
        task: Task,
        metric: TaskMetric,
        ready: deque[Task],
        attempts: dict[str, int],
        error: str,
    ) -> None:
        """Retry a failed task up to ``max_attempts``, else record it dead."""
        metric.error = error
        reason = _failure_reason(error)
        self.telemetry.add(
            M_TASKS,
            1,
            {ATTR_OP: task.op, ATTR_WORKER: metric.worker_id, ATTR_STATUS: "error"},
        )
        self.telemetry.add(M_TASK_FAILURES, 1, {ATTR_OP: task.op, ATTR_REASON: reason})
        if attempts[task.task_id] < self.config.max_attempts:
            metric.status = "pending"
            metric.worker_id = ""
            metric.dispatched_at = 0.0
            metric.completed_at = 0.0
            ready.append(task)
            self.telemetry.add(M_RETRIES, 1, {ATTR_OP: task.op})
            if self.trace:
                self.trace.emit(
                    "retry", task=task.task_id,
                    attempt=attempts[task.task_id], error=error,
                )
        else:
            metric.status = "error"
            if self.trace:
                self.trace.emit(
                    "task_failed", task=task.task_id,
                    attempts=attempts[task.task_id], error=error,
                )


def _failure_reason(error: str) -> str:
    """Classify an error into a low-cardinality metric label.

    Raw messages are unbounded, so they would explode metric cardinality;
    this keeps the interesting distinction (overflow versus unknown op
    versus transport loss) without carrying detail into the label.
    """
    lowered = error.lower()
    if "overflow" in lowered:
        return "local_store_overflow"
    if "unknown op" in lowered:
        return "unknown_op"
    if "timeout" in lowered:
        return "timeout"
    if "connection" in lowered or "broken pipe" in lowered:
        return "transport"
    if "protocol" in lowered:
        return "protocol"
    if "input buffer" in lowered:
        return "input_count_mismatch"
    return "other"
