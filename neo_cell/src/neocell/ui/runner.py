"""Runs jobs for the UI and publishes their state as a live event stream.

The UI needs to answer "which SPE is working on what right now" and "when
did the PPE get this result". Both already exist as events on the JSONL
trace, so rather than adding a second instrumentation path, this module
drives the ordinary :class:`~neocell.ppe.metrics.Trace` and republishes its
records. If a trace event ever stops firing, the UI goes quiet too, which
is a useful property: the UI cannot drift from the real run.

A run owns a thread with its own event loop, because the HTTP server is
threaded and a job must not block the server that is watching it.
"""

from __future__ import annotations

import asyncio
import itertools
import threading
import time
from dataclasses import asdict, dataclass, field
from typing import Any

from neocell import workloads
from neocell.ppe import DmaModel, Ppe, PpeConfig, Trace
from neocell.ui.scenarios import RunRequest

_SEQ = itertools.count(1)


class _QueueSink:
    """Minimal TextIO surface so Trace can push into the UI queue.

    Trace is deliberately sink-agnostic; this is the adapter that makes
    its events visible to the browser without teaching Trace about HTTP.
    """

    def __init__(self, publish) -> None:
        self._publish = publish

    def write(self, text: str) -> int:
        for line in text.splitlines():
            if line.strip():
                self._publish(line)
        return len(text)

    def flush(self) -> None:
        pass


@dataclass
class RunState:
    """Everything the UI renders, for the current run."""

    run_id: str
    request: RunRequest
    status: str = "starting"
    started_at: float = field(default_factory=time.time)
    finished_at: float | None = None
    error: str | None = None
    workers: dict[str, dict[str, Any]] = field(default_factory=dict)
    task_states: dict[str, dict[str, Any]] = field(default_factory=dict)
    counts: dict[str, int] = field(default_factory=dict)
    events: list[dict[str, Any]] = field(default_factory=list)
    result: dict[str, Any] | None = None
    log: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "request": asdict(self.request),
            "workers": self.workers,
            "task_states": self.task_states,
            "counts": self.counts,
            "result": self.result,
            "error": self.error,
            "log": self.log[-80:],
            "events": self.events[-200:],
            "elapsed_s": round(
                (self.finished_at or time.time()) - self.started_at, 4
            ),
        }


class RunManager:
    """Owns the current run and the queue of UI updates."""

    def __init__(self, max_events: int = 500) -> None:
        self.lock = threading.Lock()
        self.wakeup = threading.Condition(self.lock)
        self.state: RunState | None = None
        self.queue: list[dict[str, Any]] = []
        self.max_events = max_events
        self._thread: threading.Thread | None = None
        self._ppe: Ppe | None = None
        self._stop = threading.Event()

    # -- publication ---------------------------------------------------
    def _publish(self, payload: dict[str, Any]) -> None:
        with self.lock:
            self.queue.append(payload)
            if len(self.queue) > self.max_events:
                del self.queue[: len(self.queue) - self.max_events]
            self.wakeup.notify_all()

    def _on_trace_line(self, line: str) -> None:
        import json

        try:
            record = json.loads(line)
        except ValueError:
            return
        self._apply_trace(record)
        self._publish({"type": "trace", "record": record})

    def _apply_trace(self, record: dict[str, Any]) -> None:
        state = self.state
        if state is None:
            return
        event = record.get("event")
        worker = record.get("worker") or record.get("worker_id")
        task = record.get("task")
        with self.lock:
            if event == "worker_ready":
                state.workers.setdefault(
                    worker,
                    {
                        "worker_id": worker,
                        "pid": record.get("pid"),
                        "status": "idle",
                        "tasks": 0,
                        "tasks_dispatched": 0,
                        "bytes_in": 0,
                        "bytes_out": 0,
                        "store_bytes": record.get("store_bytes"),
                        "ops": record.get("ops", []),
                        "current_task": None,
                        "busy_ms": 0.0,
                    },
                )
            elif event == "dispatch" and worker:
                info = state.workers.setdefault(
                    worker, _blank_worker(worker)
                )
                info["status"] = "busy"
                info["current_task"] = task
                info["tasks"] += 1
                info["tasks_dispatched"] = info.get("tasks_dispatched", 0) + 1
                if task:
                    state.task_states[task] = {
                        "task_id": task,
                        "index": record.get("index"),
                        "worker": worker,
                        "state": "running",
                        "queue_ms": round((record.get("queue_s") or 0) * 1000, 2),
                        "input_bytes": record.get("input_bytes"),
                    }
            elif event == "collect" and worker:
                info = state.workers.setdefault(worker, _blank_worker(worker))
                info["status"] = "idle"
                info["current_task"] = None
                info["bytes_in"] += record.get("input_bytes") or 0
                info["bytes_out"] += record.get("output_bytes") or 0
                info["busy_ms"] += (record.get("exec_s") or 0) * 1000
                if task:
                    entry = state.task_states.get(task)
                    if entry:
                        entry["state"] = "done"
                        entry["exec_ms"] = round((record.get("exec_s") or 0) * 1000, 3)
                        entry["total_ms"] = round((record.get("total_s") or 0) * 1000, 3)
                        entry["output_bytes"] = record.get("output_bytes")
            elif event in ("retry", "task_failed", "task_error") and task:
                entry = state.task_states.setdefault(
                    task, {"task_id": task, "state": "failed"}
                )
                entry["state"] = "retry" if event == "retry" else "failed"
                entry["error"] = record.get("error") or record.get("message")
            state.events.append(record)

    # -- run control ---------------------------------------------------
    def start(self, request: RunRequest) -> str:
        with self.lock:
            if self.state and self.state.status == "running":
                raise RuntimeError("a run is already in progress")
            run_id = f"run-{next(_SEQ)}"
            self.state = RunState(run_id=run_id, request=request)
            self.queue.clear()
            self._stop.clear()

        self._thread = threading.Thread(
            target=self._run_job, args=(run_id, request), daemon=True
        )
        self._thread.start()
        return run_id

    def stop(self) -> None:
        self._stop.set()
        with self.lock:
            if self.state and self.state.status == "running":
                self.state.status = "stopping"
        self._publish({"type": "status", "status": "stopping"})

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            if self.state is None:
                return {"status": "idle"}
            return self.state.as_dict()

    def drain(self, timeout: float) -> list[dict[str, Any]]:
        """Wait for and return new published updates."""
        with self.lock:
            if not self.queue:
                self.wakeup.wait(timeout)
            out, self.queue[:] = self.queue[:], []
        return out

    # -- the job itself ------------------------------------------------
    def _run_job(self, run_id: str, request: RunRequest) -> None:
        try:
            asyncio.run(self._execute(run_id, request))
        except Exception as exc:
            with self.lock:
                if self.state:
                    self.state.status = "error"
                    self.state.error = f"{type(exc).__name__}: {exc}"
                    self.state.finished_at = time.time()
            self._publish({"type": "error", "message": str(exc)})

    async def _execute(self, run_id: str, request: RunRequest) -> None:
        state = self.state
        assert state is not None
        state.status = "running"
        self._publish({"type": "status", "status": "running"})

        for i in range(request.spe):
            wid = f"SPE{i}"
            state.workers[wid] = _blank_worker(wid)

        trace = Trace(sink=_QueueSink(self._on_trace_line))
        dma = DmaModel(bytes_per_second=request.dma_bw)
        config = PpeConfig(
            spe_count=request.spe, local_store_bytes=request.store_bytes
        )
        ppe = Ppe(config, dma=dma, trace=trace)
        self._ppe = ppe
        watcher: asyncio.Task | None = None
        try:
            await ppe.start()
            for slot in ppe.slots:
                slot.adapter.buffers.inline_limit = request.inline_limit
                info = state.workers.get(slot.info.worker_id)
                if info is not None:
                    info.update(
                        {
                            "pid": slot.info.pid,
                            "store_bytes": slot.info.local_store_bytes,
                            "ops": slot.info.ops,
                        }
                    )
            job = workloads.build(request.workload, request.elements, request.chunk)
            watcher = asyncio.create_task(self._watch(ppe, state))
            result = await ppe.submit(job)
            await self._finish(ppe, state, result)
        except Exception as exc:
            state.status = "error"
            state.error = f"{type(exc).__name__}: {exc}"
            state.finished_at = time.time()
            self._publish({"type": "error", "message": state.error})
        finally:
            if watcher is not None:
                watcher.cancel()
            await ppe.stop()
            trace.close()
            self._ppe = None

    async def _watch(self, ppe: Ppe, state: RunState) -> None:
        """Republish live worker occupancy so the UI can animate it."""
        try:
            while True:
                snapshot = [
                    {
                        "worker_id": slot.info.worker_id,
                        "busy": slot.busy_task is not None,
                        "current_task": slot.busy_task,
                        "tasks_completed": slot.tasks_completed,
                        "bytes_in": slot.bytes_in,
                        "bytes_out": slot.bytes_out,
                        "healthy": slot.adapter.healthy,
                    }
                    for slot in ppe.slots
                ]
                self._publish({"type": "workers", "workers": snapshot})
                with self.lock:
                    for entry in snapshot:
                        info = state.workers.get(entry["worker_id"])
                        if info is not None:
                            info["status"] = "busy" if entry["busy"] else "idle"
                            info["current_task"] = entry["current_task"]
                            info["tasks_completed"] = entry["tasks_completed"]
                    state.counts = _counts(state)
                await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            raise

    async def _finish(self, ppe: Ppe, state: RunState, result) -> None:
        summary = result.metrics.summary()
        for slot in ppe.slots:
            info = state.workers.get(slot.info.worker_id)
            if info is not None:
                info["tasks"] = slot.tasks_completed
                info["tasks_dispatched"] = info.get("tasks_dispatched", 0)
                info["bytes_in"] = slot.bytes_in
                info["bytes_out"] = slot.bytes_out
                info["status"] = "idle"
                info["current_task"] = None
        state.counts = _counts(state)
        state.status = "done" if result.ok else "failed"
        state.finished_at = time.time()
        scalars = result.scalars()
        state.result = {
            "ok": result.ok,
            "op": result.op,
            "wall_ms": round(result.wall_s * 1000, 3),
            "tasks": summary["tasks"],
            "failed": summary["failed"],
            "by_transport": summary.get("by_transport", {}),
            "total_bytes": summary["total_bytes"],
            "max_store_high_water": summary.get("max_store_high_water"),
            "output_preview": ["%.6g" % v for v in scalars[:12]],
            "output_len": len(scalars),
            "failures": dict(list(result.failures.items())[:5]),
            "workers": summary.get("workers", {}),
        }
        self._publish({"type": "done", "result": state.result})


def _blank_worker(worker_id: str) -> dict[str, Any]:
    return {
        "worker_id": worker_id,
        "pid": None,
        "status": "idle",
        "tasks": 0,
        "tasks_dispatched": 0,
        "bytes_in": 0,
        "bytes_out": 0,
        "store_bytes": None,
        "ops": [],
        "current_task": None,
        "busy_ms": 0.0,
    }


def _counts(state: RunState) -> dict[str, int]:
    counts: dict[str, int] = {}
    for entry in state.task_states.values():
        counts[entry.get("state", "unknown")] = (
            counts.get(entry.get("state", "unknown"), 0) + 1
        )
    return counts