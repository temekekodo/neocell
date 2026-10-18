"""Metrics and the trace log.

The point of the exercise is to make the consequences of scheduling and
data movement observable, so every task records queue wait, staged bytes,
transfer time, execution time and store high-water mark, and every event
can be dumped as JSON for inspection.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TextIO


@dataclass(slots=True)
class TaskMetric:
    """One task's life: wait, stage, execute, return."""

    task_id: str
    index: int
    worker_id: str
    queued_at: float
    dispatched_at: float = 0.0
    completed_at: float = 0.0
    stage_s: float = 0.0
    exec_s: float = 0.0
    to_worker_s: float = 0.0
    from_worker_s: float = 0.0
    input_bytes: int = 0
    output_bytes: int = 0
    store_high_water: int = 0
    store_bytes: int = 0
    transport: str = "inline"
    status: str = "pending"
    error: str | None = None
    attempts: int = 0

    @property
    def queue_s(self) -> float:
        return max(0.0, self.dispatched_at - self.queued_at)

    @property
    def total_s(self) -> float:
        return max(0.0, self.completed_at - self.queued_at)

    @property
    def transfer_bytes(self) -> int:
        return self.input_bytes + self.output_bytes

    def as_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "index": self.index,
            "worker_id": self.worker_id,
            "status": self.status,
            "attempts": self.attempts,
            "queue_s": round(self.queue_s, 9),
            "stage_s": round(self.stage_s, 9),
            "exec_s": round(self.exec_s, 9),
            "to_worker_s": round(self.to_worker_s, 9),
            "from_worker_s": round(self.from_worker_s, 9),
            "total_s": round(self.total_s, 9),
            "input_bytes": self.input_bytes,
            "output_bytes": self.output_bytes,
            "store_high_water": self.store_high_water,
            "store_bytes": self.store_bytes,
            "transport": self.transport,
            "error": self.error,
        }


class Trace:
    """Append-only event log, JSON lines, optionally to a file."""

    def __init__(self, sink: TextIO | None = None, path: Path | None = None) -> None:
        self.sink = sink
        self.path = path
        self._fh: TextIO | None = None
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)
            self._fh = path.open("a", encoding="utf-8")
        self.events = 0

    def emit(self, event: str, **fields: Any) -> None:
        self.events += 1
        record = {"t": round(time.perf_counter(), 6), "event": event, **fields}
        line = json.dumps(record, separators=(",", ":"), sort_keys=True)
        if self._fh is not None:
            self._fh.write(line + "\n")
            self._fh.flush()
        if self.sink is not None:
            self.sink.write(line + "\n")
            self.sink.flush()

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    def __enter__(self) -> Trace:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class MetricsCollector:
    """Aggregate view across every task and worker in a job run."""

    def __init__(self) -> None:
        self.tasks: list[TaskMetric] = []
        self.wall_s: float = 0.0

    def add(self, metric: TaskMetric) -> None:
        self.tasks.append(metric)

    def by_worker(self) -> dict[str, dict[str, float]]:
        out: dict[str, dict[str, float]] = {}
        for t in self.tasks:
            w = out.setdefault(
                t.worker_id,
                {"tasks": 0, "busy_s": 0.0, "queue_s": 0.0, "bytes": 0},
            )
            w["tasks"] += 1
            w["busy_s"] += t.exec_s + t.stage_s
            w["queue_s"] += t.queue_s
            w["bytes"] += t.transfer_bytes
        if self.wall_s > 0:
            for w in out.values():
                w["utilization"] = round(w["busy_s"] / self.wall_s, 4)
        return out

    def summary(self) -> dict[str, Any]:
        ok = [t for t in self.tasks if t.status == "ok"]
        failed = [t for t in self.tasks if t.status != "ok"]
        total_exec = sum(t.exec_s for t in self.tasks)
        total_stage = sum(t.stage_s for t in self.tasks)
        total_xfer = sum(t.to_worker_s + t.from_worker_s for t in self.tasks)
        total_queue = sum(t.queue_s for t in self.tasks)
        total_bytes = sum(t.transfer_bytes for t in self.tasks)
        return {
            "tasks": len(self.tasks),
            "ok": len(ok),
            "failed": len(failed),
            "wall_s": round(self.wall_s, 6),
            "sum_exec_s": round(total_exec, 6),
            "sum_stage_s": round(total_stage, 6),
            "sum_transfer_s": round(total_xfer, 6),
            "sum_queue_s": round(total_queue, 6),
            "total_bytes": total_bytes,
            "max_store_high_water": max(
                (t.store_high_water for t in self.tasks), default=0
            ),
            "by_transport": self._by_transport(),
            "workers": self.by_worker(),
        }

    def _by_transport(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for t in self.tasks:
            out[t.transport] = out.get(t.transport, 0) + 1
        return out

    def report(self) -> str:
        s = self.summary()
        lines = [
            f"tasks      : {s['tasks']} ({s['ok']} ok, {s['failed']} failed)",
            f"wall       : {s['wall_s'] * 1000:.2f} ms",
            f"exec total : {s['sum_exec_s'] * 1000:.2f} ms",
            f"stage total: {s['sum_stage_s'] * 1000:.2f} ms",
            f"xfer  total: {s['sum_transfer_s'] * 1000:.2f} ms",
            f"queue total: {s['sum_queue_s'] * 1000:.2f} ms",
            f"bytes moved: {s['total_bytes']} B",
            f"store peak : {s['max_store_high_water']} B",
            f"transport  : {s['by_transport']}",
        ]
        for wid, w in sorted(s["workers"].items()):
            util = w.get("utilization", 0.0)
            lines.append(
                f"  {wid:<8} tasks={int(w['tasks']):<6} "
                f"busy={w['busy_s'] * 1000:8.2f} ms  util={util * 100:5.1f}%  "
                f"bytes={int(w['bytes'])}"
            )
        return "\n".join(lines)


@dataclass(slots=True)
class JobResult:
    """Outcome of a scheduled job, including ordered chunk payloads."""

    job_id: str
    name: str
    op: str
    outputs: dict[int, bytes] = field(default_factory=dict)
    failures: dict[int, str] = field(default_factory=dict)
    metrics: MetricsCollector | None = None
    wall_s: float = 0.0

    @property
    def ok(self) -> bool:
        return not self.failures

    def concatenated(self) -> bytes:
        return b"".join(self.outputs[i] for i in sorted(self.outputs))

    def scalars(self) -> list[float]:
        from neocell.codec import unpack_f32

        return unpack_f32(self.concatenated())