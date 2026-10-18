"""Job and task model for the PPE.

A job is an operation plus the data it needs; the PPE divides it into
independent tasks that each fit in one worker's local store. Tasks carry
opaque input buffers so the scheduler never needs to understand the
operation, which is what lets a Java worker serve the same tasks.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any

from neocell.codec import f32_nbytes


@dataclass(slots=True, frozen=True)
class Task:
    """One unit of work: what to run, on what bytes, and where it fits."""

    task_id: str
    index: int
    op: str
    params: dict[str, Any]
    inputs: tuple[bytes, ...]
    chunk_index: int = 0
    chunk_count: int = 1
    label: str = ""

    @property
    def input_bytes(self) -> int:
        return sum(len(b) for b in self.inputs)

    @property
    def input_elements(self) -> int:
        return self.input_bytes // 4

    def packed(self) -> bytes:
        return b"".join(self.inputs)

    def input_lengths(self) -> list[int]:
        return [len(b) for b in self.inputs]


@dataclass(slots=True)
class Job:
    """A named operation over chunks of float32 data.

    ``inputs`` holds one byte buffer per op input, all the same length;
    splitting walks them in lockstep so a multi-input task always gets a
    matching slice of every buffer.
    """

    name: str
    op: str
    params: dict[str, Any] = field(default_factory=dict)
    chunk_elements: int = 4096
    inputs: list[bytes] = field(default_factory=list)
    reduce: str | None = None
    job_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])

    @property
    def total_elements(self) -> int:
        return len(self.inputs[0]) // 4 if self.inputs else 0

    @property
    def input_bytes(self) -> int:
        return sum(len(b) for b in self.inputs)

    def split(self) -> list[Task]:
        """Chop every input buffer into ``chunk_elements`` slices."""
        if not self.inputs:
            return []
        span = f32_nbytes(self.chunk_elements)
        per_input: list[list[bytes]] = [
            [buf[start : start + span] for start in range(0, len(buf), span)]
            for buf in self.inputs
        ]
        chunk_count = len(per_input[0])
        tasks: list[Task] = []
        for i in range(chunk_count):
            tasks.append(
                Task(
                    task_id=f"{self.job_id}-t{i:05d}",
                    index=i,
                    op=self.op,
                    params=dict(self.params),
                    inputs=tuple(slices[i] for slices in per_input),
                    chunk_index=i,
                    chunk_count=chunk_count,
                    label=f"{self.name}[{i * self.chunk_elements}:]"
                    f"{(i + 1) * self.chunk_elements}",
                )
            )
        return tasks
