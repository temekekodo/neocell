"""Worker adapters: the PPE's stable door to an SPE.

The PPE never imports worker code. It talks to an adapter that can
spawn a process, handshake, ship a task and collect a result. Substituting
a Java SPE means writing a new adapter, not touching the coordinator,
which is the whole point of the language-neutral protocol.
"""

from __future__ import annotations

import abc
import asyncio
import contextlib
import os
import sys
from dataclasses import dataclass, field
from neocell import protocol as proto
from neocell.buffers import DEFAULT_INLINE_LIMIT, BufferManager
from neocell.codec import pack_f32
from neocell.pb import neocell_pb2 as pb
from neocell.ppe.dma import DmaModel
from neocell.ppe.job import Task
from neocell.ppe.metrics import Trace
from neocell.telemetry import (
    ATTR_DIRECTION,
    ATTR_INPUT_BYTES,
    ATTR_OP,
    ATTR_STATUS,
    ATTR_TASK_ID,
    ATTR_TASK_INDEX,
    ATTR_WORKER,
    M_TRANSFER_BYTES,
    M_TRANSFER_SECONDS,
    SERVICE_SPE,
    Telemetry,
    disabled,
)
from neocell.version import PROTOCOL_VERSION

_READ_CHUNK = 1 << 16


@dataclass(slots=True)
class WorkerInfo:
    """What the PPE knows about one worker after the handshake."""

    worker_id: str
    pid: int | None = None
    local_store_bytes: int = 0
    ops: list[str] = field(default_factory=list)
    healthy: bool = True
    last_error: str | None = None

    def supports(self, op: str) -> bool:
        return op in self.ops


@dataclass(slots=True)
class Execution:
    """A worker's reply plus the transfer cost the PPE paid for it.

    Transfer seconds are attributed here rather than read back from the
    DMA model's running totals, because several tasks are in flight at
    once and only the caller knows which one a byte count belongs to.
    """

    frame: proto.Frame
    to_worker_s: float = 0.0
    from_worker_s: float = 0.0
    input_bytes: int = 0
    output_bytes: int = 0
    output: bytes = b""
    transport: str = "inline"


class SpeAdapter(abc.ABC):
    """The replaceable-worker interface the PPE depends on."""

    info: WorkerInfo

    @abc.abstractmethod
    async def start(self) -> WorkerInfo:
        """Launch the worker and complete the handshake."""

    @abc.abstractmethod
    async def execute(self, task: Task) -> Execution:
        """Ship one task and collect its result, across both data planes."""
        ...

    async def cancel(self, task_id: str) -> None:
        ...

    @abc.abstractmethod
    async def stop(self) -> None:
        ...

    @property
    def healthy(self) -> bool:
        return self.info.healthy

    @property
    def worker_id(self) -> str:
        return self.info.worker_id

    async def __aenter__(self) -> SpeAdapter:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()


class ProcessSpeAdapter(SpeAdapter):
    """An SPE as a child process speaking the framing over stdio."""

    def __init__(
        self,
        worker_id: str,
        local_store_bytes: int,
        dma: DmaModel | None = None,
        trace: Trace | None = None,
        command: list[str] | None = None,
        request_timeout_s: float = 120.0,
        telemetry: Telemetry | None = None,
        inline_limit: int = DEFAULT_INLINE_LIMIT,
        buffers: BufferManager | None = None,
    ) -> None:
        self.info = WorkerInfo(worker_id=worker_id, local_store_bytes=local_store_bytes)
        self.dma = dma or DmaModel()
        self.trace = trace
        self.telemetry = telemetry or disabled()
        self.local_store_bytes = local_store_bytes
        self.request_timeout_s = request_timeout_s
        self.buffers = buffers or BufferManager(inline_limit=inline_limit, tag="ppe")
        self._cmd = command or self._default_command()
        self._proc: asyncio.subprocess.Process | None = None
        self._read_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._inbox: asyncio.Queue[proto.Frame] = asyncio.Queue()
        self._decoder = proto.FrameDecoder()
        self._write_lock = asyncio.Lock()
        self._stderr_lines: list[str] = []
        self._closed = asyncio.Event()

    def _default_command(self) -> list[str]:
        cmd = [
            sys.executable,
            "-m",
            "neocell.spe.worker",
            "--id",
            self.info.worker_id,
            "--local-store-bytes",
            str(self.local_store_bytes),
        ]
        # The worker is a separate process, so it configures its own
        # Telemetry; the sink settings are passed on the command line
        # rather than inherited implicitly.
        cfg = self.telemetry.config
        if cfg.enabled:
            cmd.append("--otel")
            # The worker is its own service with its own identity; naming
            # it after the PPE would make every process look like the
            # coordinator in the backend.
            cmd += ["--otel-service", SERVICE_SPE]
            if cfg.console_export:
                cmd.append("--otel-console")
            if cfg.jsonl:
                cmd.append("--otel-jsonl")
            if cfg.jsonl_path:
                # One directory, one file per worker: interleaved writes
                # from four processes make a single file unreadable.
                stem = os.path.splitext(os.path.basename(str(cfg.jsonl_path)))[0]
                cmd += ["--otel-jsonl-path", f"{stem}-{self.info.worker_id}.jsonl"]
            cmd += ["--otel-interval-ms", str(cfg.export_interval_ms)]
        return cmd

    async def start(self) -> WorkerInfo:
        src_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        env = dict(os.environ)
        env["PYTHONPATH"] = (
            os.pathsep.join([src_root, env["PYTHONPATH"]])
            if env.get("PYTHONPATH")
            else src_root
        )
        self._proc = await asyncio.create_subprocess_exec(
            *self._cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
            limit=_READ_CHUNK,
        )
        self._read_task = asyncio.create_task(self._pump_stdout())
        self._stderr_task = asyncio.create_task(self._pump_stderr())

        await self._write(
            proto.hello_frame(self.info.worker_id, self.local_store_bytes)
        )
        reply = await self._request()
        proto.validate_worker_frame(reply)
        if reply.type != proto.HELLO_ACK:
            raise proto.ProtocolError(
                f"{self.info.worker_id}: expected hello_ack, got {reply.type}"
            )
        ack = reply.envelope.hello_ack
        self.info.pid = int(ack.pid) or None
        self.info.local_store_bytes = int(ack.local_store_bytes)
        self.info.ops = list(ack.ops)
        self.info.healthy = True
        if self.trace:
            self.trace.emit(
                "worker_ready",
                worker=self.info.worker_id,
                pid=self.info.pid,
                store_bytes=self.info.local_store_bytes,
                ops=self.info.ops,
            )
        return self.info

    async def execute(self, task: Task) -> Execution:
        """Ship one task and collect its result, across both data planes."""
        carrier: dict[str, str] = {}
        attrs = {
            ATTR_TASK_ID: task.task_id,
            ATTR_TASK_INDEX: task.index,
            ATTR_OP: task.op,
            ATTR_INPUT_BYTES: task.input_bytes,
        }
        with self.telemetry.span("neocell.spe.execute", attrs) as span:
            refs: list[pb.DataRef] = []
            inline = b""
            for buf in task.inputs:
                ref, blob = self.buffers.put_f32(buf, owner=self.info.worker_id)
                if ref is not None:
                    refs.append(ref)
                else:
                    inline += blob
            payload_bytes = task.input_bytes
            transport = "arrow_ref" if refs else "inline"

            # The modelled DMA charge covers the bytes that actually move,
            # whether they travel inline or land in a mapped file.
            to_worker_s = await self.dma.move(payload_bytes, to_worker=True)
            self.telemetry.add(
                M_TRANSFER_BYTES, payload_bytes, {ATTR_DIRECTION: "to_worker"}
            )
            self.telemetry.add(
                M_TRANSFER_SECONDS, to_worker_s, {ATTR_DIRECTION: "to_worker"}
            )
            span.set_attribute(ATTR_WORKER, self.info.worker_id)
            span.set_attribute("neocell.transport", transport)
            self.telemetry.inject_context(carrier)
            await self._write(
                proto.exec_frame(
                    task_id=task.task_id,
                    op=task.op,
                    params=_to_params(task.params),
                    input_lengths=task.input_lengths(),
                    inline_payload=inline,
                    input_refs=refs,
                    trace=carrier or None,
                    task_index=task.index,
                    label=task.label,
                )
            )
            try:
                frame = await self._request()
                proto.validate_worker_frame(frame)
            finally:
                # The input is no longer needed once the worker has it.
                for ref in refs:
                    self.buffers.release(ref)

            from_worker_s = 0.0
            output = b""
            if frame.type == proto.RESULT:
                output = self._collect_output(frame)
                from_worker_s = await self.dma.move(len(output), to_worker=False)
                self.telemetry.add(
                    M_TRANSFER_BYTES, len(output), {ATTR_DIRECTION: "from_worker"}
                )
                self.telemetry.add(
                    M_TRANSFER_SECONDS, from_worker_s, {ATTR_DIRECTION: "from_worker"}
                )
            elif frame.type == proto.ERROR:
                span.set_attribute(ATTR_STATUS, "error")
        return Execution(
            frame=frame,
            to_worker_s=to_worker_s,
            from_worker_s=from_worker_s,
            input_bytes=payload_bytes,
            output_bytes=len(output),
            output=output,
            transport=transport,
        )

    def _collect_output(self, frame: proto.Frame) -> bytes:
        """Resolve a result's data and release its ref.

        The ref is released here because this is the moment the PPE takes
        ownership of the result; leaving it to a manager sweep would pin
        the file for the whole run.
        """
        result = frame.envelope.result
        if result.HasField("output_ref") and result.output_ref.id:
            ref = result.output_ref
            try:
                return pack_f32(self.buffers.get_f32(ref))
            finally:
                self.buffers.release(ref)
        return result.inline_payload

    async def cancel(self, task_id: str) -> None:
        await self._write(proto.cancel_frame(task_id))

    async def stop(self) -> None:
        self.buffers.cleanup()
        if self._proc is None:
            return
        with contextlib.suppress(Exception):
            await self._write(proto.shutdown_frame())
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._closed.wait(), timeout=5.0)
        if self._proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                self._proc.terminate()
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._proc.wait(), timeout=5.0)
        for task in (self._read_task, self._stderr_task):
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        self.info.healthy = False

    async def _write(self, frame: proto.Frame) -> None:
        assert self._proc is not None and self._proc.stdin is not None
        data = proto.encode(frame)
        async with self._write_lock:
            self._proc.stdin.write(data)
            await self._proc.stdin.drain()

    async def _request(self) -> proto.Frame:
        """Wait for the next reply, failing fast if the worker is gone.

        Without the closed-stream check a dead worker costs the full
        request timeout before the job notices, which turns one crash
        into a multi-minute hang.
        """
        getter = asyncio.ensure_future(self._inbox.get())
        closed = asyncio.ensure_future(self._closed.wait())
        try:
            done, _ = await asyncio.wait(
                {getter, closed},
                timeout=self.request_timeout_s,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if getter in done:
                return getter.result()
            self.info.healthy = False
            if self._closed.is_set():
                self.info.last_error = "worker stream closed"
                raise ConnectionResetError(
                    f"{self.info.worker_id}: worker exited before replying"
                )
            self.info.last_error = "worker timeout"
            raise TimeoutError(
                f"{self.info.worker_id}: no reply within {self.request_timeout_s}s"
            )
        finally:
            for fut in (getter, closed):
                if not fut.done():
                    fut.cancel()

    async def _pump_stdout(self) -> None:
        assert self._proc is not None and self._proc.stdout is not None
        try:
            while True:
                chunk = await self._proc.stdout.read(_READ_CHUNK)
                if not chunk:
                    break
                self._decoder.feed(chunk)
                for frame in self._decoder:
                    await self._inbox.put(frame)
        except (asyncio.CancelledError, ValueError):
            raise
        finally:
            self._closed.set()
            self.info.healthy = False

    async def _pump_stderr(self) -> None:
        assert self._proc is not None and self._proc.stderr is not None
        forward = bool(self.telemetry.config.console_export)
        try:
            while True:
                line = await self._proc.stderr.readline()
                if not line:
                    break
                text = line.decode("utf-8", "replace").rstrip()
                self._stderr_lines.append(text)
                del self._stderr_lines[:-50]
                if self.trace:
                    self.trace.emit(
                        "worker_stderr", worker=self.info.worker_id, line=text
                    )
                # In console mode the worker's spans and metrics are on its
                # stderr; relaying them puts every process's telemetry in
                # one stream. OTLP needs no relay, the worker posts
                # directly to the collector.
                if forward:
                    sys.stderr.write(text + "\n")
                    sys.stderr.flush()
        except (asyncio.CancelledError, ValueError):
            raise

    def stderr_tail(self, n: int = 10) -> list[str]:
        return self._stderr_lines[-n:]

_PARAM_FIELDS = ("factor", "a", "b", "rounds", "radius")


def _to_params(params: dict[str, Any]) -> pb.OpParams:
    """Convert a plain params dict into the typed OpParams message.

    Unknown keys are dropped rather than raising: the PPE builds params
    from workload definitions, and an op that ignores a knob should not
    fail a job at dispatch time.
    """
    msg = pb.OpParams()
    for name in _PARAM_FIELDS:
        if name not in params:
            continue
        value = params[name]
        if name in ("rounds", "radius"):
            setattr(msg, name, int(value))
        else:
            setattr(msg, name, float(value))
    return msg


def _from_params(msg: pb.OpParams) -> dict[str, Any]:
    """Inverse of :func:`_to_params`, skipping unset proto3 defaults."""
    out: dict[str, Any] = {}
    for name in _PARAM_FIELDS:
        value = getattr(msg, name)
        if value:
            out[name] = value
    return out
