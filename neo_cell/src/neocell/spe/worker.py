"""The SPE worker: a blocking request/response loop over stdin/stdout.

An SPE owns nothing but its local store. It never sees the job, the
other workers, or the global buffer; it receives staged input with each
``exec`` and answers with a ``result``. Keeping the loop synchronous is
intentional: one SPE is one thing being done at a time, exactly like the
hardware, and it means the PPE's scheduler is the only concurrency in
the system.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from typing import Any

from neocell import protocol as proto
from neocell.buffers import DEFAULT_INLINE_LIMIT, BufferError, BufferManager
from neocell.codec import unpack_f32
from neocell.pb import neocell_pb2 as pb
from neocell.ppe.adapter import _from_params
from neocell.spe.local_store import LocalStore, LocalStoreOverflow
from neocell.spe.ops import describe, registry, run_op, supported_ops
from neocell.telemetry import (
    ATTR_INPUT_BYTES,
    ATTR_OP,
    ATTR_REASON,
    ATTR_STATUS,
    ATTR_STORE_BYTES,
    ATTR_STORE_HIGH_WATER,
    ATTR_TASK_ID,
    ATTR_WORKER,
    M_EXCEPTIONS,
    M_EXECUTIONS,
    M_STORE_BYTES,
    M_STORE_HIGH_WATER_BYTES,
    M_TASK_EXEC_SECONDS,
    M_TASK_OUTPUT_BYTES,
    M_TASK_STAGE_SECONDS,
    M_TASKS,
    M_WORKER_BUSY_SECONDS,
    SERVICE_SPE,
    Telemetry,
    TelemetryConfig,
    disabled,
)
from neocell.version import PROTOCOL_VERSION, VERSION


def log(worker_id: str, message: str) -> None:
    print(f"[{worker_id}] {message}", file=sys.stderr, flush=True)


class SpeWorker:
    """Stateful worker driven by decoded frames."""

    def __init__(
        self,
        worker_id: str,
        local_store_bytes: int,
        telemetry: Telemetry | None = None,
        inline_limit: int = DEFAULT_INLINE_LIMIT,
    ):
        self.worker_id = worker_id
        self.local_store = LocalStore(local_store_bytes)
        self.telemetry = telemetry or disabled()
        self.buffers = BufferManager(inline_limit=inline_limit, tag=f"spe-{worker_id}")
        self.tasks_done = 0

    def handle(self, frame: proto.Frame) -> proto.Frame | None:
        proto.validate_ppe_frame(frame)
        if frame.type == proto.HELLO:
            return self._on_hello(frame)
        if frame.type == proto.EXEC:
            return self._on_exec(frame)
        if frame.type == proto.CANCEL:
            return proto.cancelled_frame(frame.envelope.cancel.task_id, self.worker_id)
        if frame.type == proto.SHUTDOWN:
            return proto.bye_frame(self.worker_id)
        raise proto.ProtocolError(f"unexpected message '{frame.type}'")

    def _on_hello(self, frame: proto.Frame) -> proto.Frame:
        hello = frame.envelope.hello
        if int(hello.protocol) != PROTOCOL_VERSION:
            return proto.error_frame(
                "",
                self.worker_id,
                f"protocol mismatch: PPE={hello.protocol} worker={PROTOCOL_VERSION}",
            )
        self.local_store = LocalStore(int(hello.local_store_bytes))
        self.telemetry.record(M_STORE_BYTES, self.local_store.capacity)
        return proto.hello_ack_frame(
            self.worker_id, os.getpid(), self.local_store.capacity, supported_ops()
        )

    def _on_exec(self, frame: proto.Frame) -> proto.Frame:
        exec_msg = frame.envelope.exec
        parent = self.telemetry.extract_context(dict(exec_msg.trace))
        attrs = {
            ATTR_TASK_ID: exec_msg.task_id,
            ATTR_OP: exec_msg.op,
            ATTR_INPUT_BYTES: int(exec_msg.input_bytes),
            ATTR_WORKER: self.worker_id,
        }
        transport = "arrow_ref" if exec_msg.input_refs else "inline"
        attrs["neocell.transport"] = transport
        self.telemetry.add(M_EXECUTIONS, 1, {**attrs, "neocell.transport": transport})
        # The PPE's execute span arrives as traceparent, so this span is
        # its child in a different process: the whole dispatch/execute
        # chain reads as one trace.
        with self.telemetry.span("neocell.spe.exec", attrs, context=parent) as span:
            reply = self._execute(exec_msg, attrs, span)
            span.set_attribute(
                ATTR_STATUS, "error" if reply.type == proto.ERROR else "ok"
            )
            return reply

    def _execute(self, exec_msg: Any, attrs: dict[str, Any], span: Any) -> proto.Frame:
        task_id = exec_msg.task_id
        op_name = exec_msg.op
        params = _from_params(exec_msg.params)

        try:
            inputs = self._resolve_inputs(exec_msg)
        except BufferError as exc:
            self.telemetry.add(M_EXCEPTIONS, 1, {ATTR_REASON: "buffer"})
            return proto.error_frame(task_id, self.worker_id, str(exc))
        except pb.DecodeError as exc:
            self.telemetry.add(M_EXCEPTIONS, 1, {ATTR_REASON: "arrow_decode"})
            return proto.error_frame(task_id, self.worker_id, f"bad data reference: {exc}")

        try:
            spec = describe(op_name)
        except KeyError:
            self.telemetry.add(M_EXCEPTIONS, 1, {ATTR_REASON: "unknown_op"})
            return proto.error_frame(task_id, self.worker_id, f"unknown op '{op_name}'")
        if len(inputs) != spec["n_inputs"]:
            self.telemetry.add(M_EXCEPTIONS, 1, {ATTR_REASON: "input_count_mismatch"})
            return proto.error_frame(
                task_id,
                self.worker_id,
                f"op '{op_name}' expects {spec['n_inputs']} input buffer(s), "
                f"got {len(inputs)}",
            )

        self.local_store.reset()
        t_stage = time.perf_counter()
        try:
            staged = [self.local_store.stage_f32(buf) for buf in inputs]
        except LocalStoreOverflow as exc:
            self.telemetry.add(M_EXCEPTIONS, 1, {ATTR_REASON: "local_store_overflow"})
            span.set_attribute(ATTR_STORE_BYTES, self.local_store.capacity)
            return proto.error_frame(
                task_id, self.worker_id, f"local store overflow: {exc}"
            )
        stage_s = time.perf_counter() - t_stage
        self.telemetry.record(M_TASK_STAGE_SECONDS, stage_s, attrs)
        span.set_attribute("neocell.stage_seconds", stage_s)

        try:
            output, exec_s = run_op(self.local_store, op_name, staged, params)
        except Exception as exc:
            self.telemetry.add(
                M_EXCEPTIONS, 1, {"neocell.exception.type": type(exc).__name__}
            )
            self.telemetry.span_error(span, exc)
            return proto.error_frame(
                task_id, self.worker_id, f"{type(exc).__name__}: {exc}"
            )
        finally:
            self.local_store.reset()

        high_water = self.local_store.high_water
        self.tasks_done += 1
        self.telemetry.record(M_TASK_EXEC_SECONDS, exec_s, attrs)
        self.telemetry.record(M_STORE_HIGH_WATER_BYTES, high_water, attrs)
        self.telemetry.add(M_TASKS, 1, {**attrs, ATTR_STATUS: "ok"})
        self.telemetry.add(M_WORKER_BUSY_SECONDS, exec_s + stage_s, attrs)
        span.set_attribute("neocell.exec_seconds", exec_s)
        span.set_attribute(ATTR_STORE_HIGH_WATER, high_water)

        # Large results go back by reference, so a big output never has to
        # be copied through the pipe.
        ref, inline = self.buffers.put_f32(output, owner=self.worker_id)
        self.telemetry.record(M_TASK_OUTPUT_BYTES, len(output), attrs)
        return proto.result_frame(
            task_id,
            status=proto.STATUS_OK,
            worker_id=self.worker_id,
            inline_payload=inline,
            output_ref=ref,
            stats={
                "stage_s": stage_s,
                "exec_s": exec_s,
                "store_high_water": high_water,
                "store_bytes": self.local_store.capacity,
                "tasks_done": self.tasks_done,
            },
        )

    def _resolve_inputs(self, exec_msg: Any) -> list[list[float]]:
        """Materialise the task's inputs, inline or by reference.

        Both representations yield the same list of float lists, so the op
        path below never needs to know which one arrived.
        """
        if exec_msg.input_refs:
            buffers: list[list[float]] = []
            try:
                for ref in exec_msg.input_refs:
                    self.buffers.track(ref)
                    buffers.append(self.buffers.get_f32(ref))
            finally:
                for ref in exec_msg.input_refs:
                    self.buffers.release(ref)
            return buffers

        payload = exec_msg.inline_payload
        lengths = list(exec_msg.input_lengths)
        if not lengths:
            return [unpack_f32(payload)] if payload else []
        out: list[list[float]] = []
        pos = 0
        for n in lengths:
            chunk = payload[pos : pos + n]
            if len(chunk) != n:
                raise pb.DecodeError("inline payload shorter than declared lengths")
            out.append(unpack_f32(chunk))
            pos += n
        return out


def _run_stdio_loop(worker: SpeWorker) -> int:
    stdin_fd = sys.stdin.fileno()
    stdout = sys.stdout.buffer
    decoder = proto.FrameDecoder()
    while True:
        try:
            chunk = os.read(stdin_fd, 65536)
        except InterruptedError:
            continue
        if not chunk:
            return 0
        decoder.feed(chunk)
        for frame in decoder:
            try:
                reply = worker.handle(frame)
            except Exception as exc:
                # An SPE that dies on an unexpected error takes the whole
                # job down with it, because the PPE would wait for a reply
                # that never comes. Report it as a task error instead and
                # stay alive for the next task.
                log(
                    worker.worker_id,
                    f"unhandled {type(exc).__name__}: {exc}",
                )
                worker.telemetry.add(
                    M_EXCEPTIONS, 1, {"neocell.exception.type": type(exc).__name__}
                )
                reply = proto.error_frame(
                    getattr(frame.envelope.exec, "task_id", ""),
                    worker.worker_id,
                    f"{type(exc).__name__}: {exc}",
                )
            if reply is None:
                continue
            stdout.write(proto.encode(reply))
            stdout.flush()
            if frame.type == proto.SHUTDOWN:
                return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="neocell-spe", description="Cell-style SPE worker")
    ap.add_argument("--id", default="SPE0", help="worker identifier")
    ap.add_argument(
        "--local-store-bytes",
        type=int,
        default=131072,
        help="local store capacity in bytes (128 KiB like real hardware)",
    )
    ap.add_argument("--list-ops", action="store_true", help="print supported ops and exit")
    ap.add_argument(
        "--inline-limit",
        type=int,
        default=DEFAULT_INLINE_LIMIT,
        help="payloads above this many bytes travel as an Arrow data reference",
    )
    ap.add_argument(
        "--otel",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="enable OpenTelemetry (default: on)",
    )
    ap.add_argument("--otel-service", default=SERVICE_SPE, help="OTel service name")
    ap.add_argument(
        "--otel-console", action="store_true", help="print spans/metrics to stderr"
    )
    ap.add_argument(
        "--otel-jsonl", action="store_true", help="append spans/metrics to a JSONL file"
    )
    ap.add_argument("--otel-jsonl-path", default=None, help="JSONL output path")
    ap.add_argument("--otel-interval-ms", type=int, default=10_000)
    args = ap.parse_args(argv)

    if args.list_ops:
        for name, spec in sorted(registry().items()):
            print(f"{name}: inputs={spec['n_inputs']} out={spec['out_elements']}")
        return 0

    telemetry = Telemetry(
        TelemetryConfig(
            service_name=args.otel_service,
            enabled=args.otel,
            console_export=args.otel_console,
            jsonl=args.otel_jsonl,
            jsonl_path=args.otel_jsonl_path,
            export_interval_ms=args.otel_interval_ms,
            worker_id=args.id,
        )
    )

    log(args.id, f"neocell SPE v{VERSION} pid={os.getpid()} "
                 f"ls={args.local_store_bytes}B ops={supported_ops()}")
    worker = SpeWorker(
        args.id,
        args.local_store_bytes,
        telemetry=telemetry,
        inline_limit=args.inline_limit,
    )
    try:
        return _run_stdio_loop(worker)
    except proto.ProtocolError as exc:
        log(args.id, f"protocol error: {exc}")
        return 2
    except BrokenPipeError:
        return 0
    except KeyboardInterrupt:
        return 0
    finally:
        # Release any reference the worker still holds, then flush. Order
        # matters: a short-lived worker would otherwise drop the batched
        # spans for its final tasks, and leave Arrow files behind.
        worker.buffers.cleanup()
        telemetry.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())