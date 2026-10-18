"""Wire format: length-prefixed Protocol Buffers over a local pipe.

Transport is deliberately minimal. An SPE is a child process, so messages
travel over its stdin/stdout. There is no network stack, no HTTP and no
RPC framework: this is a data format, which is all a Java or Rust worker
needs in order to take part.

Framing is unchanged from version 0.1 and stays independent of the payload
encoding::

    +-----------------+--------------------------+
    | u32 message_len | serialized Envelope     |
    +-----------------+--------------------------+

Two planes:

* **Control plane** -- every ``Envelope``. Small data payloads ride inline
  in the ``bytes`` fields.
* **Data plane** -- anything over the inline limit (1 MiB by default)
  travels as an Arrow IPC file, referenced by ``DataRef``. The message
  carries the reference; the bytes travel beside it.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Any, Iterator

from neocell.pb import neocell_pb2 as pb
from neocell.version import PROTOCOL_VERSION

_LEN = struct.Struct("<I")
MAX_MESSAGE_BYTES = 1 << 30

# Envelope oneof field names, as a stable string API for dispatch and logs.
HELLO = "hello"
HELLO_ACK = "hello_ack"
EXEC = "exec"
RESULT = "result"
CANCEL = "cancel"
CANCELLED = "cancelled"
SHUTDOWN = "shutdown"
BYE = "bye"
ERROR = "error"

PPE_TO_SPE = frozenset({HELLO, EXEC, CANCEL, SHUTDOWN})
SPE_TO_PPE = frozenset({HELLO_ACK, RESULT, ERROR, CANCELLED, BYE})

STATUS_OK = "ok"
STATUS_ERROR = "error"


class ProtocolError(Exception):
    """Raised on a malformed message or an illegal direction."""


@dataclass(slots=True)
class Frame:
    """A decoded frame: one Envelope plus its direction-checked kind."""

    envelope: pb.Envelope

    @property
    def type(self) -> str:
        return self.envelope.WhichOneof("body") or ""

    @property
    def payload(self) -> bytes:
        """Bytes of the inline data field, whichever message this is."""
        body = self.type
        if body == EXEC:
            return self.envelope.exec.inline_payload
        if body == RESULT:
            return self.envelope.result.inline_payload
        return b""

    def body(self) -> Any:
        """The typed message inside the oneof."""
        return getattr(self.envelope, self.type)


def decode(data: bytes) -> pb.Envelope:
    envelope = pb.Envelope()
    try:
        envelope.ParseFromString(data)
    except Exception as exc:
        raise ProtocolError(f"undecodable message: {exc}") from exc
    if envelope.WhichOneof("body") is None:
        raise ProtocolError("envelope carries no message body")
    return envelope


def encode(frame: Frame) -> bytes:
    """Serialise a frame to its length-prefixed wire form."""
    blob = frame.envelope.SerializeToString()
    if len(blob) > MAX_MESSAGE_BYTES:
        raise ProtocolError(f"message too large: {len(blob)} bytes")
    return _LEN.pack(len(blob)) + blob


class FrameDecoder:
    """Incremental decoder for a stream of concatenated frames.

    Bytes are consumed only once a whole message is present, so a short
    read never loses data and any fragmentation decodes identically.
    """

    __slots__ = ("_buf", "_pos")

    def __init__(self) -> None:
        self._buf = bytearray()
        self._pos = 0

    def feed(self, chunk: bytes) -> None:
        self._buf += chunk

    def __iter__(self) -> Iterator[Frame]:
        while True:
            frame = self._try_decode_one()
            if frame is None:
                return
            yield frame

    def _compact(self) -> None:
        if self._pos and self._pos == len(self._buf):
            self._buf.clear()
            self._pos = 0
        elif self._pos > (1 << 16):
            del self._buf[: self._pos]
            self._pos = 0

    def _try_decode_one(self) -> Frame | None:
        buf, base = self._buf, self._pos
        if len(buf) - base < _LEN.size:
            return None
        (msg_len,) = _LEN.unpack_from(buf, base)
        if msg_len > MAX_MESSAGE_BYTES:
            raise ProtocolError(f"message length {msg_len} out of range")
        start = base + _LEN.size
        end = start + msg_len
        if len(buf) < end:
            return None
        blob = bytes(buf[start:end])
        self._pos = end
        self._compact()
        return Frame(decode(blob))


# -- direction -------------------------------------------------------
def validate_ppe_frame(frame: Frame) -> None:
    """Reject a message the SPE has no handler for."""
    if frame.type not in PPE_TO_SPE:
        raise ProtocolError(f"'{frame.type}' is not valid on the SPE side")


def validate_worker_frame(frame: Frame) -> None:
    """Reject a message the PPE has no handler for."""
    if frame.type not in SPE_TO_PPE:
        raise ProtocolError(f"'{frame.type}' is not valid on the PPE side")


# -- constructors ----------------------------------------------------
def hello_frame(worker_id: str, local_store_bytes: int) -> Frame:
    envelope = pb.Envelope()
    envelope.hello.CopyFrom(
        pb.Hello(
            protocol=PROTOCOL_VERSION,
            worker_id=worker_id,
            local_store_bytes=local_store_bytes,
        )
    )
    return Frame(envelope)


def hello_ack_frame(
    worker_id: str, pid: int, local_store_bytes: int, ops: list[str]
) -> Frame:
    envelope = pb.Envelope()
    envelope.hello_ack.CopyFrom(
        pb.HelloAck(
            protocol=PROTOCOL_VERSION,
            worker_id=worker_id,
            pid=pid,
            local_store_bytes=local_store_bytes,
            ops=ops,
        )
    )
    return Frame(envelope)


def exec_frame(
    task_id: str,
    op: str,
    params: pb.OpParams | None,
    input_lengths: list[int],
    inline_payload: bytes = b"",
    input_refs: list[pb.DataRef] | None = None,
    trace: dict[str, str] | None = None,
    task_index: int = 0,
    label: str = "",
) -> Frame:
    """Build an ``exec``.

    Either ``inline_payload`` carries the data or ``input_refs`` points at
    it; never both. ``input_lengths`` still describes the logical buffers
    so the worker can split either representation identically.
    """
    envelope = pb.Envelope()
    exec_msg = envelope.exec
    exec_msg.task_id = task_id
    exec_msg.op = op
    if params is not None:
        exec_msg.params.CopyFrom(params)
    exec_msg.input_lengths.extend(input_lengths)
    exec_msg.input_bytes = len(inline_payload) or sum(r.byte_length for r in (input_refs or []))
    if input_refs:
        exec_msg.input_refs.extend(input_refs)
    exec_msg.inline_payload = inline_payload
    if trace:
        exec_msg.trace.update(trace)
    exec_msg.task_index = task_index
    exec_msg.label = label
    return Frame(envelope)


def result_frame(
    task_id: str,
    status: str,
    worker_id: str,
    inline_payload: bytes = b"",
    output_ref: pb.DataRef | None = None,
    stats: dict[str, float | int] | None = None,
    error: str | None = None,
) -> Frame:
    envelope = pb.Envelope()
    result = envelope.result
    result.task_id = task_id
    result.status = status
    result.worker_id = worker_id
    result.inline_payload = inline_payload
    result.output_bytes = len(inline_payload) or (output_ref.byte_length if output_ref else 0)
    if output_ref is not None:
        result.output_ref.CopyFrom(output_ref)
    if stats:
        result.stats.CopyFrom(
            pb.ExecStats(
                stage_s=float(stats.get("stage_s", 0.0)),
                exec_s=float(stats.get("exec_s", 0.0)),
                store_high_water=int(stats.get("store_high_water", 0)),
                store_bytes=int(stats.get("store_bytes", 0)),
                tasks_done=int(stats.get("tasks_done", 0)),
            )
        )
    if error:
        result.error = error
    return Frame(envelope)


def error_frame(task_id: str, worker_id: str, message: str) -> Frame:
    envelope = pb.Envelope()
    envelope.error.CopyFrom(
        pb.ErrorFrame(task_id=task_id, worker_id=worker_id, message=message)
    )
    return Frame(envelope)


def cancel_frame(task_id: str) -> Frame:
    envelope = pb.Envelope()
    envelope.cancel.CopyFrom(pb.Cancel(task_id=task_id))
    return Frame(envelope)


def cancelled_frame(task_id: str, worker_id: str) -> Frame:
    envelope = pb.Envelope()
    envelope.cancelled.CopyFrom(pb.Cancelled(task_id=task_id, worker_id=worker_id))
    return Frame(envelope)


def shutdown_frame() -> Frame:
    envelope = pb.Envelope()
    envelope.shutdown.CopyFrom(pb.Shutdown())
    return Frame(envelope)


def bye_frame(worker_id: str) -> Frame:
    envelope = pb.Envelope()
    envelope.bye.CopyFrom(pb.Bye(worker_id=worker_id))
    return Frame(envelope)