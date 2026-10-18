"""Framing tests.

The control plane is length-prefixed protobuf, so these cover the framing
itself (fragmentation, limits, direction) rather than message contents,
which protobuf's own test suite owns.
"""

from __future__ import annotations

import pytest

from neocell import protocol as proto
from neocell.pb import neocell_pb2 as pb


def roundtrip(frame: proto.Frame) -> proto.Frame:
    decoder = proto.FrameDecoder()
    decoder.feed(proto.encode(frame))
    decoded = list(decoder)
    assert len(decoded) == 1
    return decoded[0]


def test_hello_roundtrip():
    original = proto.hello_frame("SPE0", 131072)
    decoded = roundtrip(original)
    assert decoded.type == proto.HELLO
    assert decoded.envelope.hello.worker_id == "SPE0"
    assert decoded.envelope.hello.local_store_bytes == 131072


def test_hello_ack_roundtrip():
    frame = proto.hello_ack_frame("SPE0", 4242, 65536, ["sum", "scale"])
    decoded = roundtrip(frame)
    assert decoded.type == proto.HELLO_ACK
    assert decoded.envelope.hello_ack.pid == 4242
    assert list(decoded.envelope.hello_ack.ops) == ["sum", "scale"]


def test_binary_payload_survives_exact_bytes():
    payload = bytes(range(256)) * 4
    original = proto.exec_frame("t1", "sum", None, [len(payload)], payload)
    decoded = roundtrip(original)
    assert decoded.payload == payload
    assert decoded.envelope.exec.task_id == "t1"


def test_partial_and_split_feeds_decode_identically():
    frame = proto.exec_frame("t2", "sum", None, [4], b"\x01\x02\x03\x04")
    blob = proto.encode(frame)
    decoder = proto.FrameDecoder()
    for i in range(0, len(blob) - 1):
        decoder.feed(blob[i : i + 1])
        assert list(decoder) == []
    decoder.feed(blob[-1:])
    frames = list(decoder)
    assert len(frames) == 1
    assert frames[0].payload == b"\x01\x02\x03\x04"


def test_two_frames_in_one_feed():
    a = proto.cancel_frame("x")
    b = proto.shutdown_frame()
    decoder = proto.FrameDecoder()
    decoder.feed(proto.encode(a) + proto.encode(b))
    assert [f.type for f in decoder] == [proto.CANCEL, proto.SHUTDOWN]


def test_empty_payload_roundtrip():
    decoded = roundtrip(proto.shutdown_frame())
    assert decoded.payload == b""


def test_zero_length_message_is_rejected():
    """An envelope with no oneof set is not a valid message."""
    with pytest.raises(proto.ProtocolError):
        proto.decode(pb.Envelope().SerializeToString())


def test_garbage_is_rejected():
    with pytest.raises(proto.ProtocolError):
        proto.decode(b"\xff\xff\xff\xff\xff\xff")


def test_oversized_length_is_rejected():
    import struct

    decoder = proto.FrameDecoder()
    decoder.feed(struct.pack("<I", proto.MAX_MESSAGE_BYTES + 1))
    with pytest.raises(proto.ProtocolError):
        list(decoder)


def test_direction_validation():
    with pytest.raises(proto.ProtocolError):
        proto.validate_ppe_frame(proto.result_frame("t", "ok", "SPE0"))
    with pytest.raises(proto.ProtocolError):
        proto.validate_worker_frame(proto.exec_frame("t", "sum", None, [4], b"\0" * 4))


def test_result_frame_carries_stats_and_payload():
    frame = proto.result_frame(
        "t9", "ok", "SPE2", b"\x01\x02\x03\x04", stats={"exec_s": 0.5, "store_bytes": 1024}
    )
    decoded = roundtrip(frame)
    result = decoded.envelope.result
    assert result.output_bytes == 4
    assert result.status == "ok"
    assert decoded.payload == b"\x01\x02\x03\x04"
    assert result.stats.exec_s == pytest.approx(0.5)
    assert result.stats.store_bytes == 1024


def test_error_and_cancel_frames():
    err = roundtrip(proto.error_frame("t", "SPE0", "boom"))
    assert err.envelope.error.message == "boom"
    assert err.type == proto.ERROR
    cancel = roundtrip(proto.cancel_frame("t"))
    assert cancel.envelope.cancel.task_id == "t"
    assert cancel.type == proto.CANCEL


def test_params_roundtrip_is_typed():
    params = pb.OpParams(factor=2.5, a=3.0, b=-1.0, rounds=7)
    frame = proto.exec_frame("t", "axpy", params, [4], b"\0" * 4)
    decoded = roundtrip(frame)
    got = decoded.envelope.exec.params
    assert got.factor == pytest.approx(2.5)
    assert got.a == pytest.approx(3.0)
    assert got.rounds == 7


def test_data_refs_survive_the_wire():
    ref = pb.DataRef(
        id="ppe-x",
        byte_length=4096,
        format=pb.DATA_FORMAT_ARROW_IPC,
        location="/tmp/x.arrow",
        element_count=1024,
        arrow_schema="neocell.f32",
    )
    frame = proto.exec_frame("t", "sum", None, [4096], input_refs=[ref])
    decoded = roundtrip(frame)
    refs = list(decoded.envelope.exec.input_refs)
    assert len(refs) == 1
    assert refs[0].id == "ppe-x"
    assert refs[0].format == pb.DATA_FORMAT_ARROW_IPC
    assert refs[0].element_count == 1024


def test_exec_without_refs_or_inline_still_encodes():
    frame = proto.exec_frame("t", "sum", None, [0], b"")
    assert roundtrip(frame).type == proto.EXEC