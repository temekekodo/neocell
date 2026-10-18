"""Local store and op tests: the SPE's own rules, without a process."""

from __future__ import annotations

import pytest

from neocell.codec import pack_f32, unpack_f32, unpack_f32_scalar
from neocell.spe.local_store import LocalStore, LocalStoreOverflow
from neocell.spe.ops import describe, registry, required_store_bytes, run_op


def test_capacity_is_fixed():
    ls = LocalStore(64)
    assert ls.capacity == 64
    assert ls.free == 64
    ls.stage(b"x" * 64)
    assert ls.free == 0
    with pytest.raises(LocalStoreOverflow):
        ls.stage(b"x")


def test_stage_copies_rather_than_aliases():
    ls = LocalStore(16)
    src = bytearray(b"abcdefgh")
    view = ls.stage(src)
    src[0:1] = b"Z"
    assert bytes(view) == b"abcdefgh"


def test_high_water_tracks_peak():
    ls = LocalStore(128)
    ls.stage(b"a" * 64)
    ls.stage(b"b" * 32)
    assert ls.used == 96
    ls.reset()
    assert ls.used == 0
    assert ls.high_water == 96


def test_run_sum_matches_reference():
    ls = LocalStore(1024)
    values = [1.5, -2.0, 3.25, 0.0]
    buf = ls.stage(pack_f32(values))
    payload, elapsed = run_op(ls, "sum", [buf], {})
    assert unpack_f32_scalar(payload) == pytest.approx(sum(values))
    assert elapsed >= 0.0


def test_run_scale_matches_reference():
    ls = LocalStore(1024)
    values = [1.0, 2.0, -3.0]
    buf = ls.stage(pack_f32(values))
    payload, _ = run_op(ls, "scale", [buf], {"factor": 2.5})
    assert unpack_f32(payload) == pytest.approx([2.5, 5.0, -7.5])


def test_run_axpy_uses_two_staged_buffers():
    ls = LocalStore(1024)
    x = ls.stage(pack_f32([1.0, 2.0, 3.0]))
    y = ls.stage(pack_f32([10.0, 20.0, 30.0]))
    payload, _ = run_op(ls, "axpy", [x, y], {"a": 2.0, "b": -1.0})
    assert unpack_f32(payload) == pytest.approx([-8.0, -16.0, -24.0])


def test_unknown_op_raises():
    ls = LocalStore(64)
    with pytest.raises(KeyError):
        run_op(ls, "nope", [], {})


def test_registry_metadata_is_coherent():
    for name, spec in registry().items():
        assert spec["n_inputs"] >= 1, name
        assert spec["out_elements"] in ("same", "scalar") or isinstance(
            spec["out_elements"], int
        )
        assert describe(name)["fn"] is spec["fn"]


def test_required_store_bytes_accounts_for_output():
    # one 16-byte input, scalar output: 16 + 4
    assert required_store_bytes("sum", [16], {}) == 20
    assert required_store_bytes("scale", [16], {}) == 32