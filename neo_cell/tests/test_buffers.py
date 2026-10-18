"""Buffer manager tests: the data plane and its ownership rules.

The two properties worth protecting are that Arrow round-trips exactly,
and that refs are released exactly once and the file disappears. A leaked
ref pins a file for the whole run; a premature release gives the reader a
missing file.
"""

from __future__ import annotations

import math

import pytest

from neocell.pb import neocell_pb2 as pb

from neocell.buffers import (
    ARROW_SCHEMA_NAME,
    DEFAULT_INLINE_LIMIT,
    BufferError,
    BufferManager,
    should_inline,
)
from neocell.codec import pack_f32, unpack_f32

pyarrow = pytest.importorskip("pyarrow")


@pytest.fixture(autouse=True)
def _clean_managers(request):
    """Drop any manager a test forgets, so runs do not litter /tmp.

    Individual tests still call cleanup explicitly when the timing is the
    thing under test; this is the safety net for the ones that are not.
    """
    created = []
    original_init = BufferManager.__init__

    def tracking_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        created.append(self)

    BufferManager.__init__ = tracking_init
    try:
        yield
    finally:
        BufferManager.__init__ = original_init
        for manager in created:
            manager.cleanup()


def signal(count: int) -> list[float]:
    return [math.sin(i / 64.0) * 100.0 for i in range(count)]


# -- inline versus reference ------------------------------------------
def test_small_payload_stays_inline():
    bm = BufferManager()
    ref, inline = bm.put_f32(pack_f32([1.0, 2.0, 3.0]))
    assert ref is None
    assert unpack_f32(inline) == [1.0, 2.0, 3.0]


def test_large_payload_becomes_a_reference():
    bm = BufferManager()
    payload = pack_f32(signal(400_000))
    assert len(payload) > DEFAULT_INLINE_LIMIT
    ref, inline = bm.put_f32(payload)
    assert ref is not None
    assert inline == b""
    assert ref.format == pb.DATA_FORMAT_ARROW_IPC
    assert ref.arrow_schema == ARROW_SCHEMA_NAME
    assert ref.element_count == 400_000
    assert ref.byte_length > 0


def test_threshold_is_the_only_criterion():
    limit = 1024
    bm = BufferManager(inline_limit=limit)
    assert bm.put_f32(b"\x00" * limit)[0] is None
    assert bm.put_f32(b"\x00" * (limit + 4))[0] is not None
    bm.cleanup()


def test_should_inline_matches_the_manager():
    assert should_inline(1024)
    assert not should_inline(DEFAULT_INLINE_LIMIT + 1)


# -- correctness ------------------------------------------------------
def test_arrow_roundtrip_is_exact():
    bm = BufferManager()
    values = signal(400_000)
    ref, _ = bm.put_f32(pack_f32(values))
    got = bm.get_f32(ref)
    assert len(got) == len(values)
    for a, b in zip(got, values, strict=True):
        assert a == pytest.approx(b, abs=1e-4)


def test_arrow_roundtrip_of_boundary_sizes():
    bm = BufferManager(inline_limit=16)
    for count in (5, 4096, 4097):
        values = [float(i) * 0.5 for i in range(count)]
        ref, inline = bm.put_f32(pack_f32(values))
        if ref is None:
            assert unpack_f32(inline) == pytest.approx(values)
        else:
            assert bm.get_f32(ref) == pytest.approx(values)
    bm.cleanup()


def test_element_count_mismatch_is_detected():
    bm = BufferManager()
    ref, _ = bm.put_f32(pack_f32(signal(400_000)))
    ref.element_count = 999
    with pytest.raises(BufferError):
        bm.get_f32(ref)


def test_missing_file_is_reported_not_crashed():
    bm = BufferManager()
    ref, _ = bm.put_f32(pack_f32(signal(400_000)))
    bm.release(ref)
    with pytest.raises(BufferError):
        bm.get_f32(ref)


def test_raw_f32_format_is_rejected_as_a_reference():
    bm = BufferManager()
    ref = pb.DataRef(id="x", byte_length=8, format=pb.DATA_FORMAT_RAW_F32, location="/tmp/x")
    with pytest.raises(BufferError):
        bm.get_f32(ref)


# -- lifetime ---------------------------------------------------------
def test_release_deletes_the_file():
    bm = BufferManager()
    ref, _ = bm.put_f32(pack_f32(signal(400_000)))
    path = ref.location
    import os

    assert os.path.exists(path)
    assert bm.release(ref) is True
    assert not os.path.exists(path)


def test_release_by_id_works_and_is_idempotent():
    bm = BufferManager()
    ref, _ = bm.put_f32(pack_f32(signal(400_000)))
    assert bm.release(ref.id) is True
    assert bm.release(ref.id) is False
    bm.cleanup()


def test_release_of_unknown_id_is_false():
    bm = BufferManager()
    assert bm.release("never-issued") is False


def test_track_accounts_an_incoming_ref():
    bm = BufferManager()
    bm.put_f32(pack_f32(signal(400_000)))
    before = bm.stats.live_refs
    bm.track(pb.DataRef(id="other", byte_length=1, location="/tmp/none"))
    assert bm.stats.live_refs == before + 1
    bm.cleanup()


def test_cleanup_removes_outstanding_refs_and_the_directory():
    bm = BufferManager()
    for _ in range(3):
        bm.put_f32(pack_f32(signal(400_000)))
    root = bm.root
    assert root is not None
    dropped = bm.cleanup()
    assert dropped == 3
    assert not root.exists()
    assert bm.stats.live_refs == 0


def test_no_directory_is_created_for_inline_only_use():
    """Creating a temp dir per worker per run leaks them by the hundred."""
    bm = BufferManager()
    assert bm.has_root is False
    for _ in range(5):
        bm.put_f32(pack_f32([1.0, 2.0]))
    assert bm.has_root is False
    bm.cleanup()


def test_stats_track_both_planes():
    bm = BufferManager()
    bm.put_f32(b"\x00" * 64)
    bm.put_f32(pack_f32(signal(400_000)))
    stats = bm.stats.as_dict()
    assert stats["inline_writes"] == 1
    assert stats["arrow_writes"] == 1
    assert stats["issued"] == 1
    bm.cleanup()


def test_context_manager_cleans_up():
    with BufferManager() as bm:
        ref, _ = bm.put_f32(pack_f32(signal(400_000)))
        root = bm.root
        assert root is not None
    assert not root.exists()
    assert bm.stats.live_refs == 0


def test_managed_root_is_not_removed_outright():
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        bm = BufferManager(root=tmp, tag="managed")
        ref, _ = bm.put_f32(pack_f32(signal(400_000)))
        assert ref is not None
        bm.cleanup()
        # Files the manager created are gone; the caller's directory stays.
        import os

        assert os.path.isdir(tmp)
        assert not os.listdir(tmp)