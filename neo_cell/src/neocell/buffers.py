"""Buffer manager: the data plane, and the ownership rules that go with it.

The control message carries a :class:`DataRef`; the bytes live in an Arrow
IPC file that both processes can reach, because they are on the same
machine. Small payloads skip all of this and ride inline in the message.

Two design points are deliberate:

* **Arrow IPC *file*, not stream.** The file format is random-access and
  supports ``pa.memory_map``, so the receiver maps the bytes instead of
  copying them. The stream format cannot be memory-mapped.
* **NeoCell owns lifetime, Arrow owns representation.** This module issues
  ids, tracks size and release, and deletes the file once every ref is
  released. Arrow never decides ownership or scheduling.

A DataRef is issued only after the bytes are durable, so a receiver that
resolves an id can never race the writer.
"""

from __future__ import annotations

import os
import shutil
import tempfile
import uuid
from array import array
from dataclasses import dataclass, field
from pathlib import Path

from neocell.pb import neocell_pb2 as pb

# Below this, data is small enough that a reference plus a file open costs
# more than an inline copy.
DEFAULT_INLINE_LIMIT = 1 << 20  # 1 MiB

ARROW_SCHEMA_NAME = "neocell.f32"
_ARROW_FIELD = "values"


class BufferError(Exception):
    """Raised for an unknown, released, or malformed data reference."""


@dataclass(slots=True)
class BufferStats:
    """Ownership accounting, so 'who holds what' is answerable."""

    issued: int = 0
    released: int = 0
    inline_writes: int = 0
    arrow_writes: int = 0
    inline_bytes: int = 0
    arrow_bytes: int = 0

    @property
    def live_refs(self) -> int:
        return self.issued - self.released

    def as_dict(self) -> dict[str, int]:
        return {
            "issued": self.issued,
            "released": self.released,
            "live_refs": self.live_refs,
            "inline_writes": self.inline_writes,
            "arrow_writes": self.arrow_writes,
            "inline_bytes": self.inline_bytes,
            "arrow_bytes": self.arrow_bytes,
        }


@dataclass(slots=True)
class _Entry:
    """Server-side record for one issued ref."""

    ref_id: str
    path: Path
    byte_length: int
    element_count: int
    released: bool = False
    readers: int = 0


class BufferManager:
    """Issues data references, maps them back, and cleans up after itself.

    One manager lives in the PPE and one in each SPE. They do not talk to
    each other; they only agree on the id and the local path, which is
    sufficient because they share a filesystem.
    """

    def __init__(
        self,
        root: str | os.PathLike[str] | None = None,
        inline_limit: int = DEFAULT_INLINE_LIMIT,
        tag: str = "neocell",
    ) -> None:
        self._owns_root = root is None
        self._root = Path(root) if root is not None else None
        self.inline_limit = inline_limit
        self.tag = tag
        self.stats = BufferStats()
        self._entries: dict[str, _Entry] = {}

    @property
    def root(self) -> Path | None:
        """Directory holding Arrow files, created only when first needed.

        Created lazily on purpose: an all-inline run never needs it, and
        creating a temp directory per worker per run leaks them quickly.
        """
        if self._root is None:
            self._root = Path(tempfile.mkdtemp(prefix="neocell-buf-"))
        return self._root

    @property
    def has_root(self) -> bool:
        return self._root is not None

    # -- writing -------------------------------------------------------
    def put_f32(
        self, payload: bytes | bytearray | memoryview | array, owner: str = "ppe"
    ) -> tuple[pb.DataRef | None, bytes]:
        """Publish float32 data and return ``(ref, inline_bytes)``.

        Exactly one of the two carries the data. The caller sends
        ``inline_bytes`` when ``ref`` is ``None``, and ships the file when
        it is not. The choice is made by size alone so it is predictable
        and identical on both sides.
        """
        nbytes = len(payload)
        element_count = nbytes // 4
        if nbytes <= self.inline_limit:
            self.stats.inline_writes += 1
            self.stats.inline_bytes += nbytes
            return None, bytes(payload)

        ref_id = f"{self.tag}-{owner}-{uuid.uuid4().hex[:12]}"
        path = self.root / f"{ref_id}.arrow"
        self._write_arrow(path, payload, element_count)
        encoded = path.stat().st_size
        entry = _Entry(
            ref_id=ref_id,
            path=path,
            byte_length=encoded,
            element_count=element_count,
        )
        self._entries[ref_id] = entry
        self.stats.issued += 1
        self.stats.arrow_writes += 1
        self.stats.arrow_bytes += encoded
        return (
            pb.DataRef(
                id=ref_id,
                byte_length=encoded,
                format=pb.DATA_FORMAT_ARROW_IPC,
                location=str(path),
                element_count=element_count,
                arrow_schema=ARROW_SCHEMA_NAME,
            ),
            b"",
        )

    def _write_arrow(
        self, path: Path, payload: bytes | bytearray | memoryview | array, element_count: int
    ) -> None:
        """Write one float32 column as an Arrow IPC file.

        Written to a temporary name and renamed, so the file is complete
        before any reference to it exists.
        """
        import pyarrow as pa
        from pyarrow import ipc

        if isinstance(payload, array):
            values = payload.tolist()
        else:
            raw = bytes(payload)
            values = _unpack_f32(raw)

        table = pa.table({_ARROW_FIELD: pa.array(values, type=pa.float32())})
        tmp = path.with_suffix(".arrow.tmp")
        with pa.OSFile(str(tmp), "wb") as sink:
            with ipc.new_file(sink, table.schema) as writer:
                writer.write_table(table)
        os.replace(tmp, path)

    # -- reading -------------------------------------------------------
    def get_f32(self, ref: pb.DataRef) -> list[float]:
        """Resolve a ref to float32 values.

        Prefers a memory map so the kernel pages the file in lazily rather
        than reading it, which is the whole point of the Arrow file
        format over a stream.
        """
        if ref.format == pb.DATA_FORMAT_ARROW_IPC:
            return self._read_arrow(Path(ref.location), ref.element_count)
        raise BufferError(f"unsupported data format {ref.format}")

    def _read_arrow(self, path: Path, element_count: int) -> list[float]:
        import pyarrow as pa
        from pyarrow import ipc

        if not path.exists():
            raise BufferError(f"referenced file is gone: {path}")
        try:
            source = pa.memory_map(str(path), "r")
        except pa.ArrowInvalid as exc:
            raise BufferError(f"cannot map {path}: {exc}") from exc
        try:
            reader = ipc.open_file(source)
            batch = reader.get_batch(0)
            column = batch.column(0)
        except pa.ArrowInvalid as exc:
            raise BufferError(f"malformed Arrow file {path}: {exc}") from exc
        finally:
            # The mapping must outlive the values we are about to hand
            # out, so we materialise them before releasing it.
            values = column.to_pylist()
        if element_count and len(values) != element_count:
            raise BufferError(
                f"{path}: expected {element_count} elements, found {len(values)}"
            )
        return values

    # -- lifetime ------------------------------------------------------
    def track(self, ref: pb.DataRef) -> None:
        """Record an incoming ref so this process can account for it."""
        self.stats.issued += 1
        self._entries.setdefault(
            ref.id,
            _Entry(
                ref_id=ref.id,
                path=Path(ref.location),
                byte_length=ref.byte_length,
                element_count=ref.element_count,
            ),
        )

    def release(self, ref_or_id: pb.DataRef | str) -> bool:
        """Release a ref, deleting the file once nothing else holds it.

        Safe to call twice, and safe to call for an id this process never
        issued: the PPE releases after the result is collected, the SPE
        after the op returns, and a crash can leave either side holding
        entries the other has forgotten.
        """
        ref_id = ref_or_id if isinstance(ref_or_id, str) else ref_or_id.id
        entry = self._entries.get(ref_id)
        if entry is None:
            return False
        if entry.released:
            return True
        entry.readers -= 1
        if entry.readers > 0:
            return True
        entry.released = True
        self.stats.released += 1
        self._unlink(entry)
        self._entries.pop(ref_id, None)
        return True

    @staticmethod
    def _unlink(entry: _Entry) -> None:
        try:
            entry.path.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            pass

    def cleanup(self) -> int:
        """Release everything outstanding. Returns how many were dropped."""
        pending = list(self._entries)
        for ref_id in pending:
            self.release(ref_id)
        self._entries.clear()
        if self._root is not None:
            if self._owns_root:
                # We made it, so we take it away with us.
                shutil.rmtree(self._root, ignore_errors=True)
                self._root = None
            # A caller-supplied directory is theirs; we delete only the
            # files we put in it and leave the directory itself alone.
        return len(pending)

    def __enter__(self) -> BufferManager:
        return self

    def __exit__(self, *exc: object) -> None:
        self.cleanup()

    def __repr__(self) -> str:
        return (
            f"BufferManager(root={self._root}, "
            f"inline_limit={self.inline_limit}, live={self.stats.live_refs})"
        )


def _unpack_f32(raw: bytes) -> list[float]:
    from neocell.codec import unpack_f32

    return unpack_f32(raw)


def should_inline(byte_length: int, inline_limit: int = DEFAULT_INLINE_LIMIT) -> bool:
    """Whether a payload of this size travels inside the control message."""
    return byte_length <= inline_limit