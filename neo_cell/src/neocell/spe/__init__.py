"""SPE side: bounded local store, data-parallel ops, and the worker loop."""

from neocell.spe.local_store import LocalStore, LocalStoreOverflow
from neocell.spe.ops import (
    describe,
    registry,
    required_store_bytes,
    run_op,
    supported_ops,
)

__all__ = [
    "LocalStore",
    "LocalStoreOverflow",
    "describe",
    "registry",
    "required_store_bytes",
    "run_op",
    "supported_ops",
]