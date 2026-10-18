"""neo_cell: a Cell-style PPE/SPE compute model.

The package is split so that the PPE and the SPEs only ever meet at
``neocell.protocol``: the PPE is written once, and a worker may be
implemented in any language that can speak the framing in that module.
"""

from neocell.version import PROTOCOL_VERSION, VERSION

__all__ = ["PROTOCOL_VERSION", "VERSION"]