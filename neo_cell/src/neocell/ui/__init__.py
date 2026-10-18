"""Local test UI: a live console for watching the PPE coordinate SPEs.

Not part of the compute architecture. The PPE/SPE boundary is process
pipes; nothing in the job path knows this package exists.
"""

from neocell.ui.runner import RunManager, RunState
from neocell.ui.scenarios import SCENARIOS, RunRequest, Scenario, by_key

__all__ = ["RunManager", "RunRequest", "RunState", "SCENARIOS", "Scenario", "by_key"]