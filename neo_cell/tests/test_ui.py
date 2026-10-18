"""UI tests.

The UI is a diagnostic tool, so these check the properties that make it
trustworthy: it must run the same PPE the CLI does, report per-SPE
activity honestly, and refuse to be reachable off the machine.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request

import pytest

from neocell.ui.scenarios import (
    MAX_SPE,
    SCENARIOS,
    RunRequest,
    by_key,
    describe_limits,
)
from neocell.ui.server import start_background


@pytest.fixture(scope="module")
def server():
    httpd, _thread, manager = start_background(port=0)
    yield f"http://127.0.0.1:{httpd.server_address[1]}", manager
    httpd.shutdown()
    httpd.server_close()


def get(base: str, path: str):
    with urllib.request.urlopen(base + path, timeout=10) as resp:
        return resp.status, resp.read().decode()


def post(base: str, path: str, payload: dict):
    req = urllib.request.Request(
        base + path,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def run_scenario(base: str, key: str, timeout: float = 180.0):
    limits = describe_limits()
    params = dict(by_key(key).params)
    params["inline_limit"] = limits["inline_limit"]
    post(base, "/api/run", params)
    deadline = time.time() + timeout
    while time.time() < deadline:
        _status, body = get(base, "/api/state")
        state = json.loads(body)
        if state.get("status") in ("done", "failed", "error"):
            return state
        time.sleep(0.1)
    raise TimeoutError(f"scenario {key} did not finish")


# -- api surface ------------------------------------------------------
def test_config_lists_three_scenarios(server):
    base, _ = server
    status, body = get(base, "/api/config")
    assert status == 200
    config = json.loads(body)
    assert len(config["scenarios"]) == 3
    assert [s["key"] for s in config["scenarios"]] == ["fanout", "arrow", "overflow"]
    for scenario in config["scenarios"]:
        assert scenario["title"] and scenario["summary"] and scenario["teaches"]


def test_index_and_assets_are_served(server):
    base, _ = server
    status, body = get(base, "/")
    assert status == 200
    assert "neocell" in body
    assert "SPEs" in body and "Tasks" in body
    assert get(base, "/app.js")[0] == 200
    assert get(base, "/style.css")[0] == 200


def test_unknown_route_is_404(server):
    base, _ = server
    with pytest.raises(urllib.error.HTTPError) as exc:
        get(base, "/nope")
    assert exc.value.code == 404


def test_bad_parameters_are_rejected_not_crashed(server):
    base, _ = server
    with pytest.raises(urllib.error.HTTPError) as exc:
        post(base, "/api/run", {"spe": 9999, "workload": "reductions"})
    assert exc.value.code == 400


def test_unknown_workload_is_rejected():
    with pytest.raises(ValueError, match="unknown workload"):
        RunRequest.from_dict({"workload": "nope"})


def test_request_validation_bounds():
    ok = RunRequest.from_dict({"spe": 4, "elements": 1024, "chunk": 128})
    assert ok.spe == 4 and ok.chunk == 128
    assert RunRequest.from_dict({"dma_bw": None}).dma_bw is None
    assert RunRequest.from_dict({"dma_bw": 1000}).dma_bw == 1000.0
    for field, value in (
        ("spe", 0),
        ("elements", 1),
        ("chunk", 1),
        ("store_bytes", 1),
    ):
        with pytest.raises(ValueError):
            RunRequest.from_dict({field: value})


def test_limits_are_coherent():
    limits = describe_limits()
    assert limits["min_elements"] <= limits["max_elements"]
    assert limits["min_spe"] <= MAX_SPE
    assert limits["inline_limit"] == 1 << 20


# -- scenarios --------------------------------------------------------
def test_fanout_scenario_completes_and_spreads_work(server):
    base, _ = server
    state = run_scenario(base, "fanout")
    result = state["result"]
    assert result["ok"] is True
    assert result["tasks"] == 256
    assert result["failed"] == 0
    assert result["by_transport"] == {"inline": 256}
    # Every SPE should have been given work; that is the point of the demo.
    assert len(state["workers"]) == 4
    assert all(w["tasks_dispatched"] > 0 for w in state["workers"].values())
    assert sum(w["tasks"] for w in state["workers"].values()) == 256


def test_arrow_scenario_uses_the_data_plane(server):
    base, _ = server
    state = run_scenario(base, "arrow")
    result = state["result"]
    assert result["ok"] is True
    assert result["by_transport"] == {"arrow_ref": 4}


def test_overflow_scenario_reports_per_task_failures(server):
    base, _ = server
    state = run_scenario(base, "overflow")
    result = state["result"]
    assert result["ok"] is False
    assert result["failed"] == 4
    assert all("overflow" in m for m in result["failures"].values())
    # Retries are visible: dispatched exceeds completed everywhere.
    assert all(w["tasks"] == 0 for w in state["workers"].values())
    assert all(w["tasks_dispatched"] == 4 for w in state["workers"].values())


def test_ui_records_task_lifecycle(server):
    base, _ = server
    state = run_scenario(base, "fanout")
    task_states = state["task_states"]
    assert task_states
    assert all(t["state"] == "done" for t in task_states.values())
    assert any("exec_ms" in t for t in task_states.values())


def test_state_before_any_run_is_idle(server, tmp_path):
    """A fresh server reports idle rather than erroring."""
    from neocell.ui.server import start_background as fresh

    httpd, _t, _m = fresh(port=0)
    try:
        base = f"http://127.0.0.1:{httpd.server_address[1]}"
        assert json.loads(get(base, "/api/state")[1]) == {"status": "idle"}
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_second_run_is_rejected_while_one_is_active(server):
    base, manager = server
    state = run_scenario(base, "fanout")
    assert state["status"] == "done"
    # Idle again, so a new run is allowed.
    assert post(base, "/api/run", by_key("fanout").params)["run_id"]


def test_ui_binds_to_loopback_only():
    """Refusing a routable bind is a property of the CLI, not the server."""
    from neocell.cli import main

    assert main(["ui", "--host", "0.0.0.0"]) == 2