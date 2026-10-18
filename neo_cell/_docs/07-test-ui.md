# Test UI

A local console for watching the PPE coordinate its SPEs, aimed at
troubleshooting and at explaining the architecture to someone who has not
read the code.

```bash
neocell ui                       # http://127.0.0.1:8765/
neocell ui --port 9000
```

## What it shows

| Panel | Contents |
| --- | --- |
| Run | three demo scenarios plus editable parameters |
| SPEs | one card per worker: idle/busy, current task, tasks done vs dispatched, bytes in and out, store size, pid |
| Tasks | per-task index, worker, state, queue ms, exec ms, bytes |
| Result | pass or fail, transport used, wall time, store peak, output preview, failure reasons |
| Event log | the raw trace events as they arrive |

The "tasks `done/dispatched`" split matters on the failure scenarios: a
worker that dispatched four tasks and completed none shows `0/4`, which is
the difference between "was never given work" and "was given work and
failed it".

## Three demo scenarios

| Key | Configuration | Demonstrates |
| --- | --- | --- |
| `fanout` | 256 chunks of 4 KiB over 4 SPEs | Central coordination: the PPE owns the queue, the SPEs only compute |
| `arrow` | 4 chunks of ~1.05 MB | Explicit data movement: the message carries a `DataRef`, the payload travels beside it |
| `overflow` | chunks 16x the local store | Bounded private storage: an SPE cannot hold the input, and the PPE reports per-task failures |

Each scenario is a plain parameter set in
`neocell/ui/scenarios.py`, so you can tweak one and compare.

The form shows derived facts as you type: how many tasks the job will
produce, how big a chunk is, which plane it will use, and a warning when
the chunk is larger than the local store, since that run is guaranteed to
fail on every task.

## Architecture

The UI is a diagnostic, not part of the compute path.

- The PPE/SPE boundary remains process pipes with protobuf messages.
  Nothing in the job path imports the UI.
- It binds to `127.0.0.1` only, and `neocell ui --host 0.0.0.0` is refused.
  The listen socket exists only so a browser can render a page.
- It is stdlib `http.server` with server-sent events. No web framework.

## How it observes the run

Rather than adding a second instrumentation path, the UI drives the
existing `Trace` sink. Every event it shows is an event the CLI also
writes. If a trace event ever stops firing, the UI goes quiet too, so the
UI cannot drift away from what really happened.

A run gets its own thread and event loop, because the HTTP server is
threaded and a job must not block the server watching it. The run also
publishes a live worker-occupancy snapshot every 100 ms so the UI can
animate the cards between trace events.

## Endpoints

| Route | Purpose |
| --- | --- |
| `GET /` | the page |
| `GET /api/config` | scenarios, version, limits |
| `GET /api/state` | full snapshot of the current run |
| `GET /api/stream` | server-sent events: trace records, worker occupancy, completion |
| `POST /api/run` | start a run from parameters |
| `POST /api/stop` | request a stop |

Parameters are validated server-side and clamped to sane bounds, so a
mistyped value produces a 400 with a reason rather than a runaway job.

## Limits

The UI is a test tool with a test-sized scope. It holds the current run's
task states in memory, and history is not kept across runs. Long-running
or many-concurrent-run use belongs in the JSONL telemetry files, which are
written regardless of whether the UI is open.

See [06-telemetry.md](06-telemetry.md) for reading those files directly.