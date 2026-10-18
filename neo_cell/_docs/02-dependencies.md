# Dependencies and Environment

## Runtime

The PPE/SPE job path imports only the standard library plus two data
libraries, both of which are formats rather than frameworks.

| Concern | Implementation | Why |
| --- | --- | --- |
| Process spawn / I/O | `asyncio.create_subprocess_exec`, `os.read` | pipes, no network |
| Control plane format | `protobuf` (generated from `proto/neocell.proto`) | language-neutral, typed, no runtime needed by the wire |
| Bulk data format | `pyarrow` (Arrow IPC **file**) | memory-mappable, Python and Java readers |
| float32 codec | `struct`, `array` | little-endian, host-order independent |
| Timing | `time.perf_counter` | |
| JSON (trace, UI) | `json` | diagnostics only, never the job path |

`pyarrow` is a substantial dependency, but it is the data plane and cannot
be replaced without losing the memory-mapped read. `protobuf` is small.

## Development

| Package | Why | Version used here |
| --- | --- | --- |
| `pytest` | test runner | 9.1.1 |
| `opentelemetry-api` | trace/metric API and W3C propagation | 1.45.1 |
| `opentelemetry-sdk` | tracers, meters, exporters | 1.45.1 |

No linter is installed. `pyproject.toml` carries a `[tool.ruff]` section so
`ruff check .` works the moment it is available; until then the suite is
the gate.

## Telemetry (optional, but on by default)

OpenTelemetry is an optional extra so the library imports without it, yet
`telemetry.py` enables it by default (ADR-0016). Every call site degrades
to a no-op with the same signature when the SDK is absent, so a worker
without the SDK still runs.

```bash
python3 -m pip install -e .              # core: protobuf + pyarrow
python3 -m pip install -e '.[telemetry]' # + OpenTelemetry, which is on by default
python3 -m pip install -e '.[dev]'       # + pytest
```

There is no collector exporter and no network sink. Telemetry goes to
stderr or to local JSONL files, which is consistent with the
single-machine decision (ADR-0016).

## Regenerating the protobuf bindings

`protoc` writes `src/neocell/pb/neocell_pb2.py` from `proto/neocell.proto`.
The generated file is committed so a checkout builds without `protoc`:

```bash
protoc --proto_path=proto --python_out=src/neocell/pb proto/neocell.proto
```

Verified with `protoc` 3.19.6 against the `protobuf` 7.36 runtime. The
schema deliberately imports no well-known types, so a bare `protoc`
install is enough.

Tests that need the OpenTelemetry SDK skip themselves when it is absent,
so the suite passes either way.

## Interpreter

| Requirement | Value |
| --- | --- |
| Python | **>= 3.12** (developed and tested on 3.14.6) |
| GIL | assumed present; the architecture does not rely on free-threading |
| Platform | Linux/macOS, x86-64; process model is POSIX-oriented |

### Why 3.12

The code uses PEP 695 generics (`type` aliases are avoided in favour of
plain annotations), `slots=True` dataclasses, and `asyncio.TaskGroup`-free
explicit scheduling. 3.12 is the floor where all of that is comfortable
and where `tomllib`-era tooling assumptions hold.

Note that the plan document mentions Python 3.15.0 as current; the
interpreter actually available on this machine is 3.14.6, so that is what
everything is verified against.

### Not required: free-threading

The plan correctly separates an execution option from the architecture.
The PPE/SPE split, explicit data movement and the replaceable worker
interface are the architecture; whether Python runs free-threaded is an
implementation detail. The GIL build is the tested default. If a
free-threaded interpreter is tried later, it is a benchmark, not a
redesign.

## Java

Not used in version 0.1, and not installed as a project dependency.

| Tool | Role in the next increment | Present here |
| --- | --- | --- |
| JDK 21+ | compile and run the Java SPE | 26.0.1 available |
| `javac` | build `java/NeocellSpe.java` | available |
| JSON library | header parsing | **must be added**, see below |

A Java SPE needs no third-party library beyond a JSON parser, and even that
can be avoided: the header grammar is flat and small, so a minimal parser
or a one-file dependency such as Gson would suffice. Keep it to one.

## Reproducing this environment

```bash
git clone <repo> ne_cell && cd ne_cell
python3 -m pytest                       # 45 tests, no install needed
PYTHONPATH=src python3 -m neocell.cli run --spe 4 --workload reductions
```

Optional install for the `neocell` console script:

```bash
python3 -m pip install -e .            # adds the neocell command
neocell run --spe 4 --workload axpy --elements 32768
```

`pyproject.toml` sets `pythonpath = ["src"]` for pytest, so the test suite
runs from a clean checkout with no `PYTHONPATH` and no install.

## Hardware assumptions

None. The number of SPEs is configuration (`--spe`, default 4) and is
independent of core count. A run with more SPEs than cores is valid and
simply oversubscribes; it is a demonstration of the scheduler, not a
throughput claim.