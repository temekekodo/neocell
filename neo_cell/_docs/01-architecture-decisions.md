# Architecture Decision Records

Why the system looks the way it does. Each record states the context, the
decision, and what it costs us. New decisions append to the bottom; older
records are edited in place only when the decision itself is reversed, and
reversal is recorded rather than erased.

- [ADR-0001 — Cell-style PPE/SPE split on x86-64](#adr-0001)
- [ADR-0002 — SPEs are OS processes from day one](#adr-0002)
- [ADR-0003 — Length-prefixed JSON header plus binary payload](#adr-0003)
- [ADR-0004 — The PPE depends on an adapter, never on worker code](#adr-0004)
- [ADR-0005 — asyncio in the PPE, blocking loops in the SPE](#adr-0005)
- [ADR-0006 — Bounded local store with explicit staging](#adr-0006)
- [ADR-0007 — stdlib float32 codec, no numpy](#adr-0007)
- [ADR-0008 — Transfers are counted first, charged later](#adr-0008)
- [ADR-0009 — Per-task metrics and a JSONL trace](#adr-0009)
- [ADR-0010 — Tests run async without pytest-asyncio](#adr-0010)
- [ADR-0011 — Where this deliberately departs from real Cell](#adr-0011)
- [ADR-0012 — Java SPE: the path we are holding open](#adr-0012)
- [ADR-0013 — OpenTelemetry for cross-process observability](#adr-0013)
- [ADR-0014 — Local-only transport: protobuf as a data format, not a service](#adr-0014)
- [ADR-0015 — Data plane: Arrow IPC file plus a buffer manager owning lifetime](#adr-0015)
- [ADR-0016 — Telemetry on by default, exported to local files](#adr-0016)

---

## ADR-0001 — Cell-style PPE/SPE split on x86-64

**Context.** The goal is to make the *organising ideas* of the Cell
Broadband Engine visible: one controller dividing work, specialised
workers with private scratch memory, and data movement that is explicit
and chargeable. Nothing requires reproducing instructions, timings, or
cache behaviour, and running on real hardware is impossible here.

**Decision.** Model one PPE and a configurable pool of SPEs (4 by
default) as ordinary software on x86-64. The PPE coordinates; the SPEs
execute bounded tasks against their own local buffers.

**Consequences.** The architecture is testable and inspectable, and the
demo runs on a laptop. We give up any claim about absolute performance.
The Cell's eight SPEs were hardware threads sharing one memory system;
here the pool size is a configuration knob, and a run on a 4-core box
with 4 SPEs is process-count-bound rather than core-bound.

## ADR-0002 — SPEs are OS processes from day one

**Context.** The plan calls for workers that can later be replaced by a
Java implementation. Threads inside one Python process would share the
GIL, share one heap, and make the Java port a rewrite rather than a
substitution.

**Decision.** Each SPE is a child process speaking a length-prefixed
protocol over stdin/stdout. Logs go to stderr so they cannot corrupt the
protocol stream.

**Consequences.** Real isolation: a crashing or leaking worker cannot take
down the PPE, and process boundaries already exist when Java arrives. The
cost is per-process memory and a serialisation copy per task, which is
also exactly the transfer cost we want to model honestly.

## ADR-0003 — Length-prefixed JSON header plus binary payload

**Context.** Control metadata is small and heterogeneous; bulk data is
large and homogeneous. Both sides must parse the same bytes regardless of
implementation language.

**Decision.** Each frame is `u32 header_len | JSON header | u32 payload_len |
payload`. Little-endian lengths. JSON carries `type` and request metadata,
including the `input_lengths` list that splits a payload into per-input
buffers. The payload is raw little-endian float32.

**Consequences.** A Java, Rust, or C worker needs only `DataInputStream`
and a JSON library to participate. Decoding is byte-exact and
boundary-independent, covered by tests that feed one byte at a time.
Rejected alternatives: newline-delimited JSON only (forces base64 or hex
for binary), and a fixed-width binary struct (fast, but ties the wire
format to one language's struct layout and makes protocol evolution
painful).

## ADR-0004 — The PPE depends on an adapter, never on worker code

**Context.** Replaceable workers are a stated goal. If the coordinator
imports worker modules, replacing one with Java means editing the
scheduler.

**Decision.** `SpeAdapter` is the abstract interface: `start`, `execute`,
`cancel`, `stop`. `ProcessSpeAdapter` is the process-backed implementation.
The coordinator holds only adapters and `WorkerSlot` bookkeeping, and it
never imports `neocell.spe.*` except for op metadata during validation.

**Consequences.** A Java SPE is a new adapter, not a change to the
scheduler. It also makes the failure path testable: `tests/test_ppe.py`
injects a deliberately broken adapter and asserts the scheduler fails its
tasks instead of hanging.

## ADR-0005 — asyncio in the PPE, blocking loops in the SPE

**Context.** The PPE must manage many workers concurrently while keeping
scheduling decisions inspectable. The SPE is modelling a machine that runs
one thing at a time.

**Decision.** The PPE is `async`: `asyncio.create_subprocess_exec`, one
reader task per worker feeding a queue, and an explicit dispatch loop that
assigns tasks to free slots. The SPE is a synchronous `os.read` loop that
processes frames as they fully arrive.

**Consequences.** The PPE's concurrency is real and inspectable, with a
single place where dispatch policy lives. The SPE gains nothing from
concurrency and would only obscure the model by pretending otherwise.
Rejected: threads inside the PPE, which would bury the scheduling policy
in lock discipline; free-threaded Python, which the plan correctly notes is
an implementation option and not an architecture.

## ADR-0006 — Bounded local store with explicit staging

**Context.** The defining SPE constraint is a small private scratch memory
that data must be moved into. Without it, the model is just threads.

**Decision.** `LocalStore` is a fixed-capacity byte buffer. Ops never see
the job; they receive `memoryview`s staged into the store. Overstaging
raises `LocalStoreOverflow`, and the worker converts that into a per-task
error rather than growing silently. The store is reset between tasks and
tracks a high-water mark.

**Consequences.** Task sizing becomes a real design constraint: a job whose
chunk exceeds the store fails per task, and `--store-bytes` makes that
observable in a demo. The high-water mark reported per task shows how much
scratch a workload actually needs.

## ADR-0007 — stdlib float32 codec, no numpy

**Context.** Payloads need a stable binary representation. numpy is
available in this environment but would be a native dependency on both
sides and would drag free-threading compatibility into the discussion.

**Decision.** Implement float32 packing with `struct` and `array` in
`neocell/codec.py`, explicitly little-endian regardless of host byte
order.

**Consequences.** Zero runtime dependencies, byte-identical output on any
host, and a codec a Java worker reimplements in twenty lines with
`ByteBuffer.order(LITTLE_ENDIAN)`. Ops are slower than numpy, which is
acceptable because the project explicitly does not model performance.
If throughput ever matters, numpy belongs *inside* an op implementation,
behind the same interface, and changes nothing on the PPE side.

## ADR-0008 — Transfers are counted first, charged later

**Context.** Data movement should be visible and measurable, but version
0.1 should not fake hardware timings it cannot justify.

**Decision.** `DmaModel` always counts bytes and transfer counts in both
directions. By default it is pure bookkeeping with zero cost. Passing
`--dma-bw` and/or `--dma-latency` activates a modelled charge of
`latency + bytes / bandwidth`, applied as an await in the adapter.

**Consequences.** Default runs show real byte volumes without inventing
timings, and `--dma-bw` lets the demo show transfer volume competing with
compute. Numbers from a charged run are labelled as modelled, never as
hardware measurements.

## ADR-0009 — Per-task metrics and a JSONL trace

**Context.** The point of the demo is that scheduling and data movement
have consequences. An aggregate wall time hides them.

**Decision.** Every task records queue wait, stage time, exec time,
transfer seconds, byte counts and store high-water. The metrics collector
aggregates per worker, including utilization. `Trace` writes JSON lines
with a monotonic timestamp to a file and/or stdout.

**Consequences.** `run` prints a report and `--json` gives machine-readable
output; `--trace` produces a file that can be diffed between two
scheduling policies. Trace writes are per event and synchronous, so the
trace path is only for runs small enough that its cost does not distort the
result; that is stated here rather than hidden.

## ADR-0010 — Tests run async without pytest-asyncio

**Context.** The test suite needs async tests, and the project has no
third-party dependencies.

**Decision.** `tests/conftest.py` provides an event loop per test via a
`pytest_runtest_protocol` wrapper and an async-aware `pytest_pyfunc_call`
hook. The `ppe` fixture starts and stops real worker processes on that
same loop.

**Consequences.** `python -m pytest` works on a bare interpreter with no
install step beyond pytest itself. The trade-off is that the hook is
handwritten and tied to pytest internals (`_fixtureinfo`), so a pytest
upgrade could require touching it; it is ~30 lines and isolated in one file.

## ADR-0011 — Where this deliberately departs from real Cell

**Context.** It is easy to over-claim fidelity.

**Decision.** The following are out of scope for version 0.1 and are named
here so nobody mistakes them for oversights:

- No unified 256 MB XDR main memory; each worker process has its own heap.
- No cache-coherency hazards; a shared-memory fast path would reintroduce them.
- No real DMA engine, no pipelined transfer/compute overlap within a worker.
- No SPU thread model, no mailbox semantics, no MFC channel ordering.
- No instruction-level timing, no cycle accuracy, no claim of Cell performance.
- No Graphics Synthesizer, no RSX, no hypervisor.
- Workers are one-task-at-a-time, strictly, like the hardware.

What *is* modelled is the four-part structure: central coordination,
specialised workers, explicit data movement into bounded private storage,
and a replaceable worker interface.

## ADR-0012 — Java SPE: the path we are holding open

**Context.** The plan requires that one or more SPEs can be replaced by a
Java implementation without disturbing the PPE.

**Decision.** Hold the line that the protocol is the only contract.
A Java SPE is spawned the same way (`ProcessSpeAdapter`'s `command` is
already injectable via `PpeConfig.worker_command`), implements
`hello`/`exec`/`result`/`error`/`cancel`/`shutdown` over
`System.in`/`System.out`, and registers ops in its own handshake response.
The PPE learns what it can do from the handshake, so a mixed pool of
Python and Java SPEs is valid with no coordinator change.

**Consequences.** No Java code ships in version 0.1. The constraint that
makes it possible — no Python types in the protocol, no pickles, no shared
memory required — is already enforced by ADR-0003 and ADR-0007. The next
increment adds `java/NeocellSpe.java` plus an integration test that runs
the same job through a mixed pool.
---

## ADR-0013 — OpenTelemetry for cross-process observability

**Context.** Version 0.1 could report per-task metrics and write a JSONL
trace, but both live in the PPE process. A question like "which worker was
idle while tasks queued" is answerable in-process; "did the SPE spend its
time staging or computing, and did that vary between releases" is not,
because the worker is a separate process with its own clock and its own
memory. `_docs/plan1-1` proposes OpenTelemetry for exactly this layer.

**Decision.** Instrument the PPE and every SPE with OTel. Spans cross the
process boundary using W3C `traceparent` carried in the `exec` frame
header, so `neocell.spe.exec` in the worker becomes a child of
`neocell.spe.execute` in the coordinator. The SDK is an optional extra,
not a runtime dependency: every helper degrades to a no-op with the same
call signature, so a worker with no SDK installed still runs and the Java
worker is unaffected.

**Consequences.** A single trace now spans both roles with two distinct
service names, and any OTel backend can consume it. Metric names and
attribute keys are constants in one module rather than literals at call
sites, so a typo fails at import. Two problems had to be solved rather
than assumed away:

- The OTel console exporters default to **stdout**, which is the worker's
  protocol channel. Left alone, an instrumented run writes tens of
  kilobytes of span JSON into the frame stream and the decoder fails
  permanently. Console export is therefore forced to stderr, and a
  regression test pins it.
- `opentelemetry` refuses to replace a global tracer provider, so
  providers are held per `Telemetry` instance rather than installed
  globally. Propagation still works because it flows through contextvars.

**Cost, measured.** Instrumentation is per task, about 0.2 ms: +19% wall
at 32 tasks, +59% at 128 tasks on a 4-SPE `compute_heavy` run. This is
recorded rather than hidden because it bounds when the numbers can be
trusted; see `_docs/06-telemetry.md`. It is the main argument for keeping
telemetry optional rather than default-on.

**Rejected.** Exporter selection via the global provider (single-provider
per process makes the SPE tests impossible); a hand-rolled trace format
(reinvents the ecosystem for no benefit); making OTel a hard dependency
(it would contradict ADR-0007 and break the zero-dependency worker story
for no gain on the PPE side).

## ADR-0014 — Local-only transport: protobuf as a data format, not a service

**Context.** The project is single-machine by definition: one PPE, its SPEs
as child processes, no network anywhere in the job path. That rules out
the whole family of remote-RPC options, and with them the reasons they
were ever attractive (cross-machine calls, service discovery, transport
level flow control). What survives is the part that is still worth
having: a standard, language-neutral wire format instead of a bespoke one.

**Decision.** The worker contract is a `.proto` schema
(`proto/neocell.proto`) with generated Python bindings. It travels as
length-prefixed messages over a child process's stdin/stdout. There is no
RPC framework, no service definition, no port, and no HTTP anywhere in the
job path. Bulk data over the inline limit travels beside the message as an
Arrow IPC file referenced by a `DataRef` (ADR-0015).

**Consequences.** A Java or Rust worker needs a protobuf runtime, an Arrow
reader, and two file descriptors: nothing else. The schema stays free of
`google/protobuf/*.proto` imports, because a bare `protoc` install does not
ship the well-known types, and depending on whatever vendored copy happens
to be on the machine makes the build unreproducible. `OpParams` is a typed
message instead of a `Struct`, which also gives workers compile-time
checking on the parameters an op accepts.

**Rejected.** A remote-RPC service layer: it needs a network stack, a
service definition, and generated stubs, for a transport that is two pipes.
Newline-delimited JSON: forces hex or base64 for binary payloads, which is
both wasteful and slower. Keeping the earlier hand-rolled JSON framing: it
worked, but every consumer in another language had to reimplement framing
bugs, and the tests for arbitrary byte boundaries were exercising our code
rather than a standard.

## ADR-0015 — Data plane: Arrow IPC file plus a buffer manager owning lifetime

**Context.** `plan1-1` asked for bulk data to be explicit, and for
NeoCell to own a small buffer manager that issues data-reference IDs and
tracks size and lifetime while Arrow supplies only the representation. An
earlier revision recorded the buffer manager as the largest architectural
gap; it is now implemented.

**Decision.** Payloads at or under 1 MiB ride inline in the protobuf
message. Anything larger is written as an **Arrow IPC file** and
referenced by a `DataRef` (id, byte length, format, local path, element
count, schema name). `BufferManager` issues ids, tracks issued/released
counts, writes and reads the files, and deletes a file once every ref to it
is released. One manager lives in the PPE, one in each SPE; they never talk
to each other, they only agree on the id and the path, which is sufficient
because they share a filesystem.

The **IPC file** format is chosen over the IPC stream format because the
file format is random-access and supports `pa.memory_map`, so the reader
maps the bytes instead of copying them. A file is written to a temporary
name and renamed, so a `DataRef` never refers to a half-written file.

**Consequences.** `tests/test_buffers.py` pins that inline and Arrow paths
produce identical results, and that refs are released exactly once. A leak
pins a file for the whole run; a premature release hands the reader a
missing file. Both are ownership bugs, so `release` is idempotent and
`cleanup` exists for the crash case. The manager creates its directory
lazily, because creating one per worker per run leaked ~100 directories
per test suite before that was fixed.

**Known limitation.** Ops still require the whole input resident in the
local store, so a chunk must fit the store even when it travels as a
reference. The data plane changes *how bytes move*, not *how much is
resident*. Streaming a large reference through the store in blocks is the
natural next step and is listed in the roadmap.

## ADR-0016 — Telemetry on by default, exported to local files

**Context.** ADR-0013 added OpenTelemetry and measured it at roughly
0.2 ms per task, which is why it was optional. The project's current
priority is troubleshooting, not throughput, and a diagnostic you have to
remember to switch on is a diagnostic you will not have when you need it.

**Decision.** Telemetry is enabled by default, with JSONL file export on
by default and console export off. Every run leaves
`neocell-<service>-<worker>.jsonl` locally: one file per process, because
four processes appending to one file is unreadable. `--no-otel` and
`--no-otel-jsonl` turn it off; `NEOCELL_OTEL=0` and `NEOCELL_OTEL_JSONL=0`
do the same from the environment.

**Consequences.** A run is always diagnosable: which worker was idle,
whether a task waited or executed, how much data moved and by which
transport, and whether a failure was overflow, timeout, or transport loss.
The cost is real and documented, and is the first thing to revisit once
the system stops changing. Local file export also means no collector is
needed, which is consistent with the single-machine decision.

**Rejected.** Defaulting to console output (unreadable at volume).
Shipping telemetry to a collector by default (a network exporter in a
project with no network). Shipping a telemetry default that silently writes files into the
repository root: output paths are explicit and the generated files are
gitignored.
