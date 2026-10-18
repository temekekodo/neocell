# neo_cell

A Cell-style compute model in Python: **one PPE coordinates, a pool of
SPEs computes, and every byte that moves between them is explicit.**

This is an architecture demonstration, not a PS3 emulator and not a
performance model. It reproduces the *organisation* of the Cell
Broadband Engine — central coordination, specialised workers with private
scratch memory, explicit data movement — so those ideas can be run,
measured and discussed on ordinary hardware.

```
                 +---------------------------------------+
   job  --->     |  PPE  (coordinator + scheduler)      |
                 |  split -> queue -> dispatch -> collect |
                 +---------------------------------------+
                    |        |        |        |
                 exec      exec     exec     exec        (framed stdio)
                    |        |        |        |
                 +---v--+ +--v---+ +v-----+ +-v----+
                 |SPE0 | |SPE1 | |SPE2 | |SPE3 |      (child processes)
                 | 128K| | 128K| | 128K| | 128K|
                 +-----+ +-----+ +-----+ +-----+
```

## Quick start

No install, no dependencies:

```bash
python3 -m pytest                                    # 99 tests

PYTHONPATH=src python3 -m neocell.cli run --spe 4 \
    --workload reductions --elements 16384 --chunk 2048
```

```
job        : reductions (op=sum)
failures   : 0
output[0:8]  : ['749.114', '1668.64', '367.302', '-1460.8', ...]

tasks      : 8 (8 ok, 0 failed)
wall       : 3.20 ms
exec total : 0.66 ms
store peak : 8192 B
  SPE0     tasks=2      busy=    0.18 ms  util=  5.8%  bytes=16392
  ...
dma        : -> worker 65536 B in 8 transfers; <- worker 32 B in 8 transfers
```

Optional install for a `neocell` command:

```bash
python3 -m pip install -e .
neocell run --spe 4 --workload axpy --elements 32768
```

## Commands

```bash
neocell run    --spe 4 --workload reductions   # one job on a pool
neocell sweep  --sweep 1 2 4 --workload scale   # pool-size comparison
neocell ui                                       # local test console
neocell ops                                      # list supported operations
```

Useful flags: `--chunk` (elements per task), `--store-bytes` (local store
size, default 128 KiB), `--dma-bw` / `--dma-latency` (make transfers cost
something), `--trace FILE` (JSONL event log), `--json` (machine-readable
metrics).

Try `--store-bytes 8192` with a large `--chunk` to watch tasks fail on
local-store overflow. That constraint is the point.

## Test UI

```bash
neocell ui            # then open http://127.0.0.1:8765/
```

A local console showing inputs, which SPE is working on which task, queue
and exec times, and when the PPE collects a result. Three demo scenarios:

| Scenario | Shows |
| --- | --- |
| Balanced fan-out | 256 small chunks over 4 SPEs; watch the queue drain evenly |
| Transfer-bound, Arrow | chunks over 1 MiB, so every task travels as a `DataRef` |
| Local store overflow | chunks larger than the store; every task fails and retries |

It binds to `127.0.0.1` only. It is a developer tool, not part of the
compute path: the PPE and SPEs talk over pipes and never touch HTTP.

## Layout

```
src/neocell/
  protocol.py       framing, message types, direction validation
  codec.py          little-endian float32, stdlib only
  workloads.py      demo jobs
  cli.py            run / sweep / ops
  ppe/
    coordinator.py  Ppe: split, schedule, assemble
    adapter.py      SpeAdapter interface, process-backed implementation
    job.py          Job, Task, chunk splitting
    dma.py          byte accounting, optional modelled transfer cost
    metrics.py      per-task metrics, aggregation, JSONL trace
  spe/
    worker.py       the blocking frame loop
    local_store.py  bounded scratch memory, explicit staging
    ops.py          data-parallel operation registry
tests/              framing, local store, ops, scheduling, failure paths
_docs/              architecture, decisions, dependencies, protocol, metrics
```

## Design in four points

1. **The PPE coordinates; it does not crunch.** It splits, schedules and
   assembles. All bulk arithmetic happens on an SPE.
2. **SPEs are separate processes.** Real isolation, and the process
   boundary a Java worker will slot into.
3. **Data movement is explicit and measured.** Input is staged into a
   bounded local store; nothing else touches job memory. Bytes are counted
   in both directions, and can be charged with `--dma-bw`.
4. **Workers are replaceable.** The PPE depends on an adapter interface,
   never on worker code. The protocol is the only contract: a Java worker
   needs a protobuf runtime, an Arrow reader, and two file descriptors.

## Data movement

Payloads at or under 1 MiB ride inline in the control message. Anything
larger is written as an Arrow IPC **file** and referenced by a `DataRef`,
so the message carries a pointer and the bytes travel beside it. The file
format is used rather than the stream format because it supports
`pa.memory_map`, letting the reader map the bytes instead of copying them.

`BufferManager` issues the ids, tracks size and lifetime, and deletes a
file once every reference to it is released. Arrow supplies the
representation; NeoCell owns the ownership.

## Documentation

| Document | Contents |
| --- | --- |
| [00-architecture.md](_docs/00-architecture.md) | the map: layers, task lifecycle, dependency direction |
| [01-architecture-decisions.md](_docs/01-architecture-decisions.md) | ADRs, including what we deliberately do not model |
| [02-dependencies.md](_docs/02-dependencies.md) | zero runtime deps, Python floor, Java requirements |
| [03-protocol.md](_docs/03-protocol.md) | wire format, message semantics, rules for a new-language worker |
| [04-metrics.md](_docs/04-metrics.md) | what each metric means and how to read it |
| [05-roadmap.md](_docs/05-roadmap.md) | what comes next, starting with the Java SPE |
| [06-telemetry.md](_docs/06-telemetry.md) | OTel spans/metrics, and the measured cost of instrumenting |
| [07-test-ui.md](_docs/07-test-ui.md) | the local test console and its three scenarios |

Original design document: [`plan1`](plan1).

## Requirements

Python 3.12+ (tested on 3.14.6). Runtime dependencies are `protobuf` and
`pyarrow`; everything else is stdlib. `pytest` for the tests, and
OpenTelemetry for the default-on tracing (`pip install -e '.[telemetry]'`);
without it every instrumentation call is a no-op. `protoc` only if you
change the schema. Linux/macOS on x86-64.