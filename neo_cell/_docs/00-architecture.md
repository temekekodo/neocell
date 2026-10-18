# Architecture

How neo_cell is put together and why. Read
[01-architecture-decisions.md](01-architecture-decisions.md) for the
decisions; this file is the map.

## The model in one paragraph

One PPE accepts a job, splits it into independent chunks, and hands each
chunk to an idle SPE. Each SPE stages the chunk into its own bounded
local store, computes on it, and returns a result. The PPE collects
results and assembles them in order. Nothing else touches memory, and
every byte that crosses the boundary is counted.

```
                 +---------------------------------------+
   job  --->     |  PPE  (coordinator + scheduler)      |
                 |  split -> queue -> dispatch -> collect |
                 +---------------------------------------+
                    |        |        |        |
                 exec      exec     exec     exec      (protobuf over pipes)
                    |        |        |        |
                 +---v--+ +--v---+ +v-----+ +-v----+
                 |SPE0 | |SPE1 | |SPE2 | |SPE3 |      (child processes)
                 | 128K| | 128K| | 128K| | 128K|
                 |  LS | |  LS | |  LS | |  LS |
                 +-----+ +-----+ +-----+ +-----+

  op catalogue is discovered at handshake, so a new op needs no PPE change
```

## Layers

| Layer | Module | Responsibility |
| --- | --- | --- |
| Protocol | `neocell/protocol.py` | framing, message types, direction validation |
| Schema | `proto/neocell.proto` | the control-plane contract, generated to `neocell/pb/` |
| Data plane | `neocell/buffers.py` | BufferManager, Arrow IPC files, `DataRef` lifetime |
| Coordinator | `neocell/ppe/coordinator.py` | `Ppe`: start, submit, schedule, assemble |
| Adapter | `neocell/ppe/adapter.py` | `SpeAdapter` interface, `ProcessSpeAdapter` |
| Job model | `neocell/ppe/job.py` | `Job`, `Task`, chunk splitting |
| Transfers | `neocell/ppe/dma.py` | byte accounting, optional modelled cost |
| Metrics | `neocell/ppe/metrics.py` | per-task metrics, aggregation, JSONL trace |
| Worker | `neocell/spe/worker.py` | the blocking frame loop |
| Local store | `neocell/spe/local_store.py` | bounded scratch memory, explicit staging |
| Ops | `neocell/spe/ops.py` | data-parallel operation registry |
| Codec | `neocell/codec.py` | little-endian float32 pack/unpack |
| Workloads | `neocell/workloads.py` | demo jobs |
| CLI | `neocell/cli.py` | `run`, `sweep`, `ops` |

## Two planes

Control and bulk data are deliberately separate:

- payloads <= 1 MiB travel inside the protobuf message
- payloads > 1 MiB are written as an Arrow IPC file and referenced by a
  `DataRef`, so the message carries a pointer

The Arrow **file** format is used rather than the stream format because it
is random-access and supports `pa.memory_map`. `BufferManager` issues the
ids and deletes files on release; Arrow only supplies the representation.

## The dependency direction that matters

`ppe` imports `protocol`, `job`, `dma`, `metrics`, `adapter`. It does
**not** import `spe`. The one exception is op *metadata* during
validation — which op takes how many inputs — and that comes from a plain
registry, not from worker internals. This is what keeps a Java or Rust
SPE a substitution rather than a rewrite.

## Task lifecycle

1. The PPE receives a `Job` and calls `split()`, producing `Task`s in
   index order. Chunks are independent, which is what allows overlap.
2. Each `Task` enters a FIFO `ready` deque.
3. The dispatch loop hands the next task to any `WorkerSlot` that is free
   and healthy, and stamps queue-wait and dispatch times.
4. The adapter packs the task's input buffers into one payload, charges
   the DMA model, and writes an `exec` frame.
5. The worker resets its local store, stages every input buffer, runs the
   op, resets again, and replies with `result` plus stats.
6. The PPE charges the return transfer, records the metric, and stores the
   payload keyed by task index.
7. On failure the task is retried up to `max_attempts`, then marked dead.
8. When the queue drains, results are concatenated in index order, so
   output order is independent of completion order.

## Messages

| PPE to SPE | SPE to PPE | Carries |
| --- | --- | --- |
| `hello` | `hello_ack` | protocol version, local store size, **op list** |
| `exec` | `result` | task id, op, params, input lengths / status, stats, output |
| `exec` | `error` | task id, message (e.g. local store overflow) |
| `cancel` | `cancelled` | task id |
| `shutdown` | `bye` | — |

The op list in `hello_ack` is the extension point. Adding an op to
`neocell/spe/ops.py` makes it visible to the PPE with no change on the
coordinator side.

## What the model captures

- Central coordination: the PPE does the dividing, scheduling and
  assembly, and performs no bulk arithmetic.
- Specialised workers: an SPE runs one bounded task at a time against
  private scratch memory.
- Explicit data movement: input is staged, output is returned, and both
  are counted and can be charged.
- Replaceable workers: everything above is behind `SpeAdapter`.

## What it does not capture

Named explicitly in
[ADR-0011](01-architecture-decisions.md#adr-0011--where-this-deliberately-departs-from-real-cell)
and [ADR-0014](01-architecture-decisions.md#adr-0014--local-only-transport-protobuf-as-a-data-format-not-a-service):
no unified main memory, no coherency hazards, no real DMA engine, no
mailbox or MFC semantics, no instruction-level or cycle-accurate timing,
no claim about PS3 or Cell performance, and no network anywhere in the job
path. Treat it as an architecture demonstration, not a performance model.