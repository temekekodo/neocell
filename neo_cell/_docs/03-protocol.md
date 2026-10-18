# Protocol Specification

Version 1. Two planes, one local transport.

This is the only contract between the PPE and an SPE. A worker in any
language that speaks it is interchangeable with the bundled Python worker.

## Transport

A child process's stdin and stdout. Length-prefixed messages. **No network,
no HTTP, no RPC framework, no port.** The PPE spawns the worker and writes
to its stdin; the worker writes replies to stdout and logs to stderr.

```
  PPE process                          SPE process
  parent stdin  ──────────────────▶   child  stdin
  parent stdout ◀──────────────────   child  stdout
  parent stderr ◀──────────────────   child  stderr   (logs only)
```

stdout carries protocol messages and nothing else. That constraint is
load-bearing: it is why the OTel console exporters are forced to stderr,
and why a stray print in a worker is a protocol desync.

## Framing

```
+-----------------+---------------------------+
| u32 message_len | serialized Envelope      |
+-----------------+---------------------------+
```

`message_len` is little-endian unsigned 32-bit and counts only the
protobuf message, not itself. A decoder must tolerate arbitrary
fragmentation; `tests/test_protocol.py` feeds messages one byte at a time.
Maximum message size is 1 GiB; a larger declared length is a protocol
error, not a truncation.

## Two planes

| Plane | Carries | Threshold |
| --- | --- | --- |
| Control | every `Envelope`, and payloads <= 1 MiB | inline |
| Data | Arrow IPC file, referenced by `DataRef` | > 1 MiB |

The threshold is a property of the `BufferManager`, not of the protocol.
Both sides apply the same rule, so a sender and a receiver agree without
negotiating. `neocell ui` shows which plane each task used.

## Schema

Source of truth is [`proto/neocell.proto`](../proto/neocell.proto).
Regenerate with:

```bash
protoc --proto_path=proto --python_out=src/neocell/pb proto/neocell.proto
```

The schema imports no `google/protobuf/*.proto`. A bare `protoc` install
does not ship the well-known types, and relying on a vendored copy found
on the machine would make the build unreproducible.

## Messages

Every message is one `Envelope` with a `oneof body`. That keeps framing
trivial and lets new message types be added without changing the wire
format.

| PPE to SPE | SPE to PPE |
| --- | --- |
| `hello` | `hello_ack` |
| `exec` | `result` |
| `exec` | `error` |
| `cancel` | `cancelled` |
| `shutdown` | `bye` |

### Handshake

```protobuf
message Hello  { uint32 protocol = 1; string worker_id = 2; uint64 local_store_bytes = 3; }
message HelloAck { uint32 protocol = 1; string worker_id = 2; uint32 pid = 3;
                   uint64 local_store_bytes = 4; repeated string ops = 5; }
```

1. PPE sends `hello` immediately after spawn.
2. Worker rejects a mismatched `protocol` with `error`.
3. Worker replies `hello_ack` with its store size, its `ops`, and its pid.

**The op list is the extension point.** The PPE schedules only what a
worker advertises, so adding an op requires no coordinator change.

### Exec

```protobuf
message Exec {
  string task_id = 1;
  string op = 2;
  OpParams params = 3;
  repeated uint64 input_lengths = 4;   // logical buffer sizes
  uint64 input_bytes = 5;
  repeated DataRef input_refs = 6;    // out-of-band inputs
  bytes inline_payload = 7;           // in-band inputs
  map<string, string> trace = 8;      // W3C traceparent carrier
  uint32 task_index = 9;
  string label = 10;
}
```

Exactly one of `input_refs` and `inline_payload` carries the data.
`input_lengths` describes the logical buffers either way, so the worker
resolves both representations to the same list of float lists.

`trace` carries the W3C trace context so the worker's span parents to the
dispatching span. A worker that ignores it starts an unrelated trace, so
this is optional and a Java worker need not implement it on day one.

### DataRef

```protobuf
message DataRef {
  string id = 1;               // issued by the producer's BufferManager
  uint64 byte_length = 2;      // size of the encoded data
  DataFormat format = 3;       // DATA_FORMAT_ARROW_IPC
  string location = 4;         // local path, never a URL
  uint64 element_count = 5;
  string arrow_schema = 6;     // "neocell.f32"
}
```

`location` is a filesystem path because both processes are on the same
machine. It is never a URL and is never resolved over a network.

### Result and error

```protobuf
message Result { string task_id = 1; string status = 2; string worker_id = 3;
                 bytes inline_payload = 4; DataRef output_ref = 5;
                 uint64 output_bytes = 6; ExecStats stats = 7; string error = 8; }
message ErrorFrame { string task_id = 1; string worker_id = 2; string message = 3; }
message ExecStats { double stage_s = 1; double exec_s = 2;
                    uint64 store_high_water = 3; uint64 store_bytes = 4;
                    uint64 tasks_done = 5; }
```

`stage_s` and `exec_s` are measured by the worker, since only the worker
can measure them. `store_high_water` is the peak local-store residency for
that task.

Large results come back as an `output_ref` by the same rule as inputs, so
a big output never has to be copied through the pipe.

## Data plane: Arrow

The reference implementation writes an Arrow IPC **file** containing one
`float32` column named `values`, schema `neocell.f32`.

The file format is required, not the stream format:

- **File** is random-access and supports `pa.memory_map`, so the reader
  maps the bytes instead of copying them.
- **Stream** cannot be memory-mapped.

A file is written to a temporary name and renamed into place, so a
`DataRef` never refers to a partially written file. The producer may delete
the file only after every ref to it has been released.

Ownership is NeoCell's, not Arrow's. `BufferManager` issues ids, tracks
issued and released counts, and deletes on release. `release` is idempotent
because a crashed process can leave either side holding entries the other
has forgotten.

## Execute semantics

On `exec` the worker must, in order:

1. Validate the op name against its registry; unknown op -> `error`.
2. Resolve inputs, from `input_refs` if present, else from
   `inline_payload`. Release input refs as soon as the data is resolved.
3. Validate the input count against the op's declaration; mismatch ->
   `error`.
4. Reset the local store and stage every input. Overflow -> `error` with
   `local store overflow`.
5. Run the op on the staged `memoryview`s.
6. Reset the local store.
7. Reply `result` with the output and stats.

Staging is mandatory. An op never reads job memory directly; that is what
makes the local store a constraint rather than decoration.

An unexpected exception must be reported as a task `error`, not allowed to
kill the process: a dead worker would leave the PPE waiting for a reply
that never comes.

## Cancel and shutdown

`cancel` is advisory. The bundled worker is strictly single-tasked, so a
cancel is answered once the current task finishes. The message exists so a
future pipelined worker can honour it immediately; the PPE does not rely
on cancellation for correctness.

`shutdown` expects a `bye` then process exit. The adapter escalates to
`SIGTERM` after 5 seconds, and treats a closed stdout as immediate failure
rather than waiting out its request timeout.

## Rules for a new-language worker

1. Read little-endian lengths; do not assume host byte order.
2. Write `result`/`error` to **stdout only**. Logs go to stderr. A single
   stray byte on stdout desynchronises the stream permanently.
3. Flush after every reply; the PPE reads frames as they arrive.
4. Emit float32 little-endian in inline payloads.
5. For `DataRef`, read an Arrow IPC *file* with `pa.memory_map` (or the
   Java equivalent) and release the file when the reference is done.
6. Respect `local_store_bytes` from `hello`, not a local default.
7. Advertise ops in `hello_ack`.
8. Return large results by reference rather than inlining them.