# Telemetry

OpenTelemetry traces and metrics across the PPE and every SPE process.
The goal is to answer "where did the time go" across the process boundary,
not merely to print numbers we already had.

## What is instrumented

| Component | Spans | Notes |
| --- | --- | --- |
| PPE, per job | `neocell.job` | root span; task count, chunk size, SPE count |
| PPE, per dispatch | `neocell.spe.execute` | transfer in, RPC, transfer out |
| SPE, per execution | `neocell.spe.exec` | child span in the *worker* process |

The PPE-side and SPE-side spans share a trace because the dispatch span's
W3C `traceparent` travels in the `exec` frame header. A worker that
ignores the field simply starts an unrelated trace, so propagation stays
optional and a Java worker need not implement it on day one.

```
neocell.job                     (neocell-ppe)
└── neocell.spe.execute         (neocell-ppe)  task 0 -> SPE0
    └── neocell.spe.exec        (neocell-spe)  staging + op
```

## Metrics

| Instrument | Kind | Unit | Meaning |
| --- | --- | --- | --- |
| `neocell.task.queue_seconds` | histogram | s | wait for a free SPE |
| `neocell.task.exec_seconds` | histogram | s | time inside the op |
| `neocell.task.stage_seconds` | histogram | s | copy into local store |
| `neocell.task.transfer_seconds` | histogram | s | modelled transfer per task |
| `neocell.task.total_seconds` | histogram | s | queue plus execution |
| `neocell.task.input_bytes` | histogram | By | bytes staged |
| `neocell.task.output_bytes` | histogram | By | bytes returned |
| `neocell.transfer.bytes` | counter | By | bytes by direction |
| `neocell.transfer.seconds` | counter | s | modelled cost by direction |
| `neocell.tasks` | counter | 1 | by op, worker, status |
| `neocell.task.failures` | counter | 1 | by op and classified reason |
| `neocell.task.retries` | counter | 1 | by op |
| `neocell.spe.store_high_water_bytes` | histogram | By | peak local-store residency |
| `neocell.spe.store_bytes` | histogram | By | configured capacity |
| `neocell.job.wall_seconds` | histogram | s | end-to-end |
| `neocell.worker.busy_seconds` | counter | s | worker busy time |
| `neocell.spe.executions` | counter | 1 | op executions per worker |
| `neocell.exceptions` | counter | 1 | exceptions by type or reason |

Attribute keys are constants in `neocell/telemetry.py` rather than string
literals at call sites, so a typo fails at import instead of silently
creating a new dimension.

## Failure classification

Failure reasons are classified into a small fixed set (`overflow`,
`unknown_op`, `timeout`, `transport`, `protocol`, `input_count_mismatch`,
`other`) rather than using raw error text. Raw messages are unbounded and
would explode metric cardinality; the classification keeps the
distinctions worth alerting on. The full message stays in the trace log
and in `TaskMetric.error`.

## Usage

Telemetry is **on by default** and writes local JSONL files, so a plain run
already leaves a record:

```bash
neocell run --spe 4 --workload reductions
# writes neocell-neocell-ppe-ppe.jsonl and neocell-neocell-spe-SPE*.jsonl
```

Flags: `--otel/--no-otel`, `--otel-jsonl/--no-otel-jsonl`,
`--otel-jsonl-path`, `--otel-console`, `--otel-interval-ms`.

Environment: `NEOCELL_OTEL=0`, `NEOCELL_OTEL_JSONL=0`,
`NEOCELL_OTEL_CONSOLE=1`, `NEOCELL_OTEL_JSONL_PATH`.

There is no collector exporter and no network sink: this is a
single-machine project, so spans and metrics go to files or stderr and
nowhere else.

### Reading the files

Each line is one JSON object with a `kind`:

| `kind` | Meaning | Key fields |
| --- | --- | --- |
| `header` | first line of a metric file | `service` |
| `span` | a finished span | `name`, `trace_id`, `span_id`, `parent_id`, `duration_ns`, `attributes`, `resource` |
| `metric` | a metrics snapshot | `name`, `unit`, `points[]` |

Because `parent_id` is written out, the SPE spans in
`neocell-neocell-spe-SPE0.jsonl` can be joined to the PPE spans in
`neocell-neocell-ppe-ppe.jsonl` by trace id, which is how you tell whether
a slow task was slow in the PPE or on the worker.

The `neocell ui` console renders the same information live and is usually
the faster route.

## stdout is the protocol

**The console exporters are forced to stderr.** An SPE's stdout is the
protocol pipe, and OTel's console exporters default to stdout. Left
alone, a single instrumented run writes tens of kilobytes of span JSON
into the frame stream and the decoder fails with `header length ... out of
range` permanently. `tests/test_telemetry.py::test_worker_stdout_stays_protocol_clean_with_console_export`
pins this.

In console mode the adapter also relays worker stderr to its own stderr, so
both processes' telemetry appears in one terminal. With JSONL export no
relay is needed either: each worker writes its own file.

## Cost

Measured on this machine with `python -m neocell.bench`, 4 SPEs,
`compute_heavy`, 65536 elements, 25 runs, recording to a discarding
exporter:

| Chunk | Tasks | Telemetry off (median) | On (median) | Overhead |
| --- | --- | --- | --- | --- |
| 2048 | 32 | 37.42 ms | 44.70 ms | +19% |
| 512 | 128 | 53.82 ms | 85.65 ms | +59% |

**The cost is per task, roughly 0.2 ms.** It scales with task count, not
with data volume, because the work is a span plus about ten metric
records per task. Consequences worth knowing:

- Small chunks pay the most. If tasks are in the microsecond range,
  telemetry dominates and the measurement becomes meaningless.
- The box used for these numbers is noisy (2.7x spread between min and
  max), so treat the percentages as order-of-magnitude.
- Console export adds stderr I/O on top and is not representative.
- **Always compare telemetry-off against telemetry-off**, never against a
  baseline taken with instrumentation on.

For cheap control, sampling is available through the standard OTel
environment (`OTEL_TRACES_SAMPLER`, `OTEL_TRACES_SAMPLER_ARG`), and
`neocell.job` and `neocell.spe.execute` spans can be dropped by
configuration without touching call sites.

## Relationship to the existing metrics

`neocell.ppe.metrics` and the JSONL `Trace` still exist and are still the
right tool for a single run: they need no SDK, they are exact, and they
cost nothing. Telemetry is for tracking a number *over time* and across
processes. They answer different questions and the overlap is deliberate.

See ADR-0013 for adding OpenTelemetry, ADR-0016 for turning it on by
default, and ADR-0014 for the local-only transport decision.