# Metrics

Everything the model is supposed to make visible. Collected per task,
aggregated per worker, printable and machine-readable.

## Per task

| Field | Source | Meaning |
| --- | --- | --- |
| `queue_s` | PPE | time from enqueue to dispatch: scheduler latency |
| `stage_s` | SPE | copying input into the local store (the DMA-in) |
| `exec_s` | SPE | time inside the op |
| `to_worker_s` | PPE | modelled cost of shipping input |
| `from_worker_s` | PPE | modelled cost of shipping output |
| `input_bytes` / `output_bytes` | PPE | actual bytes across the boundary |
| `store_high_water` | SPE | peak local-store residency for this task |
| `attempts` | PPE | 1 unless a retry happened |
| `status` | PPE | `ok`, `error`, or `cancelled` if never dispatched |

`queue_s` and `exec_s` measure different things and both matter. High
queue time with low exec time means the scheduler or the workers are the
bottleneck; high exec time with low queue time means the workload is too
coarse-grained or too transfer-bound.

## Aggregate

From `MetricsCollector.summary()`:

| Key | Meaning |
| --- | --- |
| `tasks`, `ok`, `failed` | task counts |
| `wall_s` | end-to-end job time |
| `sum_exec_s`, `sum_stage_s`, `sum_transfer_s`, `sum_queue_s` | summed across tasks, so they exceed `wall_s` under parallelism |
| `total_bytes` | all bytes moved in both directions |
| `max_store_high_water` | largest local-store residency seen |
| `workers` | per-worker tasks, busy time, queue time, bytes, utilization |

Per-worker `utilization` is `busy_s / wall_s` where `busy_s = stage_s + exec_s`.
It is a scheduling statistic, not a CPU-occupancy measurement: a worker
that is idle while waiting for its next task is underutilised even if the
PPE has nothing to give it.

## Transfer accounting

`DmaModel` separates two things:

- **Always**: byte counts and transfer counts, per direction. Free.
- **Only with `--dma-bw` / `--dma-latency`**: a modelled charge of
  `fixed_latency + bytes / bandwidth`, awaited in the adapter so it
  actually occupies the scheduler.

A charged run shows how transfer volume competes with compute. The numbers
are modelled, not measured, and are labelled as such in the report.

## Trace

`--trace FILE` writes JSON lines; `--trace-stdout` mirrors to the console.
Events:

| Event | When |
| --- | --- |
| `ppe_start` | pool being brought up |
| `worker_ready` | a worker completed the handshake |
| `worker_stderr` | a line from a worker, for diagnostics |
| `job_submit` | job accepted, with task count and chunk size |
| `dispatch` | a task handed to a named worker, with queue wait |
| `collect` | a result came back, with exec and total time |
| `task_error` / `retry` / `task_failed` | failure paths |
| `job_done` | final summary |

Trace writes are synchronous and per event. Use them on runs small enough
that the logging cost does not distort what you are measuring; for
anything larger, compare `--json` summaries instead.

## Using them

```bash
# human-readable
PYTHONPATH=src python3 -m neocell.cli run --spe 4 --workload reductions

# machine-readable
PYTHONPATH=src python3 -m neocell.cli run --spe 4 --workload reductions --json

# scheduler comparison across pool sizes
PYTHONPATH=src python3 -m neocell.cli sweep --sweep 1 2 4 --workload compute_heavy

# with transfer cost made non-zero
PYTHONPATH=src python3 -m neocell.cli run --spe 4 --workload scale \
    --dma-bw 2000000000 --trace /tmp/trace.jsonl
```

The last line prints a compute-versus-transfer share, which is the number
the whole architecture exists to make arguable.