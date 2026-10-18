# Roadmap

Starting state: version 0.1 has one PPE, four SPEs, a process-backed
worker adapter, a language-neutral protocol, and metrics. What follows is
ordered by what unblocks the most.

## v0.1 — PPE vertical slice

- [x] One PPE with a dispatch loop and result assembly
- [x] Four SPEs as child processes, 128 KiB local store each
- [x] Length-prefixed JSON+binary protocol with handshake and op discovery
- [x] Explicit staging with overflow enforcement
- [x] Per-task metrics, JSONL trace, JSON output
- [x] 59 tests covering framing, local store, ops, scheduling, failure paths, telemetry
- [x] `_docs` on architecture, decisions, dependencies, protocol, metrics

## v0.1.1 — Observability (done)

- [x] OpenTelemetry across PPE and every SPE, with W3C trace propagation
      over the existing `exec` frame header
- [x] Enabled by default, exporting to local JSONL files (ADR-0016)
- [x] Console export pinned to stderr, never the protocol pipe
- [x] Per-task and per-worker metrics; classified failure reasons
- [x] `neocell.bench` measuring instrumentation overhead

## v0.1.2 — Protobuf + Arrow data plane (done)

- [x] `proto/neocell.proto` control plane, generated bindings, no WKT imports
- [x] Two planes: inline payloads, `DataRef` above 1 MiB
- [x] Arrow IPC *file* format so the reader can memory-map
- [x] `BufferManager` owning id issuance, size, lifetime and cleanup
- [x] Local test UI with three demo scenarios

## v0.2 — Locality-aware scheduling

The remaining gap in the Cell idea, and the highest-value next step:

- [ ] Locality-aware dispatch: weigh the cost of moving a task's input
      to a worker, not just whether the worker is free
- [ ] Explicit task state machine with legal transitions
- [ ] Backpressure: bound the ready queue rather than relying on slots
- [ ] Resource requests per task
- [ ] Stream a large `DataRef` through the local store in blocks, so a
      chunk need not fit the store in its entirety (ADR-0015 limitation)
- [ ] Future-like result references, instead of awaiting results inline

## v0.3 — Java SPE

The point of the protocol is proved by replacing a worker in another
language. Keep it minimal:

- [ ] `java/NeocellSpe.java`: read frames from `System.in`, write to
      `System.out`, implement `sum`, `scale`, `axpy`
- [ ] Little-endian float32 via `ByteBuffer.order(LITTLE_ENDIAN)`
- [ ] One small JSON dependency, or a minimal flat-header parser
- [ ] Integration test: same job, 2 Python SPEs + 2 Java SPEs, compare
      against the serial reference
- [ ] Document the build in `02-dependencies.md`

ADR-0012 lists the constraints that make this additive rather than
invasive. If the Java worker needs a coordinator change to slot in, one of
those constraints was violated.

## v0.4 — Scheduling policies

Now that locality and tasks are observable, make the interesting comparisons:

- [ ] `--policy first-fit` vs `least-loaded` vs `lowest-transfer-cost`
- [ ] Chunk-size sweep, showing queue time versus transfer volume trade-off
- [ ] Shared-memory transfer path behind the same adapter (plan1
      section 4 explicitly reserves this)
- [ ] Pipeline transfers ahead of compute to demonstrate overlap

## v0.5 — Deeper model

Only if the project wants to teach more of the hardware:

- [ ] Local store split into two banks, as on real hardware
- [ ] A mailbox message as a first-class operation alongside DMA
- [ ] A worker that can accept several in-flight tasks, which makes
      `cancel` meaningful
- [ ] Deterministic single-threaded replay mode for debugging races

## Deliberately not planned

Named in ADR-0011 and ADR-0014 so they are decisions rather than
omissions: unified main memory, cache coherency, a real DMA engine, MFC
channel ordering, instruction-level timing, any PS3 performance claim, and
anything requiring a network.

## Open questions

- **Shared memory or not?** It would remove a copy for large payloads and
  make the coherency question real, at the cost of a weaker second-language
  story. Worth revisiting only with measurements showing transfer volume
  dominates; today the Arrow file path is already close to zero-copy.
- **When to turn telemetry back off?** At ~0.2 ms per task it is fine for
  a system still changing and wrong for a throughput measurement. The
  decision belongs when the architecture stops moving.
- **Telemetry sampling policy.** Instrumentation costs about 0.2 ms per
  task, so small chunks are expensive to observe. Decide the default
  sampler before running anything long.
- **Trace cost.** Synchronous writes distort large runs. Either buffer
  writes or document the threshold and move on.
- **Free-threaded Python.** A benchmark, not a redesign. The architecture
  does not change either way.