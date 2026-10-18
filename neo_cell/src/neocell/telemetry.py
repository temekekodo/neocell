"""OpenTelemetry instrumentation for the PPE and the SPE workers.

Observability is the one layer where reusing an ecosystem standard beats
rolling our own: spans and metrics defined here mean a run can be compared
against any other OTel-instrumented system, and trace context crosses the
process boundary using W3C ``traceparent`` rather than a bespoke scheme.

Design rules:

* **Optional.** The SDK is not a runtime dependency. Without it every
  helper degrades to a no-op that costs a dict lookup, so a worker stays
  dependency-free and the Java worker is unaffected.
* **Symmetric.** The PPE and each SPE set up identical instruments with
  different service names, so a trace reads as one system.
* **Non-distorting.** Instrumentation is off by default and cheap when on;
  see ``_docs/06-telemetry.md`` for the measured overhead.
* **Owns semantics, borrows plumbing.** This module chooses what a
  "dispatch" or a "transfer" means. OTel only carries it.
"""

from __future__ import annotations

import json
import os
import sys
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

try:  # pragma: no cover - exercised by whichever branch the env provides
    from opentelemetry.propagate import extract, inject

    OTEL_AVAILABLE = True
except ImportError:  # pragma: no cover
    extract = inject = None
    OTEL_AVAILABLE = False

SERVICE_PPE = "neocell-ppe"
SERVICE_SPE = "neocell-spe"

# Attribute keys, kept in one place so spans and metrics agree.
ATTR_OP = "neocell.op"
ATTR_TASK_ID = "neocell.task_id"
ATTR_TASK_INDEX = "neocell.task_index"
ATTR_WORKER = "neocell.worker_id"
ATTR_ATTEMPT = "neocell.attempt"
ATTR_INPUT_BYTES = "neocell.input_bytes"
ATTR_OUTPUT_BYTES = "neocell.output_bytes"
ATTR_STORE_BYTES = "neocell.store_bytes"
ATTR_STORE_HIGH_WATER = "neocell.store_high_water"
ATTR_STATUS = "neocell.status"
ATTR_DIRECTION = "neocell.direction"
ATTR_SPE_COUNT = "neocell.spe_count"
ATTR_CHUNK_ELEMENTS = "neocell.chunk_elements"
ATTR_TOTAL_ELEMENTS = "neocell.total_elements"
ATTR_REASON = "neocell.reason"
ATTR_OPS = "neocell.ops"

# Metric instrument names, as constants so typos fail loudly.
M_TASK_QUEUE_SECONDS = "neocell.task.queue_seconds"
M_TASK_EXEC_SECONDS = "neocell.task.exec_seconds"
M_TASK_STAGE_SECONDS = "neocell.task.stage_seconds"
M_TASK_TRANSFER_SECONDS = "neocell.task.transfer_seconds"
M_TASK_TOTAL_SECONDS = "neocell.task.total_seconds"
M_TASK_INPUT_BYTES = "neocell.task.input_bytes"
M_TASK_OUTPUT_BYTES = "neocell.task.output_bytes"
M_TRANSFER_BYTES = "neocell.transfer.bytes"
M_TRANSFER_SECONDS = "neocell.transfer.seconds"
M_TASKS = "neocell.tasks"
M_TASK_FAILURES = "neocell.task.failures"
M_RETRIES = "neocell.task.retries"
M_STORE_HIGH_WATER_BYTES = "neocell.spe.store_high_water_bytes"
M_STORE_BYTES = "neocell.spe.store_bytes"
M_JOB_WALL_SECONDS = "neocell.job.wall_seconds"
M_WORKER_BUSY_SECONDS = "neocell.worker.busy_seconds"
M_EXECUTIONS = "neocell.spe.executions"
M_EXCEPTIONS = "neocell.exceptions"


class _NoopSpan:
    """Stand-in span used when the SDK is absent."""

    __slots__ = ()

    def __enter__(self) -> _NoopSpan:
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def set_attribute(self, *args: object, **kwargs: object) -> None: ...

    def set_attributes(self, *args: object, **kwargs: object) -> None: ...

    def add_event(self, *args: object, **kwargs: object) -> None: ...

    def record_exception(self, *args: object, **kwargs: object) -> None: ...

    def set_status(self, *args: object, **kwargs: object) -> None: ...

    def end(self) -> None: ...

    def get_span_context(self) -> None:
        return None

    def is_recording(self) -> bool:
        return False


_NOOP_SPAN = _NoopSpan()


class _NoopInstrument:
    """Counter/histogram stand-in used when the SDK is absent."""

    __slots__ = ()

    def add(self, *args: object, **kwargs: object) -> None: ...

    def record(self, *args: object, **kwargs: object) -> None: ...


@dataclass(slots=True)
class TelemetryConfig:
    """How one process should report telemetry.

    ``enabled`` defaults to True: troubleshooting value outweighs the
    measured per-task cost while the system is still moving. ``jsonl``
    defaults on as well, so a run always leaves a local record without
    anyone having to pass a flag. Console output stays opt-in because it
    is unreadable at volume.

    There is no network sink. The project is single-machine, so spans and
    metrics go to stderr or to local files and nowhere else.
    """

    service_name: str = SERVICE_PPE
    enabled: bool = True
    console_export: bool = False
    jsonl: bool = True
    jsonl_path: str | os.PathLike[str] | None = None
    export_interval_ms: int = 10_000
    worker_id: str | None = None
    extra_attributes: dict[str, str] = field(default_factory=dict)

    def resource_attributes(self) -> dict[str, str]:
        attrs = {
            "service.name": self.service_name,
            "service.version": _version(),
        }
        if self.worker_id:
            attrs["neocell.worker_id"] = self.worker_id
        attrs.update(self.extra_attributes)
        return attrs

    def resolved_jsonl_path(self) -> str | None:
        """Path for JSONL output, defaulting per service when unset.

        Workers get their own file: four processes appending to one file
        interleaved is unreadable, and separate files are what you want
        when comparing per-SPE behaviour anyway.
        """
        if not self.jsonl:
            return None
        if self.jsonl_path:
            return str(self.jsonl_path)
        suffix = self.worker_id or "ppe"
        return f"neocell-{self.service_name}-{suffix}.jsonl"


def _version() -> str:
    try:
        from neocell.version import VERSION

        return VERSION
    except Exception:  # pragma: no cover
        return "unknown"


def config_from_env(base: TelemetryConfig | None = None) -> TelemetryConfig:
    """Honour the small set of environment variables worth honouring.

    Deliberately not the full ``OTEL_*`` convention: most of it assumes
    a collector on a network, and this project is single-machine.
    """
    cfg = base or TelemetryConfig()
    if os.environ.get("NEOCELL_OTEL") == "0":
        cfg.enabled = False
    if os.environ.get("NEOCELL_OTEL_JSONL") == "0":
        cfg.jsonl = False
    if os.environ.get("NEOCELL_OTEL_CONSOLE") == "1":
        cfg.console_export = True
        cfg.enabled = True
    path = os.environ.get("NEOCELL_OTEL_JSONL_PATH")
    if path:
        cfg.jsonl_path = path
    return cfg


class Telemetry:
    """Tracer, meter and the instruments NeoCell records.

    Constructing this is safe with or without the SDK; every instrument
    attribute is either a real OTel instrument or a no-op with the same
    surface, so call sites never branch on availability.
    """

    def __init__(self, config: TelemetryConfig | None = None) -> None:
        self.config = config or TelemetryConfig()
        self.enabled = bool(self.config.enabled and OTEL_AVAILABLE)
        self._provider: Any = None
        self._meter_provider: Any = None
        self._tracer: Any = None
        self._meter: Any = None
        self._instruments: dict[str, Any] = {}
        self._shutdown: list[Any] = []
        if self.enabled:
            self._setup()

    # -- setup ---------------------------------------------------------
    def _setup(self) -> None:
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        resource = Resource.create(self.config.resource_attributes())

        # Providers are held on the instance rather than installed
        # globally: OTel refuses to replace a global provider, so a second
        # Telemetry in the same process would otherwise silently lose its
        # exporter. Propagation still works because it flows through
        # contextvars, not the global provider.
        self._provider = TracerProvider(resource=resource)
        self._provider.add_span_processor(
            BatchSpanProcessor(self._build_span_exporter())
        )

        readers = []
        reader = self._build_metric_reader()
        if reader is not None:
            readers.append(reader)
        self._meter_provider = MeterProvider(resource=resource, metric_readers=readers)

        self._tracer = self._provider.get_tracer("neocell")
        self._meter = self._meter_provider.get_meter("neocell")
        self._build_instruments()
        self._shutdown = [self._provider, self._meter_provider]

    def _build_span_exporter(self) -> Any:
        """JSONL file if configured, else stderr, else discard.

        No network exporter exists on purpose: this project is
        single-machine, so telemetry goes to files and the terminal.
        """
        jsonl = self.config.resolved_jsonl_path()
        if jsonl:
            return JsonlSpanExporter(jsonl)
        if self.config.console_export:
            return _console_exporter()
        return _DiscardExporter()

    def _build_metric_reader(self) -> Any:
        """Return a reader only when a sink is configured.

        With no sink we register no reader at all: cheaper than a
        discarding reader, and the SDK tolerates it.
        """
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

        jsonl = self.config.resolved_jsonl_path()
        if jsonl:
            return PeriodicExportingMetricReader(
                JsonlMetricExporter(jsonl, self.config.service_name),
                export_interval_millis=self.config.export_interval_ms,
            )
        if self.config.console_export:
            return PeriodicExportingMetricReader(
                _console_metric_exporter(),
                export_interval_millis=self.config.export_interval_ms,
            )
        return None

    def _build_instruments(self) -> None:
        meter = self._meter
        self._instruments = {
            M_TASK_QUEUE_SECONDS: meter.create_histogram(
                M_TASK_QUEUE_SECONDS,
                unit="s",
                description="Time a task waits for a free SPE",
            ),
            M_TASK_EXEC_SECONDS: meter.create_histogram(
                M_TASK_EXEC_SECONDS,
                unit="s",
                description="Time inside the SPE op",
            ),
            M_TASK_STAGE_SECONDS: meter.create_histogram(
                M_TASK_STAGE_SECONDS,
                unit="s",
                description="Time copying input into the local store",
            ),
            M_TASK_TRANSFER_SECONDS: meter.create_histogram(
                M_TASK_TRANSFER_SECONDS,
                unit="s",
                description="Modelled transfer time for one task",
            ),
            M_TASK_TOTAL_SECONDS: meter.create_histogram(
                M_TASK_TOTAL_SECONDS,
                unit="s",
                description="Queue wait plus execution for one task",
            ),
            M_TASK_INPUT_BYTES: meter.create_histogram(
                M_TASK_INPUT_BYTES,
                unit="By",
                description="Input bytes staged to a worker",
            ),
            M_TASK_OUTPUT_BYTES: meter.create_histogram(
                M_TASK_OUTPUT_BYTES,
                unit="By",
                description="Output bytes returned by a worker",
            ),
            M_TRANSFER_BYTES: meter.create_counter(
                M_TRANSFER_BYTES,
                unit="By",
                description="Bytes crossing the PPE/SPE boundary",
            ),
            M_TRANSFER_SECONDS: meter.create_counter(
                M_TRANSFER_SECONDS,
                unit="s",
                description="Modelled transfer time, cumulative",
            ),
            M_TASKS: meter.create_counter(
                M_TASKS, description="Tasks completed, by op and status"
            ),
            M_TASK_FAILURES: meter.create_counter(
                M_TASK_FAILURES, description="Tasks that failed, by op and reason"
            ),
            M_RETRIES: meter.create_counter(
                M_RETRIES, description="Task retries, by op"
            ),
            M_STORE_HIGH_WATER_BYTES: meter.create_histogram(
                M_STORE_HIGH_WATER_BYTES,
                unit="By",
                description="Peak local store residency per task",
            ),
            M_STORE_BYTES: meter.create_histogram(
                M_STORE_BYTES,
                unit="By",
                description="Configured local store capacity",
            ),
            M_JOB_WALL_SECONDS: meter.create_histogram(
                M_JOB_WALL_SECONDS,
                unit="s",
                description="End-to-end job wall time",
            ),
            M_WORKER_BUSY_SECONDS: meter.create_counter(
                M_WORKER_BUSY_SECONDS,
                unit="s",
                description="Worker busy time, cumulative",
            ),
            M_EXECUTIONS: meter.create_counter(
                M_EXECUTIONS, description="Op executions on an SPE"
            ),
            M_EXCEPTIONS: meter.create_counter(
                M_EXCEPTIONS, description="Exceptions by type"
            ),
        }

    # -- spans ---------------------------------------------------------
    @contextmanager
    def span(
        self, name: str, attributes: dict[str, Any] | None = None, **span_kwargs: Any
    ):
        """Start a span and make it the current one.

        Current-ness is what lets ``inject_context`` see it, which is how
        a task span in the PPE becomes the parent of an ``exec`` span in
        an SPE process.
        """
        if not self.enabled:
            yield _NOOP_SPAN
            return
        with self._tracer.start_as_current_span(
            name, attributes=_clean(attributes), **span_kwargs
        ) as span_:
            yield span_

    def start_span(self, name: str, attributes: dict[str, Any] | None = None):
        """Start a span without making it current.

        Prefer :meth:`span`; this exists for call sites that manage their
        own lifetime and must therefore propagate context themselves.
        """
        if not self.enabled:
            return _NOOP_SPAN
        return self._tracer.start_span(name, attributes=_clean(attributes))

    @property
    def tracer(self) -> Any:
        return self._tracer

    @property
    def provider(self) -> Any:
        """The TracerProvider, exposed so tests can attach exporters."""
        return self._provider

    @property
    def meter_provider(self) -> Any:
        return self._meter_provider

    def inject_context(self, carrier: dict[str, str]) -> dict[str, str]:
        """Write W3C trace context into a carrier for the child process."""
        if not self.enabled or inject is None:
            return {}
        inject(carrier)
        return carrier

    def extract_context(self, carrier: dict[str, str] | None):
        """Rebuild a parent context from a carrier received from the PPE."""
        if not self.enabled or extract is None or not carrier:
            return None
        return extract(carrier)

    # -- metrics -------------------------------------------------------
    def instrument(self, name: str) -> Any:
        return self._instruments.get(name, _NoopInstrument())

    def record(self, name: str, value: float, attributes: dict[str, Any] | None = None) -> None:
        inst = self._instruments.get(name)
        if inst is None:
            return
        attrs = _clean(attributes)
        if isinstance(inst, _NoopInstrument):
            return
        if hasattr(inst, "record"):
            inst.record(value, attrs)
        else:
            inst.add(value, attrs)

    def add(self, name: str, value: float, attributes: dict[str, Any] | None = None) -> None:
        inst = self._instruments.get(name)
        if inst is None or isinstance(inst, _NoopInstrument):
            return
        inst.add(value, _clean(attributes))

    def span_error(self, span: Any, exc: BaseException) -> None:
        if not self.enabled or not span.is_recording():
            return
        from opentelemetry.trace import Status, StatusCode

        span.record_exception(exc)
        span.set_status(Status(StatusCode.ERROR, str(exc)))

    # -- lifecycle -----------------------------------------------------
    def shutdown(self) -> None:
        """Flush and tear down, tolerating repeated or failed calls."""
        for provider in self._shutdown:
            for method in ("force_flush", "shutdown"):
                fn = getattr(provider, method, None)
                if fn is None:
                    continue
                try:
                    fn()
                except Exception:  # pragma: no cover - best effort on exit
                    pass
        self._shutdown = []
        self.enabled = False


def _console_exporter() -> Any:
        """Console span exporter that writes to stderr, never stdout.

        An SPE's stdout is the protocol pipe. The OTel console exporters
        default to stdout, which would interleave span JSON with framed
        replies and desynchronise the decoder permanently. stderr is
        already the worker's log channel and is drained by the adapter,
        so it is the only safe sink.
        """
        from opentelemetry.sdk.trace.export import ConsoleSpanExporter

        return ConsoleSpanExporter(out=sys.stderr)


def _console_metric_exporter() -> Any:
    """Console metric exporter bound to stderr; see :func:`_console_exporter`."""
    from opentelemetry.sdk.metrics.export import ConsoleMetricExporter

    return ConsoleMetricExporter(out=sys.stderr)


class JsonlSpanExporter:
    """Write finished spans as one JSON object per line.

    The local replacement for a collector. Append-only and flushed per
    record, so a crashed run still leaves everything up to that point,
    which is exactly when the record matters most.
    """

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = str(path)
        directory = os.path.dirname(self.path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        self._fh = open(self.path, "a", encoding="utf-8")

    def export(self, spans: Any) -> Any:
        from opentelemetry.sdk.trace.export import SpanExportResult

        try:
            for span in spans:
                self._fh.write(json.dumps(_span_to_dict(span), sort_keys=True) + "\n")
            self._fh.flush()
            return SpanExportResult.SUCCESS
        except Exception:
            return SpanExportResult.FAILURE

    def force_flush(self, timeout_millis: int = 30_000) -> bool:
        try:
            self._fh.flush()
            return True
        except Exception:
            return False

    def shutdown(self) -> None:
        try:
            self._fh.flush()
            self._fh.close()
        except Exception:
            pass


class JsonlMetricExporter:
    """Write a metrics snapshot as one JSON object per line."""

    def __init__(self, path: str | os.PathLike[str], service: str) -> None:
        self.path = str(path)
        self.service = service
        # PeriodicExportingMetricReader reads these to pick aggregation
        # temporality; without them it refuses to construct the reader.
        # (no imports needed; see the attribute note below)

        # PeriodicExportingMetricReader reads these to choose aggregation
        # temporality. None means "use the SDK default", which is what the
        # built-in exporters do.
        self._preferred_temporality: dict | None = None
        self._preferred_aggregation: dict | None = None
        directory = os.path.dirname(self.path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        self._fh = open(self.path, "a", encoding="utf-8")
        self._fh.write(json.dumps({"kind": "header", "service": service}) + "\n")
        self._fh.flush()

    def export(
        self, metrics_data: Any, timeout_millis: int = 10_000, **kwargs: Any
    ) -> Any:
        from opentelemetry.sdk.metrics.export import MetricExportResult

        try:
            for record in _metrics_to_dicts(metrics_data, self.service):
                self._fh.write(json.dumps(record, sort_keys=True) + "\n")
            self._fh.flush()
            return MetricExportResult.SUCCESS
        except Exception:
            return MetricExportResult.FAILURE

    def force_flush(self, timeout_millis: int = 10_000) -> bool:
        try:
            self._fh.flush()
            return True
        except Exception:
            return False

    def shutdown(self, timeout_millis: int = 30_000, **kwargs: Any) -> None:
        try:
            self._fh.flush()
            self._fh.close()
        except Exception:
            pass


def _span_to_dict(span: Any) -> dict[str, Any]:
    ctx = span.get_span_context()
    parent = span.parent
    return {
        "kind": "span",
        "name": span.name,
        "trace_id": f"0x{ctx.trace_id:032x}" if ctx else None,
        "span_id": f"0x{ctx.span_id:016x}" if ctx else None,
        "parent_id": f"0x{parent.span_id:016x}" if parent else None,
        "start_unix_nano": span.start_time,
        "end_unix_nano": span.end_time,
        "duration_ns": (span.end_time - span.start_time)
        if span.start_time and span.end_time
        else None,
        "status": span.status.status_code.name if span.status else None,
        "attributes": dict(span.attributes or {}),
        "events": [
            {"name": e.name, "attributes": dict(e.attributes or {})}
            for e in (span.events or ())
        ],
        "resource": dict(span.resource.attributes) if span.resource else {},
    }


def _metrics_to_dicts(metrics_data: Any, service: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if metrics_data is None:
        return out
    for resource_metric in getattr(metrics_data, "resource_metrics", []):
        resource = dict(resource_metric.resource.attributes) if resource_metric.resource else {}
        for scope_metric in getattr(resource_metric, "scope_metrics", []):
            for metric in getattr(scope_metric, "metrics", []):
                data = metric.data
                points: list[dict[str, Any]] = []
                for point in getattr(data, "data_points", []) or []:
                    value = getattr(point, "sum", None)
                    if value is None:
                        value = getattr(point, "count", None)
                    if value is None:
                        value = getattr(point, "value", None)
                    if hasattr(value, "double_gauge_value"):
                        value = value.double_gauge_value
                    attributes = dict(getattr(point, "attributes", {}) or {})
                    entry = {"attributes": attributes}
                    if value is not None:
                        entry["value"] = value
                    for extra in ("count", "sum", "min", "max", "mean"):
                        v = getattr(point, extra, None)
                        if v is not None and extra not in ("count", "sum"):
                            entry[extra] = v
                    bucket_counts = getattr(point, "bucket_counts", None)
                    if bucket_counts:
                        entry["count"] = sum(bucket_counts)
                        entry["sum"] = getattr(point, "sum", None)
                    points.append(entry)
                out.append(
                    {
                        "kind": "metric",
                        "service": service,
                        "name": metric.name,
                        "unit": getattr(data, "unit", None),
                        "description": metric.description,
                        "points": points,
                        "resource": resource,
                    }
                )
    return out


class _DiscardExporter:
    """Span exporter that does nothing, used when no sink is configured.

    BatchSpanProcessor requires an exporter, so telemetry with metrics but
    no trace sink still needs one; this absorbs spans silently.
    """

    def export(self, spans: Any) -> Any:
        from opentelemetry.sdk.trace.export import SpanExportResult

        return SpanExportResult.SUCCESS

    def shutdown(self) -> None: ...

    def force_flush(self, timeout_millis: int = 30_000) -> bool:
        return True


def _clean(attributes: dict[str, Any] | None) -> dict[str, Any] | None:
    """Drop ``None`` values, which OTel would otherwise reject."""
    if not attributes:
        return None
    return {k: v for k, v in attributes.items() if v is not None}


_DISABLED = Telemetry(TelemetryConfig(enabled=False))


def disabled() -> Telemetry:
    """Shared no-op instance for call sites that have no configuration."""
    return _DISABLED


def span_status_ok() -> Any:
    if not OTEL_AVAILABLE:
        return None
    from opentelemetry.trace import Status, StatusCode

    return Status(StatusCode.OK)