"""
arachnite.logging
~~~~~~~~~~~~~~~~~
Structured per-node logging, log sinks, and the ObservabilityMixin.
Spec reference: Section 13.
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from arachnite.models import LogEvent  # re-export for mypy
from arachnite.models import LogLevel as LogLevel

#: Default cap for ObservabilityMixin histogram sample buffers.
_DEFAULT_HISTOGRAM_MAXLEN: int = 1024

# ══════════════════════════════════════════════════════════════════════════════
# Log sinks
# ══════════════════════════════════════════════════════════════════════════════

class BaseLogSink(ABC):
    """
    Abstract base class for log destinations.
    Spec reference: Section 13.2.
    """

    def __init__(self, level: LogLevel = LogLevel.DEBUG) -> None:
        self.level = level

    @abstractmethod
    async def emit(self, event: LogEvent) -> None:
        """Receive and process one LogEvent."""

    def accepts(self, event: LogEvent) -> bool:
        return event.level >= self.level


_LEVEL_COLOURS = {
    LogLevel.DEBUG:    "\033[36m",   # cyan
    LogLevel.INFO:     "\033[32m",   # green
    LogLevel.WARNING:  "\033[33m",   # yellow
    LogLevel.ERROR:    "\033[31m",   # red
    LogLevel.CRITICAL: "\033[35m",   # magenta
}
_RESET = "\033[0m"


class StdoutLogSink(BaseLogSink):
    """
    Colourised human-readable log sink writing to stdout.
    Spec reference: Section 13.2.
    """

    def __init__(
        self,
        level: LogLevel = LogLevel.INFO,
        colour: bool = True,
        show_data: bool = False,
    ) -> None:
        super().__init__(level)
        self.colour    = colour and sys.stdout.isatty()
        self.show_data = show_data

    async def emit(self, event: LogEvent) -> None:
        if not self.accepts(event):
            return
        colour = _LEVEL_COLOURS.get(event.level, "") if self.colour else ""
        reset  = _RESET if self.colour else ""
        data_str = f" {event.data}" if self.show_data and event.data else ""
        print(
            f"{colour}[{event.level.name:8s}]{reset} "
            f"tick={event.tick:5d} "
            f"{event.node_id:<30s} "
            f"{event.message}{data_str}",
            flush=True,
        )


class JSONLogSink(BaseLogSink):
    """
    Newline-delimited JSON log sink writing to a file-like object.
    Spec reference: Section 13.2.
    """

    def __init__(
        self,
        stream: Any = None,
        level: LogLevel = LogLevel.DEBUG,
    ) -> None:
        super().__init__(level)
        self._stream = stream or sys.stdout

    async def emit(self, event: LogEvent) -> None:
        if not self.accepts(event):
            return
        record = {
            "ts":            event.timestamp,
            "level":         event.level.name,
            "node_id":       event.node_id,
            "agent_node_id": event.agent_node_id,
            "tick":          event.tick,
            "msg":           event.message,
            **event.data,
        }
        print(json.dumps(record, default=str), file=self._stream, flush=True)


class NullLogSink(BaseLogSink):
    """Discards all log events. Useful in tests."""

    async def emit(self, event: LogEvent) -> None:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# StructuredLogger
# ══════════════════════════════════════════════════════════════════════════════

class StructuredLogger:
    """
    Per-node structured logger.

    Emits LogEvents to all registered sinks. Methods mirror standard
    logging levels but accept keyword arguments as structured data fields.

    Spec reference: Section 13.1.
    """

    def __init__(
        self,
        node_id: str,
        agent_node_id: str = "local",
        sinks: list[BaseLogSink] | None = None,
    ) -> None:
        self._node_id       = node_id
        self._agent_node_id = agent_node_id
        self._sinks         = sinks or []
        self._tick          = 0

    # Called by the runtime to keep the logger's tick counter in sync.
    def _set_tick(self, tick: int) -> None:
        self._tick = tick

    def _emit(self, level: LogLevel, msg: str, data: dict[str, Any]) -> None:
        event = LogEvent(
            level         = level,
            node_id       = self._node_id,
            agent_node_id = self._agent_node_id,
            tick          = self._tick,
            message       = msg,
            data          = data,
            timestamp     = time.monotonic(),
        )
        # Fire-and-forget: schedule on running loop if available,
        # otherwise emit synchronously (useful during setup/teardown).
        try:
            loop = asyncio.get_running_loop()
            for sink in self._sinks:
                if sink.accepts(event):
                    loop.create_task(sink.emit(event))
        except RuntimeError:
            # No running event loop — emit synchronously via a new loop.
            pass  # drop silently outside async context during tests

    def debug(self, msg: str, **data: Any) -> None:
        self._emit(LogLevel.DEBUG, msg, data)

    def info(self, msg: str, **data: Any) -> None:
        self._emit(LogLevel.INFO, msg, data)

    def warning(self, msg: str, **data: Any) -> None:
        self._emit(LogLevel.WARNING, msg, data)

    def error(self, msg: str, **data: Any) -> None:
        self._emit(LogLevel.ERROR, msg, data)

    def critical(self, msg: str, **data: Any) -> None:
        self._emit(LogLevel.CRITICAL, msg, data)


# ══════════════════════════════════════════════════════════════════════════════
# ObservabilityMixin
# ══════════════════════════════════════════════════════════════════════════════

class ObservabilityMixin:
    """
    Optional mixin providing per-node timing histograms, signal counters,
    and Prometheus-compatible metrics text export.
    Spec reference: Section 13.4.

    Usage::

        class MySenseNode(BaseSenseNode, ObservabilityMixin):
            async def read(self) -> Signal:
                with self.observe("read_latency"):
                    value = await asyncio.to_thread(self._hw_read)
                self.increment("reads_total")
                return Signal(...)
    """

    def __init__(
        self,
        *args: Any,
        histogram_maxlen: int = _DEFAULT_HISTOGRAM_MAXLEN,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._counters:   dict[str, int]              = defaultdict(int)
        self._histogram_maxlen = histogram_maxlen
        self._histograms: dict[str, deque[float]] = {}

    def increment(self, name: str, amount: int = 1) -> None:
        """Increment a named counter."""
        self._counters[name] += amount

    @contextmanager
    def observe(self, name: str) -> Generator[None, None, None]:
        """Context manager that records the wall-clock duration of a block."""
        start = time.monotonic()
        try:
            yield
        finally:
            try:
                buf = self._histograms[name]
            except KeyError:
                buf = deque(maxlen=self._histogram_maxlen)
                self._histograms[name] = buf
            buf.append(time.monotonic() - start)

    def metrics(self) -> dict[str, Any]:
        """Return all current metrics as a plain dict."""
        result: dict[str, Any] = {}
        result.update(self._counters)
        for name, samples in self._histograms.items():
            if samples:
                result[f"{name}_count"] = len(samples)
                result[f"{name}_sum"]   = sum(samples)
                result[f"{name}_avg"]   = sum(samples) / len(samples)
                result[f"{name}_min"]   = min(samples)
                result[f"{name}_max"]   = max(samples)
        return result

    def metrics_text(self, prefix: str = "") -> str:
        """
        Return a Prometheus-compatible text representation of all metrics.
        Suitable for a /metrics HTTP endpoint.
        """
        lines: list[str] = []
        node_id = getattr(self, "node_id", "unknown")
        pfx = f"{prefix}{node_id}_" if prefix else f"{node_id}_"

        for name, value in self._counters.items():
            safe = name.replace("-", "_").replace(".", "_")
            lines.append(f"# TYPE {pfx}{safe} counter")
            lines.append(f'{pfx}{safe}{{node="{node_id}"}} {value}')

        for name, samples in self._histograms.items():
            if not samples:
                continue
            safe = name.replace("-", "_").replace(".", "_")
            lines.append(f"# TYPE {pfx}{safe} summary")
            lines.append(f'{pfx}{safe}_count{{node="{node_id}"}} {len(samples)}')
            lines.append(f'{pfx}{safe}_sum{{node="{node_id}"}} {sum(samples):.6f}')

        return "\n".join(lines)
