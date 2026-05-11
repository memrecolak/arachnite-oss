"""Unit tests for StructuredLogger, log sinks, and ObservabilityMixin."""

from __future__ import annotations

import asyncio
import io
import json
import time

import pytest

from arachnite.logging import (
    BaseLogSink,
    JSONLogSink,
    LogLevel,
    NullLogSink,
    ObservabilityMixin,
    StdoutLogSink,
    StructuredLogger,
)
from arachnite.models import LogEvent

# ── Helpers ───────────────────────────────────────────────────────────────────

def _event(
    level: LogLevel = LogLevel.INFO,
    message: str = "hello",
    node_id: str = "TestNode",
    tick: int = 1,
    data: dict | None = None,
) -> LogEvent:
    return LogEvent(
        level         = level,
        node_id       = node_id,
        agent_node_id = "local",
        tick          = tick,
        message       = message,
        data          = data or {},
        timestamp     = time.monotonic(),
    )


# ── BaseLogSink.accepts ───────────────────────────────────────────────────────

class TestBaseLogSinkAccepts:
    def test_accepts_equal_level(self) -> None:
        sink = NullLogSink(level=LogLevel.INFO)
        assert sink.accepts(_event(LogLevel.INFO))

    def test_accepts_higher_level(self) -> None:
        sink = NullLogSink(level=LogLevel.INFO)
        assert sink.accepts(_event(LogLevel.ERROR))

    def test_rejects_lower_level(self) -> None:
        sink = NullLogSink(level=LogLevel.WARNING)
        assert not sink.accepts(_event(LogLevel.DEBUG))


# ── NullLogSink ───────────────────────────────────────────────────────────────

class TestNullLogSink:
    @pytest.mark.asyncio
    async def test_emit_does_not_raise(self) -> None:
        sink = NullLogSink()
        await sink.emit(_event())   # must not raise


# ── StdoutLogSink ─────────────────────────────────────────────────────────────

class TestStdoutLogSink:
    @pytest.mark.asyncio
    async def test_emit_below_level_is_silent(self, capsys) -> None:
        sink = StdoutLogSink(level=LogLevel.ERROR, colour=False)
        await sink.emit(_event(LogLevel.DEBUG))
        assert capsys.readouterr().out == ""

    @pytest.mark.asyncio
    async def test_emit_at_level_produces_output(self, capsys) -> None:
        sink = StdoutLogSink(level=LogLevel.INFO, colour=False)
        await sink.emit(_event(LogLevel.INFO, message="ping"))
        out = capsys.readouterr().out
        assert "ping" in out

    @pytest.mark.asyncio
    async def test_show_data_includes_data(self, capsys) -> None:
        sink = StdoutLogSink(level=LogLevel.DEBUG, colour=False, show_data=True)
        await sink.emit(_event(data={"val": 42}))
        out = capsys.readouterr().out
        assert "42" in out

    @pytest.mark.asyncio
    async def test_no_data_no_extra_field(self, capsys) -> None:
        sink = StdoutLogSink(level=LogLevel.DEBUG, colour=False, show_data=True)
        await sink.emit(_event(data={}))
        out = capsys.readouterr().out
        assert "hello" in out


# ── JSONLogSink ───────────────────────────────────────────────────────────────

class TestJSONLogSink:
    @pytest.mark.asyncio
    async def test_emit_valid_json(self) -> None:
        buf = io.StringIO()
        sink = JSONLogSink(stream=buf, level=LogLevel.DEBUG)
        await sink.emit(_event(message="test-msg", data={"x": 1}))
        record = json.loads(buf.getvalue())
        assert record["msg"] == "test-msg"
        assert record["x"] == 1
        assert record["level"] == "INFO"

    @pytest.mark.asyncio
    async def test_emit_below_level_writes_nothing(self) -> None:
        buf = io.StringIO()
        sink = JSONLogSink(stream=buf, level=LogLevel.ERROR)
        await sink.emit(_event(LogLevel.DEBUG))
        assert buf.getvalue() == ""

    @pytest.mark.asyncio
    async def test_default_stream_is_stdout(self) -> None:
        sink = JSONLogSink()
        assert sink._stream is not None

    @pytest.mark.asyncio
    async def test_includes_node_id_and_tick(self) -> None:
        buf = io.StringIO()
        sink = JSONLogSink(stream=buf)
        await sink.emit(_event(node_id="MyNode", tick=42))
        record = json.loads(buf.getvalue())
        assert record["node_id"] == "MyNode"
        assert record["tick"] == 42


# ── StructuredLogger ──────────────────────────────────────────────────────────

class TestStructuredLogger:
    @pytest.mark.asyncio
    async def test_info_reaches_sink(self) -> None:
        received: list[LogEvent] = []

        class CaptureSink(BaseLogSink):
            async def emit(self, event: LogEvent) -> None:
                received.append(event)

        logger = StructuredLogger("Node1", sinks=[CaptureSink()])
        logger.info("hello", x=1)
        await asyncio.sleep(0)   # let fire-and-forget task run
        assert len(received) == 1
        assert received[0].message == "hello"
        assert received[0].data["x"] == 1

    @pytest.mark.asyncio
    async def test_debug_filtered_by_sink_level(self) -> None:
        received: list[LogEvent] = []

        class CaptureSink(BaseLogSink):
            async def emit(self, event: LogEvent) -> None:
                if self.accepts(event):
                    received.append(event)

        logger = StructuredLogger("Node1", sinks=[CaptureSink(level=LogLevel.WARNING)])
        logger.debug("low")
        await asyncio.sleep(0)
        assert len(received) == 0

    @pytest.mark.asyncio
    async def test_all_levels_emit(self) -> None:
        received: list[LogEvent] = []

        class CaptureSink(BaseLogSink):
            async def emit(self, event: LogEvent) -> None:
                received.append(event)

        logger = StructuredLogger("N", sinks=[CaptureSink(level=LogLevel.DEBUG)])
        logger.debug("d")
        logger.info("i")
        logger.warning("w")
        logger.error("e")
        logger.critical("c")
        await asyncio.sleep(0)
        levels = [ev.level for ev in received]
        assert LogLevel.DEBUG    in levels
        assert LogLevel.INFO     in levels
        assert LogLevel.WARNING  in levels
        assert LogLevel.ERROR    in levels
        assert LogLevel.CRITICAL in levels

    def test_set_tick_updates_tick(self) -> None:
        logger = StructuredLogger("N")
        logger._set_tick(99)
        assert logger._tick == 99

    def test_emit_outside_async_context_does_not_raise(self) -> None:
        """StructuredLogger.info() outside an event loop must be silent, not crash."""
        logger = StructuredLogger("N", sinks=[NullLogSink()])
        logger.info("safe")   # must not raise


# ── ObservabilityMixin ────────────────────────────────────────────────────────

class TestObservabilityMixin:
    def _mixin(self) -> ObservabilityMixin:
        return ObservabilityMixin()

    def test_increment_default_amount(self) -> None:
        m = self._mixin()
        m.increment("reads")
        assert m._counters["reads"] == 1

    def test_increment_custom_amount(self) -> None:
        m = self._mixin()
        m.increment("bytes", 1024)
        assert m._counters["bytes"] == 1024

    def test_increment_accumulates(self) -> None:
        m = self._mixin()
        m.increment("x")
        m.increment("x")
        m.increment("x")
        assert m._counters["x"] == 3

    def test_observe_records_duration(self) -> None:
        m = self._mixin()
        with m.observe("read_latency"):
            time.sleep(0.01)
        assert len(m._histograms["read_latency"]) == 1
        assert m._histograms["read_latency"][0] >= 0.009

    def test_observe_multiple_samples(self) -> None:
        m = self._mixin()
        for _ in range(3):
            with m.observe("op"):
                pass
        assert len(m._histograms["op"]) == 3

    def test_metrics_returns_counters(self) -> None:
        m = self._mixin()
        m.increment("reqs", 5)
        result = m.metrics()
        assert result["reqs"] == 5

    def test_metrics_returns_histogram_stats(self) -> None:
        m = self._mixin()
        with m.observe("lat"):
            pass
        result = m.metrics()
        assert "lat_count" in result
        assert "lat_sum" in result
        assert "lat_avg" in result
        assert "lat_min" in result
        assert "lat_max" in result

    def test_metrics_empty_histogram_excluded(self) -> None:
        m = self._mixin()
        # Add a histogram key with no samples (shouldn't happen normally, but guard)
        from collections import deque
        m._histograms["empty"] = deque(maxlen=1024)
        result = m.metrics()
        assert "empty_count" not in result

    def test_metrics_text_contains_counter(self) -> None:
        m = self._mixin()
        m.increment("packets")
        text = m.metrics_text()
        assert "packets" in text
        assert "counter" in text

    def test_metrics_text_contains_histogram(self) -> None:
        m = self._mixin()
        with m.observe("latency"):
            pass
        text = m.metrics_text()
        assert "latency" in text
        assert "summary" in text

    def test_metrics_text_empty_histogram_excluded(self) -> None:
        m = self._mixin()
        from collections import deque
        m._histograms["empty_key"] = deque(maxlen=1024)
        text = m.metrics_text()
        assert "empty_key" not in text  # empty histogram is skipped

    def test_metrics_text_prefix(self) -> None:
        m = self._mixin()
        m.increment("x")
        text = m.metrics_text(prefix="arachnite_")
        assert "arachnite_" in text

    def test_metrics_text_uses_node_id_attr(self) -> None:
        m = self._mixin()
        m.node_id = "MyNode"  # type: ignore[attr-defined]
        m.increment("x")
        text = m.metrics_text()
        assert "MyNode" in text
