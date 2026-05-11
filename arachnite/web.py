"""
arachnite.web
~~~~~~~~~~~~~
SignalDashboard: real-time web dashboard for Arachnite agents.

Subscribes to the SignalBus (all signal kinds) and acts as a log sink so
it captures every event flowing through the framework.  Two output channels:

* **Browser** — a self-contained HTML page served at http://host:port/
  that streams events live via WebSocket, colour-coded by module, with
  a filter bar and live event-count badges.
* **File** — every event is appended as a plain-text line to a log file,
  human-readable and grep-friendly.

Usage::

    from arachnite import SignalBus, ArachniteRuntime
    from arachnite.web import SignalDashboard

    bus = SignalBus()
    dashboard = SignalDashboard(bus, log_file="run.log", port=7070)

    runtime = ArachniteRuntime(..., log_sinks=[dashboard])
    await dashboard.start()
    await runtime.start()

    # Open http://localhost:7070 in a browser.

    await runtime.stop()
    await dashboard.stop()

Optional dependencies: fastapi, uvicorn
Install: pip install "arachnite[web]"
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime
import json
from collections import deque
from pathlib import Path
from typing import Any

from arachnite.bus import SignalBus
from arachnite.logging import BaseLogSink, LogLevel
from arachnite.models import LogEvent, Signal

# ══════════════════════════════════════════════════════════════════════════════
# Embedded dashboard HTML
# ══════════════════════════════════════════════════════════════════════════════

_DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Arachnite Dashboard</title>
<style>
:root {
  --bg:      #0d0d1a;
  --surface: #16162a;
  --border:  #252545;
  --text:    #d0d0f0;
  --muted:   #5a5a90;
  --green:   #4caf82;
  --red:     #e05555;
  --yellow:  #e0b040;
  --cyan:    #40c0e0;
  --magenta: #c060e0;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  background: var(--bg);
  color: var(--text);
  font-family: 'Cascadia Code','Fira Code','Consolas',monospace;
  font-size: 12.5px;
  height: 100vh;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

/* ── Header ──────────────────────────────────────────────────────────────── */
#header {
  background: var(--surface);
  border-bottom: 1px solid var(--border);
  padding: 9px 16px;
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
  flex-shrink: 0;
}
#title { font-size: 14px; font-weight: 600; white-space: nowrap; }
#status {
  font-size: 11px;
  padding: 2px 9px;
  border-radius: 10px;
  background: #112211;
  color: var(--green);
  white-space: nowrap;
}
#status.disc { background: #221111; color: var(--red); }
#filter {
  flex: 1;
  min-width: 160px;
  background: var(--bg);
  border: 1px solid var(--border);
  color: var(--text);
  padding: 4px 10px;
  border-radius: 4px;
  outline: none;
  font-family: inherit;
  font-size: 12px;
}
#filter:focus { border-color: var(--muted); }
.btn {
  padding: 4px 12px;
  border-radius: 4px;
  border: 1px solid var(--border);
  background: var(--surface);
  color: var(--text);
  cursor: pointer;
  font-size: 11.5px;
  font-family: inherit;
  white-space: nowrap;
}
.btn:hover { border-color: var(--muted); }
#btn-pause.on { border-color: var(--yellow); color: var(--yellow); }
#count { font-size: 11px; color: var(--muted); white-space: nowrap; margin-left: auto; }

/* ── Stats bar ───────────────────────────────────────────────────────────── */
#stats {
  background: var(--surface);
  border-bottom: 1px solid var(--border);
  padding: 5px 16px;
  display: flex;
  flex-wrap: wrap;
  gap: 5px;
  min-height: 32px;
  flex-shrink: 0;
  align-items: center;
}
.badge {
  font-size: 11px;
  padding: 2px 8px;
  border-radius: 10px;
  border: 1px solid;
  cursor: default;
  white-space: nowrap;
}

/* ── Feed ────────────────────────────────────────────────────────────────── */
#feed { flex: 1; overflow-y: auto; }
.row {
  display: flex;
  align-items: baseline;
  gap: 8px;
  padding: 2px 16px;
  border-bottom: 1px solid transparent;
}
.row:hover { background: var(--surface); }
.row.hidden { display: none; }
.ts   { color: var(--muted); white-space: nowrap; width: 88px; flex-shrink: 0; }
.tb   { font-size: 10px; padding: 1px 5px; border-radius: 3px; flex-shrink: 0; width: 32px; text-align: center; }
.tb-l { background: #12202a; color: var(--cyan); }
.tb-s { background: #122218; color: var(--green); }
.lb   { font-size: 10px; padding: 1px 6px; border-radius: 3px; flex-shrink: 0; min-width: 64px; text-align: center; }
.lv-DEBUG    { background: #14141e; color: #7070a8; }
.lv-INFO     { background: #12202a; color: var(--cyan); }
.lv-WARNING  { background: #241e08; color: var(--yellow); }
.lv-ERROR    { background: #240c0c; color: var(--red); }
.lv-CRITICAL { background: #200c24; color: var(--magenta); }
.src  { flex-shrink: 0; max-width: 190px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-weight: 500; }
.msg  { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; flex: 1; min-width: 0; }
.ext  { color: var(--muted); flex-shrink: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 280px; }

/* Scrollbar */
#feed::-webkit-scrollbar { width: 5px; }
#feed::-webkit-scrollbar-track { background: var(--bg); }
#feed::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
</style>
</head>
<body>

<div id="header">
  <span id="title">&#x1F578; Arachnite Dashboard</span>
  <span id="status" class="disc">&#9679; Connecting&hellip;</span>
  <input id="filter" type="search" placeholder="Filter by source, kind, message&hellip;"
         oninput="applyFilter()" autocomplete="off">
  <button class="btn" id="btn-pause" onclick="togglePause()">&#9646;&#9646; Pause</button>
  <button class="btn" onclick="clearFeed()">&#x2715; Clear</button>
  <span id="count">0 events</span>
</div>
<div id="stats"></div>
<div id="feed"></div>

<script>
const MAX_ROWS = 2000;
let paused  = false;
let total   = 0;
const stats = {};

const feedEl   = document.getElementById('feed');
const statsEl  = document.getElementById('stats');
const statusEl = document.getElementById('status');
const countEl  = document.getElementById('count');
const filterEl = document.getElementById('filter');
const pauseBtn = document.getElementById('btn-pause');

// ── Colour helpers ──────────────────────────────────────────────────────────
function hue(s) {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = s.charCodeAt(i) + ((h << 5) - h);
  return Math.abs(h) % 360;
}
function hsl(s, l=60) { return `hsl(${hue(s)},55%,${l}%)`; }

// ── Stats badges ────────────────────────────────────────────────────────────
function bumpStat(key) {
  stats[key] = (stats[key] || 0) + 1;
  let el = document.getElementById('b-' + key);
  if (!el) {
    el = document.createElement('span');
    el.id        = 'b-' + key;
    el.className = 'badge';
    const c = hsl(key);
    el.style.borderColor = c;
    el.style.color       = c;
    statsEl.appendChild(el);
  }
  el.textContent = key + ' \xd7' + stats[key];
}

// ── Timestamp ───────────────────────────────────────────────────────────────
function fmtTs(ts) {
  const d   = new Date(ts * 1000);
  const p   = n => String(n).padStart(2,'0');
  const ms  = String(d.getMilliseconds()).padStart(3,'0');
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}.${ms}`;
}

// ── Row builder ─────────────────────────────────────────────────────────────
function esc(s) {
  return String(s)
    .replace(/&/g,'&amp;').replace(/</g,'&lt;')
    .replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function addRow(rec) {
  let typeBadge, levelCell, src, msg, ext, statKey;

  if (rec.type === 'log') {
    typeBadge = '<span class="tb tb-l">LOG</span>';
    levelCell = `<span class="lb lv-${rec.level}">${rec.level}</span>`;
    src       = rec.node_id || '';
    msg       = esc(rec.message || '');
    ext       = Object.entries(rec.data || {})
                      .map(([k,v]) => `${esc(k)}=${esc(v)}`).join('  ');
    if (rec.tick) ext = `tick=${rec.tick}  ` + ext;
    statKey   = rec.node_id || 'unknown';
  } else {
    typeBadge = '<span class="tb tb-s">SIG</span>';
    const c   = hsl(rec.kind, 58);
    levelCell = `<span class="lb" style="color:${c};border:1px solid ${c};background:rgba(0,0,0,.3)">${esc(rec.kind)}</span>`;
    src       = rec.source || '';
    const v   = rec.value === null ? 'null' : esc(String(rec.value));
    msg       = `value=${v}`;
    const mex = Object.entries(rec.metadata || {}).map(([k,v]) => `${esc(k)}=${esc(v)}`).join('  ');
    ext       = `conf=${Number(rec.confidence).toFixed(2)}${mex ? '  '+mex : ''}`;
    statKey   = rec.kind || 'signal';
  }

  const sc  = hsl(src, 62);
  const raw = (src + (rec.message||rec.kind||'') + JSON.stringify(rec.data||rec.metadata||{})).toLowerCase();

  const row = document.createElement('div');
  row.className  = 'row';
  row.dataset.raw = raw;
  row.innerHTML   = `
    <span class="ts">${fmtTs(rec.ts)}</span>
    ${typeBadge}
    ${levelCell}
    <span class="src" style="color:${sc}" title="${esc(src)}">${esc(src)}</span>
    <span class="msg">${msg}</span>
    <span class="ext">${ext}</span>
  `;

  const q = filterEl.value.trim().toLowerCase();
  if (q && !raw.includes(q)) row.classList.add('hidden');

  feedEl.appendChild(row);
  while (feedEl.children.length > MAX_ROWS) feedEl.removeChild(feedEl.firstChild);

  total++;
  countEl.textContent = total + ' events';
  bumpStat(statKey);
  if (!paused) feedEl.scrollTop = feedEl.scrollHeight;
}

// ── Controls ────────────────────────────────────────────────────────────────
function applyFilter() {
  const q = filterEl.value.trim().toLowerCase();
  for (const row of feedEl.children) {
    row.classList.toggle('hidden', q !== '' && !row.dataset.raw.includes(q));
  }
}

function togglePause() {
  paused = !paused;
  pauseBtn.classList.toggle('on', paused);
  pauseBtn.innerHTML = paused ? '&#9654; Resume' : '&#9646;&#9646; Pause';
  if (!paused) feedEl.scrollTop = feedEl.scrollHeight;
}

function clearFeed() {
  feedEl.innerHTML  = '';
  statsEl.innerHTML = '';
  Object.keys(stats).forEach(k => delete stats[k]);
  total = 0;
  countEl.textContent = '0 events';
}

// ── WebSocket with auto-reconnect ────────────────────────────────────────────
function connect() {
  const ws = new WebSocket(`ws://${location.host}/ws`);

  ws.onopen = () => {
    statusEl.textContent = '\u25cf Connected';
    statusEl.className   = '';
  };

  ws.onmessage = e => {
    const msg = JSON.parse(e.data);
    if (msg.type === 'backlog') {
      msg.records.forEach(addRow);
    } else {
      addRow(msg);
    }
  };

  ws.onclose = () => {
    statusEl.textContent = '\u25cf Disconnected';
    statusEl.className   = 'disc';
    setTimeout(connect, 2000);
  };

  ws.onerror = () => ws.close();
}

connect();
</script>
</body>
</html>
"""


# ══════════════════════════════════════════════════════════════════════════════
# JSON helper
# ══════════════════════════════════════════════════════════════════════════════

def _default_serializer(obj: Any) -> Any:
    """Fallback JSON serialiser for types json.dumps doesn't handle natively."""
    try:
        # numpy scalars / arrays
        return obj.tolist()  # type: ignore[union-attr,unused-ignore]
    except AttributeError:
        pass
    return str(obj)


def _dumps(obj: Any) -> str:
    return json.dumps(obj, default=_default_serializer)


# ══════════════════════════════════════════════════════════════════════════════
# FileLogSink
# ══════════════════════════════════════════════════════════════════════════════

class FileLogSink(BaseLogSink):
    """
    Appends structured plain-text lines to a file.

    Line format::

        [2024-01-01 12:00:00.123] LOG  INFO     ArachniteRuntime   tick=    1  Dispatching action  action_id=X
        [2024-01-01 12:00:00.124] SIG  thermal  TemperatureSense            value=42.5  conf=1.00

    Can be used standalone (passed to any node's log_sinks list) or as a
    companion to SignalDashboard (which creates one internally when
    log_file is set).
    """

    def __init__(
        self,
        path: str | Path,
        level: LogLevel = LogLevel.DEBUG,
    ) -> None:
        super().__init__(level=level)
        self._path = Path(path)
        self._fh: Any = None

    def open(self) -> None:
        """Open (or create) the log file in append mode."""
        self._fh = self._path.open("a", encoding="utf-8", buffering=1)

    def close(self) -> None:
        """Flush and close the log file."""
        if self._fh is not None:
            self._fh.flush()
            self._fh.close()
            self._fh = None

    # ── BaseLogSink ───────────────────────────────────────────────────────────

    async def emit(self, event: LogEvent) -> None:
        if not self.accepts(event) or self._fh is None:
            return
        self._write_log(event)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _ts(self, monotonic_ts: float) -> str:
        # monotonic timestamps are relative; convert to wall-clock approximation
        wall = datetime.datetime.now()
        return wall.strftime("%Y-%m-%d %H:%M:%S.") + f"{wall.microsecond // 1000:03d}"

    def _write_log(self, event: LogEvent) -> None:
        extras = "  ".join(f"{k}={v}" for k, v in event.data.items())
        ts     = self._ts(event.timestamp)
        line   = (
            f"[{ts}] LOG  {event.level.name:<8} "
            f"{event.node_id:<30} "
            f"tick={event.tick:>6}  "
            f"{event.message}"
        )
        if extras:
            line += f"  {extras}"
        self._fh.write(line + "\n")

    def write_signal(self, signal: Signal) -> None:
        """Write a Signal line to the file (called by SignalDashboard)."""
        if self._fh is None:
            return
        ts     = self._ts(signal.timestamp)
        extras = "  ".join(f"{k}={v}" for k, v in (signal.metadata or {}).items())
        line   = (
            f"[{ts}] SIG  {signal.kind:<8} "
            f"{signal.source:<30} "
            f"{'':>14}"
            f"value={signal.value}  conf={signal.confidence:.2f}"
        )
        if extras:
            line += f"  {extras}"
        self._fh.write(line + "\n")


# ══════════════════════════════════════════════════════════════════════════════
# SignalDashboard
# ══════════════════════════════════════════════════════════════════════════════

class SignalDashboard(BaseLogSink):
    """
    Real-time web dashboard for Arachnite agents.

    Subscribes to the SignalBus (all kinds via wildcard '*') and acts as a
    log sink so it captures every event flowing through the framework.

    Pass the instance as a log_sink to any node (or to ArachniteRuntime)::

        dashboard = SignalDashboard(bus, log_file="run.log", port=7070)
        runtime   = ArachniteRuntime(..., log_sinks=[dashboard])
        await dashboard.start()
        await runtime.start()
        # Visit http://localhost:7070

    Parameters
    ----------
    bus:
        The SignalBus shared by the runtime.
    host:
        Interface to bind to (default ``"127.0.0.1"``).
    port:
        HTTP/WebSocket port (default ``7070``).
    log_file:
        Path for the plain-text log file.  Pass ``None`` to disable file
        output (default ``"arachnite.log"``).
    level:
        Minimum log level to capture from StructuredLogger sinks
        (default ``LogLevel.DEBUG``).
    backlog:
        Number of recent events sent to browsers on first connect
        (default ``500``).
    """

    def __init__(
        self,
        bus: SignalBus,
        *,
        host: str = "127.0.0.1",
        port: int = 7070,
        log_file: str | Path | None = "arachnite.log",
        level: LogLevel = LogLevel.DEBUG,
        backlog: int = 500,
    ) -> None:
        super().__init__(level=level)
        self._bus     = bus
        self._host    = host
        self._port    = port
        self._backlog: deque[dict[str, Any]] = deque(maxlen=backlog)
        self._clients: set[Any] = set()
        self._server_task: asyncio.Task[None] | None = None
        self._file: FileLogSink | None = (
            FileLogSink(log_file, level=level) if log_file else None
        )

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Subscribe to the bus and start the HTTP/WebSocket server."""
        try:
            import fastapi  # noqa: F401
            import uvicorn  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "SignalDashboard requires 'fastapi' and 'uvicorn'.\n"
                "Install with:  pip install \"arachnite[web]\""
            ) from exc

        if self._file is not None:
            self._file.open()

        self._bus.subscribe("*", self._on_signal)
        self._server_task = asyncio.create_task(self._run_server())

    async def stop(self) -> None:
        """Unsubscribe from the bus, close file, shut down server."""
        self._bus.unsubscribe("*", self._on_signal)

        if self._file is not None:
            self._file.close()

        if self._server_task is not None:
            self._server_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._server_task
            self._server_task = None

    # ── BaseLogSink ───────────────────────────────────────────────────────────

    async def emit(self, event: LogEvent) -> None:
        """Receive a log event from StructuredLogger."""
        if not self.accepts(event):
            return
        record: dict[str, Any] = {
            "type":    "log",
            "ts":      event.timestamp,
            "level":   event.level.name,
            "node_id": event.node_id,
            "agent":   event.agent_node_id,
            "tick":    event.tick,
            "message": event.message,
            "data":    event.data,
        }
        await self._broadcast(record)
        if self._file is not None:
            self._file._write_log(event)

    # ── SignalBus callback ────────────────────────────────────────────────────

    async def _on_signal(self, signal: Signal) -> None:
        record: dict[str, Any] = {
            "type":       "signal",
            "ts":         signal.timestamp,
            "kind":       signal.kind,
            "source":     signal.source,
            "value":      signal.value,
            "confidence": signal.confidence,
            "metadata":   signal.metadata or {},
        }
        await self._broadcast(record)
        if self._file is not None:
            self._file.write_signal(signal)

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _broadcast(self, record: dict[str, Any]) -> None:
        """Append to backlog and push to every connected WebSocket client."""
        self._backlog.append(record)
        payload = _dumps(record)
        dead: set[Any] = set()
        for ws in list(self._clients):
            try:
                await ws.send_text(payload)
            except Exception:  # noqa: BLE001
                dead.add(ws)
        self._clients -= dead

    async def _run_server(self) -> None:
        import uvicorn
        config = uvicorn.Config(
            self._build_app(),
            host=self._host,
            port=self._port,
            log_level="error",
            access_log=False,
        )
        server = uvicorn.Server(config)
        await server.serve()

    def _build_app(self) -> Any:
        from fastapi import FastAPI, WebSocket, WebSocketDisconnect
        from fastapi.responses import HTMLResponse

        app       = FastAPI(title="Arachnite Dashboard", docs_url=None, redoc_url=None)
        dashboard = self

        @app.get("/", response_class=HTMLResponse)  # type: ignore[misc,untyped-decorator,unused-ignore]
        async def index() -> str:
            return _DASHBOARD_HTML

        @app.websocket("/ws")  # type: ignore[misc,untyped-decorator,unused-ignore]
        async def ws_endpoint(ws: WebSocket) -> None:
            await ws.accept()
            dashboard._clients.add(ws)
            # Replay recent events to the newly connected browser
            if dashboard._backlog:
                await ws.send_text(_dumps({
                    "type":    "backlog",
                    "records": list(dashboard._backlog),
                }))
            try:
                while True:
                    await ws.receive_text()  # keep connection open
            except WebSocketDisconnect:
                pass
            finally:
                dashboard._clients.discard(ws)

        return app
