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
import secrets
import time
from collections import deque
from pathlib import Path
from typing import Any

from arachnite.bus import SignalBus
from arachnite.logging import BaseLogSink, LogLevel
from arachnite.models import (
    ActionExecutionState,
    Context,
    DecisionEvent,
    InterruptRequest,
    LogEvent,
    Proposal,
    Result,
    Signal,
)

# Optional FastAPI imports. Hoisted to module level so that PEP 563 string
# annotations (`ws: WebSocket`) on the WebSocket endpoint resolve via
# typing.get_type_hints() — FastAPI relies on this to identify the WebSocket
# parameter. If the [web] extra isn't installed, the module still imports;
# SignalDashboard.start() raises a friendly ImportError on use.
try:
    from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
    from fastapi.responses import HTMLResponse, PlainTextResponse, Response
except ImportError:  # pragma: no cover - exercised only without [web] extra
    FastAPI = None  # type: ignore[assignment,misc]
    Request = None  # type: ignore[assignment,misc]
    WebSocket = None  # type: ignore[assignment,misc]
    WebSocketDisconnect = None  # type: ignore[assignment,misc]
    HTMLResponse = None  # type: ignore[assignment,misc]
    PlainTextResponse = None  # type: ignore[assignment,misc]
    Response = None  # type: ignore[assignment,misc]

# Cookie name used to remember a validated auth token across requests so
# the user doesn't have to keep the token in the URL bar.  Set HttpOnly
# (JS can't read it) and SameSite=Strict (no cross-site CSRF surface).
_AUTH_COOKIE = "arachnite_auth"
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})

# Dashboard background image — shipped inside the package.
_BG_IMAGE_PATH = Path(__file__).parent / "assets" / "dashboard_bg.png"
_bg_image_cache: bytes | None = None


def _load_bg_image() -> bytes:
    global _bg_image_cache
    if _bg_image_cache is None:
        _bg_image_cache = _BG_IMAGE_PATH.read_bytes()
    return _bg_image_cache

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
  --bg:      #02050d;
  --surface: rgba(10, 22, 40, 0.88);
  --surface-solid: #0a1628;
  --border:  #1e4d7a;
  --text:    #cfe9ff;
  --muted:   #5a82a8;
  --green:   #5fd9b3;
  --red:     #e05555;
  --yellow:  #e0b040;
  --cyan:    #4dc4ff;
  --cyan-bright: #9adcff;
  --magenta: #c060e0;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
html, body { height: 100%; }
body {
  color: var(--text);
  font-family: 'Cascadia Code','Fira Code','Consolas',monospace;
  font-size: 12.5px;
  display: flex;
  flex-direction: column;
  overflow: hidden;
  background: var(--bg);
  position: relative;
}
/* Background image — fixed, covers viewport, sits behind everything.
   Darkened by a stacked gradient so foreground rows stay readable. */
body::before {
  content: "";
  position: fixed;
  inset: 0;
  z-index: -1;
  background:
    linear-gradient(rgba(2, 5, 13, 0.78), rgba(2, 5, 13, 0.82)),
    url('/bg.png') center center / cover no-repeat;
}

/* ── Header ──────────────────────────────────────────────────────────────── */
#header {
  background: var(--surface);
  backdrop-filter: blur(8px);
  -webkit-backdrop-filter: blur(8px);
  border-bottom: 1px solid var(--border);
  box-shadow: 0 0 14px rgba(77, 196, 255, 0.12);
  padding: 9px 16px;
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
  flex-shrink: 0;
}
#title {
  font-size: 14px;
  font-weight: 600;
  white-space: nowrap;
  color: var(--cyan-bright);
  text-shadow: 0 0 8px rgba(77, 196, 255, 0.55);
}
#status {
  font-size: 11px;
  padding: 2px 9px;
  border-radius: 10px;
  background: rgba(15, 50, 40, 0.55);
  color: var(--green);
  white-space: nowrap;
  border: 1px solid rgba(95, 217, 179, 0.35);
}
#status.disc {
  background: rgba(50, 15, 15, 0.55);
  color: var(--red);
  border-color: rgba(224, 85, 85, 0.4);
}
#rt-status {
  font-size: 11px;
  color: var(--muted);
  white-space: nowrap;
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 2px 10px;
  background: rgba(2, 8, 18, 0.45);
  border: 1px solid var(--border);
  border-radius: 10px;
  letter-spacing: 0.3px;
}
#rt-status .rt-key   { color: var(--muted); }
#rt-status .rt-val   { color: var(--cyan-bright); font-weight: 500; }
#rt-status .rt-sep   { color: var(--border); }
#rt-status.idle .rt-val { color: var(--muted); font-weight: 400; }
#filter {
  flex: 1;
  min-width: 160px;
  background: rgba(2, 8, 18, 0.6);
  border: 1px solid var(--border);
  color: var(--text);
  padding: 4px 10px;
  border-radius: 4px;
  outline: none;
  font-family: inherit;
  font-size: 12px;
}
#filter:focus {
  border-color: var(--cyan);
  box-shadow: 0 0 6px rgba(77, 196, 255, 0.4);
}
.btn {
  padding: 4px 12px;
  border-radius: 4px;
  border: 1px solid var(--border);
  background: rgba(10, 22, 40, 0.7);
  color: var(--text);
  cursor: pointer;
  font-size: 11.5px;
  font-family: inherit;
  white-space: nowrap;
}
.btn:hover {
  border-color: var(--cyan);
  color: var(--cyan-bright);
  box-shadow: 0 0 6px rgba(77, 196, 255, 0.3);
}
#btn-pause.on { border-color: var(--yellow); color: var(--yellow); }
#count { font-size: 11px; color: var(--muted); white-space: nowrap; margin-left: auto; }

/* ── Stats bar ───────────────────────────────────────────────────────────── */
#stats {
  background: var(--surface);
  backdrop-filter: blur(8px);
  -webkit-backdrop-filter: blur(8px);
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
  background: rgba(2, 8, 18, 0.45);
}

/* Severity totals strip — pinned left side of #stats */
#severity {
  display: flex;
  gap: 4px;
  margin-right: 6px;
  padding-right: 8px;
  border-right: 1px solid var(--border);
  flex-shrink: 0;
}
.sev {
  font-size: 11px;
  padding: 2px 8px;
  border-radius: 10px;
  border: 1px solid;
  background: rgba(2, 8, 18, 0.45);
  cursor: pointer;
  user-select: none;
  opacity: 0.45;
  transition: opacity 0.15s, box-shadow 0.15s;
  white-space: nowrap;
}
.sev:hover { opacity: 1; }
.sev.has { opacity: 1; }
.sev.active { box-shadow: inset 0 0 0 1px currentColor; }
.sev b { font-weight: 600; margin-left: 4px; }
.sev-DEBUG    { color: #7ea8c8;        border-color: #7ea8c8; }
.sev-INFO     { color: var(--cyan);    border-color: var(--cyan); }
.sev-WARNING  { color: var(--yellow);  border-color: var(--yellow); }
.sev-ERROR    { color: var(--red);     border-color: var(--red); }
.sev-CRITICAL { color: var(--magenta); border-color: var(--magenta); }
.sev-SIG      { color: var(--green);   border-color: var(--green); }

/* Agents strip — appears only once >1 distinct agent has been seen.       */
/* Each pill is a click-to-filter shortcut for agent:<name>.               */
#agents {
  display: flex;
  gap: 4px;
  margin-right: 6px;
  padding-right: 8px;
  border-right: 1px solid var(--border);
  flex-shrink: 0;
}
.agent-pill {
  font-size: 11px;
  padding: 2px 8px;
  border-radius: 10px;
  border: 1px solid;
  background: rgba(2, 8, 18, 0.45);
  cursor: pointer;
  user-select: none;
  opacity: 0.85;
  transition: opacity 0.15s, box-shadow 0.15s;
  white-space: nowrap;
}
.agent-pill:hover { opacity: 1; }
.agent-pill.active { box-shadow: inset 0 0 0 1px currentColor; opacity: 1; }
.agent-pill b { font-weight: 600; margin-left: 4px; }

/* ── Feed ────────────────────────────────────────────────────────────────── */
#feed { flex: 1; overflow-y: auto; position: relative; }
#feed-empty {
  position: absolute;
  inset: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  color: var(--muted);
  font-style: italic;
  font-size: 12px;
  pointer-events: none;
  letter-spacing: 0.4px;
}
#feed-empty .pulse {
  display: inline-block;
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background: var(--cyan);
  margin-right: 8px;
  box-shadow: 0 0 8px var(--cyan);
  animation: pulse 1.6s ease-in-out infinite;
}
@keyframes pulse {
  0%, 100% { opacity: 0.35; }
  50%      { opacity: 1; }
}
.row {
  display: flex;
  align-items: baseline;
  gap: 8px;
  padding: 2px 16px;
  border-bottom: 1px solid transparent;
}
.row:hover { background: rgba(30, 77, 122, 0.18); }
.row.hidden { display: none; }
.ts   { color: var(--muted); white-space: nowrap; width: 88px; flex-shrink: 0; }
.dt   { color: var(--muted); white-space: nowrap; width: 56px; flex-shrink: 0; text-align: right; opacity: 0.55; font-size: 11px; }
.tb   { font-size: 10px; padding: 1px 5px; border-radius: 3px; flex-shrink: 0; width: 32px; text-align: center; }
.tb-l { background: rgba(30, 77, 122, 0.35); color: var(--cyan); }
.tb-s { background: rgba(30, 100, 90, 0.35); color: var(--green); }
.lb   { font-size: 10px; padding: 1px 6px; border-radius: 3px; flex-shrink: 0; min-width: 64px; text-align: center; }
.lv-DEBUG    { background: rgba(20, 30, 50, 0.55); color: #7ea8c8; }
.lv-INFO     { background: rgba(30, 77, 122, 0.45); color: var(--cyan); }
.lv-WARNING  { background: rgba(60, 50, 15, 0.5); color: var(--yellow); }
.lv-ERROR    { background: rgba(70, 20, 20, 0.5); color: var(--red); }
.lv-CRITICAL { background: rgba(55, 20, 70, 0.5); color: var(--magenta); }
.src  { flex-shrink: 0; max-width: 190px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-weight: 500; cursor: pointer; }
.src:hover { text-decoration: underline; text-decoration-style: dotted; text-decoration-thickness: 1px; text-underline-offset: 2px; }
.msg  { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; flex: 1; min-width: 0; }
.ext  { color: var(--muted); flex-shrink: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 280px; }

/* Scrollbar */
#feed::-webkit-scrollbar { width: 5px; }
#feed::-webkit-scrollbar-track { background: transparent; }
#feed::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
#feed::-webkit-scrollbar-thumb:hover { background: var(--cyan); }

/* ── Auto-scroll lock pill ───────────────────────────────────────────────── */
#scroll-pill {
  position: absolute;
  right: 22px;
  bottom: 14px;
  padding: 6px 14px 6px 12px;
  border-radius: 14px;
  background: rgba(10, 22, 40, 0.92);
  color: var(--cyan-bright);
  border: 1px solid var(--cyan);
  font-family: inherit;
  font-size: 11.5px;
  font-weight: 500;
  cursor: pointer;
  box-shadow: 0 4px 14px rgba(0, 0, 0, 0.55), 0 0 10px rgba(77, 196, 255, 0.35);
  display: none;
  align-items: center;
  gap: 6px;
  z-index: 5;
  opacity: 0;
  transition: opacity 0.15s ease, transform 0.15s ease;
  transform: translateY(6px);
  pointer-events: auto;
  backdrop-filter: blur(6px);
  -webkit-backdrop-filter: blur(6px);
}
#scroll-pill.visible {
  display: inline-flex;
  opacity: 1;
  transform: translateY(0);
}
#scroll-pill:hover { background: rgba(30, 77, 122, 0.92); }
#scroll-pill .arrow { font-size: 13px; line-height: 1; }

/* ── Row selection ───────────────────────────────────────────────────────── */
.row { cursor: pointer; }
.row.selected {
  background: rgba(77, 196, 255, 0.18);
  border-left: 3px solid var(--cyan);
  padding-left: 13px;
  box-shadow: inset 0 0 0 9999px rgba(77, 196, 255, 0.06);
}

/* ── Splitter ────────────────────────────────────────────────────────────── */
#splitter {
  flex-shrink: 0;
  height: 5px;
  background: var(--border);
  cursor: row-resize;
  transition: background 0.12s, box-shadow 0.12s;
}
#splitter:hover, #splitter.dragging {
  background: var(--cyan);
  box-shadow: 0 0 8px rgba(77, 196, 255, 0.55);
}

/* ── Details pane ────────────────────────────────────────────────────────── */
#details {
  flex-shrink: 0;
  height: 260px;
  min-height: 80px;
  overflow-y: auto;
  background: var(--surface);
  backdrop-filter: blur(8px);
  -webkit-backdrop-filter: blur(8px);
  border-top: 1px solid var(--border);
  padding: 10px 16px 14px;
  font-size: 12px;
}
#details::-webkit-scrollbar { width: 5px; }
#details::-webkit-scrollbar-track { background: transparent; }
#details::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
#details::-webkit-scrollbar-thumb:hover { background: var(--cyan); }

.detail-header {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
  padding-bottom: 8px;
  margin-bottom: 8px;
  border-bottom: 1px solid var(--border);
}
.detail-header .det-src { font-weight: 600; color: var(--cyan-bright); }
.detail-header .det-meta { color: var(--muted); font-size: 11px; }

.detail-section-title {
  color: var(--cyan);
  font-size: 10.5px;
  text-transform: uppercase;
  letter-spacing: 0.6px;
  margin: 10px 0 4px;
  cursor: pointer;
  user-select: none;
  display: flex;
  align-items: center;
}
.detail-section-title::before { content: "\\25be"; margin-right: 6px; }
.detail-section-title.collapsed::before { content: "\\25b8"; }
.detail-section-title.collapsed + .detail-section-body { display: none; }

/* Top-level "Context @ tick N" title — slightly heavier and offset from the
   sibling top-level sections so the user can see where Context ends. */
.detail-section-title.ctx-root {
  color: var(--cyan-bright);
  font-size: 11.5px;
  margin-top: 14px;
  padding-top: 8px;
  border-top: 1px dashed var(--border);
  text-shadow: 0 0 6px rgba(77, 196, 255, 0.35);
}
/* Nested titles inside the Context body — smaller, indented, no caps so they
   read as second-level. */
.detail-section-body .detail-section-title {
  font-size: 9.5px;
  text-transform: none;
  letter-spacing: 0.2px;
  color: var(--muted);
  margin: 8px 0 3px 4px;
}
.detail-section-body .detail-section-body { margin-left: 12px; }

.fields-table { width: 100%; border-collapse: collapse; }
.fields-table td {
  padding: 2px 8px 2px 0;
  vertical-align: top;
  font-family: inherit;
}
.fields-table td.k { color: var(--muted); width: 170px; white-space: nowrap; }
.fields-table td.v { color: var(--text); word-break: break-word; }
.fields-table tr:hover { background: rgba(30, 77, 122, 0.12); }

.raw-pre {
  background: rgba(2, 8, 18, 0.55);
  border: 1px solid var(--border);
  border-radius: 4px;
  padding: 8px 10px;
  color: var(--cyan-bright);
  white-space: pre-wrap;
  word-break: break-word;
  font-size: 11.5px;
}

.copy-btn {
  font-size: 10px;
  padding: 1px 8px;
  border-radius: 3px;
  border: 1px solid var(--border);
  background: rgba(10, 22, 40, 0.7);
  color: var(--text);
  cursor: pointer;
  margin-left: 8px;
  font-family: inherit;
}
.copy-btn:hover { border-color: var(--cyan); color: var(--cyan-bright); }

body.dragging { user-select: none; cursor: row-resize; }

/* ── Light theme ─────────────────────────────────────────────────────────── */
/* Overrides the core palette plus the handful of hard-coded rgba() backgrounds
   that would otherwise stay near-black under a light background.            */
body.light {
  --bg:          #f4f6fa;
  --surface:     rgba(255, 255, 255, 0.92);
  --surface-solid: #ffffff;
  --border:      #c4d4e6;
  --text:        #1a2a3f;
  --muted:       #6a8398;
  --green:       #1a9c70;
  --red:         #c83434;
  --yellow:      #a87010;
  --cyan:        #1a72b8;
  --cyan-bright: #0a508c;
  --magenta:     #8a3ab8;
}
body.light::before { background: var(--bg); }
body.light #title { text-shadow: none; }
body.light #header { box-shadow: 0 0 14px rgba(26, 114, 184, 0.08); }
body.light #status { background: rgba(220, 240, 230, 0.7); border-color: rgba(26, 156, 112, 0.4); }
body.light #status.disc { background: rgba(245, 220, 220, 0.7); border-color: rgba(200, 52, 52, 0.4); }
body.light #rt-status { background: rgba(245, 248, 252, 0.6); }
body.light #filter { background: rgba(245, 248, 252, 0.85); }
body.light #filter:focus { box-shadow: 0 0 6px rgba(26, 114, 184, 0.3); }
body.light .btn { background: rgba(245, 248, 252, 0.85); }
body.light .btn:hover { box-shadow: 0 0 6px rgba(26, 114, 184, 0.25); }
body.light .sev,
body.light .agent-pill,
body.light .badge { background: rgba(245, 248, 252, 0.85); }
body.light .sev-DEBUG { color: #4a6a86; border-color: #4a6a86; }
body.light .row:hover { background: rgba(26, 114, 184, 0.08); }
body.light .row.selected {
  background: rgba(26, 114, 184, 0.12);
  box-shadow: inset 0 0 0 9999px rgba(26, 114, 184, 0.04);
}
body.light .tb-l { background: rgba(26, 114, 184, 0.18); color: var(--cyan); }
body.light .tb-s { background: rgba(26, 156, 112, 0.18); color: var(--green); }
body.light .lv-DEBUG    { background: rgba(74, 106, 134, 0.15); color: #4a6a86; }
body.light .lv-INFO     { background: rgba(26, 114, 184, 0.15); color: var(--cyan); }
body.light .lv-WARNING  { background: rgba(168, 112, 16, 0.18); color: var(--yellow); }
body.light .lv-ERROR    { background: rgba(200, 52, 52, 0.15); color: var(--red); }
body.light .lv-CRITICAL { background: rgba(138, 58, 184, 0.15); color: var(--magenta); }
body.light #scroll-pill {
  background: rgba(255, 255, 255, 0.95);
  box-shadow: 0 4px 14px rgba(0, 0, 0, 0.12), 0 0 10px rgba(26, 114, 184, 0.25);
}
body.light #scroll-pill:hover { background: rgba(230, 240, 250, 0.95); }
body.light .raw-pre { background: rgba(245, 248, 252, 0.85); color: var(--cyan-bright); }
body.light .copy-btn { background: rgba(245, 248, 252, 0.85); }
</style>
</head>
<body>

<div id="header">
  <span id="title">&#x1F578; Arachnite Dashboard</span>
  <span id="status" class="disc">&#9679; Connecting&hellip;</span>
  <span id="rt-status" class="idle">
    <span><span class="rt-key">tick</span> <span class="rt-val" id="rt-tick">&mdash;</span></span>
    <span class="rt-sep">&middot;</span>
    <span><span class="rt-val" id="rt-rate">&mdash;</span> <span class="rt-key">Hz</span></span>
    <span class="rt-sep">&middot;</span>
    <span><span class="rt-val" id="rt-uptime">0:00</span></span>
    <span class="rt-sep">&middot;</span>
    <span><span class="rt-val" id="rt-clients">0</span> <span class="rt-key" id="rt-clients-label">clients</span></span>
    <span id="rt-drop-wrap" style="display:none">
      <span class="rt-sep">&middot;</span>
      <span><span class="rt-val" id="rt-drop" style="color:var(--yellow)">0</span> <span class="rt-key">dropped</span></span>
    </span>
  </span>
  <input id="filter" type="search"
         placeholder="Filter: text, level:warn, kind:temp, src:fan, agent:edge1, type:log, -src:noisy"
         title="Free text matches source/message/data. Operators: level: kind: src: type: (prefix '-' to negate)"
         oninput="applyFilter()" autocomplete="off">
  <button class="btn" id="btn-pause" onclick="togglePause()">&#9646;&#9646; Pause</button>
  <button class="btn" onclick="clearFeed()">&#x2715; Clear</button>
  <button class="btn" id="btn-export" onclick="exportVisible()" title="Download currently-visible rows as NDJSON">&#x2B07; Export</button>
  <button class="btn" id="btn-theme" onclick="toggleTheme()" title="Toggle light / dark theme">&#9790; Dark</button>
  <span id="count">0 events</span>
</div>
<div id="stats">
  <div id="severity">
    <span class="sev sev-DEBUG"    data-token="level:debug"    onclick="toggleSevFilter('level:debug')"    title="Click to filter to DEBUG only">DBG <b>0</b></span>
    <span class="sev sev-INFO"     data-token="level:info"     onclick="toggleSevFilter('level:info')"     title="Click to filter to INFO only">INF <b>0</b></span>
    <span class="sev sev-WARNING"  data-token="level:warn"     onclick="toggleSevFilter('level:warn')"     title="Click to filter to WARNING only">WRN <b>0</b></span>
    <span class="sev sev-ERROR"    data-token="level:error"    onclick="toggleSevFilter('level:error')"    title="Click to filter to ERROR only">ERR <b>0</b></span>
    <span class="sev sev-CRITICAL" data-token="level:critical" onclick="toggleSevFilter('level:critical')" title="Click to filter to CRITICAL only">CRT <b>0</b></span>
    <span class="sev sev-SIG"      data-token="type:sig"       onclick="toggleSevFilter('type:sig')"       title="Click to filter to signals only">SIG <b>0</b></span>
  </div>
  <div id="agents" style="display:none" title="Originating agents — click to filter"></div>
</div>
<div id="feed">
  <div id="feed-empty"><span class="pulse"></span>Waiting for events&hellip;</div>
  <button id="scroll-pill" onclick="jumpToBottom()" type="button">
    <span class="arrow">&#x2193;</span><span id="scroll-pill-text">0 new</span>
  </button>
</div>
<div id="splitter" title="Drag to resize details pane" style="display:none"></div>
<div id="details" style="display:none">
  <div id="details-content"></div>
</div>

<script>
const MAX_ROWS         = 2000;
const CONTEXT_CACHE_MAX = 200;
let paused       = false;
let total        = 0;
let selectedRow  = null;
const stats          = {};
const contextByTick  = new Map();  // tick → context record
const decisionByTick = new Map();  // tick → decision record

const feedEl        = document.getElementById('feed');
const feedEmptyEl   = document.getElementById('feed-empty');
const scrollPillEl  = document.getElementById('scroll-pill');
const scrollPillTxt = document.getElementById('scroll-pill-text');
const statsEl       = document.getElementById('stats');
const rtStatusEl    = document.getElementById('rt-status');
const rtTickEl      = document.getElementById('rt-tick');
const rtRateEl      = document.getElementById('rt-rate');
const rtUptimeEl    = document.getElementById('rt-uptime');
const rtClientsEl   = document.getElementById('rt-clients');
const rtClientsLbl  = document.getElementById('rt-clients-label');
const rtDropEl      = document.getElementById('rt-drop');
const rtDropWrapEl  = document.getElementById('rt-drop-wrap');

// ── Runtime status strip ────────────────────────────────────────────────────
function fmtUptime(s) {
  s = Math.floor(s);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const ss = s % 60;
  const pad = n => String(n).padStart(2, '0');
  return h > 0 ? `${h}:${pad(m)}:${pad(ss)}` : `${m}:${pad(ss)}`;
}
function updateRtStatus(msg) {
  // Server emits tick=0 + rate=0 before the runtime is wired or has ticked.
  // Render that as "idle" so the strip doesn't look broken.
  const hasTicked = msg.tick > 0;
  rtStatusEl.classList.toggle('idle', !hasTicked);
  rtTickEl.textContent    = hasTicked ? msg.tick.toLocaleString() : '—';
  rtRateEl.textContent    = hasTicked ? msg.tick_rate.toFixed(1) : '—';
  rtUptimeEl.textContent  = fmtUptime(msg.uptime_s);
  rtClientsEl.textContent = msg.clients;
  rtClientsLbl.textContent = msg.clients === 1 ? 'client' : 'clients';
  const dropped = msg.dropped || 0;
  if (dropped > 0) {
    rtDropEl.textContent = dropped.toLocaleString();
    rtDropWrapEl.style.display = '';
  } else {
    rtDropWrapEl.style.display = 'none';
  }
}

// ── Auto-scroll lock ────────────────────────────────────────────────────────
// When the user scrolls up to read older rows, suppress auto-scroll-to-bottom
// on incoming events.  A pill in the bottom-right of the feed counts the
// rows that arrived while scrolled up; click it to jump back and resume.
const SCROLL_BOTTOM_PX = 8;   // px from bottom to count as "at bottom"
let userScrolledUp     = false;
let newSinceScrollUp   = 0;

function atBottom() {
  return feedEl.scrollHeight - (feedEl.scrollTop + feedEl.clientHeight) <= SCROLL_BOTTOM_PX;
}
function showPill() {
  scrollPillTxt.textContent = newSinceScrollUp + ' new';
  scrollPillEl.classList.add('visible');
}
function hidePill() {
  newSinceScrollUp = 0;
  scrollPillEl.classList.remove('visible');
}
function jumpToBottom() {
  userScrolledUp = false;
  feedEl.scrollTop = feedEl.scrollHeight;
  hidePill();
}
feedEl.addEventListener('scroll', () => {
  if (atBottom()) {
    if (userScrolledUp) { userScrolledUp = false; hidePill(); }
  } else {
    userScrolledUp = true;
  }
});
const statusEl      = document.getElementById('status');
const countEl       = document.getElementById('count');
const filterEl      = document.getElementById('filter');
const pauseBtn      = document.getElementById('btn-pause');
const splitterEl    = document.getElementById('splitter');
const detailsEl     = document.getElementById('details');
const detailsContent= document.getElementById('details-content');

function rememberContext(rec) {
  if (rec.tick == null) return;
  contextByTick.set(rec.tick, rec);
  // Evict oldest ticks if we're over budget
  while (contextByTick.size > CONTEXT_CACHE_MAX) {
    const firstKey = contextByTick.keys().next().value;
    contextByTick.delete(firstKey);
  }
}

function rememberDecision(rec) {
  if (rec.tick == null) return;
  decisionByTick.set(rec.tick, rec);
  while (decisionByTick.size > CONTEXT_CACHE_MAX) {
    const firstKey = decisionByTick.keys().next().value;
    decisionByTick.delete(firstKey);
  }
}

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

// ── Severity totals strip ───────────────────────────────────────────────────
const severityCounts = {DEBUG:0, INFO:0, WARNING:0, ERROR:0, CRITICAL:0, SIG:0};
const sevEls = {};
function initSeverity() {
  for (const k of Object.keys(severityCounts)) {
    const wrap = document.querySelector('.sev.sev-' + k);
    sevEls[k] = wrap ? { wrap, num: wrap.querySelector('b') } : null;
  }
}
function bumpSeverity(rec) {
  const k = (rec.type === 'log') ? (rec.level || 'INFO') : 'SIG';
  if (!(k in severityCounts)) return;
  severityCounts[k]++;
  const e = sevEls[k];
  if (!e) return;
  e.num.textContent = severityCounts[k];
  e.wrap.classList.add('has');
}
function resetSeverity() {
  for (const k of Object.keys(severityCounts)) {
    severityCounts[k] = 0;
    const e = sevEls[k];
    if (e) { e.num.textContent = '0'; e.wrap.classList.remove('has'); }
  }
}
function toggleSevFilter(token) {
  const cur = filterEl.value.trim();
  filterEl.value = (cur === token) ? '' : token;
  applyFilter();
}
function syncSevActive() {
  const cur = filterEl.value.trim();
  document.querySelectorAll('.sev').forEach(e => {
    e.classList.toggle('active', e.dataset.token === cur);
  });
  document.querySelectorAll('.agent-pill').forEach(e => {
    e.classList.toggle('active', e.dataset.token === cur);
  });
}

// ── Per-agent strip ─────────────────────────────────────────────────────────
// Mesh setups deliver signals from multiple agents over the wire.  We count
// every distinct agent we see and surface a click-to-filter pill per agent
// once more than one has appeared (a solo-agent run shouldn't show clutter).
const agentCounts = {};
const agentEls = {};
const agentsEl = document.getElementById('agents');

function bumpAgent(rec) {
  const a = rec.agent;
  if (!a) return;
  agentCounts[a] = (agentCounts[a] || 0) + 1;
  let el = agentEls[a];
  if (!el) {
    el = document.createElement('span');
    el.className = 'agent-pill';
    const token = 'agent:' + a.toLowerCase();
    el.dataset.token = token;
    el.title = 'Click to filter to agent ' + a;
    el.addEventListener('click', () => toggleAgentFilter(token));
    const c = hsl(a, 65);
    el.style.borderColor = c;
    el.style.color       = c;
    agentsEl.appendChild(el);
    agentEls[a] = el;
  }
  el.innerHTML = esc(a) + ' <b>' + agentCounts[a] + '</b>';
  // Show the strip once we have at least one agent (multi-agent meshes
  // benefit most, but the count is useful even for a single labelled
  // agent — uniform behaviour keeps the UI predictable).
  if (Object.keys(agentEls).length > 0) agentsEl.style.display = '';
}

function resetAgents() {
  for (const a of Object.keys(agentCounts)) delete agentCounts[a];
  for (const a of Object.keys(agentEls)) {
    agentEls[a].remove();
    delete agentEls[a];
  }
  agentsEl.style.display = 'none';
}

function toggleAgentFilter(token) {
  const cur = filterEl.value.trim();
  filterEl.value = (cur === token) ? '' : token;
  applyFilter();
}

// ── Timestamp ───────────────────────────────────────────────────────────────
function fmtTs(ts) {
  const d   = new Date(ts * 1000);
  const p   = n => String(n).padStart(2,'0');
  const ms  = String(d.getMilliseconds()).padStart(3,'0');
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}.${ms}`;
}

// Inter-row delta — the gap from the previously rendered row's timestamp.
// "Ago-from-now" drifts as the user reads; gap-to-previous stays stable and
// is what's actually useful for spotting tick clustering or stalls.
let lastTs = null;
function formatDelta(d) {
  if (d == null) return '';
  if (d < 0) d = 0;
  if (d < 1)  return '+' + Math.round(d * 1000) + 'ms';
  if (d < 10) return '+' + d.toFixed(2) + 's';
  if (d < 60) return '+' + d.toFixed(1) + 's';
  const m = Math.floor(d / 60);
  const s = Math.floor(d % 60);
  return '+' + m + 'm' + String(s).padStart(2, '0') + 's';
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
  row.dataset.raw   = raw;
  row.dataset.type  = (rec.type === 'log') ? 'log' : 'sig';
  row.dataset.src   = (src || '').toLowerCase();
  row.dataset.level = (rec.type === 'log' && rec.level) ? String(rec.level).toLowerCase() : '';
  row.dataset.kind  = (rec.type === 'log') ? '' : String(rec.kind || '').toLowerCase();
  row.dataset.agent = String(rec.agent || '').toLowerCase();
  row._record    = rec;
  const delta = (lastTs == null) ? null : (rec.ts - lastTs);
  lastTs = rec.ts;
  row.innerHTML   = `
    <span class="ts">${fmtTs(rec.ts)}</span>
    <span class="dt" title="gap since previous row">${formatDelta(delta)}</span>
    ${typeBadge}
    ${levelCell}
    <span class="src" style="color:${sc}" title="${esc(src)}">${esc(src)}</span>
    <span class="msg">${msg}</span>
    <span class="ext">${ext}</span>
  `;

  if (!rowMatches(row, parsedFilter)) row.classList.add('hidden');

  if (feedEmptyEl.style.display !== 'none') feedEmptyEl.style.display = 'none';

  feedEl.appendChild(row);
  // Cap visible rows — the empty-state placeholder is a sibling of rows
  // but kept out of the count by querying for .row directly.  If the row
  // being pruned is the currently-selected one, keep the details pane open
  // (the user is reading it) and just detach the selection.
  let rowEls = feedEl.getElementsByClassName('row');
  while (rowEls.length > MAX_ROWS) {
    const dropped = rowEls[0];
    if (dropped === selectedRow) {
      selectedRow.classList.remove('selected');
      selectedRow = null;
    }
    feedEl.removeChild(dropped);
  }

  total++;
  countEl.textContent = total + ' events';
  bumpStat(statKey);
  bumpSeverity(rec);
  bumpAgent(rec);

  // Auto-scroll only if we should: not paused, no row selected, and the
  // user hasn't manually scrolled up.  If they have, count the new row
  // toward the pill instead of yanking them back to the bottom.
  if (!paused && !selectedRow && !userScrolledUp) {
    feedEl.scrollTop = feedEl.scrollHeight;
  } else if (userScrolledUp && !row.classList.contains('hidden')) {
    newSinceScrollUp++;
    showPill();
  }
}

// ── Selection / detail pane ─────────────────────────────────────────────────
function showDetailsPane() {
  splitterEl.style.display = '';
  detailsEl.style.display  = '';
}
function hideDetailsPane() {
  splitterEl.style.display = 'none';
  detailsEl.style.display  = 'none';
}

function selectRow(row) {
  if (selectedRow === row) { clearSelection(); return; }
  if (selectedRow) selectedRow.classList.remove('selected');
  selectedRow = row;
  row.classList.add('selected');
  showDetailsPane();
  renderDetails(row._record);
}

function clearSelection() {
  if (selectedRow) selectedRow.classList.remove('selected');
  selectedRow = null;
  detailsContent.innerHTML = '';
  hideDetailsPane();
  // Resume auto-scroll only if the user hadn't manually scrolled up.
  if (!paused && !userScrolledUp) feedEl.scrollTop = feedEl.scrollHeight;
}

feedEl.addEventListener('click', (e) => {
  // Click on a source pill = toggle a src:<name> filter, not row-select.
  // This lets the user pin/unpin the feed to one node (e.g. EmergencyStop)
  // with a single click, without opening the details pane.
  const srcEl = e.target.closest('.src');
  if (srcEl) {
    const row = srcEl.closest('.row');
    if (row && row.dataset.src) {
      toggleSrcFilter('src:' + row.dataset.src);
      e.stopPropagation();
      return;
    }
  }
  const row = e.target.closest('.row');
  if (row) selectRow(row);
});

function toggleSrcFilter(token) {
  const cur = filterEl.value.trim();
  filterEl.value = (cur === token) ? '' : token;
  applyFilter();
}

document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && selectedRow) { e.preventDefault(); clearSelection(); }
});

function renderField(k, v) {
  let valHtml;
  if (v === null || v === undefined) {
    valHtml = '<em style="color:var(--muted)">null</em>';
  } else if (typeof v === 'object') {
    valHtml = `<code>${esc(JSON.stringify(v))}</code>`;
  } else {
    valHtml = esc(String(v));
  }
  return `<tr><td class="k">${esc(k)}</td><td class="v">${valHtml}</td></tr>`;
}

function renderDetails(rec) {
  const isLog = rec.type === 'log';
  const wallTs = fmtTs(rec.ts);

  let headerLabel;
  if (isLog) {
    headerLabel = `<span class="tb tb-l">LOG</span>
                   <span class="lb lv-${rec.level}">${rec.level}</span>`;
  } else {
    const c = hsl(rec.kind, 58);
    headerLabel = `<span class="tb tb-s">SIG</span>
                   <span class="lb" style="color:${c};border:1px solid ${c};background:rgba(0,0,0,.3)">${esc(rec.kind)}</span>`;
  }

  const src = isLog ? (rec.node_id || '') : (rec.source || '');
  const tickStr = rec.tick != null ? `tick=${rec.tick}` : '';
  const agentStr = rec.agent ? `agent=${esc(rec.agent)}` : '';

  // Core fields table
  const coreRows = [];
  if (isLog) {
    coreRows.push(renderField('node_id', rec.node_id));
    if (rec.agent)   coreRows.push(renderField('agent_node_id', rec.agent));
    if (rec.tick != null) coreRows.push(renderField('tick', rec.tick));
    coreRows.push(renderField('level', rec.level));
    coreRows.push(renderField('message', rec.message));
  } else {
    coreRows.push(renderField('source', rec.source));
    coreRows.push(renderField('kind', rec.kind));
    coreRows.push(renderField('value', rec.value));
    coreRows.push(renderField('confidence', Number(rec.confidence).toFixed(4)));
  }
  coreRows.push(renderField('timestamp', `${rec.ts.toFixed(6)} (${wallTs})`));

  // Data / metadata
  const extraObj = isLog ? (rec.data || {}) : (rec.metadata || {});
  const extraRows = Object.entries(extraObj).map(([k, v]) => renderField(k, v));
  const extraTitle = isLog ? 'Data' : 'Metadata';

  const raw = JSON.stringify(rec, null, 2);

  // ── Context section (only when we have a snapshot for this tick) ─────────
  const ctxRec  = (rec.tick != null) ? contextByTick.get(rec.tick) : null;
  const ctxHtml = ctxRec ? renderContextSection(ctxRec) : '';

  // ── Decision section (only when we have a snapshot for this tick) ────────
  const decRec  = (rec.tick != null) ? decisionByTick.get(rec.tick) : null;
  const decHtml = decRec ? renderDecisionSection(decRec) : '';

  detailsContent.innerHTML = `
    <div class="detail-header">
      ${headerLabel}
      <span class="det-src">${esc(src)}</span>
      <span class="det-meta">${esc(wallTs)}${tickStr ? '  &middot;  ' + esc(tickStr) : ''}${agentStr ? '  &middot;  ' + agentStr : ''}</span>
      <button class="copy-btn" onclick="copyRaw(this)">Copy JSON</button>
    </div>

    <div class="detail-section-title">Core</div>
    <div class="detail-section-body">
      <table class="fields-table">${coreRows.join('')}</table>
    </div>

    <div class="detail-section-title${extraRows.length === 0 ? ' collapsed' : ''}">${extraTitle} (${extraRows.length})</div>
    <div class="detail-section-body">
      ${extraRows.length
          ? `<table class="fields-table">${extraRows.join('')}</table>`
          : '<div style="color:var(--muted);font-style:italic">(empty)</div>'}
    </div>

    ${ctxHtml}

    ${decHtml}

    <div class="detail-section-title collapsed">Raw JSON</div>
    <div class="detail-section-body">
      <pre class="raw-pre" id="raw-json">${esc(raw)}</pre>
    </div>
  `;

  // Wire section-title toggles
  detailsContent.querySelectorAll('.detail-section-title').forEach(el => {
    el.addEventListener('click', () => el.classList.toggle('collapsed'));
  });
}

// Render the Arachnite Context snapshot for a given tick — collapsible
// subsections for State, Signals, Last Result(s), Action States.
function renderContextSection(ctx) {
  const stateRows = Object.entries(ctx.state || {}).map(([k, v]) => renderField(k, v));
  const stateBody = stateRows.length
    ? `<table class="fields-table">${stateRows.join('')}</table>`
    : '<div style="color:var(--muted);font-style:italic">(empty)</div>';

  const signalsBody = (ctx.signals && ctx.signals.length)
    ? `<table class="fields-table">
         <tr><td class="k" style="color:var(--cyan)">source</td>
             <td class="k" style="color:var(--cyan)">kind</td>
             <td class="k" style="color:var(--cyan)">value</td>
             <td class="k" style="color:var(--cyan)">conf</td></tr>
         ${ctx.signals.map(s => `
           <tr>
             <td class="v">${esc(s.source)}</td>
             <td class="v">${esc(s.kind)}</td>
             <td class="v">${esc(JSON.stringify(s.value))}</td>
             <td class="v">${Number(s.confidence).toFixed(2)}</td>
           </tr>`).join('')}
       </table>`
    : '<div style="color:var(--muted);font-style:italic">no signals this tick</div>';

  let resultBody;
  if (ctx.last_results && ctx.last_results.length) {
    resultBody = `<table class="fields-table">
      <tr><td class="k" style="color:var(--cyan)">action_id</td>
          <td class="k" style="color:var(--cyan)">success</td>
          <td class="k" style="color:var(--cyan)">duration_s</td>
          <td class="k" style="color:var(--cyan)">error</td></tr>
      ${ctx.last_results.map(r => `
        <tr>
          <td class="v">${esc(r.action_id)}</td>
          <td class="v">${r.success ? '<span style="color:var(--green)">true</span>' : '<span style="color:var(--red)">false</span>'}</td>
          <td class="v">${Number(r.duration_s).toFixed(4)}</td>
          <td class="v">${r.error ? esc(r.error) : '<em style="color:var(--muted)">none</em>'}</td>
        </tr>`).join('')}
    </table>`;
  } else if (ctx.last_result) {
    const r = ctx.last_result;
    resultBody = `<table class="fields-table">
      ${renderField('action_id', r.action_id)}
      ${renderField('success', r.success)}
      ${renderField('duration_s', Number(r.duration_s).toFixed(4))}
      ${renderField('error', r.error)}
    </table>`;
  } else {
    resultBody = '<div style="color:var(--muted);font-style:italic">no prior result</div>';
  }

  const actionBody = (ctx.action_states && ctx.action_states.length)
    ? `<table class="fields-table">
         <tr><td class="k" style="color:var(--cyan)">action_id</td>
             <td class="k" style="color:var(--cyan)">step</td>
             <td class="k" style="color:var(--cyan)">interruptible</td>
             <td class="k" style="color:var(--cyan)">block_remaining</td></tr>
         ${ctx.action_states.map(a => `
           <tr>
             <td class="v">${a.action_id ? esc(a.action_id) : '<em style="color:var(--muted)">idle</em>'}</td>
             <td class="v">${a.current_step ? esc(a.current_step) : '<em style="color:var(--muted)">&mdash;</em>'}</td>
             <td class="v">${a.interruptible ? 'yes' : 'no'}</td>
             <td class="v">${Number(a.mandatory_block_remaining_s).toFixed(3)}s</td>
           </tr>`).join('')}
       </table>`
    : '<div style="color:var(--muted);font-style:italic">no active actions</div>';

  const histInfo = ctx.history
    ? `depth=${ctx.history.depth}, last tick had ${ctx.history.last_count} signal(s)`
    : '';

  return `
    <div class="detail-section-title ctx-root">
      &#x1F578; Context @ tick ${ctx.tick}
      <span style="color:var(--muted);font-size:9.5px;margin-left:8px;text-transform:none;letter-spacing:0;font-weight:400;text-shadow:none">${esc(histInfo)}</span>
    </div>
    <div class="detail-section-body">
      <div class="detail-section-title">State (${stateRows.length})</div>
      <div class="detail-section-body">${stateBody}</div>

      <div class="detail-section-title">Signals this tick (${(ctx.signals || []).length})</div>
      <div class="detail-section-body">${signalsBody}</div>

      <div class="detail-section-title">Last Result${ctx.last_results && ctx.last_results.length > 1 ? 's' : ''}</div>
      <div class="detail-section-body">${resultBody}</div>

      <div class="detail-section-title">Action States (${(ctx.action_states || []).length})</div>
      <div class="detail-section-body">${actionBody}</div>
    </div>
  `;
}

// Render the decision-layer snapshot for a given tick — strategy name, all
// considered proposals (with selected ones highlighted), and any interrupts.
function renderDecisionSection(dec) {
  const considered = dec.considered || [];
  const interrupts = dec.interrupts || [];
  const dispatched = dec.dispatched || [];

  let propBody;
  if (considered.length) {
    const rows = considered.map(p => {
      const tag = p.selected
        ? '<span style="color:var(--green);font-weight:600">SELECTED</span>'
        : '<span style="color:var(--muted)">rejected</span>';
      const persistTag = p.persist
        ? '  <span style="color:var(--yellow);font-size:9.5px">[persist]</span>'
        : '';
      const rationale = p.rationale
        ? `<div style="color:var(--muted);font-size:10px;margin-top:2px">${esc(p.rationale)}</div>`
        : '';
      return `<tr>
        <td class="v">${esc(p.instinct_id)}${persistTag}</td>
        <td class="v">${esc(p.action_id)}</td>
        <td class="v" style="text-align:right">${p.priority}</td>
        <td class="v" style="text-align:right">${Number(p.urgency).toFixed(3)}</td>
        <td class="v">${tag}${rationale}</td>
      </tr>`;
    }).join('');
    propBody = `<table class="fields-table">
      <tr>
        <td class="k" style="color:var(--cyan)">instinct</td>
        <td class="k" style="color:var(--cyan)">action</td>
        <td class="k" style="color:var(--cyan);text-align:right">priority</td>
        <td class="k" style="color:var(--cyan);text-align:right">urgency</td>
        <td class="k" style="color:var(--cyan)">outcome</td>
      </tr>${rows}
    </table>`;
  } else {
    propBody = '<div style="color:var(--muted);font-style:italic">no proposals considered</div>';
  }

  let interruptBody = '';
  if (interrupts.length) {
    const rows = interrupts.map(r => {
      const np = r.new_proposal || {};
      return `<tr>
        <td class="v">${esc(r.requesting_instinct_id)}</td>
        <td class="v">${esc(np.action_id || '')}</td>
        <td class="v" style="text-align:right">${np.priority != null ? np.priority : ''}</td>
        <td class="v">${esc(r.reason || '')}</td>
      </tr>`;
    }).join('');
    interruptBody = `
      <div class="detail-section-title">Interrupts (${interrupts.length})</div>
      <div class="detail-section-body">
        <table class="fields-table">
          <tr>
            <td class="k" style="color:var(--cyan)">requested by</td>
            <td class="k" style="color:var(--cyan)">target action</td>
            <td class="k" style="color:var(--cyan);text-align:right">priority</td>
            <td class="k" style="color:var(--cyan)">reason</td>
          </tr>${rows}
        </table>
      </div>`;
  }

  const dispatchedNote = dispatched.length
    ? `${dispatched.length} dispatched`
    : 'none dispatched';

  return `
    <div class="detail-section-title ctx-root">
      &#x2699;&#xfe0f; Decision @ tick ${dec.tick}
      <span style="color:var(--muted);font-size:9.5px;margin-left:8px;text-transform:none;letter-spacing:0;font-weight:400;text-shadow:none">strategy=${esc(dec.strategy)}  &middot;  ${esc(dispatchedNote)}</span>
    </div>
    <div class="detail-section-body">
      <div class="detail-section-title">Proposals (${considered.length})</div>
      <div class="detail-section-body">${propBody}</div>
      ${interruptBody}
    </div>
  `;
}

function copyRaw(btn) {
  const text = document.getElementById('raw-json').textContent;
  navigator.clipboard.writeText(text).then(() => {
    const orig = btn.textContent;
    btn.textContent = 'Copied!';
    setTimeout(() => { btn.textContent = orig; }, 1200);
  });
}

// ── Splitter drag ───────────────────────────────────────────────────────────
(function () {
  let dragging = false;
  let startY = 0;
  let startH = 0;

  const SAVED_KEY = 'arachnite.detailsHeight';
  const saved = parseInt(localStorage.getItem(SAVED_KEY) || '', 10);
  if (saved && saved > 80 && saved < window.innerHeight - 200) {
    detailsEl.style.height = saved + 'px';
  }

  splitterEl.addEventListener('mousedown', (e) => {
    dragging = true;
    startY = e.clientY;
    startH = detailsEl.getBoundingClientRect().height;
    splitterEl.classList.add('dragging');
    document.body.classList.add('dragging');
    e.preventDefault();
  });

  document.addEventListener('mousemove', (e) => {
    if (!dragging) return;
    const dy = e.clientY - startY;
    const newH = Math.max(80, Math.min(window.innerHeight - 180, startH - dy));
    detailsEl.style.height = newH + 'px';
  });

  document.addEventListener('mouseup', () => {
    if (!dragging) return;
    dragging = false;
    splitterEl.classList.remove('dragging');
    document.body.classList.remove('dragging');
    localStorage.setItem(SAVED_KEY, parseInt(detailsEl.style.height, 10));
  });
})();

// ── Filter parsing ──────────────────────────────────────────────────────────
// Tokens like `level:warn`, `kind:temp`, `src:fan`, `type:log` become typed
// terms; everything else is collected as a single free-text substring match
// against row.dataset.raw.  A leading `-` negates either form.
let parsedFilter = { terms: [], text: '' };

function parseFilter(q) {
  const tokens = q.trim().toLowerCase().split(/\\s+/).filter(Boolean);
  const terms = [];
  const textParts = [];
  for (let tok of tokens) {
    const negate = tok.startsWith('-');
    if (negate) tok = tok.slice(1);
    if (!tok) continue;
    const i = tok.indexOf(':');
    const key = i > 0 ? tok.slice(0, i) : '';
    const val = i > 0 ? tok.slice(i + 1) : '';
    if (val && (key === 'level' || key === 'kind' || key === 'src' || key === 'type' || key === 'agent')) {
      terms.push({ key, val, negate });
    } else if (negate) {
      terms.push({ key: 'text', val: tok, negate: true });
    } else {
      textParts.push(tok);
    }
  }
  return { terms, text: textParts.join(' ') };
}

function rowMatches(row, parsed) {
  if (parsed.text && !row.dataset.raw.includes(parsed.text)) return false;
  for (const t of parsed.terms) {
    let hit;
    if (t.key === 'text') {
      hit = row.dataset.raw.includes(t.val);
    } else if (t.key === 'level') {
      // Prefix-match so `level:warn` catches both "warn" and "warning".
      hit = !!row.dataset.level && row.dataset.level.startsWith(t.val);
    } else if (t.key === 'kind') {
      hit = !!row.dataset.kind && row.dataset.kind.includes(t.val);
    } else if (t.key === 'src') {
      hit = row.dataset.src.includes(t.val);
    } else if (t.key === 'type') {
      hit = row.dataset.type === t.val;
    } else if (t.key === 'agent') {
      hit = !!row.dataset.agent && row.dataset.agent.includes(t.val);
    }
    if (t.negate ? hit : !hit) return false;
  }
  return true;
}

// ── Controls ────────────────────────────────────────────────────────────────
function applyFilter() {
  parsedFilter = parseFilter(filterEl.value);
  for (const row of feedEl.children) {
    if (!row.classList || !row.dataset || row.dataset.raw === undefined) continue;
    row.classList.toggle('hidden', !rowMatches(row, parsedFilter));
  }
  syncSevActive();
}

function togglePause() {
  paused = !paused;
  pauseBtn.classList.toggle('on', paused);
  pauseBtn.innerHTML = paused ? '&#9654; Resume' : '&#9646;&#9646; Pause';
  // Resuming snaps to bottom — but respect a deliberate scroll-up.
  if (!paused && !userScrolledUp) feedEl.scrollTop = feedEl.scrollHeight;
}

function clearFeed() {
  selectedRow = null;
  detailsContent.innerHTML = '';
  hideDetailsPane();
  // Remove only .row elements; keep the empty-state placeholder element
  // so we don't have to re-create it.
  Array.from(feedEl.getElementsByClassName('row')).forEach(r => r.remove());
  feedEmptyEl.style.display = '';
  // Remove only the dynamic source badges; the pinned severity strip stays.
  Array.from(statsEl.getElementsByClassName('badge')).forEach(e => e.remove());
  Object.keys(stats).forEach(k => delete stats[k]);
  resetSeverity();
  resetAgents();
  total = 0;
  lastTs = null;
  countEl.textContent = '0 events';
  // Feed is now empty — scroll lock is meaningless until rows arrive again.
  userScrolledUp = false;
  hidePill();
}

// Export every currently-visible row as NDJSON.  We export the original
// record attached to each row (row._record), not the rendered DOM — so the
// download contains the exact JSON each frame arrived as, useful for piping
// into jq / grep or archiving an investigation.  Hidden (filtered-out)
// rows are skipped so the export honours the active filter.
function exportVisible() {
  const rows = Array.from(feedEl.getElementsByClassName('row'))
    .filter(r => !r.classList.contains('hidden') && r._record);
  if (!rows.length) {
    // Brief visual nudge — flash the count.  Don't trigger a download.
    countEl.style.color = 'var(--yellow)';
    setTimeout(() => { countEl.style.color = ''; }, 600);
    return;
  }
  const lines = rows.map(r => JSON.stringify(r._record));
  const blob = new Blob([lines.join('\\n') + '\\n'],
                       { type: 'application/x-ndjson' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  // YYYY-MM-DDTHH-MM-SS — filesystem-safe, sorts chronologically
  const ts = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  a.download = `arachnite-${ts}.ndjson`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

// ── WebSocket with auto-reconnect ────────────────────────────────────────────
function connect() {
  // Match the page protocol so a TLS-served dashboard upgrades over WSS.
  const wsProto = (location.protocol === 'https:') ? 'wss' : 'ws';
  const ws = new WebSocket(`${wsProto}://${location.host}/ws`);

  ws.onopen = () => {
    statusEl.textContent = '\u25cf Connected';
    statusEl.className   = '';
    // Fresh server connection — drop any context snapshots cached from a
    // prior run.  Tick numbering restarts at 0 on runtime restart, so
    // stale snapshots would otherwise collide with new rows.
    contextByTick.clear();
    decisionByTick.clear();
  };

  ws.onmessage = e => {
    const msg = JSON.parse(e.data);
    if (msg.type === 'backlog') {
      msg.records.forEach(addRow);
    } else if (msg.type === 'status') {
      updateRtStatus(msg);
    } else if (msg.type === 'context_backlog') {
      msg.records.forEach(rememberContext);
    } else if (msg.type === 'context') {
      rememberContext(msg);
      // Refresh the open details pane in place — but only when not paused,
      // so the user can study a frozen view without it twitching beneath them.
      if (!paused && selectedRow && selectedRow._record &&
          selectedRow._record.tick === msg.tick) {
        renderDetails(selectedRow._record);
      }
    } else if (msg.type === 'decision_backlog') {
      msg.records.forEach(rememberDecision);
    } else if (msg.type === 'decision') {
      rememberDecision(msg);
      if (!paused && selectedRow && selectedRow._record &&
          selectedRow._record.tick === msg.tick) {
        renderDetails(selectedRow._record);
      }
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

// Strip any ?token=... from the URL bar after first load.  The server has
// already validated it and set an HttpOnly cookie, so leaving it visible
// would just leak the token to the browser history and any URL-bar
// snoopers.  No-op when no token was supplied.
(function stripTokenParam() {
  if (!location.search) return;
  const url = new URL(location.href);
  if (url.searchParams.has('token')) {
    url.searchParams.delete('token');
    history.replaceState(null, '', url.pathname + url.search + url.hash);
  }
})();

// ── Theme toggle ────────────────────────────────────────────────────────────
function applyTheme(theme) {
  const isLight = (theme === 'light');
  document.body.classList.toggle('light', isLight);
  const btn = document.getElementById('btn-theme');
  if (btn) btn.innerHTML = isLight ? '&#9728; Light' : '&#9790; Dark';
}
function toggleTheme() {
  const next = document.body.classList.contains('light') ? 'dark' : 'light';
  try { localStorage.setItem('arachnite.theme', next); } catch (e) {}
  applyTheme(next);
}
try { applyTheme(localStorage.getItem('arachnite.theme') || 'dark'); }
catch (e) { applyTheme('dark'); }

initSeverity();
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

class _ClientState:
    """Per-WebSocket state with a bounded outbox and a dedicated sender task.

    Producers (log/signal/context/status broadcasts) call put_nowait on the
    outbox queue.  When the queue is full, the oldest pending payload is
    dropped so the live tail keeps flowing — a single slow client can never
    stall the tick loop or balloon memory.
    """

    __slots__ = ("ws", "outbox", "sender_task", "dropped")

    def __init__(self, ws: Any, max_outbox: int) -> None:
        self.ws = ws
        self.outbox: asyncio.Queue[str] = asyncio.Queue(maxsize=max_outbox)
        self.sender_task: asyncio.Task[None] | None = None
        self.dropped: int = 0


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
    agent_node_id:
        Identifier used to label locally-originated signals on the
        dashboard.  Signals delivered over a transport are attributed
        to the originating agent via the transport envelope; this
        value is used only for signals that lack that envelope marker
        (i.e. produced on this agent's local bus).  Default ``"local"``.
    auth_token:
        When set, every HTTP and WebSocket request must present this
        token via ``Authorization: Bearer <token>``, ``?token=<token>``
        query string, or the ``arachnite_auth`` cookie (set
        automatically after a successful query-string auth).  Default
        ``None`` (no auth — appropriate for localhost-only binds).
    ssl_certfile / ssl_keyfile:
        Paths to a TLS certificate and private key.  When both are
        supplied the server speaks HTTPS (and the dashboard speaks
        WSS).  Default ``None``.
    allow_unauthenticated:
        Explicit opt-in required to bind to a non-loopback host
        without an ``auth_token``.  Default ``False`` — ``start()``
        raises if the host is non-loopback, no token is set, and this
        flag is False.  Use only if you understand the exposure.
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
        outbox_size: int = 256,
        agent_node_id: str = "local",
        auth_token: str | None = None,
        ssl_certfile: str | Path | None = None,
        ssl_keyfile: str | Path | None = None,
        allow_unauthenticated: bool = False,
    ) -> None:
        super().__init__(level=level)
        self._bus     = bus
        self._host    = host
        self._port    = port
        self._agent_node_id = agent_node_id
        self._auth_token = auth_token
        self._ssl_certfile = str(ssl_certfile) if ssl_certfile else None
        self._ssl_keyfile  = str(ssl_keyfile)  if ssl_keyfile  else None
        self._allow_unauthenticated = allow_unauthenticated
        self._backlog: deque[dict[str, Any]] = deque(maxlen=backlog)
        # Context snapshots are a separate stream — much larger payloads, and
        # we want to keep them indexable by tick on the client side.  The
        # cap is intentionally smaller than the log backlog.
        self._context_backlog: deque[dict[str, Any]] = deque(maxlen=200)
        # Decision snapshots — one per tick, indexed by tick on the client.
        self._decision_backlog: deque[dict[str, Any]] = deque(maxlen=200)
        # Per-client state: each WS has a bounded outbox queue + sender task.
        # When the queue saturates, oldest payloads are dropped (drop-oldest)
        # so a slow client can never stall the producer side.
        self._clients: dict[Any, _ClientState] = {}
        self._outbox_size: int = outbox_size
        self._drop_total: int = 0
        self._server_task: asyncio.Task[None] | None = None
        self._status_task: asyncio.Task[None] | None = None
        self._file: FileLogSink | None = (
            FileLogSink(log_file, level=level) if log_file else None
        )
        # Status-strip state — fed by submit_context() and surfaced via a
        # periodic "status" frame.  _tick_history stores recent (tick, ts)
        # pairs for a rolling tick-rate estimate.
        self._started_at: float | None = None
        self._first_tick_ts: float | None = None
        self._latest_tick: int = 0
        self._tick_history: deque[tuple[int, float]] = deque(maxlen=20)

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

        # Refuse to bind to a non-loopback interface without auth unless
        # explicitly waived.  The dashboard streams every signal, log,
        # decision and context snapshot — leaving it exposed on a LAN/
        # public address is a meaningful information leak.
        is_loopback = self._host in _LOOPBACK_HOSTS
        if (
            not is_loopback
            and not self._auth_token
            and not self._allow_unauthenticated
        ):
            raise RuntimeError(
                "SignalDashboard refusing to bind to non-loopback host "
                f"{self._host!r} without auth_token.  Either set "
                "auth_token=..., bind to 127.0.0.1, or pass "
                "allow_unauthenticated=True if you intentionally want an "
                "open dashboard."
            )

        if self._file is not None:
            self._file.open()

        self._started_at = time.monotonic()
        self._bus.subscribe("*", self._on_signal)
        self._server_task = asyncio.create_task(self._run_server())
        self._status_task = asyncio.create_task(self._status_loop())

    async def stop(self) -> None:
        """Unsubscribe from the bus, close file, shut down server."""
        self._bus.unsubscribe("*", self._on_signal)

        if self._file is not None:
            self._file.close()

        if self._status_task is not None:
            self._status_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._status_task
            self._status_task = None

        # Cancel any lingering per-client sender tasks.  Normal disconnect
        # already cancels them in ws_endpoint's finally clause, but if the
        # server is being torn down with clients still attached, clean up
        # here too.
        for state in list(self._clients.values()):
            if state.sender_task is not None and not state.sender_task.done():
                state.sender_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await state.sender_task
        self._clients.clear()

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
        # Origin agent: prefer the transport-injected marker (set when a
        # signal arrives over the wire), otherwise fall back to the
        # dashboard's local agent label.  Strip the reserved marker key
        # from the metadata view shown in the details pane so users see
        # only the real metadata.
        meta = dict(signal.metadata or {})
        origin = meta.pop("__origin_agent__", None) or self._agent_node_id
        record: dict[str, Any] = {
            "type":       "signal",
            "ts":         signal.timestamp,
            "kind":       signal.kind,
            "source":     signal.source,
            "value":      signal.value,
            "confidence": signal.confidence,
            "agent":      origin,
            "metadata":   meta,
        }
        await self._broadcast(record)
        if self._file is not None:
            self._file.write_signal(signal)

    # ── Context observer (registered with ArachniteRuntime) ───────────────────

    def submit_context(self, ctx: Context) -> None:
        """
        Receive a per-tick :class:`Context` snapshot from the runtime.

        Registered via ``ArachniteRuntime(context_observers=[dashboard.submit_context])``.
        The runtime invokes this synchronously inside its tick loop, so we
        serialise the snapshot and fire-and-forget a broadcast task on the
        running event loop.  Failures are swallowed — the dashboard must
        never stall a tick.
        """
        # Feed status-strip state.  We track (tick, monotonic_ts) pairs so
        # the status loop can compute a rolling tick rate.
        if self._first_tick_ts is None:
            self._first_tick_ts = ctx.timestamp
        self._latest_tick = ctx.tick
        self._tick_history.append((ctx.tick, ctx.timestamp))

        record = self._serialize_context(ctx)
        self._context_backlog.append(record)
        self._enqueue_all(_dumps(record))

    # ── Decision observer (registered with ArachniteRuntime) ──────────────────

    def submit_decision(self, event: DecisionEvent) -> None:
        """
        Receive a per-tick :class:`DecisionEvent` snapshot from the runtime.

        Registered via ``ArachniteRuntime(decision_observers=[dashboard.submit_decision])``.
        Captures everything the decision layer considered, what it chose to
        dispatch, and any interrupts issued — so the details pane can show
        *why* an action was (or wasn't) taken on a given tick.
        """
        record = self._serialize_decision(event)
        self._decision_backlog.append(record)
        self._enqueue_all(_dumps(record))

    # ── Per-client outbox / backpressure ─────────────────────────────────────

    def _enqueue_to(self, state: _ClientState, payload: str) -> None:
        """Enqueue a payload for a single client; drop oldest on overflow."""
        q = state.outbox
        if q.full():
            try:
                q.get_nowait()
            except asyncio.QueueEmpty:
                pass
            else:
                state.dropped += 1
                self._drop_total += 1
        try:
            q.put_nowait(payload)
        except asyncio.QueueFull:
            # Extremely unlikely — we just made room — but stay safe.
            state.dropped += 1
            self._drop_total += 1

    def _enqueue_all(self, payload: str) -> None:
        """Fan a payload out to every connected client."""
        for state in list(self._clients.values()):
            self._enqueue_to(state, payload)

    async def _client_sender(self, state: _ClientState) -> None:
        """Drain a client's outbox onto its WebSocket until disconnect."""
        try:
            while True:
                payload = await state.outbox.get()
                await state.ws.send_text(payload)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            # Send failed (closed socket, etc.) — drop this client.
            pass
        finally:
            self._clients.pop(state.ws, None)

    # ── Status strip ─────────────────────────────────────────────────────────

    def _compute_status(self) -> dict[str, Any]:
        """Build a 'status' record reflecting tick / rate / uptime / clients."""
        # Rolling tick rate from the recent history window.  Falls back to 0
        # when we don't yet have two samples to draw a slope from.
        rate = 0.0
        if len(self._tick_history) >= 2:
            first_tick, first_ts = self._tick_history[0]
            last_tick, last_ts = self._tick_history[-1]
            dt = last_ts - first_ts
            if dt > 0:
                rate = (last_tick - first_tick) / dt

        uptime = (
            time.monotonic() - self._started_at if self._started_at is not None else 0.0
        )

        return {
            "type":      "status",
            "tick":      self._latest_tick,
            "tick_rate": rate,
            "uptime_s":  uptime,
            "clients":   len(self._clients),
            "dropped":   self._drop_total,
        }

    async def _broadcast_status(self) -> None:
        self._enqueue_all(_dumps(self._compute_status()))

    async def _status_loop(self) -> None:
        """Broadcast a 'status' frame once per second for the header strip."""
        try:
            while True:
                await asyncio.sleep(1.0)
                if self._clients:
                    await self._broadcast_status()
        except asyncio.CancelledError:
            raise

    # ── Internal ──────────────────────────────────────────────────────────────

    @staticmethod
    def _serialize_signal(s: Signal) -> dict[str, Any]:
        return {
            "source":     s.source,
            "kind":       s.kind,
            "value":      s.value,
            "confidence": s.confidence,
            "ts":         s.timestamp,
            "metadata":   dict(s.metadata) if s.metadata else {},
        }

    @staticmethod
    def _serialize_result(r: Result) -> dict[str, Any]:
        return {
            "action_id":       r.action_id,
            "success":         r.success,
            "output":          r.output,
            "error":           str(r.error) if r.error is not None else None,
            "duration_s":      r.duration_s,
            "interrupted":     r.interrupted,
            "stopped_at_step": r.stopped_at_step,
            "rolled_back":     r.rolled_back,
        }

    @staticmethod
    def _serialize_action_state(a: ActionExecutionState) -> dict[str, Any]:
        return {
            "action_id":                   a.action_id,
            "current_step":                a.current_step,
            "completed_steps":             list(a.completed_steps),
            "interruptible":               a.interruptible,
            "mandatory_block_remaining_s": a.mandatory_block_remaining_s,
        }

    def _serialize_proposal(self, p: Proposal) -> dict[str, Any]:
        return {
            "instinct_id": p.instinct_id,
            "action_id":   p.action_id,
            "priority":    p.priority,
            "urgency":     p.urgency,
            "rationale":   p.rationale,
            "parameters":  dict(p.parameters or {}),
            "evidence":    dict(p.evidence or {}),
            "persist":     p.persist,
        }

    def _serialize_interrupt(self, r: InterruptRequest) -> dict[str, Any]:
        return {
            "requesting_instinct_id": r.requesting_instinct_id,
            "reason":                 r.reason,
            "timestamp":              r.timestamp,
            "new_proposal":           self._serialize_proposal(r.new_proposal),
        }

    def _serialize_decision(self, event: DecisionEvent) -> dict[str, Any]:
        dispatched_ids = {p.instinct_id for p in event.dispatched}
        considered = []
        for p in event.considered:
            considered.append({
                **self._serialize_proposal(p),
                "selected": p.instinct_id in dispatched_ids,
            })
        return {
            "type":       "decision",
            "ts":         event.timestamp,
            "tick":       event.tick,
            "strategy":   event.strategy,
            "considered": considered,
            "dispatched": [self._serialize_proposal(p) for p in event.dispatched],
            "interrupts": [self._serialize_interrupt(r) for r in event.interrupts],
        }

    def _serialize_context(self, ctx: Context) -> dict[str, Any]:
        return {
            "type":          "context",
            "ts":            ctx.timestamp,
            "tick":          ctx.tick,
            "signals":       [self._serialize_signal(s) for s in ctx.signals],
            "state":         dict(ctx.state),
            "last_result":   self._serialize_result(ctx.last_result) if ctx.last_result else None,
            "last_results":  [self._serialize_result(r) for r in ctx.last_results],
            "action_states": [self._serialize_action_state(a) for a in ctx.action_states],
            "history":       {
                "depth":      len(ctx.history),
                "last_count": len(ctx.history[-1]) if ctx.history else 0,
            },
        }


    async def _broadcast(self, record: dict[str, Any]) -> None:
        """Append to backlog and enqueue for every connected WebSocket client.

        Sends are non-blocking — payloads land in each client's bounded outbox
        and are drained by a dedicated sender task per client.  A stuck or
        slow client cannot stall the producer.
        """
        self._backlog.append(record)
        self._enqueue_all(_dumps(record))

    async def _run_server(self) -> None:
        import uvicorn
        config = uvicorn.Config(
            self._build_app(),
            host=self._host,
            port=self._port,
            log_level="error",
            access_log=False,
            ssl_certfile=self._ssl_certfile,
            ssl_keyfile=self._ssl_keyfile,
        )
        server = uvicorn.Server(config)
        await server.serve()

    # ── Auth helpers ─────────────────────────────────────────────────────────

    def _extract_token(
        self,
        *,
        headers: Any,
        query_params: Any,
        cookies: Any,
    ) -> str | None:
        """Look for an auth token in header, query, or cookie (in that order)."""
        auth_header = headers.get("authorization")
        if auth_header and isinstance(auth_header, str):
            prefix = "bearer "
            if auth_header.lower().startswith(prefix):
                return auth_header[len(prefix):].strip()
        qp = query_params.get("token")
        if qp:
            return qp
        return cookies.get(_AUTH_COOKIE)

    def _is_request_authorised(self, request: Any) -> bool:
        if not self._auth_token:
            return True
        sent = self._extract_token(
            headers=request.headers,
            query_params=request.query_params,
            cookies=request.cookies,
        )
        return bool(sent) and secrets.compare_digest(sent, self._auth_token)

    def _is_ws_authorised(self, ws: Any) -> bool:
        if not self._auth_token:
            return True
        sent = self._extract_token(
            headers=ws.headers,
            query_params=ws.query_params,
            cookies=ws.cookies,
        )
        return bool(sent) and secrets.compare_digest(sent, self._auth_token)

    def _build_app(self) -> Any:
        app       = FastAPI(title="Arachnite Dashboard", docs_url=None, redoc_url=None)
        dashboard = self
        cookie_secure = bool(dashboard._ssl_certfile and dashboard._ssl_keyfile)

        def _apply_auth_cookie(response: Any, request: Any) -> None:
            """If the user supplied ?token=... on a validated request, persist
            it as an HttpOnly cookie so future requests don't need the URL
            parameter.  Only set when auth is active *and* the request used
            the query string (cookie/header users already have what they need)."""
            if dashboard._auth_token and request.query_params.get("token"):
                response.set_cookie(
                    _AUTH_COOKIE,
                    dashboard._auth_token,
                    httponly=True,
                    samesite="strict",
                    secure=cookie_secure,
                )

        @app.get("/", response_class=HTMLResponse)  # type: ignore[misc,untyped-decorator,unused-ignore]
        async def index(request: Request) -> Any:
            if not dashboard._is_request_authorised(request):
                return PlainTextResponse(
                    "Unauthorized — supply token via ?token=, Authorization "
                    "header, or arachnite_auth cookie.",
                    status_code=401,
                )
            response = HTMLResponse(_DASHBOARD_HTML)
            _apply_auth_cookie(response, request)
            return response

        @app.get("/bg.png")  # type: ignore[misc,untyped-decorator,unused-ignore]
        async def bg_image(request: Request) -> Response:
            if not dashboard._is_request_authorised(request):
                return PlainTextResponse("Unauthorized", status_code=401)
            response = Response(
                content=_load_bg_image(),
                media_type="image/png",
                headers={"Cache-Control": "public, max-age=86400"},
            )
            _apply_auth_cookie(response, request)
            return response

        @app.websocket("/ws")  # type: ignore[misc,untyped-decorator,unused-ignore]
        async def ws_endpoint(ws: WebSocket) -> None:
            if not dashboard._is_ws_authorised(ws):
                # 1008 = policy violation. Close before accept so no
                # frames are exchanged.
                await ws.close(code=1008)
                return
            await ws.accept()
            state = _ClientState(ws, max_outbox=dashboard._outbox_size)
            dashboard._clients[ws] = state
            state.sender_task = asyncio.create_task(dashboard._client_sender(state))
            # Replay recent events to the newly connected browser via the
            # outbox so order is preserved relative to any concurrent live
            # broadcasts that arrive while the sender task is starting.
            if dashboard._backlog:
                dashboard._enqueue_to(state, _dumps({
                    "type":    "backlog",
                    "records": list(dashboard._backlog),
                }))
            if dashboard._context_backlog:
                dashboard._enqueue_to(state, _dumps({
                    "type":    "context_backlog",
                    "records": list(dashboard._context_backlog),
                }))
            if dashboard._decision_backlog:
                dashboard._enqueue_to(state, _dumps({
                    "type":    "decision_backlog",
                    "records": list(dashboard._decision_backlog),
                }))
            # Initial status snapshot so the header strip populates immediately.
            dashboard._enqueue_to(state, _dumps(dashboard._compute_status()))
            try:
                while True:
                    await ws.receive_text()  # keep connection open
            except WebSocketDisconnect:
                pass
            finally:
                # Cancel the sender so it stops awaiting on a dead socket.
                if state.sender_task is not None:
                    state.sender_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await state.sender_task
                dashboard._clients.pop(ws, None)

        return app
