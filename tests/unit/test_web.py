"""Unit tests for arachnite.web (SignalDashboard WebSocket handshake policy)."""

from __future__ import annotations

import contextlib
from typing import Any

import pytest

from arachnite import SignalBus

pytest.importorskip("fastapi")

from arachnite.web import SignalDashboard  # noqa: E402


class _Accepted(Exception):
    """Raised by the fake socket's accept() to stop the handler right there."""


class _FakeWebSocket:
    """Just enough of a Starlette WebSocket for the /ws handshake."""

    def __init__(self, headers: dict[str, str]) -> None:
        self.headers = {k.lower(): v for k, v in headers.items()}
        self.query_params: dict[str, str] = {}
        self.cookies: dict[str, str] = {}
        self.closed_with: int | None = None
        self.accepted = False

    async def close(self, code: int = 1000) -> None:
        self.closed_with = code

    async def accept(self) -> None:
        self.accepted = True
        raise _Accepted


async def _handshake(dashboard: SignalDashboard, headers: dict[str, str]) -> _FakeWebSocket:
    app = dashboard._build_app()
    route: Any = next(r for r in app.routes if getattr(r, "path", None) == "/ws")
    ws = _FakeWebSocket(headers)
    with contextlib.suppress(_Accepted):
        await route.endpoint(ws)
    return ws


class TestReview6DashboardOrigin:
    """R6-F19: the /ws stream is same-origin by default, with an explicit allowlist."""

    @pytest.mark.asyncio
    async def test_cross_origin_handshake_refused(self) -> None:
        """R6-F19: a page from another origin cannot open the live stream"""
        dashboard = SignalDashboard(SignalBus(), log_file=None)
        evil = await _handshake(
            dashboard, {"Origin": "http://evil.example", "Host": "127.0.0.1:7070"},
        )
        assert evil.closed_with == 1008 and not evil.accepted
        same = await _handshake(
            dashboard, {"Origin": "http://127.0.0.1:7070", "Host": "127.0.0.1:7070"},
        )
        assert same.accepted
        no_origin = await _handshake(dashboard, {"Host": "127.0.0.1:7070"})  # CLI client
        assert no_origin.accepted

    @pytest.mark.asyncio
    async def test_allowed_origins_extend_the_policy(self) -> None:
        """R6-F19: allowed_origins admits listed origins only ("*" admits all)"""
        dashboard = SignalDashboard(
            SignalBus(), log_file=None, allowed_origins=["http://localhost:3000/"],
        )
        listed = await _handshake(
            dashboard, {"Origin": "http://LOCALHOST:3000", "Host": "127.0.0.1:7070"},
        )
        assert listed.accepted
        other = await _handshake(
            dashboard, {"Origin": "http://localhost:3001", "Host": "127.0.0.1:7070"},
        )
        assert other.closed_with == 1008
        anyone = SignalDashboard(SignalBus(), log_file=None, allowed_origins=["*"])
        ws = await _handshake(anyone, {"Origin": "http://evil.example", "Host": "x:1"})
        assert ws.accepted
