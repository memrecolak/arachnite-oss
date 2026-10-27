"""
Shared pytest fixtures for Arachnite tests.
"""

from __future__ import annotations

import asyncio
import fnmatch
import time
from collections import deque
from collections.abc import Iterator
from typing import Any

import pytest
import pytest_asyncio

from arachnite import ContextNode, SignalBus
from arachnite.models import (
    ActionStep,
    Context,
    InterruptPolicy,
    Permission,
    Proposal,
    Result,
    Signal,
    StepResult,
)
from arachnite.nodes.action import (
    ActionMasterNode,
    BaseActionNode,
    MultiStepActionNode,
)
from arachnite.nodes.decision import DecisionMasterNode, GreedyDecisionNode
from arachnite.nodes.instinct import (
    BaseInstinctNode,
    BaseReflexInstinctNode,
    InstinctMasterNode,
)
from arachnite.nodes.sense import BaseSenseNode, SenseMasterNode
from arachnite.runtime import ArachniteRuntime

# ── Shared helpers ────────────────────────────────────────────────────────────

def make_signal(
    kind: str = "thermal",
    value: float = 20.0,
    confidence: float = 1.0,
    source: str = "test",
) -> Signal:
    return Signal(
        source=source,
        kind=kind,
        value=value,
        confidence=confidence,
        timestamp=time.monotonic(),
    )


def make_context(
    signals: list[Signal] | None = None,
    tick: int = 1,
) -> Context:
    return Context(
        tick        = tick,
        signals     = signals or [],
        history     = deque(),
        state       = {},
        last_result = None,
        timestamp   = time.monotonic(),
    )


def make_proposal(
    action_id: str = "TestAction",
    priority: int = 50,
    urgency: float = 0.5,
    instinct_id: str = "test_instinct",
) -> Proposal:
    return Proposal(
        instinct_id = instinct_id,
        action_id   = action_id,
        priority    = priority,
        urgency     = urgency,
    )


class ManualClock:
    """
    Stand-in for the ``time`` module whose clocks read ``now``.

    Monkeypatch it over a framework module's ``time`` attribute to make
    durations and signal ages exact: ``monotonic()`` and ``perf_counter()``
    return ``now`` (the test advances it); every other attribute is the
    real ``time`` module's.
    """

    def __init__(self, now: float = 0.0) -> None:
        self.now = now

    def monotonic(self) -> float:
        return self.now

    def perf_counter(self) -> float:
        return self.now

    def __getattr__(self, name: str) -> object:
        return getattr(time, name)


# ── Concrete test nodes ───────────────────────────────────────────────────────

class ConstantSenseNode(BaseSenseNode):
    """Emits a constant value each tick."""
    node_id     = "ConstantSenseNode"
    signal_kind = "thermal"

    def __init__(self, bus: SignalBus, value: float = 25.0, **kwargs: object) -> None:
        super().__init__(bus, **kwargs)  # type: ignore[arg-type]
        self._value = value

    async def read(self) -> Signal:
        return make_signal(kind=self.signal_kind, value=self._value)


class SlowSenseNode(BaseSenseNode):
    """Sensor that sleeps during read() to simulate slow hardware"""
    node_id     = "SlowSenseNode"
    signal_kind = "thermal"
    poll_interval_s = 0.0

    def __init__(
        self, bus: SignalBus, delay: float = 0.05, **kwargs: object
    ) -> None:
        super().__init__(bus, **kwargs)  # type: ignore[arg-type]
        self._delay = delay

    async def read(self) -> Signal:
        await asyncio.sleep(self._delay)
        return make_signal(kind=self.signal_kind, value=99.0)


class ThresholdInstinct(BaseInstinctNode):
    """Fires when thermal signal exceeds threshold."""
    node_id  = "ThresholdInstinct"
    priority = 100

    def __init__(self, bus: SignalBus, threshold: float = 80.0, **kwargs: object) -> None:
        super().__init__(bus, **kwargs)  # type: ignore[arg-type]
        self.threshold = threshold

    async def evaluate(self, ctx: Context) -> Proposal | None:
        thermal = [s for s in ctx.signals if s.kind == "thermal"]
        if thermal and thermal[-1].value > self.threshold:
            return make_proposal(
                action_id   = "CoolDownAction",
                priority    = self.priority,
                instinct_id = self.node_id,
            )
        return None


class EmergencyReflex(BaseReflexInstinctNode):
    """Reflex: fires when thermal > critical_threshold."""
    node_id  = "EmergencyReflex"
    priority = 200

    def __init__(
        self, bus: SignalBus, critical_threshold: float = 95.0, **kwargs: object
    ) -> None:
        super().__init__(bus, **kwargs)  # type: ignore[arg-type]
        self.critical = critical_threshold

    async def evaluate(self, ctx: Context) -> Proposal | None:
        thermal = [s for s in ctx.signals if s.kind == "thermal"]
        if thermal and thermal[-1].value > self.critical:
            return make_proposal(
                action_id   = "EmergencyStop",
                priority    = self.priority,
                urgency     = 1.0,
                instinct_id = self.node_id,
            )
        return None


class RecordingAction(BaseActionNode):
    """Records all proposals it receives."""
    node_id = "RecordingAction"

    def __init__(self, bus: SignalBus, **kwargs: object) -> None:
        super().__init__(bus, **kwargs)  # type: ignore[arg-type]
        self.calls: list[Proposal] = []

    async def execute(self, proposal: Proposal) -> Result:
        self.calls.append(proposal)
        return Result(action_id=self.node_id, success=True, output=proposal.parameters)


class FailingAction(BaseActionNode):
    """Always returns a failed Result."""
    node_id = "FailingAction"

    async def execute(self, proposal: Proposal) -> Result:
        return Result(
            action_id = self.node_id,
            success   = False,
            error     = RuntimeError("Intentional failure"),
        )


class TwoStepAction(MultiStepActionNode):
    """Simple two-step action: step1 (interruptible) → step2 (interruptible)."""
    node_id          = "TwoStepAction"
    interrupt_policy = InterruptPolicy.ALWAYS

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("step1", interruptible=True),
            ActionStep("step2", interruptible=True),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        await asyncio.sleep(0.02)  # simulate async work so interrupt tasks can fire
        return StepResult(step_name=step.name, success=True, output=step.name)


class MandatoryBlockAction(MultiStepActionNode):
    """Three-step action with a mandatory middle block."""
    node_id          = "MandatoryBlockAction"
    interrupt_policy = InterruptPolicy.ROLLBACK

    def __init__(self, bus: SignalBus, **kwargs: object) -> None:
        super().__init__(bus, **kwargs)  # type: ignore[arg-type]
        self.rolled_back: list[str] = []

    async def _undo_step2(self) -> None:
        self.rolled_back.append("step2")

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("step1", interruptible=True),
            ActionStep("step2", interruptible=False, rollback=self._undo_step2),
            ActionStep("step3", interruptible=True),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        return StepResult(step_name=step.name, success=True, output=step.name)


# ── Review-6 distributed nodes (node_id deliberately differs from class name) ─

class R6TempSense(BaseSenseNode):
    """Emits temperature=99.0 every tick."""
    node_id         = "r6.temp"
    signal_kind     = "temperature"
    poll_interval_s = 0.0

    async def read(self) -> Signal:
        return Signal(source=self.node_id, kind=self.signal_kind, value=99.0,
                      confidence=1.0, timestamp=time.monotonic())


class R7AttrReflex(BaseInstinctNode):
    """A reflex by attribute only (reflex = True), not by subclassing the reflex base."""
    node_id  = "r7.attr_reflex"
    priority = 250
    reflex   = True

    async def evaluate(self, ctx: Context) -> Proposal | None:
        return None


class R6HotReflex(BaseReflexInstinctNode):
    """Reflex targeting action node_id 'Brake'."""
    node_id  = "r6.hot_reflex"
    priority = 250

    async def evaluate(self, ctx: Context) -> Proposal | None:
        if any(s.kind == "temperature" and s.value > 50 for s in ctx.signals):
            return Proposal(instinct_id=self.node_id, action_id="Brake",
                            priority=self.priority, urgency=1.0)
        return None


class R6BrakeActuator(BaseActionNode):
    """The action proposals target as 'Brake' (class name differs)."""
    node_id = "Brake"

    async def execute(self, proposal: Proposal) -> Result:
        return Result(action_id=self.node_id, success=True)


class Brake(BaseActionNode):
    """An unrelated action whose CLASS NAME is 'Brake' (its node_id is 'BrakeLog')."""
    node_id = "BrakeLog"

    async def execute(self, proposal: Proposal) -> Result:
        return Result(action_id=self.node_id, success=True)


class R6SharedIdA(BaseActionNode):
    node_id = "Shared"

    async def execute(self, proposal: Proposal) -> Result:
        return Result(action_id=self.node_id, success=True)


class R6SharedIdB(R6SharedIdA):
    node_id = "Shared"


class R6NetSense(R6TempSense):
    node_id     = "r6.net"
    permissions = frozenset({Permission.NETWORK})


class R6SignalRecorder(BaseInstinctNode):
    """Records the (source, kind) of every ctx.signals entry per evaluation."""
    node_id  = "r6.recorder"
    priority = 10

    def __init__(self, bus: SignalBus, **kwargs: object) -> None:
        super().__init__(bus, **kwargs)  # type: ignore[arg-type]
        self.seen: list[list[tuple[str, str]]] = []

    async def evaluate(self, ctx: Context) -> Proposal | None:
        self.seen.append([(s.source, s.kind) for s in ctx.signals])
        return None


# ── In-memory broker fakes (no network; used to drive the real transports) ───

async def settle(n: int = 10) -> None:
    """Let scheduled callbacks run."""
    for _ in range(n):
        await asyncio.sleep(0)


def _nats_match(pattern: str, subject: str) -> bool:
    p, s = pattern.split("."), subject.split(".")
    for i, tok in enumerate(p):
        if tok == ">":
            return len(s) > i
        if i >= len(s) or (tok != "*" and tok != s[i]):
            return False
    return len(p) == len(s)


class FakeNatsServer:
    """NATS server honouring subject wildcards; one delivery per matching sub."""

    def __init__(self) -> None:
        self.subs: list[FakeNatsSub] = []
        self.fail_next_subscribe = 0

    async def publish(self, subject: str, payload: bytes) -> None:
        for sub in list(self.subs):
            if sub.active and sub.nc.open and _nats_match(sub.subject, subject):
                await sub.cb(type("Msg", (), {"data": payload, "subject": subject})())

    def live_subjects(self, nc: FakeNatsConn) -> list[str]:
        return sorted(s.subject for s in self.subs if s.active and s.nc is nc and nc.open)


class FakeNatsSub:
    def __init__(self, nc: FakeNatsConn, subject: str, cb: Any) -> None:
        self.nc, self.subject, self.cb, self.active = nc, subject, cb, True

    async def unsubscribe(self) -> None:
        self.active = False


class FakeNatsConn:
    def __init__(self, server: FakeNatsServer) -> None:
        self.server, self.open = server, True

    async def subscribe(self, subject: str, cb: Any) -> FakeNatsSub:
        if self.server.fail_next_subscribe:
            self.server.fail_next_subscribe -= 1
            raise TimeoutError("nats: timeout")
        sub = FakeNatsSub(self, subject, cb)
        self.server.subs.append(sub)
        return sub

    async def publish(self, subject: str, payload: bytes) -> None:
        await self.server.publish(subject, payload)

    async def drain(self) -> None:
        self.open = False


class FakeMqttBroker:
    """MQTT broker routing with paho's topic filter matching."""

    def __init__(self) -> None:
        self.up = True
        self.clients: list[FakeMqttClient] = []
        self.connect_attempts = 0

    async def route(self, topic: str, payload: bytes) -> None:
        from paho.mqtt.client import topic_matches_sub
        for c in list(self.clients):
            if c.alive and any(topic_matches_sub(f, topic) for f in c.subs):
                c.queue.put_nowait(type("Msg", (), {"payload": payload, "topic": topic})())

    def drop_all(self) -> None:
        for c in list(self.clients):
            c.kill()


class _FakeSock:
    def setsockopt(self, *_a: Any) -> None:
        pass


class FakeMqttClient:
    """The aiomqtt.Client surface MQTTTransport uses."""
    broker: FakeMqttBroker

    def __init__(self, **kw: Any) -> None:
        self.kw = kw
        self.subs: set[str] = set()
        self.alive = False
        self.queue: asyncio.Queue[Any] = asyncio.Queue()
        self._client = type("Paho", (), {"socket": lambda _self: _FakeSock()})()

    async def __aenter__(self) -> FakeMqttClient:
        import aiomqtt
        self.broker.connect_attempts += 1
        await asyncio.sleep(0)
        if not self.broker.up:
            raise aiomqtt.MqttError("connection refused")
        self.alive = True
        self.broker.clients.append(self)
        return self

    async def __aexit__(self, *_a: Any) -> None:
        self.alive = False
        if self in self.broker.clients:
            self.broker.clients.remove(self)

    async def subscribe(self, topic: str, qos: int = 0) -> None:
        import aiomqtt
        if not self.alive:
            raise aiomqtt.MqttError("not connected")
        self.subs.add(topic)

    async def unsubscribe(self, topic: str) -> None:
        self.subs.discard(topic)

    async def publish(self, topic: str, payload: bytes, qos: int = 0) -> None:
        import aiomqtt
        if not self.alive:
            raise aiomqtt.MqttError("not connected")
        await self.broker.route(topic, payload)

    def kill(self) -> None:
        self.alive = False
        if self in self.broker.clients:
            self.broker.clients.remove(self)
        self.queue.put_nowait(None)

    @property
    def messages(self) -> Any:
        client = self

        class _It:
            def __aiter__(self) -> Any:
                return self

            async def __anext__(self) -> Any:
                import aiomqtt
                m = await client.queue.get()
                if m is None:
                    raise aiomqtt.MqttError("Disconnected during message iteration")
                return m
        return _It()


class FakeRedisServer:
    """Redis pub/sub: SUBSCRIBE channels and PSUBSCRIBE glob patterns."""

    def __init__(self) -> None:
        self.up = True
        self.pubsubs: list[FakeRedisPubSub] = []
        self.pings = 0

    async def publish(self, channel: str, data: bytes) -> None:
        for ps in list(self.pubsubs):
            if channel in ps.channels:
                ps.queue.put_nowait({"type": "message", "channel": channel,
                                     "pattern": None, "data": data})
            for pattern in list(ps.patterns):
                if fnmatch.fnmatchcase(channel, pattern):
                    ps.queue.put_nowait({"type": "pmessage", "channel": channel,
                                         "pattern": pattern, "data": data})


class FakeRedisClient:
    def __init__(self, server: FakeRedisServer) -> None:
        self.server = server

    def __await__(self) -> Any:
        async def _init() -> FakeRedisClient:
            return self
        return _init().__await__()

    async def ping(self) -> bool:
        self.server.pings += 1
        if not self.server.up:
            raise ConnectionError("Error connecting: connection refused")
        return True

    async def publish(self, channel: str, data: bytes) -> int:
        await self.server.publish(channel, data)
        return 1

    def pubsub(self) -> FakeRedisPubSub:
        ps = FakeRedisPubSub(self.server)
        self.server.pubsubs.append(ps)
        return ps

    async def aclose(self) -> None:
        pass


class FakeRedisPubSub:
    def __init__(self, server: FakeRedisServer) -> None:
        self.server = server
        self.channels: dict[str, None] = {}
        self.patterns: dict[str, None] = {}
        self.queue: asyncio.Queue[Any] = asyncio.Queue()

    def _check(self) -> None:
        if not self.server.up:
            raise ConnectionError("Connection closed by server.")

    async def subscribe(self, *channels: str) -> None:
        self._check()
        self.channels.update(dict.fromkeys(channels))

    async def psubscribe(self, *patterns: str) -> None:
        self._check()
        self.patterns.update(dict.fromkeys(patterns))

    async def unsubscribe(self, *channels: str) -> None:
        for c in channels:
            self.channels.pop(c, None)

    async def punsubscribe(self, *patterns: str) -> None:
        for p in patterns:
            self.patterns.pop(p, None)

    async def listen(self) -> Any:
        while self.channels or self.patterns:
            msg = await self.queue.get()
            if msg is None:
                raise ConnectionError("Connection closed by server.")
            yield msg


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def fake_nats_server(monkeypatch: pytest.MonkeyPatch) -> FakeNatsServer:
    """Route NATSTransport.connect() to an in-memory FakeNatsServer."""
    pytest.importorskip("nats")
    import arachnite.transport.nats as nats_mod
    server = FakeNatsServer()

    async def connect(**_kw: Any) -> FakeNatsConn:
        return FakeNatsConn(server)

    monkeypatch.setattr(nats_mod.nats, "connect", connect)
    return server


@pytest.fixture
def fake_mqtt_broker(monkeypatch: pytest.MonkeyPatch) -> Iterator[FakeMqttBroker]:
    """Route MQTTTransport's aiomqtt.Client to an in-memory FakeMqttBroker."""
    pytest.importorskip("aiomqtt")
    import arachnite.transport.mqtt as mqtt_mod
    broker = FakeMqttBroker()
    client_cls = type("BoundFakeMqttClient", (FakeMqttClient,), {"broker": broker})
    monkeypatch.setattr(mqtt_mod.aiomqtt, "Client", client_cls)
    yield broker


@pytest.fixture
def fake_redis_server(monkeypatch: pytest.MonkeyPatch) -> FakeRedisServer:
    """Route RedisTransport's redis.asyncio.from_url to an in-memory FakeRedisServer."""
    pytest.importorskip("redis.asyncio")
    import arachnite.transport.redis as redis_mod
    server = FakeRedisServer()

    def from_url(_url: str, **_kw: Any) -> FakeRedisClient:
        return FakeRedisClient(server)

    monkeypatch.setattr(redis_mod.aioredis, "from_url", from_url)
    return server

@pytest.fixture
def bus() -> SignalBus:
    return SignalBus()


@pytest.fixture
def context() -> ContextNode:
    return ContextNode(history_length=5)


@pytest.fixture
def sense_master(bus: SignalBus) -> SenseMasterNode:
    return SenseMasterNode(bus=bus)


@pytest.fixture
def instinct_master(bus: SignalBus) -> InstinctMasterNode:
    return InstinctMasterNode(bus=bus)


@pytest.fixture
def decision_master(bus: SignalBus) -> DecisionMasterNode:
    return DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))


@pytest.fixture
def action_master(bus: SignalBus) -> ActionMasterNode:
    return ActionMasterNode(bus=bus)


@pytest.fixture
def recording_action(bus: SignalBus) -> RecordingAction:
    return RecordingAction(bus=bus)


@pytest_asyncio.fixture
async def runtime(
    bus:             SignalBus,
    context:         ContextNode,
    sense_master:    SenseMasterNode,
    instinct_master: InstinctMasterNode,
    decision_master: DecisionMasterNode,
    action_master:   ActionMasterNode,
    recording_action: RecordingAction,
) -> ArachniteRuntime:
    sense_master.register(ConstantSenseNode(bus=bus, value=25.0))
    instinct_master.register(ThresholdInstinct(bus=bus, threshold=80.0))
    action_master.register(recording_action)

    rt = ArachniteRuntime(
        sense_master    = sense_master,
        context         = context,
        instinct_master = instinct_master,
        decision_master = decision_master,
        action_master   = action_master,
        bus             = bus,
        tick_rate_hz    = 100.0,
    )
    await rt.start()
    yield rt
    await rt.stop()
