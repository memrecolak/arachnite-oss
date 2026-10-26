"""Shutdown, pause and loop-robustness semantics of the runtime (second review, 2026-10-26)"""

from __future__ import annotations

import asyncio
import sys
import time

import pytest

from arachnite import SignalBus
from arachnite.context import ContextNode
from arachnite.models import Context, Proposal, Result, ShutdownPhase, Signal
from arachnite.nodes.action import ActionMasterNode, BaseActionNode
from arachnite.nodes.decision import DecisionMasterNode, GreedyDecisionNode
from arachnite.nodes.instinct import (
    BaseInstinctNode,
    BaseReflexInstinctNode,
    InstinctMasterNode,
)
from arachnite.nodes.sense import MAX_PENDING_PUSHES, BaseSenseNode, SenseMasterNode
from arachnite.runtime import ArachniteRuntime

TICK_HZ = 50.0


def _sig(source: str, kind: str, value: float) -> Signal:
    return Signal(source=source, kind=kind, value=value, confidence=1.0,
                  timestamp=time.monotonic())


class _Work(BaseActionNode):
    """Runs for a long time; its safe-stop takes ``safe_stop_s``; logs everything."""
    timeout_s = 30.0
    safe_stop_timeout_s = 2.0

    def __init__(
        self, bus: SignalBus, node_id: str, log: list[str], *,
        resources: frozenset[str] = frozenset({"arm"}), run_s: float = 10.0,
        safe_stop_s: float = 0.1,
    ) -> None:
        self.node_id = node_id  # type: ignore[misc]
        self.resources = resources  # type: ignore[misc]
        super().__init__(bus)
        self.log = log
        self.run_s = run_s
        self.safe_stop_s = safe_stop_s

    async def execute(self, proposal: Proposal) -> Result:
        self.log.append(f"execute {self.node_id}")
        await asyncio.sleep(self.run_s)
        return Result(action_id=self.node_id, success=True)

    async def on_preempted(self, proposal: Proposal, preempted_by: Proposal) -> None:
        self.log.append(f"safe-stop start {self.node_id}")
        await asyncio.sleep(self.safe_stop_s)
        self.log.append(f"safe-stop done {self.node_id}")

    async def teardown(self) -> None:
        self.log.append(f"teardown {self.node_id}")


class _Propose(BaseInstinctNode):
    """Proposes ``action_id`` every tick; runs ``hook`` on the given tick."""
    priority = 60

    def __init__(self, bus: SignalBus, node_id: str, action_id: str) -> None:
        self.node_id = node_id  # type: ignore[misc]
        super().__init__(bus)
        self.action_id = action_id
        self.runtime: ArachniteRuntime | None = None
        self.hook_tick: int | None = None
        self.hook: object = None
        self.delay_s = 0.0

    async def evaluate(self, ctx: Context) -> Proposal | None:
        if self.delay_s:
            await asyncio.sleep(self.delay_s)
        rt = self.runtime
        if rt is not None and rt.tick_count == self.hook_tick and callable(self.hook):
            await self.hook()
        return Proposal(instinct_id=self.node_id, action_id=self.action_id,
                        priority=self.priority, urgency=0.5)


class _Hazard(BaseSenseNode):
    """Event-driven sensor (never polled with data); logs its teardown."""
    node_id = "Hazard"
    signal_kind = "hazard"
    poll_interval_s = 0.0

    def __init__(self, bus: SignalBus, log: list[str]) -> None:
        super().__init__(bus)
        self.log = log

    async def read(self) -> Signal | None:
        return None

    async def teardown(self) -> None:
        self.log.append("teardown Hazard")


class _HazardReflex(BaseReflexInstinctNode):
    """Event-lane reflex; ``action_id`` None calls ``hook`` in evaluate()."""
    node_id = "HazardReflex"
    priority = 250
    trigger_on_signals = ["hazard"]

    def __init__(self, bus: SignalBus, action_id: str | None) -> None:
        super().__init__(bus)
        self.action_id = action_id
        self.hook: object = None

    async def evaluate(self, ctx: Context) -> Proposal | None:
        if not any(s.kind == "hazard" and s.value > 0.5 for s in ctx.signals):
            return None
        if self.action_id is None:
            if callable(self.hook):
                await self.hook()
            return None
        return Proposal(instinct_id=self.node_id, action_id=self.action_id,
                        priority=self.priority, urgency=1.0)


class _CallsRuntime(BaseActionNode):
    """Action whose execute() calls ``hook`` (stop / emergency_stop)."""
    resources = frozenset({"power"})

    def __init__(self, bus: SignalBus, node_id: str, log: list[str]) -> None:
        self.node_id = node_id  # type: ignore[misc]
        super().__init__(bus)
        self.log = log
        self.hook: object = None

    async def execute(self, proposal: Proposal) -> Result:
        self.log.append(f"execute {self.node_id}")
        if callable(self.hook):
            await self.hook()
        self.log.append(f"{self.node_id}: hook returned")
        return Result(action_id=self.node_id, success=True)

    async def teardown(self) -> None:
        self.log.append(f"teardown {self.node_id}")


def _build(
    bus: SignalBus,
    *,
    sense: list[BaseSenseNode] | None = None,
    instincts: list[BaseInstinctNode] | None = None,
    actions: list[BaseActionNode] | None = None,
    **kwargs: object,
) -> ArachniteRuntime:
    sm = SenseMasterNode(bus=bus)
    for s in sense or ():
        sm.register(s)
    im = InstinctMasterNode(bus=bus)
    for i in instincts or ():
        im.register(i)
    am = ActionMasterNode(bus=bus)
    for a in actions or ():
        am.register(a)
    return ArachniteRuntime(
        sense_master=sm, context=ContextNode(), instinct_master=im,
        decision_master=DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus)),
        action_master=am, bus=bus, tick_rate_hz=TICK_HZ, log_sinks=[],
        **kwargs,  # type: ignore[arg-type]
    )


def _work_runtime(
    log: list[str], *, run_s: float = 10.0, **kwargs: object,
) -> tuple[ArachniteRuntime, _Propose]:
    bus = SignalBus()
    goal = _Propose(bus, "Goal", "Work")
    rt = _build(bus, instincts=[goal], actions=[_Work(bus, "Work", log, run_s=run_s)],
                **kwargs)
    goal.runtime = rt
    return rt, goal


async def _until(predicate, timeout: float = 2.0) -> None:  # type: ignore[no-untyped-def]
    deadline = time.perf_counter() + timeout
    while not predicate():
        if time.perf_counter() > deadline:
            raise AssertionError("condition not reached")
        await asyncio.sleep(0.005)


class TestWaitAndEmergencyStop:
    @pytest.mark.asyncio
    async def test_wait_returns_after_the_emergency_stop_finished(self) -> None:
        """R1: wait() returns only once the safe-stops and the teardown are done"""
        log: list[str] = []
        rt, _ = _work_runtime(log)
        await rt.start()
        await _until(lambda: "execute Work" in log)
        # what loop.add_signal_handler(SIGTERM, ...) schedules
        asyncio.get_running_loop().call_soon(
            lambda: asyncio.ensure_future(rt.emergency_stop()),
        )
        await rt.wait()
        assert "safe-stop done Work" in log
        assert "teardown Work" in log

    @pytest.mark.asyncio
    async def test_in_tick_emergency_stop_then_wait_and_restart(self) -> None:
        """R1: an emergency stop from a tick is awaitable, and a restart is not undone by it"""
        log: list[str] = []
        rt, goal = _work_runtime(log, run_s=0.3)
        goal.hook_tick = 5
        goal.hook = rt.emergency_stop
        await rt.start()
        await rt.wait()
        await rt.stop()
        log.append("main resumed")
        assert log.index("safe-stop done Work") < log.index("main resumed")
        assert log.index("teardown Work") < log.index("main resumed")

        log.clear()
        goal.hook = None
        await rt.start()
        await _until(lambda: "execute Work" in log)
        await asyncio.sleep(0.1)
        assert rt.is_running
        assert "teardown Work" not in log
        await rt.stop()

    @pytest.mark.asyncio
    async def test_task_spawned_in_a_tick_waits_for_the_shutdown(self) -> None:
        """R2: a task spawned in a tick gets the full emergency stop, not the in-tick shortcut"""
        log: list[str] = []
        bus = SignalBus()
        rt_box: list[ArachniteRuntime] = []

        class WatchdogSense(BaseSenseNode):
            node_id = "WatchdogSense"
            signal_kind = "wd"
            poll_interval_s = 0.0
            spawned: asyncio.Task[None] | None = None

            async def _watchdog(self) -> None:
                await asyncio.sleep(0.2)                     # heartbeat lost
                await rt_box[0].emergency_stop()
                log.append("watchdog: power off")

            async def read(self) -> Signal | None:
                if self.spawned is None:
                    self.spawned = asyncio.get_running_loop().create_task(self._watchdog())
                return None

        goal = _Propose(bus, "Goal", "Work")
        rt = _build(bus, sense=[WatchdogSense(bus)], instincts=[goal],
                    actions=[_Work(bus, "Work", log)])
        rt_box.append(rt)
        await rt.start()
        await _until(lambda: "watchdog: power off" in log)
        assert log.index("safe-stop done Work") < log.index("watchdog: power off")
        assert log.index("teardown Work") < log.index("watchdog: power off")

    @pytest.mark.parametrize("where", ["lane action", "lane evaluate"])
    @pytest.mark.asyncio
    async def test_emergency_stop_from_the_event_lane_completes(self, where: str) -> None:
        """R3: an emergency stop from the event lane is neither cancelled nor cut short"""
        log: list[str] = []
        bus = SignalBus()
        sensor = _Hazard(bus, log)
        goal = _Propose(bus, "Goal", "Work")
        estop = _CallsRuntime(bus, "EStop", log)
        reflex = _HazardReflex(bus, "EStop" if where == "lane action" else None)
        rt = _build(bus, sense=[sensor], instincts=[goal, reflex],
                    actions=[_Work(bus, "Work", log), estop],
                    reflex_mode="event", reflex_eval_timeout_s=0.05)
        estop.hook = rt.emergency_stop
        reflex.hook = rt.emergency_stop
        await rt.start()
        await _until(lambda: "execute Work" in log)
        await sensor.push(_sig("Hazard", "hazard", 1.0))
        await rt.wait()
        assert not rt.is_running
        assert "safe-stop done Work" in log
        assert {"teardown Work", "teardown Hazard", "teardown EStop"} <= set(log)


class TestStopFromInside:
    @pytest.mark.asyncio
    async def test_stop_from_inside_a_tick_completes(self) -> None:
        """R4: stop() called by node code in a tick returns and the shutdown completes"""
        log: list[str] = []
        # A short action: graceful stop lets a plain action run to its end.
        rt, goal = _work_runtime(log, run_s=0.3)
        goal.hook_tick = 3
        goal.hook = rt.stop
        loop_errors: list[dict[str, object]] = []
        asyncio.get_running_loop().set_exception_handler(
            lambda loop, context: loop_errors.append(context)
        )
        try:
            await rt.start()
            await asyncio.wait_for(rt.wait(), 5.0)
        finally:
            asyncio.get_running_loop().set_exception_handler(None)
        assert loop_errors == []
        assert rt._shutdown_coordinator.phase is ShutdownPhase.COMPLETE
        assert "teardown Work" in log

    @pytest.mark.asyncio
    async def test_stop_from_inside_an_action_completes(self) -> None:
        """R4: stop() called by a running action returns and phases 5-7 still run"""
        log: list[str] = []
        bus = SignalBus()
        goal = _Propose(bus, "Goal", "Halt")
        halt = _CallsRuntime(bus, "Halt", log)
        rt = _build(bus, instincts=[goal], actions=[halt])
        halt.hook = rt.stop
        started = time.perf_counter()
        await rt.start()
        await asyncio.wait_for(rt.wait(), 5.0)
        assert time.perf_counter() - started < 2.0
        assert "Halt: hook returned" in log
        assert rt._shutdown_coordinator.phase is ShutdownPhase.COMPLETE
        assert "teardown Halt" in log

    @pytest.mark.asyncio
    async def test_emergency_stop_takes_over_a_graceful_stop(self) -> None:
        """R5: an emergency stop during stop() phase 2 does not raise into it or tear down twice"""
        log: list[str] = []
        rt, goal = _work_runtime(log)
        await rt.start()
        await _until(lambda: "execute Work" in log)
        goal.delay_s = 0.5                                  # a slow tick to drain
        await asyncio.sleep(0.05)
        stopping = asyncio.ensure_future(rt.stop())
        await _until(lambda: rt._shutdown_coordinator.phase is ShutdownPhase.DRAIN_REFLEXES)
        await rt.emergency_stop()
        await asyncio.wait_for(stopping, 5.0)               # returns, does not raise
        assert log.count("teardown Work") == 1
        assert "safe-stop done Work" in log


class TestRepeatedShutdown:
    @pytest.mark.asyncio
    async def test_second_stop_waits_for_the_first(self) -> None:
        """R10: a concurrent second stop() returns only after the shutdown finished"""
        log: list[str] = []
        rt, _ = _work_runtime(log, run_s=0.3)
        await rt.start()
        await _until(lambda: "execute Work" in log)
        first = asyncio.ensure_future(rt.stop())
        await asyncio.sleep(0)
        await rt.stop()
        log.append("second stop() returned")
        await first
        assert log.index("teardown Work") < log.index("second stop() returned")

    @pytest.mark.asyncio
    async def test_nodes_are_torn_down_once(self) -> None:
        """R11: two emergency stops, or one after a completed stop(), tear down once"""
        log: list[str] = []
        rt, _ = _work_runtime(log)
        await rt.start()
        await _until(lambda: "execute Work" in log)
        await asyncio.gather(rt.emergency_stop(), rt.emergency_stop())
        assert log.count("teardown Work") == 1

        log.clear()
        rt2, _ = _work_runtime(log, run_s=0.3)
        await rt2.start()
        await rt2.stop()
        await rt2.emergency_stop()
        assert log.count("teardown Work") == 1


class TestPauseAndLoop:
    @pytest.mark.asyncio
    async def test_pause_with_a_tick_in_flight_launches_nothing_after(self) -> None:
        """R8: pause() waits for the loop's tick, which then launches nothing deliberative"""
        log: list[str] = []
        rt, goal = _work_runtime(log, run_s=0.3)
        goal.delay_s = 0.1                                  # the tick is in instinct eval
        await rt.start()
        await asyncio.sleep(0.03)
        await rt.pause()
        log.append("pause() returned")
        await asyncio.sleep(0.2)
        assert "execute Work" not in log
        await rt.stop()

    @pytest.mark.asyncio
    async def test_pushes_while_paused_are_bounded(self) -> None:
        """R9: the lane stays live while paused, but pushed signals queue up to a cap"""
        log: list[str] = []
        bus = SignalBus()
        sensor = _Hazard(bus, log)
        rt = _build(bus, sense=[sensor], instincts=[_HazardReflex(bus, None)],
                    reflex_mode="event")
        await rt.start()
        await rt.pause()
        for _ in range(MAX_PENDING_PUSHES + 100):
            await sensor.push(_sig("Hazard", "hazard", 0.0))
        assert len(sensor._pushed) == MAX_PENDING_PUSHES
        await rt.resume()
        await rt.stop()

    @pytest.mark.skipif(
        sys.version_info < (3, 11),
        reason="Python 3.10 cannot tell a stray CancelledError from a cancel of the loop",
    )
    @pytest.mark.asyncio
    async def test_stray_cancelled_error_in_a_node_does_not_end_the_loop(self) -> None:
        """R7: a node raising CancelledError on its own is logged; ticking continues"""
        bus = SignalBus()

        class Broken(BaseInstinctNode):
            node_id = "Broken"
            priority = 10

            async def evaluate(self, ctx: Context) -> Proposal | None:
                fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
                fut.cancel()
                await fut
                return None

        rt = _build(bus, instincts=[Broken(bus)])
        await rt.start()
        await asyncio.sleep(0.1)
        before = rt.tick_count
        await asyncio.sleep(0.1)
        assert rt.tick_count > before
        await rt.stop()

    @pytest.mark.asyncio
    async def test_subscriber_raising_cancelled_error_does_not_end_the_loop(self) -> None:
        """R7: a bus subscriber raising CancelledError is a subscriber error, not a cancel"""
        bus = SignalBus()

        class Near(BaseSenseNode):
            node_id = "Near"
            signal_kind = "proximity"
            poll_interval_s = 0.0

            async def read(self) -> Signal:
                return _sig(self.node_id, self.signal_kind, 0.5)

        async def broken(signal: Signal) -> None:
            fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            fut.cancel()
            await fut

        bus.subscribe("proximity", broken)
        rt = _build(bus, sense=[Near(bus)])
        await rt.start()
        await asyncio.sleep(0.1)
        before = rt.tick_count
        await asyncio.sleep(0.1)
        assert rt.tick_count > before
        await rt.stop()
