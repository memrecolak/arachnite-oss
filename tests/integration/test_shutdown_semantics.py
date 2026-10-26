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


# ── Third review round (docs/audits 2026-10-26, runtime findings T1–T12) ──────

class _SlowSetupSense(BaseSenseNode):
    """Sense node whose setup() takes 0.2 s (hardware bring-up)."""
    node_id = "SlowSetup"
    signal_kind = "x"
    poll_interval_s = 0.0

    def __init__(self, bus: SignalBus, log: list[str]) -> None:
        super().__init__(bus)
        self.log = log
        self.setting_up = asyncio.Event()

    async def setup(self) -> None:
        self.setting_up.set()
        await asyncio.sleep(0.2)
        self.log.append("setup done")

    async def read(self) -> Signal | None:
        self.log.append("read")
        return None

    async def teardown(self) -> None:
        self.log.append("teardown SlowSetup")


class TestThirdReviewRuntime:
    @pytest.mark.parametrize("how", ["stop", "emergency_stop"])
    @pytest.mark.asyncio
    async def test_stop_during_start_aborts_the_start(self, how: str) -> None:
        """T1: a stop or emergency stop while start() sets up is not lost"""
        log: list[str] = []
        bus = SignalBus()
        sense = _SlowSetupSense(bus, log)
        rt = _build(bus, sense=[sense])
        starting = asyncio.ensure_future(rt.start())
        await sense.setting_up.wait()
        await (rt.stop() if how == "stop" else rt.emergency_stop())
        await starting
        await asyncio.sleep(0.05)
        assert not rt.is_running
        assert "read" not in log
        assert "teardown SlowSetup" in log

    @pytest.mark.asyncio
    async def test_concurrent_starts_set_up_once(self) -> None:
        """T9: a second start() during the first waits for it; masters are set up once"""
        log: list[str] = []
        bus = SignalBus()
        rt = _build(bus, sense=[_SlowSetupSense(bus, log)])
        await asyncio.gather(rt.start(), rt.start())
        assert log.count("setup done") == 1
        await rt.stop()

    @pytest.mark.asyncio
    async def test_stop_inside_a_tick_launches_nothing_more(self) -> None:
        """T2: after stop() from a tick, that tick launches no deliberative action"""
        log: list[str] = []
        bus = SignalBus()
        goal = _Propose(bus, "Goal", "Fresh")
        rt = _build(bus, instincts=[goal], actions=[_Work(bus, "Fresh", log, run_s=0.2)])
        goal.runtime = rt
        goal.hook_tick = 1
        goal.hook = rt.stop
        await rt.start()
        await asyncio.wait_for(rt.wait(), 5.0)
        assert "execute Fresh" not in log

    @pytest.mark.asyncio
    async def test_emergency_stop_cancels_pending_restarts(self) -> None:
        """T3: no supervisor restart tears a node down and sets it up again after an e-stop"""
        log: list[str] = []
        bus = SignalBus()

        class Flaky(BaseSenseNode):
            node_id = "Flaky"
            signal_kind = "x"
            poll_interval_s = 0.0
            failed = False

            async def setup(self) -> None:
                log.append("setup Flaky")

            async def teardown(self) -> None:
                log.append("teardown Flaky")

            async def read(self) -> Signal | None:
                if not self.failed:
                    self.failed = True
                    raise RuntimeError("sensor glitch")
                return None

        rt = _build(bus, sense=[Flaky(bus)])
        for sup in rt._supervisors:
            sup._restart_delay = 0.2
        await rt.start()
        await asyncio.sleep(0.1)                       # fault seen, restart pending
        await rt.emergency_stop()
        log.append("emergency_stop returned")
        await asyncio.sleep(0.4)
        assert log[log.index("emergency_stop returned") + 1:] == []
        assert log.count("teardown Flaky") == 1

    @pytest.mark.parametrize(
        "case", ["on_shutdown_action", "supervisor_subscriber", "teardown"],
    )
    @pytest.mark.asyncio
    async def test_stop_called_by_the_shutdown_itself_returns(self, case: str) -> None:
        """T4: a stop called from code the shutdown runs does not wait for itself"""
        from arachnite.shutdown import ShutdownCoordinator

        bus = SignalBus()
        box: list[ArachniteRuntime] = []

        class StopOnTeardown(BaseSenseNode):
            node_id = "StopOnTeardown"
            signal_kind = "x"

            async def read(self) -> Signal | None:
                return None

            async def teardown(self) -> None:
                await box[0].emergency_stop()

        async def safe_position() -> None:
            await box[0].stop()

        async def on_supervisor(signal: Signal) -> None:
            await box[0].stop()

        if case == "on_shutdown_action":
            rt = _build(bus, shutdown_coordinator=ShutdownCoordinator(
                on_shutdown_action=safe_position,
            ))
            call = rt.stop
        elif case == "supervisor_subscriber":
            bus.subscribe("supervisor", on_supervisor)
            rt = _build(bus, sense=[_Hazard(bus, [])])
            call = rt.stop
        else:
            rt = _build(bus, sense=[StopOnTeardown(bus)])
            call = rt.emergency_stop
        box.append(rt)
        await rt.start()
        await asyncio.sleep(0.05)
        await asyncio.wait_for(call(), 3.0)
        assert not rt.is_running

    @pytest.mark.asyncio
    async def test_emergency_stop_from_a_safe_stop_hook_returns(self) -> None:
        """T4/X2: emergency_stop() from on_preempted() returns at once; the hook completes"""
        log: list[str] = []
        bus = SignalBus()
        box: list[ArachniteRuntime] = []

        class Owner(_Work):
            async def on_preempted(self, proposal: Proposal, preempted_by: Proposal) -> None:
                self.log.append("hook start")
                await box[0].emergency_stop()
                self.log.append("brake engaged")

        rt = _build(bus, actions=[
            Owner(bus, "Work", log),
            _Work(bus, "Retract", log, run_s=0.05),
        ])
        box.append(rt)
        await rt.start()
        # Launched from this task, not a tick: nothing but the action run
        # itself can mark the hook as "inside".
        rt._action_master.launch(Proposal(
            instinct_id="Goal", action_id="Work", priority=60, urgency=0.5,
        ))
        await _until(lambda: "execute Work" in log)
        started = time.perf_counter()
        rt._action_master.launch_reflex(Proposal(
            instinct_id="Reflex", action_id="Retract", priority=250, urgency=1.0,
        ))
        await asyncio.wait_for(rt.wait(), 3.0)
        assert time.perf_counter() - started < 1.0
        assert "brake engaged" in log

    @pytest.mark.asyncio
    async def test_a_raising_teardown_does_not_cut_the_others(self) -> None:
        """T5: one node's raising teardown is reported after every other teardown finished"""
        log: list[str] = []
        bus = SignalBus()

        class Bad(BaseSenseNode):
            node_id = "Bad"
            signal_kind = "x"

            async def read(self) -> Signal | None:
                return None

            async def teardown(self) -> None:
                raise RuntimeError("driver fault")

        class SlowArm(_Work):
            async def teardown(self) -> None:
                await asyncio.sleep(0.1)
                self.log.append("teardown Arm done")

        rt = _build(bus, sense=[Bad(bus)], actions=[SlowArm(bus, "Arm", log)])
        await rt.start()
        with pytest.raises(RuntimeError):
            await rt.emergency_stop()
        assert "teardown Arm done" in log

    @pytest.mark.asyncio
    async def test_unregistered_action_is_not_relaunched(self) -> None:
        """T6: no tick relaunches an action while unregister_action_live() removes it"""
        log: list[str] = []
        bus = SignalBus()

        class ParkingWork(_Work):
            async def teardown(self) -> None:
                await asyncio.sleep(0.2)                 # e.g. parking a motor driver
                self.log.append("teardown Work done")

        goal = _Propose(bus, "Goal", "Work")
        rt = _build(bus, instincts=[goal], actions=[ParkingWork(bus, "Work", log)])
        await rt.start()
        await _until(lambda: "execute Work" in log)
        await rt.unregister_action_live("Work")
        await asyncio.sleep(0.1)
        assert log.count("execute Work") == 1
        assert rt._action_master.running_action_ids() == set()
        await rt.stop()

    @pytest.mark.asyncio
    async def test_resume_during_pause_leaves_nodes_resumed(self) -> None:
        """T7: a resume() while pause() waits for the tick wins; no node stays in on_pause"""
        bus = SignalBus()

        class Hooked(_Hazard):
            low_power = False

            async def on_pause(self) -> None:
                self.low_power = True

            async def on_resume(self) -> None:
                self.low_power = False

        goal = _Propose(bus, "Goal", "Work")
        goal.delay_s = 0.2                                # a long tick to wait for
        sensor = Hooked(bus, [])
        rt = _build(bus, sense=[sensor], instincts=[goal],
                    actions=[_Work(bus, "Work", [], run_s=0.2)])
        await rt.start()
        await asyncio.sleep(0.05)                         # the loop is inside a tick
        pausing = asyncio.ensure_future(rt.pause())
        await asyncio.sleep(0.01)
        await rt.resume()
        await pausing
        assert not rt.is_paused
        assert sensor.low_power is False
        await rt.stop()

    @pytest.mark.asyncio
    async def test_live_registered_node_is_set_up_before_use(self) -> None:
        """T8: a node registered live is not read or launched before its setup() finished"""
        log: list[str] = []
        bus = SignalBus()
        rt = _build(bus)
        await rt.start()
        await rt.register_sense_live(_SlowSetupSense(bus, log))
        await asyncio.sleep(0.05)
        assert log.index("setup done") < log.index("read")
        await rt.stop()

    @pytest.mark.asyncio
    async def test_lane_evaluation_across_a_restart_launches_nothing(self) -> None:
        """T11: an evaluation in flight across stop()+start() does not launch in the new run"""
        log: list[str] = []
        bus = SignalBus()
        sensor = _Hazard(bus, log)

        class SlowReflex(_HazardReflex):
            async def evaluate(self, ctx: Context) -> Proposal | None:
                await asyncio.sleep(0.04)
                return await super().evaluate(ctx)

        rt = _build(bus, sense=[sensor], instincts=[SlowReflex(bus, "Retract")],
                    actions=[_Work(bus, "Retract", log, run_s=0.1)],
                    reflex_mode="event", reflex_eval_timeout_s=0.2)
        await rt.start()
        push = asyncio.ensure_future(sensor.push(_sig("Hazard", "hazard", 1.0)))
        await asyncio.sleep(0.005)
        await rt.emergency_stop()
        await rt.start()
        await push
        await asyncio.sleep(0.15)
        await rt.stop()
        assert "execute Retract" not in log

    @pytest.mark.asyncio
    async def test_restart_is_not_paused(self) -> None:
        """T12: a runtime stopped while paused starts unpaused and ticks"""
        rt, _ = _work_runtime([], run_s=0.1)
        await rt.start()
        await rt.pause()
        await rt.stop()
        await rt.start()
        before = rt.tick_count
        await asyncio.sleep(0.1)
        assert not rt.is_paused and rt.tick_count > before
        await rt.stop()
