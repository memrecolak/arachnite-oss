"""Shutdown, pause and loop-robustness semantics of the runtime (second review, 2026-10-26)"""

from __future__ import annotations

import asyncio
import sys
import time
from collections.abc import Awaitable, Iterable

import pytest

from arachnite import SignalBus
from arachnite.context import ContextNode
from arachnite.models import (
    ActionStep,
    Context,
    InterruptPolicy,
    NodeState,
    Proposal,
    Result,
    ShutdownPhase,
    Signal,
    StateUpdateSignal,
    StepResult,
    SupervisorSignal,
)
from arachnite.nodes.action import ActionMasterNode, BaseActionNode, MultiStepActionNode
from arachnite.nodes.decision import DecisionMasterNode, GreedyDecisionNode
from arachnite.nodes.instinct import (
    BaseInstinctNode,
    BaseReflexInstinctNode,
    InstinctMasterNode,
)
from arachnite.nodes.sense import MAX_PENDING_PUSHES, BaseSenseNode, SenseMasterNode
from arachnite.runtime import ArachniteRuntime
from arachnite.shutdown import ShutdownCoordinator

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


# ── Fourth review round (runtime lifecycle findings R4-T1–R4-T14) ────────────

# Pending-buffer cap of the fourth review: ``MAX_PENDING_RESULTS``
# (arachnite.nodes.action) and ``_MAX_PENDING_SIGNALS`` (arachnite.runtime).
_PENDING_CAP = 1024


async def _own_cancel() -> None:
    """End in the caller's own CancelledError (a future someone else cancelled)."""
    fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    fut.cancel()
    await fut


def _fault_ids(bus: SignalBus) -> list[str]:
    """Collect the node_id of every node_fault signal published on ``bus``."""
    seen: list[str] = []

    async def on_fault(signal: Signal) -> None:
        if isinstance(signal, SupervisorSignal):
            seen.append(signal.node_id)

    bus.subscribe("node_fault", on_fault)
    return seen


class _SlowTeardownSense(BaseSenseNode):
    """Sense node whose teardown() takes 0.2 s (e.g. parking hardware)."""
    node_id = "SlowTeardown"
    signal_kind = "x"
    poll_interval_s = 0.0

    def __init__(self, bus: SignalBus, log: list[str]) -> None:
        super().__init__(bus)
        self.log = log
        self.tearing_down = asyncio.Event()

    async def setup(self) -> None:
        self.log.append("setup")
        await asyncio.sleep(0.02)

    async def read(self) -> Signal | None:
        return None

    async def teardown(self) -> None:
        self.tearing_down.set()
        await asyncio.sleep(0.2)
        self.log.append("teardown")


class _HookedSense(_Hazard):
    """Event-driven sensor logging its setup and its pause/resume hooks."""

    async def setup(self) -> None:
        self.log.append("setup Hazard")

    async def on_pause(self) -> None:
        self.log.append("on_pause")

    async def on_resume(self) -> None:
        self.log.append("on_resume")


class _Near(BaseSenseNode):
    """Reports an obstacle 5 cm away on every read."""
    node_id = "Near"
    signal_kind = "proximity"
    poll_interval_s = 0.0

    async def read(self) -> Signal | None:
        return _sig(self.node_id, self.signal_kind, 0.05)


class _Collision(BaseReflexInstinctNode):
    """Tick-mode reflex: an obstacle under 10 cm fires ``Brake``."""
    node_id = "Collision"
    priority = 250

    async def evaluate(self, ctx: Context) -> Proposal | None:
        if any(s.kind == "proximity" and s.value < 0.1 for s in ctx.signals):
            return Proposal(instinct_id=self.node_id, action_id="Brake",
                            priority=self.priority, urgency=1.0)
        return None


def _brake_and_work(bus: SignalBus, log: list[str]) -> list[BaseActionNode]:
    """A reflex ``Brake`` and a deliberative ``Work``, short and on disjoint resources."""
    return [
        _Work(bus, "Brake", log, resources=frozenset({"brake"}), run_s=0.01),
        _Work(bus, "Work", log, resources=frozenset({"arm"}), run_s=0.01),
    ]


class TestFourthReviewRuntime:
    @pytest.mark.asyncio
    async def test_starts_during_a_shutdown_set_up_once(self) -> None:
        """R4-T1: two start() calls while the previous shutdown runs set the masters up once"""
        log: list[str] = []
        bus = SignalBus()
        sense = _SlowTeardownSense(bus, log)
        rt = _build(bus, sense=[sense])
        await rt.start()
        stopping = asyncio.ensure_future(rt.stop())
        await sense.tearing_down.wait()
        log.clear()
        outcomes = await asyncio.gather(rt.start(), rt.start(), return_exceptions=True)
        await stopping
        assert outcomes == [None, None]
        assert log == ["teardown", "setup"]
        assert rt.is_running
        await rt.stop()

    @pytest.mark.asyncio
    async def test_stop_while_start_waits_for_the_shutdown_aborts_it(self) -> None:
        """R4-T2: a stop() while start() waits for the previous shutdown aborts the start"""
        log: list[str] = []
        bus = SignalBus()
        sense = _SlowTeardownSense(bus, log)
        rt = _build(bus, sense=[sense])
        await rt.start()
        stopping = asyncio.ensure_future(rt.stop())
        await sense.tearing_down.wait()
        log.clear()
        starting = asyncio.ensure_future(rt.start())
        await asyncio.sleep(0)                          # start() waits for the shutdown
        await rt.stop()
        await starting
        await stopping
        running = rt.is_running
        if running:
            await rt.stop()
        assert not running
        assert log == ["teardown"]                      # no setup for an aborted start

    @pytest.mark.parametrize("how", ["stop", "emergency_stop"])
    @pytest.mark.asyncio
    async def test_stop_while_start_marks_nodes_running_aborts_it(self, how: str) -> None:
        """R4-T3: a stop arriving after the master setups (nodes marked RUNNING) is not lost"""
        log: list[str] = []
        bus = SignalBus()
        marking = asyncio.Event()

        async def slow_supervisor_subscriber(signal: Signal) -> None:
            marking.set()                               # e.g. a telemetry forwarder
            await asyncio.sleep(0.05)

        bus.subscribe("supervisor", slow_supervisor_subscriber)
        rt = _build(bus, sense=[_Hazard(bus, log)], instincts=[_Propose(bus, "Goal", "Work")],
                    actions=[_Work(bus, "Work", log, run_s=0.05)])
        starting = asyncio.ensure_future(rt.start())
        await marking.wait()                            # setups done; marking RUNNING
        await (rt.stop() if how == "stop" else rt.emergency_stop())
        await starting
        await asyncio.sleep(0.1)
        running = rt.is_running
        if running:
            await rt.stop()
        assert not running
        assert "execute Work" not in log
        assert "teardown Work" in log
        # The next start is a normal run: the action master launches again.
        await rt.start()
        await _until(lambda: "execute Work" in log)
        await rt.stop()

    @pytest.mark.asyncio
    async def test_pause_during_start_holds(self) -> None:
        """R4-T4: pause() during start() waits for it; the runtime starts paused"""
        log: list[str] = []
        bus = SignalBus()

        class Hooked(_SlowSetupSense):
            async def on_pause(self) -> None:
                self.log.append("on_pause")

            async def on_resume(self) -> None:
                self.log.append("on_resume")

        sense = Hooked(bus, log)
        rt = _build(bus, sense=[sense], instincts=[_Propose(bus, "Goal", "Work")],
                    actions=[_Work(bus, "Work", log, run_s=0.05)])
        starting = asyncio.ensure_future(rt.start())
        await sense.setting_up.wait()
        await rt.pause()
        log.append("pause() returned")
        await starting
        await asyncio.sleep(0.1)
        paused = rt.is_paused
        acted = [e for e in log if e in ("read", "execute Work")]
        await rt.resume()
        await _until(lambda: "execute Work" in log)
        await rt.stop()
        assert paused
        assert acted == []
        assert log.index("setup done") < log.index("on_pause") < log.index("pause() returned")
        assert log.index("pause() returned") < log.index("on_resume")

    @pytest.mark.asyncio
    async def test_restart_after_a_paused_stop_resumes_the_nodes(self) -> None:
        """R4-T5: start() after a stop while paused calls on_resume() on the paused nodes"""
        log: list[str] = []
        bus = SignalBus()
        rt = _build(bus, sense=[_HookedSense(bus, log)])
        await rt.start()
        await rt.pause()
        await rt.stop()
        await rt.start()
        await rt.stop()
        assert log == [
            "setup Hazard", "on_pause", "teardown Hazard",
            "setup Hazard", "on_resume", "teardown Hazard",
        ]

    @pytest.mark.asyncio
    async def test_emergency_stop_during_a_restart_is_followed(self) -> None:
        """R4-T6: an e-stop taking over the shutdown a restart waits for ends that restart"""
        log: list[str] = []
        bus = SignalBus()
        rt = _build(bus, sense=[_HookedSense(bus, log)], instincts=[_Propose(bus, "Goal", "Work")],
                    actions=[_Work(bus, "Work", log)],
                    shutdown_coordinator=ShutdownCoordinator(teardown_timeout_s=5.0))
        await rt.start()
        await _until(lambda: "execute Work" in log)
        started = time.perf_counter()
        stopping = asyncio.ensure_future(rt.stop())   # phase 4 waits up to 5 s for Work
        await _until(lambda: rt._shutdown_coordinator.phase is ShutdownPhase.INTERRUPT_ACTION)
        restarting = asyncio.ensure_future(rt.start())
        await asyncio.sleep(0)                         # start() waits for the graceful stop
        await rt.emergency_stop()                      # takes the graceful stop over
        await asyncio.gather(stopping, restarting)
        elapsed = time.perf_counter() - started
        running, loop_task = rt.is_running, rt._loop_task
        if running:
            await rt.emergency_stop()
        assert elapsed < 2.5
        assert not running and loop_task is None
        assert log.count("setup Hazard") == log.count("teardown Hazard") == 1
        # A later start is not undone by the emergency teardown.
        await rt.start()
        ticks = rt.tick_count
        await _until(lambda: rt.tick_count > ticks + 2)
        assert rt.is_running
        assert log.count("setup Hazard") == 2 and log.count("teardown Hazard") == 1
        await rt.emergency_stop()

    @pytest.mark.asyncio
    async def test_wait_during_a_restart_waits_for_the_new_run(self) -> None:
        """R4-T7: wait() during a restart's start() waits for that run's stop"""
        bus = SignalBus()
        rt = _build(bus, sense=[_SlowSetupSense(bus, [])])
        await rt.start()
        await rt.stop()
        starting = asyncio.ensure_future(rt.start())
        await asyncio.sleep(0)
        waiting = asyncio.ensure_future(rt.wait())
        await starting
        await asyncio.sleep(0.1)
        returned_early = waiting.done()
        await rt.stop()
        await asyncio.wait_for(waiting, 2.0)
        assert not returned_early

    @pytest.mark.asyncio
    async def test_pause_during_a_tick_interrupts_nothing(self) -> None:
        """R4-T8: the tick in flight when pause() is called interrupts no running action"""
        log: list[str] = []
        bus = SignalBus()

        class Patrol(MultiStepActionNode):
            node_id = "Patrol"
            resources = frozenset({"arm"})
            interrupt_policy = InterruptPolicy.ALWAYS
            timeout_s = 60.0

            def steps(self) -> list[ActionStep]:
                return [ActionStep(name=f"s{i}") for i in range(200)]

            async def execute_step(
                self, step: ActionStep, proposal: Proposal, completed: list[StepResult],
            ) -> StepResult:
                if step.name == "s0":
                    log.append("execute Patrol")
                await asyncio.sleep(0.02)
                return StepResult(step_name=step.name, success=True)

            async def on_interrupted(
                self, completed: list[StepResult], pending: list[ActionStep],
                proposal: Proposal,
            ) -> None:
                log.append("Patrol interrupted")
                await super().on_interrupted(completed, pending, proposal)

        class Slow(_Propose):
            """A slow (e.g. LLM) instinct that, once armed, outbids Patrol."""
            priority = 80
            armed = False

            async def evaluate(self, ctx: Context) -> Proposal | None:
                if not self.armed:
                    return None
                entered.set()
                await asyncio.sleep(0.1)
                return await super().evaluate(ctx)

        entered = asyncio.Event()
        low = _Propose(bus, "Low", "Patrol")
        low.priority = 30
        high = Slow(bus, "High", "Grab")
        rt = _build(bus, instincts=[low, high],
                    actions=[Patrol(bus), _Work(bus, "Grab", log, resources=frozenset({"arm"}))])
        await rt.start()
        await _until(lambda: "execute Patrol" in log)
        high.armed = True
        await entered.wait()                           # the loop is inside the tick
        await rt.pause()
        await asyncio.sleep(0.3)
        running = rt._action_master.running_action_ids()
        paused_log = list(log)
        high.armed = False
        await rt.stop()
        assert running == {"Patrol"}
        assert "Patrol interrupted" not in paused_log
        assert "execute Grab" not in paused_log

    @pytest.mark.parametrize("where", ["sense read", "reflex evaluate", "instinct evaluate"])
    @pytest.mark.asyncio
    async def test_node_raising_its_own_cancelled_error_is_its_fault(self, where: str) -> None:
        """R4-T9: a node's own CancelledError is that node's fault; the tick goes on"""
        log: list[str] = []
        bus = SignalBus()
        faults = _fault_ids(bus)

        class BrokenSense(BaseSenseNode):
            node_id = "Broken"
            signal_kind = "x"
            poll_interval_s = 0.0

            async def read(self) -> Signal | None:
                await _own_cancel()
                return None

        class BrokenReflex(BaseReflexInstinctNode):
            node_id = "Broken"
            priority = 210

            async def evaluate(self, ctx: Context) -> Proposal | None:
                await _own_cancel()
                return None

        class BrokenInstinct(BaseInstinctNode):
            node_id = "Broken"
            priority = 10

            async def evaluate(self, ctx: Context) -> Proposal | None:
                await _own_cancel()
                return None

        sense: list[BaseSenseNode] = [_Near(bus)]
        instincts: list[BaseInstinctNode] = [_Collision(bus), _Propose(bus, "Goal", "Work")]
        if where == "sense read":
            sense.append(BrokenSense(bus))
        elif where == "reflex evaluate":
            instincts.append(BrokenReflex(bus))
        else:
            instincts.append(BrokenInstinct(bus))
        rt = _build(bus, sense=sense, instincts=instincts, actions=_brake_and_work(bus, log))
        await rt.start()
        try:
            await _until(lambda: (
                log.count("execute Brake") >= 3 and log.count("execute Work") >= 3
                and "Broken" in faults
            ))
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_lane_results_while_paused_are_bounded(self) -> None:
        """R4-T10: Results of lane-launched actions while paused are capped, oldest dropped"""
        bus = SignalBus()
        sensor = _Hazard(bus, [])

        class CountReflex(BaseReflexInstinctNode):
            node_id = "CountReflex"
            priority = 250
            trigger_on_signals = ["hazard"]

            async def evaluate(self, ctx: Context) -> Proposal | None:
                values = [s.value for s in ctx.signals if s.kind == "hazard"]
                if not values:
                    return None
                return Proposal(instinct_id=self.node_id, action_id="Count",
                                priority=self.priority, urgency=1.0,
                                parameters={"n": max(values)})

        class Count(BaseActionNode):
            node_id = "Count"

            async def execute(self, proposal: Proposal) -> Result:
                return Result(action_id=self.node_id, success=True,
                              output=proposal.parameters["n"])

        rt = _build(bus, sense=[sensor], instincts=[CountReflex(bus)], actions=[Count(bus)],
                    reflex_mode="event")
        await rt.start()
        await rt.pause()
        total = _PENDING_CAP + 50
        for n in range(1, total + 1):
            await sensor.push(_sig("Hazard", "hazard", float(n)))
            await rt._action_master.wait_idle(1.0)
        outputs = [r.output for r in rt._action_master._completed]
        await rt.resume()
        await rt.stop()
        assert len(outputs) == _PENDING_CAP
        assert outputs[0] == total - _PENDING_CAP + 1 and outputs[-1] == total

        from arachnite.nodes.action import MAX_PENDING_RESULTS
        assert MAX_PENDING_RESULTS == _PENDING_CAP

    @pytest.mark.asyncio
    async def test_state_and_supervisor_buffers_are_bounded(self) -> None:
        """R4-T10: buffered state updates / supervisor signals are capped while paused or stopped"""
        bus = SignalBus()
        sensor = _Hazard(bus, [])
        rt = _build(bus, sense=[sensor])
        total = _PENDING_CAP + 50

        def state_update(source: str, n: int) -> StateUpdateSignal:
            return StateUpdateSignal(source=source, kind="state_update", value=None,
                                     confidence=1.0, timestamp=time.monotonic(),
                                     key="soc", state_value=n)

        def kept(buffer: Iterable[Signal]) -> list[object]:
            return [s.state_value if isinstance(s, StateUpdateSignal) else s.node_id
                    for s in buffer if isinstance(s, (StateUpdateSignal, SupervisorSignal))]

        await rt.start()
        await rt.pause()
        for n in range(total):
            await sensor.push(state_update("Hazard", n))
        paused = kept(rt._state_update_buffer)
        await rt.resume()
        await rt.stop()
        for n in range(total):
            await bus.publish(state_update("Remote", n))
            await bus.publish(SupervisorSignal(
                source="Remote", kind="supervisor", value="running", confidence=1.0,
                timestamp=time.monotonic(), node_id=f"n{n}",
            ))
        stopped_state = kept(rt._state_update_buffer)
        stopped_supervisor = kept(rt._supervisor_signal_buffer)
        assert paused == list(range(50, total))
        assert stopped_state == list(range(50, total))
        assert stopped_supervisor == [f"n{n}" for n in range(50, total)]

        from arachnite.runtime import _MAX_PENDING_SIGNALS
        assert _MAX_PENDING_SIGNALS == _PENDING_CAP

    @pytest.mark.asyncio
    async def test_restart_first_tick_sees_nothing_from_the_previous_run(self) -> None:
        """R4-T10: the first tick of a restart gets no stale state update, signal or Result"""
        log: list[str] = []
        bus = SignalBus()

        class Recorder(BaseInstinctNode):
            node_id = "Recorder"
            priority = 10

            def __init__(self, bus: SignalBus) -> None:
                super().__init__(bus)
                self.seen: list[Context] = []

            async def evaluate(self, ctx: Context) -> Proposal | None:
                self.seen.append(ctx)
                return None

        recorder = Recorder(bus)
        rt = _build(bus, instincts=[_Propose(bus, "Goal", "Work"), recorder],
                    actions=[_Work(bus, "Work", log, run_s=0.2)])
        await rt.start()
        await _until(lambda: "execute Work" in log)
        await rt.stop()                     # Work finishes in phase 4: its Result is undrained
        await bus.publish(StateUpdateSignal(
            source="Stale", kind="state_update", value=None, confidence=1.0,
            timestamp=time.monotonic(), key="stale_key", state_value=1,
        ))
        await bus.publish(SupervisorSignal(
            source="Stale", kind="supervisor", value="running", confidence=1.0,
            timestamp=time.monotonic(), node_id="Ghost",
        ))
        recorder.seen = []
        await rt.start()
        await _until(lambda: bool(recorder.seen))
        first = recorder.seen[0]
        await rt.stop()
        assert first.last_results == [] and first.last_result is None
        assert "stale_key" not in first.state
        assert [s for s in first.signals if s.source == "Stale"] == []
        assert [
            s for s in first.signals
            if isinstance(s, SupervisorSignal) and s.current_state is NodeState.STOPPED
        ] == []

    @pytest.mark.asyncio
    async def test_repair_cycle_cancels_the_pending_restart(self) -> None:
        """R4-T11: a restart pending for a removed node never hits its replacement"""
        log: list[str] = []
        bus = SignalBus()

        class Lidar(BaseSenseNode):
            node_id = "Lidar"
            signal_kind = "x"
            poll_interval_s = 0.0

            def __init__(self, bus: SignalBus, tag: str, fail: bool) -> None:
                super().__init__(bus)
                self.tag, self.fail = tag, fail

            async def setup(self) -> None:
                log.append(f"setup {self.tag}")

            async def teardown(self) -> None:
                log.append(f"teardown {self.tag}")

            async def read(self) -> Signal | None:
                if self.fail:
                    self.fail = False
                    raise RuntimeError("driver crashed")
                return None

        rt = _build(bus, sense=[Lidar(bus, "old", fail=True)])
        for sup in rt._supervisors:
            sup._restart_delay = 0.2
        await rt.start()
        await _until(lambda: rt._supervisors[0].restart_task_count == 1)
        await rt.unregister_sense_live("Lidar")             # the documented repair cycle
        await rt.register_sense_live(Lidar(bus, "new", fail=False))
        log.append("replaced")
        await asyncio.sleep(0.4)
        after = log[log.index("replaced") + 1:]
        await rt.stop()
        assert after == []
        assert log.count("teardown old") == 1

    @pytest.mark.parametrize("kind", ["sense", "instinct", "action"])
    @pytest.mark.asyncio
    async def test_node_registered_live_before_start_is_supervised(self, kind: str) -> None:
        """R4-T12: a node added with register_*_live() before start() is restarted on a fault"""
        log: list[str] = []
        bus = SignalBus()

        class Sense(BaseSenseNode):
            node_id = "Glitchy"
            signal_kind = "x"
            poll_interval_s = 0.0
            failed = False

            async def setup(self) -> None:
                log.append("setup")

            async def read(self) -> Signal | None:
                if not self.failed:
                    self.failed = True
                    raise RuntimeError("glitch")
                return None

        class Instinct(BaseInstinctNode):
            node_id = "Glitchy"
            priority = 10
            failed = False

            async def setup(self) -> None:
                log.append("setup")

            async def evaluate(self, ctx: Context) -> Proposal | None:
                if not self.failed:
                    self.failed = True
                    raise RuntimeError("glitch")
                return None

        class Action(BaseActionNode):
            node_id = "Glitchy"
            failed = False

            async def setup(self) -> None:
                log.append("setup")

            async def execute(self, proposal: Proposal) -> Result:
                if not self.failed:
                    self.failed = True
                    raise RuntimeError("glitch")    # contract violation: reported as a fault
                return Result(action_id=self.node_id, success=True)

        if kind == "action":
            rt = _build(bus, instincts=[_Propose(bus, "Goal", "Glitchy")])
            await rt.register_action_live(Action(bus))
        elif kind == "instinct":
            rt = _build(bus)
            await rt.register_instinct_live(Instinct(bus))
        else:
            rt = _build(bus)
            await rt.register_sense_live(Sense(bus))
        for sup in rt._supervisors:
            sup._restart_delay = 0.05
        await rt.start()
        try:
            await _until(lambda: log.count("setup") >= 2)
        finally:
            await rt.stop()

    @pytest.mark.parametrize("kind", ["sense", "instinct", "action"])
    @pytest.mark.asyncio
    async def test_unregister_live_cancels_background_tasks(self, kind: str) -> None:
        """R4-T13: unregister_*_live() stops the node's spawn_background_task() listeners"""
        bus = SignalBus()
        listeners: list[asyncio.Task[None]] = []

        async def listen() -> None:
            await asyncio.sleep(3600)

        class Sense(_Hazard):
            async def setup(self) -> None:
                listeners.append(self.spawn_background_task(listen()))

        class Instinct(BaseInstinctNode):
            node_id = "Listener"
            priority = 10

            async def setup(self) -> None:
                listeners.append(self.spawn_background_task(listen()))

            async def evaluate(self, ctx: Context) -> Proposal | None:
                return None

        class Action(BaseActionNode):
            node_id = "Listener"

            async def setup(self) -> None:
                listeners.append(self.spawn_background_task(listen()))

            async def execute(self, proposal: Proposal) -> Result:
                return Result(action_id=self.node_id, success=True)

        if kind == "sense":
            rt = _build(bus, sense=[Sense(bus, [])])
            unregister, node_id = rt.unregister_sense_live, Sense.node_id
        elif kind == "instinct":
            rt = _build(bus, instincts=[Instinct(bus)])
            unregister, node_id = rt.unregister_instinct_live, Instinct.node_id
        else:
            rt = _build(bus, actions=[Action(bus)])
            unregister, node_id = rt.unregister_action_live, Action.node_id
        await rt.start()
        await unregister(node_id)
        leaked = [t for t in listeners if not t.done()]
        for task in leaked:
            task.cancel()
        await rt.stop()
        assert len(listeners) == 1
        assert leaked == []

    @pytest.mark.parametrize("hook", ["on_tick_start", "on_tick_end", "on_pause", "on_resume"])
    @pytest.mark.asyncio
    async def test_raising_hook_is_that_nodes_fault(self, hook: str) -> None:
        """R4-T14: one node's raising tick/pause hook is its fault; the rest carry on"""
        log: list[str] = []
        bus = SignalBus()
        faults = _fault_ids(bus)

        def maybe_fail(name: str) -> None:
            if name == hook:
                raise RuntimeError(f"{name}: metrics backend unreachable")

        class Metrics(BaseSenseNode):
            node_id = "Metrics"
            signal_kind = "metrics"
            poll_interval_s = 0.0

            async def read(self) -> Signal | None:
                return None

            async def on_tick_start(self, tick: int) -> None:
                await super().on_tick_start(tick)
                maybe_fail("on_tick_start")

            async def on_tick_end(self, tick: int, duration_s: float) -> None:
                maybe_fail("on_tick_end")

            async def on_pause(self) -> None:
                maybe_fail("on_pause")

            async def on_resume(self) -> None:
                maybe_fail("on_resume")

        class Sibling(_Near):
            async def on_pause(self) -> None:
                log.append("Sibling on_pause")

            async def on_resume(self) -> None:
                log.append("Sibling on_resume")

        rt = _build(bus, sense=[Metrics(bus), Sibling(bus)],
                    instincts=[_Collision(bus), _Propose(bus, "Goal", "Work")],
                    actions=_brake_and_work(bus, log))
        await rt.start()
        try:
            if hook in ("on_pause", "on_resume"):
                await rt.pause()
                await rt.resume()
                assert {"Sibling on_pause", "Sibling on_resume"} <= set(log)
            braked = log.count("execute Brake")
            await _until(lambda: (
                log.count("execute Brake") >= braked + 3 and log.count("execute Work") >= 3
                and "Metrics" in faults
            ))
        finally:
            await rt.stop()

    @pytest.mark.parametrize("how", ["stop", "emergency_stop"])
    @pytest.mark.asyncio
    async def test_teardown_ending_in_its_own_cancel_is_an_error(self, how: str) -> None:
        """R4-T14: a teardown's own CancelledError completes the shutdown and is re-raised"""
        bus = SignalBus()

        class Listener(BaseSenseNode):
            node_id = "Listener"
            signal_kind = "x"
            poll_interval_s = 0.0

            async def setup(self) -> None:
                self.task = asyncio.ensure_future(asyncio.sleep(3600))

            async def teardown(self) -> None:
                self.task.cancel()
                await self.task                         # ends in CancelledError

            async def read(self) -> Signal | None:
                return None

        rt = _build(bus, sense=[Listener(bus)])
        await rt.start()
        await asyncio.sleep(0.05)
        with pytest.raises(RuntimeError) as raised:
            await (rt.stop() if how == "stop" else rt.emergency_stop())
        assert isinstance(raised.value.__cause__, asyncio.CancelledError)
        with pytest.raises(RuntimeError):
            await rt.wait()
        assert not rt.is_running
        if how == "stop":
            assert rt._shutdown_coordinator.phase is ShutdownPhase.COMPLETE

    @pytest.mark.asyncio
    async def test_emergency_take_over_in_teardown_carries_the_error(self) -> None:
        """R4-T14: an e-stop during graceful phase 6 re-raises its teardown error to everyone"""
        bus = SignalBus()

        class Bad(BaseSenseNode):
            node_id = "Bad"
            signal_kind = "x"
            poll_interval_s = 0.0

            async def read(self) -> Signal | None:
                return None

            async def teardown(self) -> None:
                await asyncio.sleep(0.1)
                raise RuntimeError("driver fault")

        async def outcome(call: Awaitable[None]) -> str:
            try:
                await call
            except Exception as exc:  # noqa: BLE001
                return repr(exc)
            return "returned"

        rt = _build(bus, sense=[Bad(bus)])
        await rt.start()
        await asyncio.sleep(0.05)
        stopping = asyncio.ensure_future(outcome(rt.stop()))
        waiting = asyncio.ensure_future(outcome(rt.wait()))
        await _until(lambda: rt._shutdown_coordinator.phase is ShutdownPhase.TEARDOWN_NODES)
        estop = await outcome(rt.emergency_stop())
        outcomes = [await stopping, await waiting, estop]
        assert outcomes == [repr(RuntimeError("driver fault"))] * 3
        assert rt._shutdown_coordinator.phase is ShutdownPhase.COMPLETE
