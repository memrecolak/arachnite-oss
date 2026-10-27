"""
arachnite.shutdown
~~~~~~~~~~~~~~~~~~
ShutdownCoordinator: orchestrates the 7-phase graceful shutdown sequence.
Spec reference: Section 15.2.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from arachnite.models import InterruptRequest, Proposal, ShutdownPhase

if TYPE_CHECKING:
    from arachnite.runtime import ArachniteRuntime


class ShutdownCoordinator:
    """
    Orchestrates graceful shutdown of an ArachniteRuntime.

    Runs the 7 ordered phases defined in spec Section 15.1:
    1. Stop sensing
    2. Drain reflexes
    3. Complete mandatory block
    4. Interrupt remaining action
    5. Stop supervisors
    6. Teardown nodes
    7. Disconnect transport

    Spec reference: Section 15.2.
    """

    def __init__(
        self,
        teardown_timeout_s: float = 5.0,
        mandatory_block_timeout_multiplier: float = 1.1,
        on_shutdown_action: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        """
        Args:
            teardown_timeout_s: Grace period for node teardown (phase 6).
            mandatory_block_timeout_multiplier: Multiplier applied to the
                remaining mandatory block time to compute the phase 3 timeout.
            on_shutdown_action: Optional async callable invoked at phase 1
                before sensing stops — e.g. to trigger a 'safe position'
                action before the pipeline drains.
        """
        self._teardown_timeout_s = teardown_timeout_s
        self._multiplier = mandatory_block_timeout_multiplier
        self._on_shutdown_action = on_shutdown_action
        self._phase = ShutdownPhase.NOT_STARTED

    @property
    def phase(self) -> ShutdownPhase:
        """Current shutdown phase."""
        return self._phase

    @property
    def completed(self) -> bool:
        """True after execute() has completed all phases."""
        return self._phase == ShutdownPhase.COMPLETE

    async def execute(self, runtime: ArachniteRuntime) -> None:
        """
        Run all shutdown phases in order.

        Called by ArachniteRuntime.stop().
        Sets runtime._running = False at phase 1 to halt the tick loop.
        """
        # Phase 1: Stop sensing — and deliver the shutdown interrupt to every
        # running action right away (held while a block runs), so no action
        # starts another step while the in-flight tick drains.
        self._phase = ShutdownPhase.STOP_SENSING
        runtime._running = False
        runtime._stop_event.set()
        action_master = runtime._action_master
        shutdown_request = InterruptRequest(
            new_proposal           = Proposal(
                instinct_id = "shutdown",
                action_id   = "__shutdown__",
                priority    = 9999,
                urgency     = 1.0,
                rationale   = "Graceful shutdown",
            ),
            requesting_instinct_id = "shutdown",
            reason                 = "stop() called",
        )
        action_master.interrupt_all(shutdown_request)
        if self._on_shutdown_action is not None:
            with contextlib.suppress(Exception):
                await self._on_shutdown_action()

        # Phase 2: Drain reflexes — the in-flight tick (and its reflex stage)
        # finishes; no new ticks start because _running is False. Actions run
        # as tasks outside the tick (ADR 0005), so this wait is one tick long.
        self._phase = ShutdownPhase.DRAIN_REFLEXES
        # asyncio.wait() never raises the loop task's own cancellation into
        # this coroutine (an emergency stop taking over may cancel it).
        loop_task = runtime._loop_task
        if loop_task is not None and not loop_task.done():
            done, _ = await asyncio.wait({loop_task}, timeout=self._teardown_timeout_s)
            if not done:
                runtime._loop_cancel_requested = True
                loop_task.cancel()
                await asyncio.wait({loop_task})
            runtime._loop_task = None

        # Phase 3: Complete mandatory block — re-deliver the shutdown interrupt
        # (covers actions the drained tick launched) and let every running
        # action leave its mandatory block, up to the computed timeout. Each
        # action then stops at the first boundary after its block — no new
        # step starts once shutdown has begun.
        self._phase = ShutdownPhase.COMPLETE_MANDATORY
        if action_master.running_action_ids():
            action_master.interrupt_all(shutdown_request)
            await action_master.wait_until_interruptible(self._mandatory_timeout(runtime))

        # Phase 4: Interrupt remaining actions — wait for the interrupted
        # actions to exit cleanly (ROLLBACK actions unwind), then cancel what
        # is left (single-step actions, NEVER policy) with its safe-stop.
        self._phase = ShutdownPhase.INTERRUPT_ACTION
        if action_master.running_action_ids() and not await action_master.wait_idle(
            timeout=self._teardown_timeout_s,
        ):
            await action_master.cancel_all(timeout=action_master.safe_stop_budget_s())

        # Phase 5: Stop supervisors — cancel restart tasks, then mark nodes STOPPED.
        self._phase = ShutdownPhase.STOP_SUPERVISORS
        for sup in runtime._supervisors:
            await sup.cancel_restart_tasks()
            for node_id in sup.all_states():
                await sup.mark_stopped(node_id)

        # Phase 6: Teardown nodes — concurrent with grace period.
        self._phase = ShutdownPhase.TEARDOWN_NODES
        # ``asyncio.wait_for`` raises ``asyncio.TimeoutError``, which is a distinct
        # class from the builtin ``TimeoutError`` on Python 3.10 (aliased only on
        # 3.11+). Suppress the asyncio one — same reasoning as the Phase 3 handler
        # above — so a teardown that overruns the grace period doesn't escape stop().
        # A raising master teardown is logged by ``_teardown_masters`` once
        # the others have finished; the shutdown still completes, and the
        # error is re-raised to its awaiters at the end.
        teardown_error: Exception | None = None
        try:
            await asyncio.wait_for(
                runtime._teardown_masters(), timeout=self._teardown_timeout_s,
            )
        except asyncio.TimeoutError:
            pass
        except Exception as exc:  # noqa: BLE001
            teardown_error = exc

        # Phase 7: Disconnect transport (no-op for LocalTransport; distributed
        # AgentNode handles actual transport disconnection after stop() returns).
        self._phase = ShutdownPhase.DISCONNECT_TRANSPORT

        self._phase = ShutdownPhase.COMPLETE
        if teardown_error is not None:
            raise teardown_error

    def _mandatory_timeout(self, runtime: ArachniteRuntime) -> float:
        """Compute the wait budget for phase 3 (complete mandatory block).

        Takes the maximum remaining mandatory time across all running
        actions to ensure the worst-case block is fully covered.
        """
        max_remaining = 0.0
        for node in runtime._action_master.current_actions().values():
            if hasattr(node, "execution_state"):
                state = node.execution_state()
                remaining: float = state.mandatory_block_remaining_s
                if remaining > max_remaining:
                    max_remaining = remaining
        if max_remaining > 0:
            return max_remaining * self._multiplier
        return self._teardown_timeout_s
