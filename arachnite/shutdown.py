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
        # Phase 1: Stop sensing
        self._phase = ShutdownPhase.STOP_SENSING
        runtime._running = False
        runtime._stop_event.set()
        if self._on_shutdown_action is not None:
            with contextlib.suppress(Exception):
                await self._on_shutdown_action()

        # Phase 2: Drain reflexes — any in-flight reflex evaluation completes;
        # no new ticks start because _running is False.
        self._phase = ShutdownPhase.DRAIN_REFLEXES

        # Phase 3: Complete mandatory block — wait for the loop task to finish
        # naturally up to the computed timeout.
        self._phase = ShutdownPhase.COMPLETE_MANDATORY
        loop_task = runtime._loop_task
        if loop_task is not None and not loop_task.done():
            timeout = self._mandatory_timeout(runtime)
            try:
                await asyncio.wait_for(asyncio.shield(loop_task), timeout=timeout)
            except asyncio.TimeoutError:
                # ``asyncio.TimeoutError`` is aliased to the builtin
                # ``TimeoutError`` only on Python >= 3.11; catch the asyncio
                # one explicitly so 3.10 reaches the cancellation path.
                loop_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await loop_task
            runtime._loop_task = None

        # Phase 4: Interrupt all remaining actions — send a shutdown interrupt
        # to every running action so they exit cleanly (ROLLBACK policy runs).
        self._phase = ShutdownPhase.INTERRUPT_ACTION
        running = runtime._action_master.current_actions()
        if running:
            shutdown_proposal = Proposal(
                instinct_id = "shutdown",
                action_id   = "__shutdown__",
                priority    = 9999,
                urgency     = 1.0,
                rationale   = "Graceful shutdown",
            )
            req = InterruptRequest(
                new_proposal           = shutdown_proposal,
                requesting_instinct_id = "shutdown",
                reason                 = "stop() called",
            )
            for action_id in running:
                with contextlib.suppress(Exception):
                    await runtime._action_master.request_interrupt(req, action_id=action_id)

        # Phase 5: Stop supervisors — cancel restart tasks, then mark nodes STOPPED.
        self._phase = ShutdownPhase.STOP_SUPERVISORS
        for sup in runtime._supervisors:
            await sup.cancel_restart_tasks()
            for node_id in sup.all_states():
                await sup.mark_stopped(node_id)

        # Phase 6: Teardown nodes — concurrent with grace period.
        self._phase = ShutdownPhase.TEARDOWN_NODES
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(
                asyncio.gather(
                    runtime._sense_master.teardown(),
                    runtime._instinct_master.teardown(),
                    runtime._decision_master.teardown(),
                    runtime._action_master.teardown(),
                ),
                timeout=self._teardown_timeout_s,
            )

        # Phase 7: Disconnect transport (no-op for LocalTransport; distributed
        # AgentNode handles actual transport disconnection after stop() returns).
        self._phase = ShutdownPhase.DISCONNECT_TRANSPORT

        self._phase = ShutdownPhase.COMPLETE

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
