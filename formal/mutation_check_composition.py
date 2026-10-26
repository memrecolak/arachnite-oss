"""Discriminative-power validation of ``verify_runtime_composition.py``.

Seeds deliberate semantic faults into the composition model and asserts
that each is rejected by the check designed to catch it.  A verifier whose
checks are vacuous would let one of these mutants pass; this harness is
therefore the composition-model analogue of the mutation-testing pass
applied to the safety enforcement sites.

Run:  python mutation_check_composition.py
"""
from __future__ import annotations

import sys

import verify_runtime_composition as V

ORIGINALS = {
    "owner_succ": V.owner_succ,
    "owner_preempt": V.owner_preempt,
    "tick_succ": V.tick_succ,
    "system_succ": V.system_succ,
    "honours": V.Layout.honours,
}


def restore() -> None:
    V.owner_succ = ORIGINALS["owner_succ"]
    V.owner_preempt = ORIGINALS["owner_preempt"]
    V.tick_succ = ORIGINALS["tick_succ"]
    V.system_succ = ORIGINALS["system_succ"]
    V.Layout.honours = ORIGINALS["honours"]


# An interruptible step before the block, so a request can be pending as the
# block starts (the harmonised instance begins with its block).
MIXED = V.Layout(4, frozenset({1, 4}), frozenset({4}), frozenset({2, 3}))


def run(label: str, mutate, expect: str) -> bool:
    V.FAILS.clear()
    mutate()
    try:
        for policy in V.POLICIES:
            V.check_layout(V.HARMONISED, policy, quiet=True)
            V.check_layout(MIXED, policy, quiet=True)
        V.check_blockatomic_harmonised()
        V.check_compositions([V.HARMONISED], quiet=True)
    finally:
        restore()
    caught = any(f.split(" ", 1)[0] == expect for f in V.FAILS)
    print(f"{'CAUGHT' if caught else '!!MISSED!!'}  {label} (expected check {expect})")
    return caught


def _wrap_owner(transform):
    orig = ORIGINALS["owner_succ"]

    def bad(state, lay, policy, rb=False):
        yield from transform(state, lay, policy, rb, orig)
    V.owner_succ = bad


# ── action-alone mutants ────────────────────────────────────────────────────

def m_gate_accept_mandatory():
    def t(state, lay, policy, rb, orig):
        for e, s2 in orig(state, lay, policy, rb):
            if e == "reqR":  # gate wrongly forwards during a mandatory step
                _, idx, in_step, _, rpend, done = state
                yield ("reqA", ("exec", idx, in_step, True, rpend, done))
            else:
                yield (e, s2)
    _wrap_owner(t)


def m_bail_in_mandatory():
    def t(state, lay, policy, rb, orig):
        yield from orig(state, lay, policy, rb)
        if state != "END" and state[0] == "exec":
            _, idx, in_step, pending, _, _ = state
            if in_step and pending and idx <= lay.n and not lay.interruptible(idx):
                yield (f"bail_{idx}", ("bailed",))  # check_interrupt broken
    _wrap_owner(t)


def m_ack_inside_block():
    def bad(self, policy, i):  # interior boundaries wrongly honoured
        return policy in ("ALWAYS", "ROLLBACK") or (
            policy == "CHECKPOINT" and i in self.ckpt)
    V.Layout.honours = bad


def m_forward_rollback():
    def t(state, lay, policy, rb, orig):
        for e, s2 in orig(state, lay, policy, rb):
            if isinstance(s2, tuple) and s2[0] == "undo":
                yield (e, ("undo", tuple(sorted(s2[1]))))  # forward order
            else:
                yield (e, s2)
    _wrap_owner(t)


def m_never_acks():
    orig = ORIGINALS["honours"]

    def bad(self, policy, i):
        if policy == "NEVER":
            return self.stop_point(i)
        return orig(self, policy, i)
    V.Layout.honours = bad


def m_spurious_ack():
    def t(state, lay, policy, rb, orig):
        yield from orig(state, lay, policy, rb)
        if (state != "END" and state[0] == "exec" and not state[2]
                and state[1] <= lay.n and lay.stop_point(state[1])):
            yield ("ack", ("acked",))  # ack without any request
    _wrap_owner(t)


def m_honour_skipped():
    def t(state, lay, policy, rb, orig):
        yield from orig(state, lay, policy, rb)
        if state != "END" and state[0] == "exec":
            _, idx, in_step, pending, rpend, done = state
            if not in_step and pending and idx <= lay.n and lay.honours(policy, idx):
                yield (f"begin_{idx}", ("exec", idx, True, pending, rpend, done))
    _wrap_owner(t)


def m_estop_refused_in_block():
    def t(state, lay, policy, rb, orig):
        for e, s2 in orig(state, lay, policy, rb):
            if (e == "estop" and state[0] == "exec" and state[2]
                    and state[1] <= lay.n and not lay.interruptible(state[1])):
                continue  # estop refused inside a mandatory step
            yield (e, s2)
    _wrap_owner(t)


def m_estop_rolls_back():
    def t(state, lay, policy, rb, orig):
        if state != "END" and state[0] == "estop_ss":
            yield ("roll_1", ("halted",))  # undoes work during shutdown
            return
        yield from orig(state, lay, policy, rb)
    _wrap_owner(t)


# ── composition mutants ─────────────────────────────────────────────────────

def m_reflex_after_decide():
    def bad(state):
        kind = state[0]
        if kind == "rfired":
            yield ("instinct_eval", ("decide_r",))  # launch deferred!
        elif kind == "decide_r":
            yield ("decide_done", ("act_r",))
        elif kind == "act_r":
            yield ("rlaunch", ("act",))
        else:
            yield from ORIGINALS["tick_succ"](state)
    V.tick_succ = bad


def m_reflex_runs_with_owner():
    orig = ORIGINALS["system_succ"]

    def bad(state, lay, policy, mode, rb):
        yield from orig(state, lay, policy, mode, rb)
        owner, rstate, tick = state
        if rstate == "r_armed" and V.owner_holds(owner):
            yield ("r_begin", (owner, "r_run", tick))  # ignores ownership
    V.system_succ = bad


def m_preempt_lets_step_finish():
    orig = ORIGINALS["owner_preempt"]

    def bad(state, lay, mode, rb):
        if mode == "PREEMPT" and state[0] == "exec" and state[2]:
            _, idx, in_step, pending, _, done = state
            return ("exec", idx, in_step, pending, True, done)  # waits instead
        return orig(state, lay, mode, rb)
    V.owner_preempt = bad


def m_wait_ignores_stop_point():
    def t(state, lay, policy, rb, orig):
        if state != "END" and state[0] == "exec" and state[4]:
            _, idx, in_step, pending, _, done = state
            # rpend forgotten: the action carries on past the stop point
            yield from orig(("exec", idx, in_step, pending, False, done),
                            lay, policy, rb)
            return
        yield from orig(state, lay, policy, rb)
    _wrap_owner(t)


def m_rollback_on_preempt_always():
    orig = ORIGINALS["owner_preempt"]

    def bad(state, lay, mode, rb):
        return orig(state, lay, mode, True)  # ignores rollback_on_preempt
    V.owner_preempt = bad


def m_preempt_restarts_walk():
    orig = ORIGINALS["owner_preempt"]

    def bad(state, lay, mode, rb):
        if state[0] == "undo" and rb:
            full = tuple(sorted(lay.roll, reverse=True))
            return ("pre_ss", full)  # restarts the deliberative walk
        return orig(state, lay, mode, rb)
    V.owner_preempt = bad


def m_dispatch_while_armed():
    orig = ORIGINALS["system_succ"]

    def bad(state, lay, policy, mode, rb):
        yield from orig(state, lay, policy, mode, rb)
        owner, rstate, tick = state
        if owner == ("idle",) and rstate == "r_armed":
            yield ("dispatch", (("exec", 1, False, False, False, ()), rstate, tick))
    V.system_succ = bad


MUTANTS = (
    ("gate forwards during mandatory step", m_gate_accept_mandatory, "3"),
    ("cooperative bail inside mandatory step", m_bail_in_mandatory, "4"),
    ("deliberative ack at an interior block boundary", m_ack_inside_block, "5"),
    ("rollback in forward order", m_forward_rollback, "7"),
    ("NEVER honours interrupts", m_never_acks, "8b"),
    ("ack without any request", m_spurious_ack, "6"),
    ("honour point skipped", m_honour_skipped, "8a"),
    ("estop refused inside a mandatory step", m_estop_refused_in_block, "2"),
    ("estop rolls back", m_estop_rolls_back, "9"),
    ("deliberative ack inside the block (instance)", m_ack_inside_block, "10"),
    ("reflex launched after decide", m_reflex_after_decide, "12"),
    ("reflex runs while the owner holds the resource", m_reflex_runs_with_owner, "14"),
    ("reflex begins before the owner released", m_reflex_runs_with_owner, "15"),
    ("PREEMPT lets the current step finish", m_preempt_lets_step_finish, "16"),
    ("WAIT_FOR_BLOCK carries on past the stop point", m_wait_ignores_stop_point, "17"),
    ("rollback on preemption without rollback_on_preempt",
     m_rollback_on_preempt_always, "18"),
    ("preemption restarts a deliberative rollback walk", m_preempt_restarts_walk, "18"),
    ("deliberative dispatch while the reflex is armed", m_dispatch_while_armed, "18"),
)


def main() -> None:
    ok = True
    for label, mutate, expect in MUTANTS:
        ok &= run(label, mutate, expect)
    print("\nMUTATION RESULT:", "ALL CAUGHT" if ok else "GAPS PRESENT")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
