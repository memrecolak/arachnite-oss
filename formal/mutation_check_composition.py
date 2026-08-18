"""Discriminative-power validation of ``verify_runtime_composition.py``.

Seeds eight deliberate semantic faults into the composition model and
asserts that each is rejected by the check designed to catch it.  A
verifier whose checks are vacuous would let one of these mutants pass;
this harness is therefore the composition-model analogue of the
mutation-testing pass applied to the safety enforcement sites.

Run:  python mutation_check_composition.py
"""
from __future__ import annotations

import sys

import verify_runtime_composition as V


def run(label: str, mutate, expect_fail_check: str) -> bool:
    V.FAILS.clear()
    orig_succ = V.action_succ
    orig_tick = V.tick_succ
    orig_honours = V.Layout.honours
    mutate(V)
    try:
        ok = True
        for policy in V.POLICIES:
            ok &= V.check_layout(V.HARMONISED, policy, quiet=True)
        if expect_fail_check.startswith("11"):
            ok &= V.check_tick_composition()
    finally:
        V.action_succ = orig_succ
        V.tick_succ = orig_tick
        V.Layout.honours = orig_honours
    caught = any(f.startswith(expect_fail_check) for f in V.FAILS)
    print(f"{'CAUGHT' if caught else '!!MISSED!!'}  {label} "
          f"(expected check {expect_fail_check})")
    return caught


def m_gate_accept_mandatory(V):
    orig = V.action_succ

    def bad(state, lay, policy):
        for e, t in orig(state, lay, policy):
            if e == "reqR":  # gate wrongly forwards during a mandatory step
                _, idx, in_step, _, done = state
                yield ("reqA", ("exec", idx, in_step, True, done))
            else:
                yield (e, t)
    V.action_succ = bad


def m_bail_in_mandatory(V):
    orig = V.action_succ

    def bad(state, lay, policy):
        yield from orig(state, lay, policy)
        if state[0] == "exec":
            _, idx, in_step, pending, done = state
            if in_step and pending and not lay.interruptible(idx):
                yield (f"bail_{idx}", ("bailed",))  # check_interrupt broken
    V.action_succ = bad


def m_forward_rollback(V):
    orig = V.action_succ

    def bad(state, lay, policy):
        for e, t in orig(state, lay, policy):
            if isinstance(t, tuple) and t[0] == "undo":
                yield (e, ("undo", tuple(sorted(t[1]))))  # forward order
            else:
                yield (e, t)
    V.action_succ = bad


def m_estop_missing(V):
    orig = V.action_succ

    def bad(state, lay, policy):
        for e, t in orig(state, lay, policy):
            if (e == "estop" and state[0] == "exec" and not state[2]
                    and state[1] <= lay.n
                    and not lay.interruptible(state[1])):
                continue  # estop refused at mandatory boundaries
            yield (e, t)
    V.action_succ = bad


def m_never_acks(V):
    orig = V.Layout.honours

    def bad(self, policy, i):
        if policy == "NEVER":
            return self.interruptible(i)
        return orig(self, policy, i)
    V.Layout.honours = bad


def m_spurious_ack(V):
    orig = V.action_succ

    def bad(state, lay, policy):
        yield from orig(state, lay, policy)
        if (state[0] == "exec" and not state[2] and state[1] <= lay.n
                and lay.interruptible(state[1])):
            yield ("ack", ("acked",))  # ack without any request
    V.action_succ = bad


def m_honour_skipped(V):
    orig = V.action_succ

    def bad(state, lay, policy):
        yield from orig(state, lay, policy)
        if state[0] == "exec":
            _, idx, in_step, pending, done = state
            if (not in_step and pending and idx <= lay.n
                    and lay.honours(policy, idx)):
                # wrongly keeps running past the honour point
                yield (f"begin_{idx}", ("exec", idx, True, pending, done))
    V.action_succ = bad


def m_reflex_after_decide(V):
    def bad(state):
        kind = state[0]
        if kind == "t_start":
            yield ("sense", ("sensed",))
        elif kind == "sensed":
            yield ("reflex_fire", ("rfired",))
            yield ("no_reflex", ("instinct",))
        elif kind == "rfired":
            yield ("instinct_eval", ("decide_r",))  # dispatch deferred!
        elif kind == "decide_r":
            yield ("decide_done", ("act_r",))
        elif kind == "act_r":
            yield ("reflex_dispatch", ("act",))
        elif kind == "instinct":
            yield ("instinct_eval", ("decide",))
        elif kind == "decide":
            yield ("decide_done", ("act",))
            for e in ("reqA", "reqR", "reqD"):
                yield (e, ("act",))
        elif kind == "act":
            yield ("act_done", ("t_end",))
        elif kind == "t_end":
            yield ("t_loop", ("t_start",))
    V.tick_succ = bad


def main() -> None:
    ok = True
    ok &= run("gate forwards during mandatory step", m_gate_accept_mandatory, "3")
    ok &= run("cooperative bail inside mandatory step", m_bail_in_mandatory, "4")
    ok &= run("rollback in forward order", m_forward_rollback, "6")
    ok &= run("estop refused at mandatory boundary", m_estop_missing, "2")
    ok &= run("NEVER honours interrupts", m_never_acks, "8")
    ok &= run("ack without any request", m_spurious_ack, "5")
    ok &= run("honour point skipped", m_honour_skipped, "7")
    ok &= run("reflex dispatched after decide", m_reflex_after_decide, "11")
    print("\nMUTATION RESULT:", "ALL CAUGHT" if ok else "GAPS PRESENT")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
