"""Exhaustive verification of the interrupt policies composed with the
runtime: ActionMasterNode's interrupt gate, mid-step observation points,
the emergency-stop path, and the tick loop's reflex-arbitration stage.

This extends ``verify_interrupt_policies.py`` (which proves the four policy
processes in isolation) to the composition that the running system actually
executes.  The model is derived line-by-line from v0.12.2 source:

* ``MultiStepActionNode.execute``      — boundary interrupt check before each
  step, mandatory-block hold, cooperative mid-step bail via
  ``check_interrupt()``, rollback via ``on_interrupted`` (reverse order,
  non-interruptible steps with a rollback callable only).
* ``ActionMasterNode.request_interrupt`` — the gate: raises
  ``MandatoryBlockViolation`` while the current step is non-interruptible
  (modelled as ``reqR``), forwards otherwise (``reqA``), and silently
  drops requests when the target action is not running (``reqD`` — the
  documented request-before-execute pitfall).
* ``ArachniteRuntime.emergency_stop``  — bypasses the gate and sets the
  node's pending flag directly (``estop``); never refused.
* ``ArachniteRuntime._tick``           — stage 3 dispatches reflex proposals
  sequentially BEFORE stage 5 (decide) runs; decision-layer interrupts are
  issued through the gate and a refusal is caught and logged, never fatal.

Steps are refined into ``begin_i``/``end_i`` event pairs so that
"interrupts are observed only at step boundaries or at cooperative
mid-step observation points" is a checkable property rather than a
modelling assumption.

For each policy the reachable composed state space is enumerated
exhaustively (a few hundred states), so every PASS is a proof for the
modelled configuration.  Checks 1-8 are additionally run for EVERY step
layout with N <= MAX_N steps (all interruptible masks x checkpoint masks
x rollback masks), removing the dependence on the specific five-step
benchmark instance.

Checks:
  1. deadlock freedom of the composed system (all four policies)
  2. estop is enabled in every reachable state           (never refused)
  3. gate soundness: reqR exactly when the current step is mandatory;
     reqA never during a mandatory step
  4. mandatory-step atomicity: no ack/roll/bail strictly inside a
     mandatory begin/end pair
  5. no spurious interrupt: ack only after a reqA or a running estop
  6. rollback order: roll events are the completed mandatory
     rollback-bearing steps in strictly reverse completion order, then ack
  7. honour-at-first-eligible-boundary: no begin_i from a pending
     boundary state where the policy honours the request at i
  8. NEVER admits no ack in any reachable state
  9. five-step harmonised instance: DEFER (no ack before the mandatory
     prefix completes) — prefix layouts only
 10. tick composition: deadlock freedom; a gate refusal does not stall
     the tick loop
 11. reflex arbitration: whenever a reflex fires, its dispatch precedes
     decide in the same tick (the reflex never enters, and so can never
     lose, the decision competition)
 12. reflex independence: reflex dispatch is enabled even while the
     composed action is inside a mandatory block

Run:  python verify_runtime_composition.py
"""
from __future__ import annotations

from collections import deque
from itertools import combinations

MAX_N = 6
POLICIES = ("ALWAYS", "NEVER", "CHECKPOINT", "ROLLBACK")


class Layout:
    """One step layout: which steps are interruptible / checkpoints /
    carry rollback callables (checkpoints only on interruptible steps;
    rollback only relevant on mandatory steps, mirroring on_interrupted)."""

    def __init__(self, n: int, inter: frozenset, ckpt: frozenset, roll: frozenset):
        self.n, self.inter, self.ckpt, self.roll = n, inter, ckpt, roll

    def interruptible(self, i: int) -> bool:
        return i in self.inter

    def honours(self, policy: str, i: int) -> bool:
        """execute()'s boundary predicate: would a pending request be
        honoured at the boundary before step i?"""
        if not self.interruptible(i):
            return False
        if policy in ("ALWAYS", "ROLLBACK"):
            return True
        if policy == "CHECKPOINT":
            return i in self.ckpt
        return False  # NEVER

    def bails(self, policy: str, i: int) -> bool:
        """check_interrupt() inside step i (cooperative mid-step bail)."""
        return self.honours(policy, i)  # same predicate in the source

    def rollback_queue(self, done: tuple) -> tuple:
        return tuple(j for j in reversed(done)
                     if not self.interruptible(j) and j in self.roll)


HARMONISED = Layout(5, frozenset({4, 5}), frozenset({5}), frozenset({1, 2, 3}))


# ── Composed action + gate LTS ──────────────────────────────────────────────
# State: ("idle",) | ("exec", idx, in_step, pending, done)
#        | ("undo", queue) | ("acked",) | ("stopped",) | "END"
# Events: dispatch, begin_i, end_i, bail_i, roll_i, ack, stop, tick,
#         reqA, reqR, reqD, estop.


def action_succ(state, lay: Layout, policy: str):
    kind = state[0]
    if kind == "idle":
        yield ("dispatch", ("exec", 1, False, False, ()))
        yield ("reqD", state)   # not running: master drops silently
        yield ("estop", state)  # not running: loop over current_actions is empty
        return
    if kind == "exec":
        _, idx, in_step, pending, done = state
        # environment (interleaved coroutines) — the gate resolves req by
        # the CURRENT step (the one being executed, or the upcoming one at
        # a boundary, exactly as execute() sets _current_step at loop top)
        cur = idx if idx <= lay.n else None
        if cur is None:
            yield ("reqD", state)  # finished; _current_step is None -> True,
            # but the node is leaving _running_nodes; model as drop
        elif lay.interruptible(cur):
            yield ("reqA", ("exec", idx, in_step, True, done))
        else:
            yield ("reqR", state)  # MandatoryBlockViolation, caught by caller
        yield ("estop", ("exec", idx, in_step, True, done))
        # the action's own next transition
        if idx > lay.n:
            yield ("stop", ("stopped",))
            return
        if not in_step:  # boundary before step idx
            if pending and lay.honours(policy, idx):
                queue = lay.rollback_queue(done) if policy == "ROLLBACK" else ()
                if queue:
                    yield (f"roll_{queue[0]}", ("undo", queue[1:]))
                else:
                    yield ("ack", ("acked",))
                return
            yield (f"begin_{idx}", ("exec", idx, True, pending, done))
        else:  # inside step idx
            yield (f"end_{idx}", ("exec", idx + 1, False, pending, done + (idx,)))
            if pending and lay.bails(policy, idx):
                # cooperative bail -> _handle_interrupt (step NOT completed)
                queue = lay.rollback_queue(done) if policy == "ROLLBACK" else ()
                if queue:
                    yield (f"bail_{idx}", ("undo", queue))
                else:
                    yield (f"bail_{idx}", ("bailed",))
        return
    if kind == "bailed":
        yield ("ack", ("acked",))
        yield ("estop", state)
        return
    if kind == "undo":
        _, queue = state
        yield ("estop", state)  # flag set again; harmless mid-teardown
        if queue:
            yield (f"roll_{queue[0]}", ("undo", queue[1:]))
        else:
            yield ("ack", ("acked",))
        return
    if kind == "acked":
        yield ("stop", ("stopped",))
        yield ("estop", state)
        return
    if kind == "stopped":
        yield ("tick", "END")
        yield ("estop", state)
        return


def build(init, succ):
    trans, q = {}, deque([init])
    while q:
        s = q.popleft()
        if s in trans or s == "END":
            continue
        outs = list(succ(s))
        trans[s] = outs
        for _, t in outs:
            if t != "END" and t not in trans:
                q.append(t)
    return trans


FAILS: list[str] = []


def report(ok: bool, label: str, quiet: bool) -> bool:
    if not ok:
        FAILS.append(label)
        print(f"  FAIL {label}")
    elif not quiet:
        print(f"  PASS {label}")
    return ok


# ── Checks 1-8 (layout-generic) ─────────────────────────────────────────────

def check_layout(lay: Layout, policy: str, quiet: bool = True) -> bool:
    trans = build(("idle",), lambda s: action_succ(s, lay, policy))
    ok = True
    tag = f"[N={lay.n} I={sorted(lay.inter)} C={sorted(lay.ckpt)} " \
          f"R={sorted(lay.roll)}] {policy}"

    # 1. deadlock freedom
    ok &= report(all(outs for outs in trans.values()),
                 f"1 deadlock-free {tag} ({len(trans)} states)", quiet)

    # 2. estop enabled everywhere
    ok &= report(all(any(e == "estop" for e, _ in outs)
                     for outs in trans.values()),
                 f"2 estop-never-refused {tag}", quiet)

    # 3. gate soundness
    def gate_ok():
        for s, outs in trans.items():
            cur = s[1] if s[0] == "exec" and s[1] <= lay.n else None
            for e, _ in outs:
                if e == "reqR" and (cur is None or lay.interruptible(cur)):
                    return False
                if e == "reqA" and (cur is None or not lay.interruptible(cur)):
                    return False
        return True
    ok &= report(gate_ok(), f"3 gate-soundness {tag}", quiet)

    # 4/5/6. trace automata over the reachable product
    def product_check(label, step, init_sp):
        seen, q = set(), deque([(("idle",), init_sp)])
        while q:
            s, sp = q.popleft()
            if (s, sp) in seen or s == "END":
                continue
            seen.add((s, sp))
            for e, t in trans.get(s, []):
                nsp = step(sp, e)
                if nsp is None:
                    return report(False, f"{label} {tag}: '{e}' at {sp}", quiet)
                q.append((t, nsp))
        return report(True, f"{label} {tag}", quiet)

    # 4. mandatory atomicity: inside begin_i..end_i of a mandatory i,
    #    no ack / roll / bail
    def atomic(sp, e):
        if sp == "out":
            if e.startswith("begin_"):
                i = int(e.split("_")[1])
                return "in" if not lay.interruptible(i) else "out"
            return "out"
        if e.startswith("end_"):
            return "out"
        if e == "ack" or e.startswith("roll_") or e.startswith("bail_"):
            return None
        return "in"
    ok &= product_check("4 mandatory-atomicity", atomic, "out")

    # 5. no spurious ack: ack (or roll/bail) requires a prior reqA, or an
    #    estop delivered while running
    def spurious(sp, e):
        if e == "reqA":
            return "armed"
        if e == "estop" and sp in ("running", "armed"):
            return "armed"
        if e == "dispatch":
            return "armed" if sp == "armed" else "running"
        if e == "ack" or e.startswith("roll_") or e.startswith("bail_"):
            return "armed" if sp == "armed" else None
        return sp
    ok &= product_check("5 no-spurious-interrupt", spurious, "cold")

    # 6. rollback order: roll events strictly decreasing step indices,
    #    all mandatory-with-rollback, then ack; only for ROLLBACK
    def rollord(sp, e):
        if e.startswith("roll_"):
            i = int(e.split("_")[1])
            if policy != "ROLLBACK":
                return None
            if lay.interruptible(i) or i not in lay.roll:
                return None
            if isinstance(sp, int) and i >= sp:
                return None
            return i
        if e == "ack":
            return "done"
        return sp
    ok &= product_check("6 rollback-order", rollord, "free")

    # 7. honour at first eligible boundary: no begin_i out of a pending
    #    boundary state where the policy honours at i
    def honour_ok():
        for s, outs in trans.items():
            if s[0] != "exec":
                continue
            _, idx, in_step, pending, _ = s
            if in_step or not pending or idx > lay.n:
                continue
            if lay.honours(policy, idx):
                if any(e == f"begin_{idx}" for e, _ in outs):
                    return False
        return True
    ok &= report(honour_ok(), f"7 honour-first-boundary {tag}", quiet)

    # 8. NEVER: no ack anywhere
    if policy == "NEVER":
        ok &= report(not any(e == "ack" for outs in trans.values()
                             for e, _ in outs),
                     f"8 never-no-ack {tag}", quiet)
    return ok


# ── Check 9: five-step harmonised DEFER ─────────────────────────────────────

def check_defer_harmonised() -> bool:
    ok = True
    for policy in POLICIES:
        trans = build(("idle",),
                      lambda s: action_succ(s, HARMONISED, policy))
        # no ack before end_3 (mandatory prefix completes)
        def defer(sp, e):
            if sp == "pre":
                if e == "ack":
                    return None
                return "post" if e == "end_3" else "pre"
            return "post"
        seen, q, good = set(), deque([(("idle",), "pre")]), True
        while q and good:
            s, sp = q.popleft()
            if (s, sp) in seen or s == "END":
                continue
            seen.add((s, sp))
            for e, t in trans.get(s, []):
                nsp = defer(sp, e)
                if nsp is None:
                    good = False
                    break
                q.append((t, nsp))
        ok &= report(good, f"9 DEFER harmonised {policy}", quiet=False)
    return ok


# ── Checks 10-12: tick-loop composition with reflex arbitration ─────────────
# Tick process (one agent, one deliberative multi-step action, one reflex):
#   t_start -> sense -> [reflex_fire -> reflex_dispatch]? ->
#   instinct -> decide(may issue req through the gate) -> act -> t_end -> loop
# The composed alphabet synchronises on {reqA, reqR, reqD}; every other
# event interleaves (asyncio interleaving at await points).


def tick_succ(state):
    kind = state[0]
    if kind == "t_start":
        yield ("sense", ("sensed",))
    elif kind == "sensed":
        yield ("reflex_fire", ("rfired",))     # a reflex proposal this tick
        yield ("no_reflex", ("instinct",))     # or none
    elif kind == "rfired":
        yield ("reflex_dispatch", ("instinct",))
    elif kind == "instinct":
        yield ("instinct_eval", ("decide",))
    elif kind == "decide":
        # decision layer may issue an interrupt through the gate; ANY of the
        # three gate outcomes lets the tick proceed (refusal is caught and
        # logged in _tick; see runtime.py "Interrupt blocked by mandatory
        # step"), or it may issue none
        yield ("decide_done", ("act",))
        for e in ("reqA", "reqR", "reqD"):
            yield (e, ("act",))
    elif kind == "act":
        yield ("act_done", ("t_end",))
    elif kind == "t_end":
        yield ("t_loop", ("t_start",))


def compose(a_trans, a_init, t_trans, t_init, sync):
    trans, q = {}, deque([(a_init, t_init)])
    while q:
        s = q.popleft()
        if s in trans:
            continue
        a, t = s
        outs = []
        a_outs = a_trans.get(a, []) if a != "END" else []
        t_outs = t_trans.get(t, [])
        for e, a2 in a_outs:
            if e in sync:
                for e2, t2 in t_outs:
                    if e2 == e:
                        outs.append((e, (a2, t2)))
            else:
                outs.append((e, (a2, t)))
        for e, t2 in t_outs:
            if e not in sync:
                outs.append((e, (a, t2)))
        trans[s] = outs
        for _, s2 in outs:
            if s2 not in trans:
                q.append(s2)
    return trans


def check_tick_composition() -> bool:
    ok = True
    sync = {"reqA", "reqR", "reqD"}
    for policy in POLICIES:
        a_trans = build(("idle",),
                        lambda s: action_succ(s, HARMONISED, policy))
        t_trans = build(("t_start",), tick_succ)
        comp = compose(a_trans, ("idle",), t_trans, ("t_start",), sync)
        tag = f"harmonised {policy}"

        # 10. deadlock freedom of the composition (a gate refusal cannot
        # stall the tick: reqR is always followed by a live tick)
        ok &= report(all(outs for outs in comp.values()),
                     f"10 tick-composition deadlock-free {tag} "
                     f"({len(comp)} states)", quiet=False)

        # 11. reflex_dispatch precedes decide_done whenever reflex_fire
        # occurred in the same tick
        def reflex_order(sp, e):
            if e == "t_loop":
                return "clean"
            if e == "reflex_fire":
                return "fired"
            if e == "reflex_dispatch":
                return "clean" if sp == "fired" else None
            if e in ("decide_done", "reqA", "reqR", "reqD") and sp == "fired":
                return None  # decide reached with an undispatched reflex
            return sp
        seen, q, good = set(), deque([((("idle",), ("t_start",)), "clean")]), True
        while q and good:
            s, sp = q.popleft()
            if (s, sp) in seen:
                continue
            seen.add((s, sp))
            for e, t in comp.get(s, []):
                nsp = reflex_order(sp, e)
                if nsp is None:
                    good = False
                    break
                q.append((t, nsp))
        ok &= report(good, f"11 reflex-before-decide {tag}", quiet=False)

        # 12. reflex independence: in every composed state where the tick
        # is at 'rfired', reflex_dispatch is enabled — including states
        # where the action sits inside a mandatory step
        def indep():
            hit_mandatory = False
            for (a, t), outs in comp.items():
                if t == ("rfired",):
                    if not any(e == "reflex_dispatch" for e, _ in outs):
                        return False
                    if (a != "END" and a[0] == "exec" and len(a) == 5
                            and a[2] and not HARMONISED.interruptible(a[1])):
                        hit_mandatory = True
            return hit_mandatory  # vacuity guard: such states must exist
        ok &= report(indep(), f"12 reflex-independent-of-mandatory {tag}",
                     quiet=False)
    return ok


# ── Layout sweep ────────────────────────────────────────────────────────────

def all_layouts(max_n: int):
    for n in range(1, max_n + 1):
        steps = list(range(1, n + 1))
        for k in range(n + 1):
            for inter in combinations(steps, k):
                inter_f = frozenset(inter)
                mand = [i for i in steps if i not in inter_f]
                for kc in range(len(inter) + 1):
                    for ckpt in combinations(inter, kc):
                        for kr in range(len(mand) + 1):
                            for roll in combinations(mand, kr):
                                yield Layout(n, inter_f, frozenset(ckpt),
                                             frozenset(roll))


def main() -> None:
    print(f"Harmonised five-step instance (checks 1-8, verbose):")
    ok = True
    for policy in POLICIES:
        ok &= check_layout(HARMONISED, policy, quiet=False)

    print("Five-step DEFER (check 9):")
    ok &= check_defer_harmonised()

    print("Tick-loop composition with reflex arbitration (checks 10-12):")
    ok &= check_tick_composition()

    print(f"Layout sweep, all step layouts with N <= {MAX_N} (checks 1-8):")
    n_layouts = 0
    for lay in all_layouts(MAX_N):
        n_layouts += 1
        for policy in POLICIES:
            ok &= check_layout(lay, policy, quiet=True)
    print(f"  {n_layouts} layouts x {len(POLICIES)} policies checked")

    print("\nRESULT:", "ALL PASS" if ok and not FAILS
          else f"{len(FAILS)} FAILURES")
    raise SystemExit(0 if ok and not FAILS else 1)


if __name__ == "__main__":
    main()
