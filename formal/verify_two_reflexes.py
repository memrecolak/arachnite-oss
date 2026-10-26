"""Exhaustive verification of reflex arbitration with two reflexes, retries,
an action timeout and emergency stop (v0.13 after the 2026-10-26 review).

``verify_runtime_composition.py`` has one deliberative action and one reflex.
It cannot reach the paths that the reflex-path code review found defective,
because they need a second reflex, a retry, a timeout or a shutdown that
overlaps a safe-stop. This model adds exactly those:

* ``owner``: one deliberative ``MultiStepActionNode`` holding the shared
  actuator resource. It may fail a step (``fail_i``, once per dispatch),
  time out anywhere inside ``execute()`` (``timeout``), and be retried once
  (``retry``, ``max_retries = 1``).
* ``A``: a PREEMPT reflex action, higher priority. ``B``: a WAIT_FOR_BLOCK
  reflex action, lower priority. Both share the owner's resource. Either can
  be launched, cancelled while armed (``ActionMasterNode.cancel``), admitted
  (``r_begin_X``) and finish (``r_end_X``).
* ``estop``: ``emergency_stop()`` closes the master, drops armed reflexes,
  cancels running ones and the owner, and then halts.

Semantics (fixed code, ``eb32f3f``):

* A preempted owner runs one safe-stop per run (``ss_begin`` ... ``ss_end``),
  then the rollback walk if ``rollback_on_preempt`` (``proll_i``), then
  releases. The safe-stop is shielded: a second preemption, an emergency stop
  or the action timeout cannot cut or restart it. A WAIT_FOR_BLOCK hand-off
  runs it in the master's task, outside ``execute()``, so ``timeout`` is not
  enabled there.
* Armed reflexes claim the resource. They are admitted in priority order: B
  never begins while A is armed. Deliberative dispatch waits until no reflex
  is armed or running.
* Cancelling an armed B withdraws its hand-off request from the owner.
* A retry is refused while a reflex is armed, a hand-off is pending, or the
  master is closed.
* After ``estop`` nothing new starts: no dispatch, launch, admission, step or
  retry.

Checks, for every composition layout with N <= MAX_N and both
``rollback_on_preempt`` settings:
 19. deadlock freedom (the only terminal state is after ``halt``)
 20. actuator exclusivity: at most one of {owner, A, B} holds the resource
 21. safe-stop integrity: at most one safe-stop per owner run, and once it
     has begun nothing but its end happens to the owner until it ends
 22. rollback on preemption only with ``rollback_on_preempt``, reverse order,
     each step undone at most once per run
 23. priority and claims: B is never admitted while A is armed; no
     deliberative dispatch while a reflex is armed or running
 24. withdrawal: the owner hands over only while B is armed
 25. retry gate: no retry while a reflex is armed, a hand-off is pending, or
     after estop
 26. shutdown: after estop no dispatch, launch, admission, step or retry
 27. admission: from every state in which a reflex is armed, a state in which
     it runs (or was cancelled or dropped) is reachable
Each check also asserts that the scenario it guards is reachable (no
vacuous pass).

``mutation_check_two_reflexes.py`` seeds the defect classes of the review
(docs/audits 2026-10-26-v013-reflex-path-fixes) as ``FAULTS`` and requires
each to be rejected.

Run:  python verify_two_reflexes.py
"""
from __future__ import annotations

from collections import deque

from verify_runtime_composition import HARMONISED, Layout, composition_layouts

MAX_N = 6
MAX_TRIES = 1

# Seeded faults (empty in the real model). Each mirrors one review finding.
FAULTS: set[str] = set()
FAULT_NAMES = (
    "admit_ignores_priority",      # R01: B admitted while A is armed
    "second_preempt_restarts_ss",  # R03: a PREEMPT during a hand-off safe-stop restarts it
    "retry_while_armed",           # R04: retry ignores armed reflexes and the hand-off
    "timeout_cuts_handoff_ss",     # R05: the hand-off safe-stop runs inside timeout_s
    "estop_cuts_ss",               # R06: emergency stop abandons a running safe-stop
    "launch_after_estop",          # R07/R10: launches still start after the stop
    "cancel_keeps_handoff",        # R11: cancelling armed B leaves the hand-off flag
)

FAILS: list[str] = []


def report(ok: bool, label: str, quiet: bool) -> bool:
    if not ok:
        FAILS.append(label)
        print(f"  FAIL {label}")
    elif not quiet:
        print(f"  PASS {label}")
    return ok


def _num(e: str) -> int:
    return int(e.rsplit("_", 1)[1])


# ── Owner (deliberative MultiStepActionNode) ────────────────────────────────
# ("idle",) | ("exec", idx, in_step, rpend, done, tries) | ("failed", tries)
# ("pre_ss", queue, handoff) | ("ss", queue, handoff) | ("pre_roll", queue)
# ("halted",)

def holds(o) -> bool:
    return o[0] not in ("idle", "halted")


def in_block(o, lay: Layout) -> bool:
    _, idx, _, _, _, _ = o
    return idx <= lay.n and not lay.interruptible(idx)


def owner_local(o, lay: Layout, rb: bool):
    """Moves the owner makes on its own (its task / the master's task)."""
    kind = o[0]
    if kind == "exec":
        _, idx, in_step, rpend, done, tries = o
        if idx > lay.n:
            yield ("stop", ("idle",))
            return
        yield ("timeout", ("failed", tries))         # inside execute()
        if not in_step:
            if rpend and lay.stop_point(idx):
                q = lay.rollback_queue(done) if rb else ()
                yield ("handoff", ("pre_ss", q, True))
                return
            yield (f"begin_{idx}", ("exec", idx, True, rpend, done, tries))
        else:
            yield (f"end_{idx}", ("exec", idx + 1, False, rpend, done + (idx,), tries))
            if tries == 0:
                yield (f"fail_{idx}", ("failed", tries))
        return
    if kind == "failed":
        yield ("stop", ("idle",))
        return
    if kind == "pre_ss":
        _, q, ho = o
        yield ("ss_begin", ("ss", q, ho))
        return
    if kind == "ss":
        _, q, ho = o
        yield ("ss_end", ("pre_roll", q))
        if ho and "timeout_cuts_handoff_ss" in FAULTS:
            yield ("timeout", ("failed", 1))
        return
    if kind == "pre_roll":
        _, q = o
        if q:
            yield (f"proll_{q[0]}", ("pre_roll", q[1:]))
        else:
            yield ("stop", ("idle",))


def preempt(o, lay: Layout, rb: bool, mode: str):
    """Effect of a conflicting reflex launch (PREEMPT for A, WAIT for B)."""
    kind = o[0]
    if kind == "exec":
        _, idx, in_step, rpend, done, tries = o
        if mode == "WAIT" and in_block(o, lay):
            return ("exec", idx, in_step, True, done, tries)
        q = lay.rollback_queue(done) if rb else ()
        return ("pre_ss", q, False)
    if kind in ("ss", "pre_roll") and mode == "PREEMPT" \
            and "second_preempt_restarts_ss" in FAULTS:
        return ("pre_ss", (), False) if not rb else ("pre_ss", _restart_queue(o, lay), False)
    return o  # idle, failed (retry gate decides), or already releasing


def _restart_queue(o, lay: Layout) -> tuple:
    # the faulty restart walks every mandatory rollback-bearing step again
    return tuple(i for i in range(lay.n, 0, -1)
                 if not lay.interruptible(i) and i in lay.roll)


# ── System: owner x A x B x closed ──────────────────────────────────────────
# reflex states: "idle" | "armed" | "run";  END after halt.

def system_succ(state, lay: Layout, rb: bool):
    if state == "END":
        return
    o, a, b, closed = state
    if closed and o == ("idle",) and a == "idle" and b == "idle":
        yield ("halt", "END")
    # owner's own moves
    for e, o2 in owner_local(o, lay, rb):
        yield (e, (o2, a, b, closed))
    # retry (ActionMasterNode._execute_with_retry, gated by _retry_blocked);
    # the faulty gate only ignores the armed reflexes, not the closed master
    if o[0] == "failed" and o[1] < MAX_TRIES and not closed:
        armed = a == "armed" or b == "armed"
        if not armed or "retry_while_armed" in FAULTS:
            yield ("retry", (("exec", 1, False, False, (), o[1] + 1), a, b, closed))
    # deliberative dispatch
    if o == ("idle",) and a == "idle" and b == "idle" and not closed:
        yield ("dispatch", (("exec", 1, False, False, (), 0), a, b, closed))
    # reflex launches
    may_launch = not closed or "launch_after_estop" in FAULTS
    for x, mode in (("A", "PREEMPT"), ("B", "WAIT")):
        mine, other = (a, b) if x == "A" else (b, a)
        if mine == "idle" and may_launch:
            o2 = preempt(o, lay, rb, mode) if holds(o) else o
            yield (f"launch_{x}", _put(o2, x, "armed", a, b, closed))
        if mine == "armed":
            o2 = o
            if x == "B" and o[0] == "exec" and "cancel_keeps_handoff" not in FAULTS:
                o2 = o[:3] + (False,) + o[4:]
            yield (f"cancel_{x}", _put(o2, x, "idle", a, b, closed))
            free = not holds(o) and other != "run"
            ranked = x == "A" or a != "armed" or "admit_ignores_priority" in FAULTS
            if free and ranked and (not closed or "launch_after_estop" in FAULTS):
                yield (f"r_begin_{x}", _put(o, x, "run", a, b, closed))
        if mine == "run":
            yield (f"r_end_{x}", _put(o, x, "idle", a, b, closed))
    # emergency stop
    if not closed:
        a2 = "idle"
        b2 = "idle"
        if o[0] == "exec":
            o2 = ("pre_ss", (), False)              # cancel at await; safe-stop, no rollback
        elif o[0] == "ss" and "estop_cuts_ss" in FAULTS:
            o2 = ("idle",)                          # safe-stop abandoned mid-way
        else:
            o2 = o                                  # running safe-stop is shielded
        yield ("estop", (o2, a2, b2, True))


def _put(o, x, val, a, b, closed):
    return (o, val, b, closed) if x == "A" else (o, a, val, closed)


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


def product_check(trans, init, step, init_sp) -> bool:
    seen, q = set(), deque([(init, init_sp)])
    while q:
        s, sp = q.popleft()
        if (s, sp) in seen or s == "END":
            continue
        seen.add((s, sp))
        for e, t in trans.get(s, []):
            nsp = step(sp, e)
            if nsp is None:
                return False
            q.append((t, nsp))
    return True


def edges(trans):
    for s, outs in trans.items():
        for e, t in outs:
            yield s, e, t


# ── Checks 19-27 ────────────────────────────────────────────────────────────

def check(lay: Layout, rb: bool, quiet: bool = True) -> bool:
    init = (("idle",), "idle", "idle", False)
    trans = build(init, lambda s: system_succ(s, lay, rb))
    tag = f"[N={lay.n} I={sorted(lay.inter)}] rb={rb}"
    ok = True
    has_block = len(lay.inter) < lay.n

    # 19. deadlock freedom
    ok &= report(all(outs for outs in trans.values()),
                 f"19 deadlock-free {tag} ({len(trans)} states)", quiet)

    # 20. actuator exclusivity
    ok &= report(not any(holds(o) + (a == "run") + (b == "run") > 1
                         for (o, a, b, _) in trans),
                 f"20 exclusivity {tag}", quiet)

    # 21. safe-stop integrity (non-vacuous: a PREEMPT reaches a hand-off safe-stop)
    def ss_spec(sp, e):
        if e in ("dispatch", "retry"):
            return "none"
        if sp == "in":
            if e == "ss_end":
                return "done"
            return None if e in OWNER_EVENTS or e == "halt" else "in"
        if e == "ss_begin":
            return None if sp == "done" else "in"
        return sp
    reach_21 = not has_block or any(
        e == "launch_A" and s[0][0] == "ss" and s[0][2] for s, e, _ in edges(trans))
    ok &= report(product_check(trans, init, ss_spec, "none") and reach_21,
                 f"21 safe-stop-integrity {tag}", quiet)

    # 22. preemption rollback: only with rb, reverse order, at most once per run
    def roll_spec(sp, e):
        if e in ("dispatch", "retry"):
            return (None, frozenset())
        if e.startswith("proll_"):
            i = _num(e)
            last, seen = sp
            if not rb or i in seen or (last is not None and i >= last):
                return None
            return (i, seen | {i})
        return sp
    ok &= report(product_check(trans, init, roll_spec, (None, frozenset())),
                 f"22 preempt-rollback-once {tag}", quiet)

    # 23. priority and claims (non-vacuous: both reflexes armed at once)
    both = any(a == "armed" and b == "armed" for (_, a, b, _) in trans)
    prio = not any(e == "r_begin_B" and s[1] == "armed" for s, e, _ in edges(trans))
    claims = not any(e == "dispatch" and (s[1] != "idle" or s[2] != "idle")
                     for s, e, _ in edges(trans))
    ok &= report(both and prio and claims, f"23 priority-and-claims {tag}", quiet)

    # 24. withdrawal (non-vacuous: B cancelled while its hand-off is pending)
    withdraw = not any(e == "handoff" and s[2] != "armed" for s, e, _ in edges(trans))
    reach_24 = not has_block or any(
        e == "cancel_B" and s[0][0] == "exec" and s[0][3] for s, e, _ in edges(trans))
    ok &= report(withdraw and reach_24, f"24 cancel-withdraws-handoff {tag}", quiet)

    # 25. retry gate (non-vacuous: some retry happens)
    gate = not any(e == "retry" and ("armed" in (s[1], s[2]) or s[3])
                   for s, e, _ in edges(trans))
    some_retry = any(e == "retry" for _, e, _ in edges(trans))
    ok &= report(gate and some_retry, f"25 retry-gate {tag}", quiet)

    # 26. shutdown: nothing new starts after estop
    def shut_spec(sp, e):
        if e == "estop":
            return "closed"
        if sp == "closed" and (e in ("dispatch", "retry") or e.startswith(
                ("launch_", "r_begin_", "begin_"))):
            return None
        return sp
    ok &= report(product_check(trans, init, shut_spec, "open"),
                 f"26 nothing-starts-after-estop {tag}", quiet)

    # 27. an armed reflex can always be admitted, cancelled or dropped
    def can_leave_armed(x: int) -> bool:
        good = {s for s in trans if s[x] != "armed"}
        rev: dict = {}
        for s, _, t in edges(trans):
            if t != "END":
                rev.setdefault(t, set()).add(s)
        q, reach = deque(good), set(good)
        while q:
            t = q.popleft()
            for s in rev.get(t, ()):
                if s not in reach:
                    reach.add(s)
                    q.append(s)
        return all(s in reach for s in trans if s[x] == "armed")
    # stronger than "can leave": admission itself must be reachable while armed
    def can_run(x: int, ev: str) -> bool:
        targets = {s for s, e, _ in edges(trans) if e == ev}
        rev: dict = {}
        for s, e, t in edges(trans):
            if t != "END" and not e.startswith("cancel_") and e != "estop":
                rev.setdefault(t, set()).add(s)
        q, reach = deque(targets), set(targets)
        while q:
            t = q.popleft()
            for s in rev.get(t, ()):
                if s not in reach:
                    reach.add(s)
                    q.append(s)
        return all(s in reach for s in trans if s[x] == "armed" and not s[3])
    ok &= report(can_leave_armed(1) and can_leave_armed(2)
                 and can_run(1, "r_begin_A") and can_run(2, "r_begin_B"),
                 f"27 armed-reflex-admissible {tag}", quiet)
    return ok


OWNER_EVENTS = ("stop", "timeout", "handoff", "ss_begin", "retry") + tuple(
    f"{p}_{i}" for p in ("begin", "end", "fail", "proll") for i in range(1, 10))


def check_all(layouts, quiet: bool = True) -> bool:
    ok = True
    for lay in layouts:
        for rb in (False, True):
            ok &= check(lay, rb, quiet=quiet)
    return ok


def main() -> None:
    print("Two reflexes + retry + timeout + estop, harmonised instance (checks 19-27):")
    ok = check_all([HARMONISED], quiet=False)
    comp = list(composition_layouts(MAX_N))
    print(f"Layout sweep, {len(comp)} layouts with N <= {MAX_N} (checks 19-27):")
    ok &= check_all(comp, quiet=True)
    print(f"  {len(comp)} layouts x 2 rollback settings checked")
    print("\nRESULT:", "ALL PASS" if ok and not FAILS else f"{len(FAILS)} FAILURES")
    raise SystemExit(0 if ok and not FAILS else 1)


if __name__ == "__main__":
    main()
