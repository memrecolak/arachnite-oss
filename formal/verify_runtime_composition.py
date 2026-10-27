"""Exhaustive verification of the interrupt policies composed with the
runtime (v0.13 semantics, ADR 0005): ActionMasterNode's interrupt gate,
mid-step observation points, the emergency-stop path, non-blocking action
tasks, actuator-resource ownership, and reflex arbitration
(ReflexPreemption.PREEMPT / WAIT_FOR_BLOCK, rollback_on_preempt).

This extends ``verify_interrupt_policies.py`` (which proves the four policy
processes in isolation) to the composition that the running system actually
executes.  The model is derived from the v0.13 source:

* ``MultiStepActionNode.execute``  -- before each step a pending request is
  honoured at a *stop point*: a boundary not strictly inside a mandatory
  block (before an interruptible step, or before a block).  Cooperative
  mid-step bail via ``check_interrupt()`` only inside interruptible steps;
  rollback via ``on_interrupted`` (reverse order, mandatory steps with a
  rollback callable only).  A WAIT_FOR_BLOCK reflex waiting on the action
  is handed over at the first stop point, whatever the policy.
* ``ActionMasterNode.request_interrupt`` -- the gate: while the current
  step is mandatory the request is latched (``reqR``, ADR 0009) and honoured
  at the first stop point after the block; forwarded otherwise (``reqA``);
  no-op when the action is not running (``reqD``).
* ``ActionMasterNode.interrupt_all`` (graceful ``stop()``) -- sets the
  pending flag, also inside a mandatory block (``sreq``); the flag is then
  honoured at the next stop point.
* ``ArachniteRuntime.emergency_stop`` -- cancels the action at its current
  await, runs its safe-stop, never rolls back, then halts (``estop``,
  ``safe_stop``, ``halt``).
* ``ActionMasterNode.launch`` / ``launch_reflex`` -- the deliberative action
  and the reflex action share one actuator resource.  A deliberative
  dispatch is blocked while the reflex runs or is armed (its claim).  A
  reflex that conflicts with the running action is armed and the owner is
  preempted: PREEMPT cancels it now (even inside a mandatory block);
  WAIT_FOR_BLOCK cancels it now outside a block and at the first stop point
  after the block otherwise.  The owner then runs ``on_preempted()``
  (``safe_stop``), the rollback walk only with ``rollback_on_preempt``
  (``proll_i``) and only if no deliberative walk was already in progress,
  releases (``stop``), and only then does the reflex start (``r_begin``).
* ``ArachniteRuntime.tick`` -- actions run as tasks, so the tick process and
  the action interleave freely; the reflex stage launches reflex proposals
  before the decide stage, which may issue interrupts through the gate.

Steps are refined into ``begin_i``/``end_i`` pairs so that "interrupts are
observed only at stop points or at cooperative observation points" is a
checkable property.

Checks on the action alone (1-9; 1-8 for EVERY step layout with N <= MAX_N):
  1. deadlock freedom (the only terminal state is after ``halt``)
  2. estop is enabled in every state before shutdown        (never refused)
  3. gate soundness: reqR exactly while the current step is mandatory,
     and reqR latches the request (it is pending afterwards)
  4. mandatory-step atomicity: no ack/roll/bail inside a mandatory step
  5. deliberative stops only at stop points: an ack/roll walk starts only
     at a boundary that is not strictly inside a block
  6. no spurious interrupt: ack/roll/bail only after a reqA, reqR or sreq
  7. rollback order: strictly reverse completion order over mandatory
     rollback-bearing steps, then ack
  8. honour at the first eligible stop point; NEVER never acks
  9. emergency stop: after estop, no step, rollback or ack -- only the
     safe-stop, then halt
 10. five-step instance: BLOCKATOMIC (no ack between begin_1 and end_3)

Checks on the composition with the tick and a reflex sharing the resource
(11-18; every policy x preemption mode x rollback_on_preempt):
 11. deadlock freedom
 12. reflex-before-decide: a fired reflex is launched before the decide
     stage of the same tick completes
 13. reflex independence: the reflex is launched even while the action is
     inside a mandatory step (non-vacuous)
 14. actuator exclusivity: the reflex action never runs while the
     deliberative action holds the resource
 15. preemption ordering: after a conflicting launch, the reflex begins only
     after the owner released (safe-stop first on the preemption path)
 16. PREEMPT promptness: after the launch, the owner starts or completes no
     further step before it releases
 17. WAIT_FOR_BLOCK bound: after the launch, the owner only finishes the
     mandatory block it is in -- it never begins a step at a stop point
 18. rollback on preemption only with rollback_on_preempt, in reverse order,
     never repeating a deliberative walk; no deliberative dispatch while the
     reflex is armed or running (claims honoured)

Run:  python verify_runtime_composition.py
"""
from __future__ import annotations

from collections import deque
from itertools import combinations

MAX_N = 6
POLICIES = ("ALWAYS", "NEVER", "CHECKPOINT", "ROLLBACK")
MODES = ("PREEMPT", "WAIT_FOR_BLOCK")


class Layout:
    """One step layout: which steps are interruptible / checkpoints /
    carry rollback callables (checkpoints only on interruptible steps;
    rollback only relevant on mandatory steps, mirroring on_interrupted)."""

    def __init__(self, n: int, inter: frozenset, ckpt: frozenset, roll: frozenset):
        self.n, self.inter, self.ckpt, self.roll = n, inter, ckpt, roll

    def interruptible(self, i: int) -> bool:
        return i in self.inter

    def stop_point(self, i: int) -> bool:
        """Boundary before step i is not strictly inside a mandatory block."""
        return self.interruptible(i) or i == 1 or self.interruptible(i - 1)

    def honours(self, policy: str, i: int) -> bool:
        """execute()'s boundary predicate for a pending deliberative request."""
        if not self.stop_point(i):
            return False
        if policy in ("ALWAYS", "ROLLBACK"):
            return True
        if policy == "CHECKPOINT":
            return i in self.ckpt
        return False  # NEVER

    def bails(self, policy: str, i: int) -> bool:
        """check_interrupt() inside step i (cooperative mid-step bail)."""
        if not self.interruptible(i):
            return False
        if policy in ("ALWAYS", "ROLLBACK"):
            return True
        if policy == "CHECKPOINT":
            return i in self.ckpt
        return False

    def rollback_queue(self, done: tuple) -> tuple:
        return tuple(j for j in reversed(done)
                     if not self.interruptible(j) and j in self.roll)


HARMONISED = Layout(5, frozenset({4, 5}), frozenset({5}), frozenset({1, 2, 3}))


# ── The deliberative action (node + gate + emergency path) ──────────────────
# States:
#   ("idle",)
#   ("exec", idx, in_step, pending, rpend, done)   rpend: WAIT_FOR_BLOCK reflex waiting
#   ("undo", queue)  ("bailed",)  ("acked",)       deliberative unwinding
#   ("pre_ss", queue)  ("pre_roll", queue)         preempted: safe-stop, opt. rollback
#   ("estop_ss",)  ("halted",)  "END"              emergency stop

def owner_succ(state, lay: Layout, policy: str, rb: bool = False):
    if state == "END":
        return
    kind = state[0]
    if kind == "estop_ss":
        yield ("safe_stop", ("halted",))
        return
    if kind == "halted":
        yield ("halt", "END")
        return
    # emergency stop: cancels the task at its current await (any state)
    yield ("estop", ("estop_ss",))
    if kind == "idle":
        yield ("dispatch", ("exec", 1, False, False, False, ()))
        yield ("reqD", state)
        return
    if kind == "exec":
        _, idx, in_step, pending, rpend, done = state
        cur = idx if idx <= lay.n else None
        if cur is None:
            yield ("reqD", state)
        elif lay.interruptible(cur):
            yield ("reqA", ("exec", idx, in_step, True, rpend, done))
        else:
            # latched, honoured at the first stop point after the block
            yield ("reqR", ("exec", idx, in_step, True, rpend, done))
        if cur is not None:
            # graceful stop(): interrupt_all() bypasses the gate
            yield ("sreq", ("exec", idx, in_step, True, rpend, done))
        if idx > lay.n:
            yield ("stop", ("idle",))
            return
        if not in_step:  # boundary before step idx
            if rpend and lay.stop_point(idx):
                queue = lay.rollback_queue(done) if rb else ()
                yield ("cancel", ("pre_ss", queue))  # WAIT_FOR_BLOCK hand-over
                return
            if pending and lay.honours(policy, idx):
                queue = lay.rollback_queue(done) if policy == "ROLLBACK" else ()
                if queue:
                    yield (f"roll_{queue[0]}", ("undo", queue[1:]))
                else:
                    yield ("ack", ("acked",))
                return
            yield (f"begin_{idx}", ("exec", idx, True, pending, rpend, done))
        else:  # inside step idx
            yield (f"end_{idx}", ("exec", idx + 1, False, pending, rpend, done + (idx,)))
            if pending and lay.bails(policy, idx):
                queue = lay.rollback_queue(done) if policy == "ROLLBACK" else ()
                if queue:
                    yield (f"bail_{idx}", ("undo", queue))
                else:
                    yield (f"bail_{idx}", ("bailed",))
        return
    if kind == "undo":
        _, queue = state
        if queue:
            yield (f"roll_{queue[0]}", ("undo", queue[1:]))
        else:
            yield ("ack", ("acked",))
        return
    if kind == "bailed":
        yield ("ack", ("acked",))
        return
    if kind == "acked":
        yield ("stop", ("idle",))
        return
    if kind == "pre_ss":
        _, queue = state
        yield ("safe_stop", ("pre_roll", queue))
        return
    if kind == "pre_roll":
        _, queue = state
        if queue:
            yield (f"proll_{queue[0]}", ("pre_roll", queue[1:]))
        else:
            yield ("stop", ("idle",))  # release
        return


def owner_holds(state) -> bool:
    """The deliberative action owns the shared resource (task not ended)."""
    return state != "END" and state[0] not in ("idle", "halted", "estop_ss")


def owner_preempt(state, lay: Layout, mode: str, rb: bool):
    """Effect of a conflicting reflex launch on the owner (launch_reflex)."""
    kind = state[0]
    if kind == "exec":
        _, idx, in_step, pending, rpend, done = state
        cur = idx if idx <= lay.n else None
        in_block = cur is not None and not lay.interruptible(cur)
        if mode == "WAIT_FOR_BLOCK" and in_block:
            return ("exec", idx, in_step, pending, True, done)
        queue = lay.rollback_queue(done) if rb else ()
        return ("pre_ss", queue)  # cancelled now; the current step is not completed
    if kind == "undo":
        # cancelled during a deliberative rollback walk: safe-stop only,
        # the walk is not restarted (no double undo)
        return ("pre_ss", ())
    # idle / acked / bailed / preempt path: no await to cancel at, or
    # already releasing -- the launch only arms the reflex
    return state


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


def product_check(trans, init, step, init_sp) -> bool:
    """Every trace from init is accepted by the deterministic spec ``step``."""
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


def _num(e: str) -> int:
    return int(e.rsplit("_", 1)[1])


# ── Checks 1-9 on the action alone (layout-generic) ─────────────────────────

def check_layout(lay: Layout, policy: str, quiet: bool = True) -> bool:
    trans = build(("idle",), lambda s: owner_succ(s, lay, policy))
    ok = True
    tag = (f"[N={lay.n} I={sorted(lay.inter)} C={sorted(lay.ckpt)} "
           f"R={sorted(lay.roll)}] {policy}")
    init = ("idle",)

    # 1. deadlock freedom
    ok &= report(all(outs for outs in trans.values()),
                 f"1 deadlock-free {tag} ({len(trans)} states)", quiet)

    # 2. estop enabled in every state before shutdown
    ok &= report(all(any(e == "estop" for e, _ in outs)
                     for s, outs in trans.items()
                     if s[0] not in ("estop_ss", "halted")),
                 f"2 estop-never-refused {tag}", quiet)

    # 3. gate soundness
    def gate_ok() -> bool:
        for s, outs in trans.items():
            cur = s[1] if s[0] == "exec" and s[1] <= lay.n else None
            for e, t in outs:
                if e == "reqR" and (cur is None or lay.interruptible(cur)):
                    return False
                if e == "reqR" and not (t[0] == "exec" and t[3]):
                    return False  # an in-block request must be latched
                if e == "reqA" and (cur is None or not lay.interruptible(cur)):
                    return False
        return True
    ok &= report(gate_ok(), f"3 gate-soundness {tag}", quiet)

    # 4. mandatory-step atomicity
    def atomic(sp, e):
        if sp == "out":
            if e.startswith("begin_"):
                return "out" if lay.interruptible(_num(e)) else "in"
            return "out"
        if e.startswith("end_") or e == "estop":
            return "out"
        if e == "ack" or e.startswith("roll_") or e.startswith("bail_"):
            return None
        return "in"
    ok &= report(product_check(trans, init, atomic, "out"),
                 f"4 mandatory-atomicity {tag}", quiet)

    # 5. deliberative stops only at stop points
    def stops_ok() -> bool:
        for s, outs in trans.items():
            if s[0] != "exec" or s[2] or s[1] > lay.n:
                continue
            starts = any(e == "ack" or e.startswith("roll_") for e, _ in outs)
            if starts and not lay.stop_point(s[1]):
                return False
        return True
    ok &= report(stops_ok(), f"5 stop-points-only {tag}", quiet)

    # 6. no spurious interrupt: ack/roll/bail need a prior reqA or reqR
    #    (or the shutdown request) in this run
    def spurious(sp, e):
        if e in ("reqA", "reqR", "sreq"):
            return "armed"
        if e == "dispatch":
            return "running"
        if e == "ack" or e.startswith("roll_") or e.startswith("bail_"):
            return "armed" if sp == "armed" else None
        return sp
    ok &= report(product_check(trans, init, spurious, "cold"),
                 f"6 no-spurious-interrupt {tag}", quiet)

    # 7. rollback order: strictly decreasing, mandatory with rollback, then ack
    def rollord(sp, e):
        if e.startswith("roll_"):
            i = _num(e)
            if policy != "ROLLBACK" or lay.interruptible(i) or i not in lay.roll:
                return None
            if isinstance(sp, int) and i >= sp:
                return None
            return i
        if e in ("ack", "dispatch"):
            return "free"
        return sp
    ok &= report(product_check(trans, init, rollord, "free"),
                 f"7 rollback-order {tag}", quiet)

    # 8. honour at the first eligible stop point; NEVER never acks
    def honour_ok() -> bool:
        for s, outs in trans.items():
            if s[0] != "exec":
                continue
            _, idx, in_step, pending, _, _ = s
            if in_step or not pending or idx > lay.n:
                continue
            if lay.honours(policy, idx) and any(e == f"begin_{idx}" for e, _ in outs):
                return False
        return True
    ok &= report(honour_ok(), f"8a honour-first-stop-point {tag}", quiet)
    if policy == "NEVER":
        ok &= report(not any(e == "ack" for outs in trans.values() for e, _ in outs),
                     f"8b never-no-ack {tag}", quiet)

    # 9. emergency stop: after estop only safe_stop, then halt
    def estop_spec(sp, e):
        if sp == "run":
            return "stopping" if e == "estop" else "run"
        if sp == "stopping":
            return "stopped" if e == "safe_stop" else None
        return "end" if e == "halt" and sp == "stopped" else None
    ok &= report(product_check(trans, init, estop_spec, "run"),
                 f"9 estop-safe-stop-no-rollback {tag}", quiet)
    return ok


# ── Check 10: five-step BLOCKATOMIC ─────────────────────────────────────────

def check_blockatomic_harmonised() -> bool:
    ok = True
    for policy in POLICIES:
        trans = build(("idle",),
                      lambda s, policy=policy: owner_succ(s, HARMONISED, policy))

        def blockatomic(sp, e):
            if e == "dispatch":
                return "pre"
            if sp == "pre":
                return "in" if e == "begin_1" else "pre"
            if sp == "in":
                if e == "ack":
                    return None
                return "post" if e == "end_3" else "in"
            return sp
        ok &= report(product_check(trans, ("idle",), blockatomic, "pre"),
                     f"10 BLOCKATOMIC harmonised {policy}", quiet=False)
    return ok


# ── Checks 11-18: tick + action + reflex sharing one resource ───────────────
# Tick (one agent): sense -> [reflex_fire -> rlaunch]? -> instinct ->
# decide (may issue a gate request) -> act -> loop.  The deliberative action
# and the reflex action run as tasks and interleave with the tick.
# Reflex action states: "r_idle" | "r_armed" | "r_run".

def tick_succ(state):
    kind = state[0]
    if kind == "t_start":
        yield ("sense", ("sensed",))
    elif kind == "sensed":
        yield ("reflex_fire", ("rfired",))
        yield ("no_reflex", ("instinct",))
    elif kind == "rfired":
        yield ("rlaunch", ("instinct",))
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


GATE = ("reqA", "reqR", "reqD")


def system_succ(state, lay: Layout, policy: str, mode: str, rb: bool):
    owner, rstate, tick = state
    # deliberative action (estop is checked on the action alone; here the
    # runtime keeps running)
    for e, o2 in owner_succ(owner, lay, policy, rb):
        if e in ("estop",) or e in GATE:
            continue
        if e == "dispatch" and rstate != "r_idle":
            continue  # resource owned or claimed by the reflex
        yield (e, (o2, rstate, tick))
    # tick, synchronised with the gate on reqA/reqR/reqD
    for e, t2 in tick_succ(tick):
        if e in GATE:
            for e2, o2 in owner_succ(owner, lay, policy, rb):
                if e2 == e:
                    yield (e, (o2, rstate, t2))
        elif e == "rlaunch":
            if rstate != "r_idle":
                yield ("rlaunch_dedup", (owner, rstate, t2))
            elif owner_holds(owner):
                yield ("rlaunch_c",
                       (owner_preempt(owner, lay, mode, rb), "r_armed", t2))
            else:
                yield ("rlaunch_f", (owner, "r_armed", t2))
        else:
            yield (e, (owner, rstate, t2))
    # reflex action task
    if rstate == "r_armed" and not owner_holds(owner):
        yield ("r_begin", (owner, "r_run", tick))
    if rstate == "r_run":
        yield ("r_end", (owner, "r_idle", tick))


def check_system(lay: Layout, policy: str, mode: str, rb: bool,
                 quiet: bool = False) -> bool:
    init = (("idle",), "r_idle", ("t_start",))
    trans = build(init, lambda s: system_succ(s, lay, policy, mode, rb))
    tag = f"{policy} {mode} rb={rb}"
    ok = True

    # 11. deadlock freedom
    ok &= report(all(outs for outs in trans.values()),
                 f"11 system deadlock-free {tag} ({len(trans)} states)", quiet)

    # 12. reflex launched before decide completes in the same tick
    def rbd(sp, e):
        if e == "t_loop":
            return "clean"
        if e == "reflex_fire":
            return "fired"
        if e.startswith("rlaunch"):
            return "clean" if sp == "fired" else None
        if (e == "decide_done" or e in GATE) and sp == "fired":
            return None
        return sp
    ok &= report(product_check(trans, init, rbd, "clean"),
                 f"12 reflex-before-decide {tag}", quiet)

    # 13. reflex launch enabled while the owner is inside a mandatory step
    #     (vacuity guard: such states must exist when the layout has one)
    def indep() -> bool:
        hit = len(lay.inter) == lay.n
        for (o, _, t), outs in trans.items():
            if t == ("rfired",):
                if not any(e.startswith("rlaunch") for e, _ in outs):
                    return False
                if (o[0] == "exec" and o[2] and o[1] <= lay.n
                        and not lay.interruptible(o[1])):
                    hit = True
        return hit
    ok &= report(indep(), f"13 reflex-independent-of-mandatory {tag}", quiet)

    # 14. actuator exclusivity (state invariant)
    ok &= report(not any(r == "r_run" and owner_holds(o) for (o, r, _) in trans),
                 f"14 actuator-exclusivity {tag}", quiet)

    # 15. preemption ordering: after rlaunch_c, r_begin only after release;
    #     a preemption path releases only after safe_stop
    def order(sp, e):
        if e == "rlaunch_c":
            return "wait"
        if e == "cancel" and sp == "wait":
            return "wait"
        if e == "safe_stop" and sp == "wait":
            return "safe"
        if e == "stop" and sp in ("wait", "safe"):
            return "free"
        if e == "r_begin":
            return "idle" if sp in ("idle", "free") else None
        return sp
    ok &= report(product_check(trans, init, order, "idle"),
                 f"15 preemption-ordering {tag}", quiet)

    # 16. PREEMPT: the owner starts or completes no further step until it
    #     releases (it may still acknowledge a deliberative stop it had
    #     already committed to: a cooperative bail is not an await)
    if mode == "PREEMPT":
        def prompt(sp, e):
            if e == "rlaunch_c":
                return "pre"
            if sp == "pre":
                if e == "stop":
                    return "idle"
                if e.startswith(("begin_", "end_")):
                    return None
            return sp
        ok &= report(product_check(trans, init, prompt, "idle"),
                     f"16 preempt-promptness {tag}", quiet)

    # 17. WAIT_FOR_BLOCK: the owner only finishes the block it is in
    if mode == "WAIT_FOR_BLOCK":
        def bounded(sp, e):
            if e == "rlaunch_c":
                return "wait"
            if sp == "wait":
                if e == "stop":
                    return "idle"
                if e.startswith("begin_") and lay.stop_point(_num(e)):
                    return None
                if e.startswith("end_") and lay.interruptible(_num(e)):
                    return None
            return sp
        ok &= report(product_check(trans, init, bounded, "idle"),
                     f"17 wait-for-block-bound {tag}", quiet)

    # 18. rollback on preemption only with rb, reverse order, no double
    #     undo; no deliberative dispatch while the reflex claims the resource
    def preroll(sp, e):
        if e.startswith("roll_"):
            return ("rolled", sp[1] | {_num(e)})
        if e.startswith("proll_"):
            i = _num(e)
            if not rb or i in sp[1]:
                return None
            if isinstance(sp[0], int) and i >= sp[0]:
                return None
            return (i, sp[1] | {i})
        if e == "dispatch":
            return ("free", frozenset())
        if e == "stop":
            return ("free", sp[1])
        return sp
    claims_ok = not any(
        e == "dispatch" for (o, r, _), outs in trans.items()
        if r in ("r_armed", "r_run") for e, _ in outs
    )
    ok &= report(product_check(trans, init, preroll, ("free", frozenset()))
                 and claims_ok,
                 f"18 preempt-rollback-and-claims {tag}", quiet)
    return ok


def check_compositions(layouts, quiet: bool = False) -> bool:
    ok = True
    for lay in layouts:
        for policy in POLICIES:
            for mode in MODES:
                for rb in (False, True):
                    ok &= check_system(lay, policy, mode, rb, quiet=quiet)
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


def composition_layouts(max_n: int):
    """Layouts for the composed sweep: every interruptible mask, all
    mandatory steps carrying rollback, the last interruptible step a
    checkpoint (the composed checks do not depend on further variation)."""
    for n in range(1, max_n + 1):
        steps = list(range(1, n + 1))
        for k in range(n + 1):
            for inter in combinations(steps, k):
                inter_f = frozenset(inter)
                ckpt = frozenset({max(inter)}) if inter else frozenset()
                roll = frozenset(i for i in steps if i not in inter_f)
                yield Layout(n, inter_f, ckpt, roll)


def main() -> None:
    print("Harmonised five-step instance, action alone (checks 1-9, verbose):")
    ok = True
    for policy in POLICIES:
        ok &= check_layout(HARMONISED, policy, quiet=False)

    print("Five-step BLOCKATOMIC (check 10):")
    ok &= check_blockatomic_harmonised()

    print("Composition with the tick and a resource-sharing reflex "
          "(checks 11-18, harmonised instance):")
    ok &= check_compositions([HARMONISED], quiet=False)

    print(f"Layout sweep, all step layouts with N <= {MAX_N} (checks 1-9):")
    n_layouts = 0
    for lay in all_layouts(MAX_N):
        n_layouts += 1
        for policy in POLICIES:
            ok &= check_layout(lay, policy, quiet=True)
    print(f"  {n_layouts} layouts x {len(POLICIES)} policies checked")

    comp = list(composition_layouts(MAX_N))
    print(f"Composition sweep, {len(comp)} layouts with N <= {MAX_N} "
          f"(checks 11-18):")
    ok &= check_compositions(comp, quiet=True)
    print(f"  {len(comp)} layouts x {len(POLICIES)} policies x {len(MODES)} "
          f"modes x 2 rollback settings checked")

    print("\nRESULT:", "ALL PASS" if ok and not FAILS
          else f"{len(FAILS)} FAILURES")
    raise SystemExit(0 if ok and not FAILS else 1)


if __name__ == "__main__":
    main()
