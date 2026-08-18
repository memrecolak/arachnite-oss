r"""Exhaustive state-space verification of the four interrupt-policy CSP
processes in ``interrupt_policies.csp`` (same five-step instance).

For this finite model (< 100 states per policy) trace refinement is language
inclusion over finite automata, checked here by an explicit product walk, so a
PASS is a proof for the modelled instance, not a sampled test. The CSP_M file
remains the canonical encoding for FDR users; this script removes the
licensing dependency.

Checks (mirroring the FDR assertions):
  1. deadlock freedom of ALWAYS / NEVER / CHECKPOINT / ROLLBACK
  2. DEFER  [T=  each policy   (no ack before the mandatory block completes)
  3. ROLLORD [T= ROLLBACK      (undo is exactly roll3, roll2, roll1, ack)
  4. hiding irq: NEVER\irq [T-within] CHECKPOINT\irq [T-within] ALWAYS\irq

Run:  python verify_interrupt_policies.py
"""
from __future__ import annotations

from collections import deque

N = 5
TERM = "tick"  # successful-termination pseudo-event


def interruptible(i: int) -> bool:
    return i >= 4


def checkpoint(i: int) -> bool:
    return i == 5


def has_rollback(i: int) -> bool:
    return i <= 3


# ── Policy LTSs ─────────────────────────────────────────────────────────────
# A state is a hashable tuple; successors() yields (event, next_state).
# The special state "END" (after tick) has no successors and is legal.


def succ_guarded(state, guard):
    """ALWAYS / CHECKPOINT family: state = (idx, pending, phase)."""
    idx, pending, phase = state
    if phase == "done":
        yield ("stop", (idx, pending, "stopped"))
        return
    if phase == "stopped":
        yield (TERM, "END")
        return
    if phase == "acking":
        yield ("ack", (idx, pending, "done"))
        return
    # executing: idx = next step position (1-based); > N means finished
    if idx > N:
        yield ("stop", (idx, pending, "stopped"))
        return
    if pending and guard(idx):
        yield ("ack", (idx, pending, "done"))
        return
    yield (f"step{idx}", (idx + 1, pending, "exec"))
    yield ("irq", (idx, True, "exec"))


def succ_never(state):
    idx, phase = state
    if phase == "stopped":
        yield (TERM, "END")
        return
    if idx > N:
        yield ("stop", (idx, "stopped"))
        return
    yield (f"step{idx}", (idx + 1, "exec"))
    yield ("irq", (idx, "exec"))


def succ_rollback(state):
    """state = (idx, done, pending, phase[, undo_queue])"""
    if state[0] == "undo":
        _, queue, phase = state
        if queue:
            j = queue[0]
            yield (f"roll{j}", ("undo", queue[1:], phase))
        else:
            yield ("ack", ("post", (), "done"))
        return
    if state[0] == "post":
        _, _, phase = state
        if phase == "done":
            yield ("stop", ("post", (), "stopped"))
        else:
            yield (TERM, "END")
        return
    idx, done, pending = state
    if idx > N:
        yield ("stop", ("post", (), "done2"))  # done2 -> stopped path below
        return
    if pending and interruptible(idx):
        queue = tuple(j for j in reversed(done) if has_rollback(j))
        yield from succ_rollback(("undo", queue, "x"))
        return
    yield (f"step{idx}", (idx + 1, done + (idx,), pending))
    yield ("irq", (idx, done, True))


def succ_rollback_fixed(state):
    """Cleaner rollback LTS: phases exec -> undo -> ack -> stop -> tick."""
    kind = state[0]
    if kind == "exec":
        _, idx, done, pending = state
        if idx > N:
            yield ("stop", ("stopped",))
            return
        if pending and interruptible(idx):
            queue = tuple(j for j in reversed(done) if has_rollback(j))
            if queue:
                yield (f"roll{queue[0]}", ("undo", queue[1:]))
            else:
                yield ("ack", ("acked",))
            return
        yield (f"step{idx}", ("exec", idx + 1, done + (idx,), pending))
        yield ("irq", ("exec", idx, done, True))
    elif kind == "undo":
        _, queue = state
        if queue:
            yield (f"roll{queue[0]}", ("undo", queue[1:]))
        else:
            yield ("ack", ("acked",))
    elif kind == "acked":
        yield ("stop", ("stopped",))
    elif kind == "stopped":
        yield (TERM, "END")


def build_lts(init, succ):
    states, trans = set(), {}
    q = deque([init])
    while q:
        s = q.popleft()
        if s in states or s == "END":
            continue
        states.add(s)
        outs = list(succ(s))
        trans[s] = outs
        for _, t in outs:
            if t != "END" and t not in states:
                q.append(t)
    return trans


# ── Specs as deterministic automata ─────────────────────────────────────────

def defer_spec(state, event):
    """DEFER: no ack before step3. States: 'pre' -> 'post'."""
    if state == "pre":
        if event == "ack":
            return None
        return "post" if event == "step3" else "pre"
    return "post"


def rollord_spec(state, event):
    """ROLLORD: rolls, if any, are exactly roll3 roll2 roll1 then ack stop tick."""
    if state == "free":
        if event == "roll3":
            return "r2"
        if event.startswith("roll"):
            return None
        return "free"
    seqs = {"r2": ("roll2", "r1"), "r1": ("roll1", "a"),
            "a": ("ack", "s"), "s": ("stop", "t"), "t": (TERM, "end")}
    want, nxt = seqs[state]
    return nxt if event == want else None


def check_refinement(name, impl_init, impl_trans, spec_step, spec_init):
    """Every impl trace must be accepted by the (deterministic, total-or-None)
    spec. BFS over (impl_state, spec_state)."""
    seen = set()
    q = deque([(impl_init, spec_init)])
    while q:
        s, sp = q.popleft()
        if (s, sp) in seen or s == "END":
            continue
        seen.add((s, sp))
        for e, t in impl_trans.get(s, []):
            nsp = spec_step(sp, e)
            if nsp is None:
                print(f"  FAIL {name}: event '{e}' rejected in spec state {sp}")
                return False
            q.append((t, nsp))
    print(f"  PASS {name} ({len(seen)} product states)")
    return True


def check_deadlock_free(name, trans):
    dead = [s for s, outs in trans.items() if not outs]
    if dead:
        print(f"  FAIL deadlock-free {name}: {dead[:3]}")
        return False
    print(f"  PASS deadlock-free {name} ({len(trans)} states)")
    return True


def check_trace_inclusion(name, big_trans, big_init, small_trans, small_init):
    """traces(small) subset-of traces(big): determinized big as spec."""
    from functools import lru_cache

    def big_step(states, event):
        nxt = set()
        for s in states:
            for e, t in (big_trans.get(s, []) if s != "END" else []):
                if e == event:
                    nxt.add(t)
        return frozenset(nxt) or None

    seen = set()
    q = deque([(small_init, frozenset([big_init]))])
    while q:
        s, spset = q.popleft()
        if (s, spset) in seen or s == "END":
            continue
        seen.add((s, spset))
        for e, t in small_trans.get(s, []):
            nxt = big_step(spset, e)
            if nxt is None:
                print(f"  FAIL {name}: '{e}' not offered by superset process")
                return False
            q.append((t, nxt))
    print(f"  PASS {name} ({len(seen)} product states)")
    return True


def main() -> None:
    always_init = (1, False, "exec")
    ckpt_init = (1, False, "exec")
    never_init = (1, "exec")
    roll_init = ("exec", 1, (), False)

    always = build_lts(always_init, lambda s: succ_guarded(s, interruptible))
    ckpt = build_lts(ckpt_init, lambda s: succ_guarded(s, checkpoint))
    never = build_lts(never_init, succ_never)
    roll = build_lts(roll_init, succ_rollback_fixed)

    ok = True
    print("Deadlock freedom:")
    for name, tr in (("ALWAYS", always), ("NEVER", never),
                     ("CHECKPOINT", ckpt), ("ROLLBACK", roll)):
        ok &= check_deadlock_free(name, tr)

    print("Deferred-interrupt property (DEFER [T= P):")
    for name, tr, init in (("ALWAYS", always, always_init),
                           ("CHECKPOINT", ckpt, ckpt_init),
                           ("NEVER", never, never_init),
                           ("ROLLBACK", roll, roll_init)):
        ok &= check_refinement(f"DEFER<={name}", init, tr, defer_spec, "pre")

    print("Rollback ordering (ROLLORD [T= ROLLBACK):")
    ok &= check_refinement("ROLLORD<=ROLLBACK", roll_init, roll,
                           rollord_spec, "free")

    print("Trace inclusion after hiding irq "
          "(NEVER/CHECKPOINT/ALWAYS after hiding irq):")
    def hide_irq(trans):
        """Turn irq edges into epsilon: fold each irq-successor's behaviour
        into the source state (epsilon closure at build time)."""
        import copy
        closed = {}
        def closure(s, seen=None):
            seen = seen or set()
            if s in seen or s == "END":
                return set()
            seen.add(s)
            outs = set()
            for e, tgt in trans.get(s, []):
                if e == "irq":
                    outs |= closure(tgt, seen)
                else:
                    outs.add((e, tgt))
            return outs
        for s in trans:
            closed[s] = list(closure(s))
        return closed
    always_h, ckpt_h, never_h = hide_irq(always), hide_irq(ckpt), hide_irq(never)
    ok &= check_trace_inclusion("CHECKPOINT-hide-irq within ALWAYS-hide-irq",
                                always_h, always_init, ckpt_h, ckpt_init)
    ok &= check_trace_inclusion("NEVER-hide-irq within CHECKPOINT-hide-irq",
                                ckpt_h, ckpt_init, never_h, never_init)

    print("\nRESULT:", "ALL PASS" if ok else "FAILURES PRESENT")
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
