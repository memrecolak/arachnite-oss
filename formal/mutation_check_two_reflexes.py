"""Discriminative-power validation of ``verify_two_reflexes.py``.

Each fault in ``FAULT_NAMES`` re-creates, in the model, one defect class that
the 2026-10-26 reflex-path code review found in the implementation. Each must
be rejected by the check designed for it, and the fault-free model must pass.

Run:  python mutation_check_two_reflexes.py
"""
from __future__ import annotations

import sys

import verify_two_reflexes as V
from verify_runtime_composition import HARMONISED, Layout

# An interruptible step before the block as well, so a hand-off can be
# requested before, inside and after a block.
MIXED = Layout(4, frozenset({1, 4}), frozenset({4}), frozenset({2, 3}))

MUTANTS = (
    ("R01 lower reflex admitted while a higher one is armed", "admit_ignores_priority", "23"),
    ("R03 PREEMPT during a hand-off restarts the safe-stop", "second_preempt_restarts_ss", "21"),
    ("R04 retry while a reflex is armed", "retry_while_armed", "25"),
    ("R05 action timeout cuts the hand-off safe-stop", "timeout_cuts_handoff_ss", "21"),
    ("R06 emergency stop abandons a running safe-stop", "estop_cuts_ss", "21"),
    ("R07/R10 launches start after the emergency stop", "launch_after_estop", "26"),
    ("R11 cancelling an armed reflex leaves its hand-off", "cancel_keeps_handoff", "24"),
)


def run(label: str, fault: str, expect: str) -> bool:
    V.FAILS.clear()
    V.FAULTS.clear()
    V.FAULTS.add(fault)
    try:
        V.check_all([HARMONISED, MIXED], quiet=True)
    finally:
        V.FAULTS.clear()
    caught = any(f.split(" ", 1)[0] == expect for f in V.FAILS)
    print(f"{'CAUGHT' if caught else '!!MISSED!!'}  {label} (expected check {expect})")
    return caught


def main() -> None:
    assert set(V.FAULT_NAMES) == {m[1] for m in MUTANTS}, "a fault has no mutant"
    V.FAILS.clear()
    clean = V.check_all([HARMONISED, MIXED], quiet=True)
    print(f"{'PASS' if clean else 'FAIL'}  fault-free model")
    caught = [run(*m) for m in MUTANTS]
    ok = clean and all(caught)
    print(f"\nMUTATION RESULT: {'ALL CAUGHT' if ok else 'MISSED'} "
          f"({sum(caught)}/{len(caught)})")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
