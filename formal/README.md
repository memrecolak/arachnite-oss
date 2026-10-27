# Formal models

Mechanically checked models of Arachnite's interrupt and reflex-arbitration
semantics (v0.13, ADR 0005). The first two models have two encodings:

- a CSP_M file, the canonical encoding for FDR4 users;
- a licence-free Python checker that enumerates the reachable state space
  exhaustively, so a PASS is a proof for the modelled configurations.

The two-reflex model has the Python checker only.

| File | What it models |
|---|---|
| `interrupt_policies.csp`, `verify_interrupt_policies.py` | The four interrupt policies (ALWAYS, NEVER, CHECKPOINT, ROLLBACK) in isolation, on the five-step harmonised action (steps 1–3 mandatory with rollback, 4–5 interruptible, 5 a checkpoint). |
| `runtime_composition.csp`, `verify_runtime_composition.py` | The policies composed with the runtime: the `ActionMasterNode` interrupt gate, cooperative mid-step observation points, the graceful-stop request, emergency stop, non-blocking action tasks, and a reflex action that shares one actuator resource with the deliberative action (PREEMPT / WAIT_FOR_BLOCK, `rollback_on_preempt`). |
| `mutation_check_composition.py` | Discriminative power of the composition checks. It seeds 19 semantic faults into the model and requires each to be rejected by the check designed for it. |
| `verify_two_reflexes.py` | Arbitration with **two** resource-sharing reflexes, one PREEMPT and one WAIT_FOR_BLOCK, at different priorities. The deliberative owner can fail a step and be retried once, can time out, and can be stopped by emergency stop. Armed reflexes can be cancelled. This covers the paths of the 2026-10-26 reflex-path review, which the one-reflex model cannot reach. |
| `mutation_check_two_reflexes.py` | Re-creates each defect class of that review (R01, R03–R07, R10, R11) as a model fault and requires the check designed for it to reject the fault. |

## Semantics modelled

**Stop points.** A pending deliberative request is honoured at a step
boundary that is not strictly inside a mandatory block. That includes the
boundary *before* a block, so a request pending as the block starts stops the
action there. Boundaries between two mandatory steps are never stop points.

**Gate.** `request_interrupt` never refuses. While the current step is
mandatory the request is latched (`reqR`) and held through the block, to be
honoured at the first stop point after it (ADR 0009). The graceful-stop
request (`interrupt_all`) is held the same way.

**Emergency stop.** Cancels the action at its current await, runs the
safe-stop, and never rolls back.

**Actions and the tick.** Actions are tasks, so the tick (sense → reflex →
instinct → decide → act) interleaves freely with them.

**A conflicting reflex launch.**
- The reflex is armed and claims the resource, so no deliberative dispatch
  happens in between.
- The owner is preempted. PREEMPT cancels it now, even inside a block.
  WAIT_FOR_BLOCK lets the block finish and hands over at the next stop point.
- The owner runs its safe-stop, then the rollback walk only with
  `rollback_on_preempt` (and never restarting a deliberative walk already in
  progress), then releases.
- Only then does the reflex action begin.

## Properties

**Action alone** — checks 1–9 run for every step layout with N ≤ 6 (5,460
layouts × 4 policies); check 10 runs on the five-step instance:

| # | Property |
|---|---|
| 1 | Deadlock freedom. |
| 2 | Emergency stop never refused. |
| 3 | Gate soundness. |
| 4 | Mandatory-step atomicity. |
| 5 | Deliberative stops only at stop points. |
| 6 | No spurious interrupt. |
| 7 | Reverse rollback order. |
| 8 | Honour at the first eligible stop point; NEVER never acknowledges. |
| 9 | Emergency stop only safe-stops, then halts. |
| 10 | Block atomicity: no acknowledgement between the block's first begin and its last end. |

**Composition with the tick and a resource-sharing reflex** — checks 11–18,
for every policy × preemption mode × rollback setting, on the five-step
instance and on 126 layouts with N ≤ 6:

| # | Property |
|---|---|
| 11 | Deadlock freedom. |
| 12 | A fired reflex is launched before the decide stage completes. |
| 13 | The reflex is launched even while the action is inside a mandatory step. |
| 14 | Actuator exclusivity: the reflex action never runs while the deliberative action holds the resource. |
| 15 | Preemption ordering: the reflex begins only after the owner released, and the safe-stop comes first on the preemption path. |
| 16 | PREEMPT: no further step begins or ends before the release. |
| 17 | WAIT_FOR_BLOCK: only the block in progress may continue. |
| 18 | Rollback on preemption only with `rollback_on_preempt`, in reverse order, with no double undo; no deliberative dispatch while the reflex is armed or running. |

**Two reflexes, retry, timeout and emergency stop** — checks 19–27, for
both rollback settings, on the five-step instance and on 126 layouts with
N ≤ 6 (40–186 reachable states each). Each check also requires that the
scenario it guards is reachable, so none passes vacuously:

| # | Property |
|---|---|
| 19 | Deadlock freedom. |
| 20 | Actuator exclusivity among the owner and both reflex actions. |
| 21 | Safe-stop integrity: at most one safe-stop per owner run. Once it begins, nothing happens to the owner until it ends: no second preemption restart, no timeout, no emergency-stop cut. |
| 22 | Rollback on preemption only with `rollback_on_preempt`, in reverse order, each step at most once per run. |
| 23 | Priority and claims: the lower-priority reflex is never admitted while the higher one is armed. There is no deliberative dispatch while a reflex is armed or running. |
| 24 | Withdrawal: the owner hands over to the WAIT_FOR_BLOCK reflex only while that reflex is still armed. |
| 25 | Retry gate: no retry while a reflex is armed or after emergency stop. |
| 26 | After emergency stop, nothing new starts: no dispatch, launch, admission, step or retry. |
| 27 | Every armed reflex can still be admitted (or cancelled or dropped). |

## Run

```bash
cd formal
python verify_interrupt_policies.py      # 13 checks
python verify_runtime_composition.py     # a few seconds; RESULT: ALL PASS
python mutation_check_composition.py     # MUTATION RESULT: ALL CAUGHT
python verify_two_reflexes.py            # under a second; RESULT: ALL PASS
python mutation_check_two_reflexes.py    # MUTATION RESULT: ALL CAUGHT (7/7)
```

With FDR4: `refines interrupt_policies.csp` and `refines runtime_composition.csp`.

**Scope.** The models are untimed. Latency bounds (time to admit a reflex)
are derived from declared timeouts and measured empirically (spec §17.12,
§22); they are not proved here. Undeclared actuator access, GIL contention,
and OS or GC pauses are outside the model. So are asyncio implementation
details, for example a cancel swallowed by Python 3.10's `asyncio.wait_for`
(review finding R02), the awaited `dispatch()` return path (R08) and the
event bus (R09). Those are covered by
regression tests run on Python 3.10, 3.12 and 3.14.
