# Formal models

Mechanically checked models of Arachnite's interrupt and reflex-arbitration
semantics (v0.13, ADR 0005). Each model has two encodings:

- a CSP_M file, the canonical encoding for FDR4 users;
- a licence-free Python checker that enumerates the reachable state space
  exhaustively, so a PASS is a proof for the modelled configurations.

| File | What it models |
|---|---|
| `interrupt_policies.csp`, `verify_interrupt_policies.py` | The four interrupt policies (ALWAYS, NEVER, CHECKPOINT, ROLLBACK) in isolation, on the five-step harmonised action (steps 1–3 mandatory with rollback, 4–5 interruptible, 5 a checkpoint). |
| `runtime_composition.csp`, `verify_runtime_composition.py` | The policies composed with the runtime: the `ActionMasterNode` interrupt gate, cooperative mid-step observation points, the graceful-stop request, emergency stop, non-blocking action tasks, and a reflex action that shares one actuator resource with the deliberative action (PREEMPT / WAIT_FOR_BLOCK, `rollback_on_preempt`). |
| `mutation_check_composition.py` | Discriminative power of the composition checks. It seeds 18 semantic faults into the model and requires each to be rejected by the check designed for it. |

## Semantics modelled

**Stop points.** A pending deliberative request is honoured at a step
boundary that is not strictly inside a mandatory block. That includes the
boundary *before* a block, so a request pending as the block starts stops the
action there. Boundaries between two mandatory steps are never stop points.

**Gate.** `request_interrupt` refuses while the current step is mandatory
(`reqR`). The graceful-stop request (`interrupt_all`) bypasses the gate and is
held through a block.

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

## Run

```bash
cd formal
python verify_interrupt_policies.py      # 13 checks
python verify_runtime_composition.py     # a few seconds; RESULT: ALL PASS
python mutation_check_composition.py     # MUTATION RESULT: ALL CAUGHT
```

With FDR4: `refines interrupt_policies.csp` and `refines runtime_composition.csp`.

**Scope.** The models are untimed. Latency bounds (time to admit a reflex)
are derived from declared timeouts and measured empirically (spec §17.12,
§22); they are not proved here. Undeclared actuator access, GIL contention,
and OS or GC pauses are outside the model.
