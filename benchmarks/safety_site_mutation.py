"""
benchmarks/safety_site_mutation.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Coverage and mutation-testing evidence for the safety enforcement sites.

Motivation (revision experiment E7, referee point R2-D): Table 6 of the paper
backs each safety property with file-level test counts only — no line/branch
coverage and no mutation-adequacy evidence. This tool produces both, scoped
precisely to the enforcement functions Table 6 names:

  coverage — line + branch coverage of each enforcement function under the
             full test suite (pytest-cov / coverage.py in branch mode).
  mutation — mutation score of each enforcement function via cosmic-ray.
             Mutants are generated for the whole containing module, then
             session work items whose mutation lies outside the enforcement
             functions' line ranges are pruned from the sqlite session
             before execution, so only in-scope mutants are ever run.
             Mutants inside type annotations are additionally excluded as
             equivalent by construction: under ``from __future__ import
             annotations`` (project-wide policy) annotations are never
             evaluated at runtime, so e.g. ``Proposal | None`` ->
             ``Proposal + None`` cannot change behaviour.

cosmic-ray was chosen over mutmut deliberately: mutmut >= 3 requires
``os.fork`` (Linux/macOS only), while cosmic-ray runs everywhere the test
suite runs, and its sqlite session format permits the exact function-level
scoping and per-mutant audit trail reported in the paper.

Each site is exercised by the same test modules Table 6 lists for it, so the
mutation score answers "would the tests the paper cites notice a defect in
the enforcement code?" — the referee's actual question.

v0.13 (ADR 0005) moved action totality into the task body
(``ActionMasterNode._run``) and added two sites: actuator exclusivity
(resource ownership and reflex arbitration) and the preemption safe-stop.
Every surviving mutant is also written to a CSV (one row per mutant, with
its source line and diff) for manual classification; rows whose source
line is a logger call are pre-labelled ``log-only`` as a hint.

cosmic-ray mutates source files IN PLACE during ``exec`` (and restores them
afterwards). The tool therefore refuses to run the mutation mode on a dirty
working copy of a target module unless ``--allow-dirty`` is given.

Requires: pip install cosmic-ray pytest-cov  (or: pip install -e ".[mutation,dev]")

Run:
    python benchmarks/safety_site_mutation.py                   # coverage + mutation
    python benchmarks/safety_site_mutation.py --mode coverage
    python benchmarks/safety_site_mutation.py --sites permission_isolation --max-mutants 20
"""

from __future__ import annotations

import argparse
import ast
import csv
import datetime as _dt
import json
import os
import platform
import shutil
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = REPO_ROOT / "benchmarks" / "results"
SCRATCH_DIR = RESULTS_DIR / "_mutation_scratch"

_TIMEOUT_FLOOR_S = 15.0
_TIMEOUT_FACTOR = 5.0


@dataclass(frozen=True)
class Site:
    """One safety property row of Table 6, resolved to concrete functions"""

    key: str
    prop: str
    # (module path relative to repo root, qualnames inside that module)
    targets: tuple[tuple[str, tuple[str, ...]], ...]
    tests: tuple[str, ...]
    note: str = ""


SITES: tuple[Site, ...] = (
    Site(
        key="reflex_responsiveness",
        prop="Reflex responsiveness",
        targets=(
            (
                "arachnite/nodes/instinct.py",
                (
                    "InstinctMasterNode.evaluate_reflexes",
                    "InstinctMasterNode.evaluate_reflex_pairs",
                    "InstinctMasterNode._checked_proposal",
                ),
            ),
            (
                "arachnite/runtime.py",
                ("ArachniteRuntime._launch_reflexes", "ArachniteRuntime._on_reflex_signal"),
            ),
            (
                "arachnite/nodes/base.py",
                ("FaultReportingMixin._gather_nodes", "FaultReportingMixin._gather_live"),
            ),
        ),
        tests=(
            "tests/unit/test_instinct.py",
            "tests/integration/test_framework_contracts.py",
            "tests/integration/test_reflex_arbitration_runtime.py",
            "tests/integration/test_reflex_event_lane.py",
            "tests/integration/test_shutdown_semantics.py",
            "tests/unit/test_mutation_gaps_reflex_supervisor.py",
        ),
        note="v0.13 R4: masters isolate node calls (_gather_nodes), so one "
             "node's raise or own CancelledError cannot suppress the reflex stage.",
    ),
    Site(
        key="colocation_invariant",
        prop="Co-location invariant",
        targets=(
            ("arachnite/distributed/manifest.py", ("DeploymentManifest.validate",)),
            ("arachnite/distributed/colocation.py", ("validate_colocation",)),
        ),
        tests=(
            "tests/unit/test_manifest.py",
            "tests/unit/test_permissions.py",
            "tests/unit/test_mutation_gaps_reflex_supervisor.py",
        ),
        note="DeploymentManifest.validate() enforces both the co-location "
             "invariant and permission isolation (Table 6 lists it for both), "
             "so its mutants are judged against the union of the two "
             "properties' test modules.",
    ),
    Site(
        key="mandatory_block_atomicity",
        prop="Mandatory block atomicity",
        targets=(
            (
                "arachnite/nodes/action.py",
                (
                    "MultiStepActionNode.execute",
                    "MultiStepActionNode.check_interrupt",
                    "MultiStepActionNode.request_interrupt",
                ),
            ),
        ),
        tests=(
            "tests/unit/test_action.py",
            "tests/unit/test_reflex_arbitration.py",
            "tests/unit/test_mutation_gaps_arbitration.py",
            "tests/integration/test_interrupt_policies.py",
        ),
    ),
    Site(
        key="action_totality",
        prop="Action totality",
        targets=(
            (
                "arachnite/nodes/action.py",
                (
                    "ActionMasterNode._run",
                    "ActionMasterNode._execute_with_retry",
                    "ActionMasterNode._retry_blocked",
                    "ActionMasterNode._timed_out",
                    "ActionMasterNode._on_timeout_result",
                    "ActionMasterNode.dispatch",
                    "ActionMasterNode.dispatch_many",
                    "ActionMasterNode._dispatch_one",
                    "_master_cancel_overflow",
                    "MultiStepActionNode.walk_cut_short",
                    "MultiStepActionNode._abort_timed_out",
                ),
            ),
        ),
        tests=(
            "tests/unit/test_action.py",
            "tests/unit/test_reflex_arbitration.py",
            "tests/unit/test_mutation_gaps_action_totality.py",
            "tests/integration/test_framework_contracts.py",
        ),
        note="v0.13: actions run as tasks, so execute() is wrapped by the task "
             "body _run (which turns a preemption cancel into a Result) and "
             "_execute_with_retry (timeout / exception -> Result).",
    ),
    Site(
        key="supervisor_bounded_restart",
        prop="Supervisor bounded restart",
        targets=(
            (
                "arachnite/supervisor.py",
                (
                    "NodeSupervisor.on_fault",
                    "NodeSupervisor._restart",
                    "NodeSupervisor.untrack",
                    "NodeSupervisor._schedule_restart",
                ),
            ),
        ),
        tests=(
            "tests/unit/test_supervisor.py",
            "tests/integration/test_shutdown_semantics.py",
            "tests/unit/test_mutation_gaps_reflex_supervisor.py",
        ),
    ),
    Site(
        key="permission_isolation",
        prop="Permission isolation",
        targets=(
            ("arachnite/distributed/permissions.py", ("validate_permissions",)),
        ),
        tests=("tests/unit/test_permissions.py",),
    ),
    Site(
        key="actuator_exclusivity",
        prop="Actuator exclusivity",
        targets=(
            (
                "arachnite/nodes/action.py",
                (
                    "ActionMasterNode._running_conflicts",
                    "ActionMasterNode._claim_conflicts",
                    "ActionMasterNode.conflicting_owners",
                    "ActionMasterNode.launch",
                    "ActionMasterNode.launch_reflex",
                    "ActionMasterNode._preempt",
                    "ActionMasterNode._admit_armed",
                    "ActionMasterNode._start",
                    "ActionMasterNode._release",
                    "ActionMasterNode._on_task_done",
                    "ActionMasterNode._redirect_handoffs",
                    "ActionMasterNode.cancel",
                    "ActionMasterNode.close",
                    "ActionMasterNode._drop_armed",
                    "ActionMasterNode._cancel_run",
                ),
            ),
        ),
        tests=(
            "tests/unit/test_reflex_arbitration.py",
            "tests/unit/test_mutation_gaps_arbitration.py",
            "tests/integration/test_reflex_arbitration_runtime.py",
            "tests/integration/test_runtime_monitor_state.py",
        ),
        note="ADR 0005: at most one running action owns a resource; a "
             "conflicting reflex preempts (PREEMPT) or is armed until the "
             "block ends (WAIT_FOR_BLOCK); armed reflexes claim resources.",
    ),
    Site(
        key="preemption_safe_stop",
        prop="Preemption safe-stop",
        targets=(
            (
                "arachnite/nodes/action.py",
                (
                    "_run_shielded",
                    "BaseActionNode._run_safe_stop",
                    "MultiStepActionNode.request_reflex_preempt",
                    "MultiStepActionNode._run_safe_stop",
                    "MultiStepActionNode._handle_reflex_preempt",
                    "MultiStepActionNode._rollback_completed",
                    "MultiStepActionNode._end_walk",
                    "BaseActionNode.safe_stop_bound_s",
                    "MultiStepActionNode.safe_stop_bound_s",
                    "ActionMasterNode.cancel_all",
                    "ActionMasterNode.safe_stop_budget_s",
                ),
            ),
        ),
        tests=(
            "tests/unit/test_action.py",
            "tests/unit/test_reflex_arbitration.py",
            "tests/unit/test_mutation_gaps_arbitration.py",
            "tests/integration/test_reflex_arbitration_runtime.py",
        ),
        note="The preempted owner's on_preempted() runs (bounded) before the "
             "reflex starts; rollback only with rollback_on_preempt, never "
             "restarting a walk already in progress.",
    ),
)


# --------------------------------------------------------------------------
# shared helpers
# --------------------------------------------------------------------------

def function_ranges(module_path: Path, qualnames: tuple[str, ...]) -> dict[str, tuple[int, int]]:
    """Resolve dotted qualnames to (first, last) line numbers via the AST"""
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    found: dict[str, tuple[int, int]] = {}

    def walk(node: ast.AST, prefix: str) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                qual = f"{prefix}{child.name}"
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    assert child.end_lineno is not None
                    found[qual] = (child.lineno, child.end_lineno)
                walk(child, f"{qual}.")

    walk(tree, "")
    missing = [q for q in qualnames if q not in found]
    if missing:
        raise SystemExit(
            f"{module_path}: qualname(s) not found: {', '.join(missing)} — "
            "Table 6 site definitions in this script are stale; fix SITES."
        )
    return {q: found[q] for q in qualnames}


def annotation_spans(module_path: Path) -> list[tuple[int, int, int, int]]:
    """Positions of every type annotation in the module.

    Under ``from __future__ import annotations`` (project-wide policy),
    annotations are never evaluated at runtime, so a mutation inside one
    (e.g. ``Proposal | None`` -> ``Proposal + None``) is an equivalent
    mutant by construction and must not count against the mutation score.
    Returns (start_row, start_col, end_row, end_col) spans, rows 1-based
    and cols 0-based — the same convention cosmic-ray's parso positions use.
    """
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    spans: list[tuple[int, int, int, int]] = []

    def add(node: ast.expr | None) -> None:
        if node is not None and node.end_lineno is not None:
            assert node.end_col_offset is not None
            spans.append((node.lineno, node.col_offset, node.end_lineno, node.end_col_offset))

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            add(node.returns)
            args = node.args
            for arg in (*args.posonlyargs, *args.args, *args.kwonlyargs,
                        args.vararg, args.kwarg):
                if arg is not None:
                    add(arg.annotation)
        elif isinstance(node, ast.AnnAssign):
            add(node.annotation)
    return spans


def _pos_in_spans(row: int, col: int, spans: list[tuple[int, int, int, int]]) -> bool:
    for lo_r, lo_c, hi_r, hi_c in spans:
        if (row, col) >= (lo_r, lo_c) and (row, col) < (hi_r, hi_c):
            return True
    return False


def git_describe() -> str:
    try:
        out = subprocess.run(
            ["git", "describe", "--tags", "--always", "--dirty"],
            cwd=REPO_ROOT, capture_output=True, text=True, check=True,
        )
        return out.stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def test_command(tests: tuple[str, ...]) -> str:
    # -B: never write .pyc files, so a mutant can never be judged against
    # bytecode compiled from a different (same-size, same-second) source.
    return f'"{sys.executable}" -B -m pytest -x -q ' + " ".join(tests)


# Environment for every test run the harness starts: no bytecode written, and
# UTF-8 stdout/stderr. cosmic-ray decodes the test command's output as UTF-8;
# on a Windows console code page a non-ASCII character in a failure message
# made it raise instead of recording the kill, so the mutant was counted as
# incompetent (23 such cases in one run).
_NO_BYTECODE_ENV = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "PYTHONUTF8": "1"}


def _purge_pycache() -> int:
    """
    Delete every ``__pycache__`` under the package and the tests. A .pyc is
    validated by source size and whole-second mtime, so a one-character
    mutant written within the same second as the original (or restored)
    source would otherwise run on the other version's bytecode.
    """
    removed = 0
    for root in (REPO_ROOT / "arachnite", REPO_ROOT / "tests"):
        for cache in root.rglob("__pycache__"):
            shutil.rmtree(cache, ignore_errors=True)
            removed += 1
    return removed


# --------------------------------------------------------------------------
# coverage mode
# --------------------------------------------------------------------------

def run_coverage(sites: list[Site], coverage_json: Path, skip_suite: bool) -> dict[str, object]:
    if not skip_suite:
        cmd = [
            sys.executable, "-m", "pytest", "-q",
            "--cov=arachnite", "--cov-branch",
            f"--cov-report=json:{coverage_json}",
            "tests",
        ]
        print(f"[coverage] running full suite: {' '.join(cmd)}")
        proc = subprocess.run(cmd, cwd=REPO_ROOT)
        if proc.returncode != 0:
            raise SystemExit("[coverage] test suite failed — aborting (fix tests first)")
    data = json.loads(coverage_json.read_text(encoding="utf-8"))
    files = {k.replace("\\", "/"): v for k, v in data["files"].items()}

    report: dict[str, object] = {}
    for site in sites:
        site_out: dict[str, object] = {}
        for module_rel, qualnames in site.targets:
            entry = files.get(module_rel)
            if entry is None:
                raise SystemExit(f"[coverage] {module_rel} absent from {coverage_json}")
            executed = set(entry["executed_lines"])
            missing = set(entry["missing_lines"])
            exec_br = entry.get("executed_branches", [])
            miss_br = entry.get("missing_branches", [])
            for qual, (lo, hi) in function_ranges(REPO_ROOT / module_rel, qualnames).items():
                lines_hit = sorted(n for n in executed if lo <= n <= hi)
                lines_miss = sorted(n for n in missing if lo <= n <= hi)
                br_hit = [b for b in exec_br if lo <= b[0] <= hi]
                br_miss = [b for b in miss_br if lo <= b[0] <= hi]
                n_lines = len(lines_hit) + len(lines_miss)
                n_br = len(br_hit) + len(br_miss)
                line_pct = round(100.0 * len(lines_hit) / n_lines, 1) if n_lines else None
                site_out[f"{module_rel}::{qual}"] = {
                    "lines": {"covered": len(lines_hit), "total": n_lines,
                              "pct": line_pct,
                              "missing_lines": lines_miss},
                    "branches": {"covered": len(br_hit), "total": n_br,
                                 "pct": round(100.0 * len(br_hit) / n_br, 1) if n_br else None,
                                 "missing_branches": br_miss},
                }
        report[site.key] = site_out
    return report


# --------------------------------------------------------------------------
# mutation mode
# --------------------------------------------------------------------------

def _require_clean(module_rel: str, allow_dirty: bool) -> None:
    out = subprocess.run(
        ["git", "status", "--porcelain", "--", module_rel],
        cwd=REPO_ROOT, capture_output=True, text=True,
    )
    if out.stdout.strip() and not allow_dirty:
        raise SystemExit(
            f"[mutation] {module_rel} has uncommitted changes; cosmic-ray mutates "
            "files in place. Commit/stash first, or pass --allow-dirty."
        )


def _write_config(path: Path, module_rel: str, tests: tuple[str, ...], timeout_s: float) -> None:
    path.write_text(
        "[cosmic-ray]\n"
        f'module-path = "{module_rel}"\n'
        f"timeout = {timeout_s:.1f}\n"
        "excluded-modules = []\n"
        f'test-command = \'{test_command(tests)}\'\n'
        "\n"
        "[cosmic-ray.distributor]\n"
        'name = "local"\n',
        encoding="utf-8",
    )


def _prune_session(
    session: Path,
    ranges: dict[str, tuple[int, int]],
    ann_spans: list[tuple[int, int, int, int]],
    max_mutants: int | None,
) -> tuple[int, int, int]:
    """Delete out-of-scope and annotation-equivalent work items.

    Returns (total_generated, annotation_excluded, kept).
    """
    con = sqlite3.connect(session)
    try:
        cols = {row[1] for row in con.execute("PRAGMA table_info(mutation_specs)")}
        if "start_pos_row" not in cols or "start_pos_col" not in cols:
            raise SystemExit(
                "[mutation] mutation_specs lacks start_pos_row/start_pos_col — "
                f"unsupported cosmic-ray session schema (columns: {sorted(cols)})"
            )
        (total,) = con.execute("SELECT COUNT(*) FROM work_items").fetchone()
        where = " OR ".join(f"(start_pos_row BETWEEN {lo} AND {hi})" for lo, hi in ranges.values())
        con.execute(
            "DELETE FROM work_items WHERE job_id NOT IN "
            f"(SELECT job_id FROM mutation_specs WHERE {where})"
        )
        con.execute(
            "DELETE FROM mutation_specs WHERE job_id NOT IN (SELECT job_id FROM work_items)"
        )
        # Equivalent-mutant exclusion: mutations inside type annotations are
        # runtime-dead under `from __future__ import annotations`.
        ann_jobs = [
            (job_id,)
            for job_id, row, col in con.execute(
                "SELECT job_id, start_pos_row, start_pos_col FROM mutation_specs"
            )
            if _pos_in_spans(int(row), int(col), ann_spans)
        ]
        con.executemany("DELETE FROM work_items WHERE job_id = ?", ann_jobs)
        con.executemany("DELETE FROM mutation_specs WHERE job_id = ?", ann_jobs)
        if max_mutants is not None:
            con.execute(
                "DELETE FROM work_items WHERE job_id NOT IN "
                f"(SELECT job_id FROM work_items LIMIT {int(max_mutants)})"
            )
            con.execute(
                "DELETE FROM mutation_specs WHERE job_id NOT IN (SELECT job_id FROM work_items)"
            )
        con.commit()
        (kept,) = con.execute("SELECT COUNT(*) FROM work_items").fetchone()
        return int(total), len(ann_jobs), int(kept)
    finally:
        con.close()


def _read_outcomes(session: Path, ranges: dict[str, tuple[int, int]],
                   source_lines: list[str] | None = None) -> dict[str, object]:
    def qual_of(line: int) -> str | None:
        for qual, (lo, hi) in ranges.items():
            if lo <= line <= hi:
                return qual
        return None

    con = sqlite3.connect(session)
    try:
        rows = con.execute(
            "SELECT ms.job_id, ms.operator_name, ms.start_pos_row, "
            "       wr.worker_outcome, wr.test_outcome, wr.diff "
            "FROM mutation_specs ms "
            "LEFT JOIN work_results wr ON wr.job_id = ms.job_id"
        ).fetchall()
    finally:
        con.close()

    counts = {"killed": 0, "killed_by_timeout": 0, "survived": 0, "incompetent": 0, "unexecuted": 0}
    survivors: list[dict[str, object]] = []
    for job_id, operator, line, worker, test, diff in rows:
        worker = (worker or "").upper()
        test = (test or "").upper()
        if worker == "TIMEOUT":
            counts["killed_by_timeout"] += 1
        elif test == "KILLED":
            counts["killed"] += 1
        elif test == "SURVIVED":
            counts["survived"] += 1
            src = ""
            if source_lines and 0 < int(line) <= len(source_lines):
                src = source_lines[int(line) - 1].strip()
            survivors.append({
                "job_id": job_id, "operator": operator,
                "line": line, "function": qual_of(int(line)),
                "source": src, "diff": diff or "",
            })
        elif test == "INCOMPETENT" or worker in ("EXCEPTION", "ABNORMAL"):
            counts["incompetent"] += 1
        else:
            counts["unexecuted"] += 1
    detected = counts["killed"] + counts["killed_by_timeout"]
    scoreable = detected + counts["survived"]
    return {
        **counts,
        "mutation_score_pct": round(100.0 * detected / scoreable, 1) if scoreable else None,
        "surviving_mutants": survivors,
    }


def run_mutation(sites: list[Site], max_mutants: int | None, timeout_factor: float,
                 allow_dirty: bool, keep_sessions: bool) -> dict[str, object]:
    cr = shutil.which("cosmic-ray")
    if cr is None:
        raise SystemExit(
            "[mutation] cosmic-ray executable not found — "
            'pip install cosmic-ray  (or: pip install -e ".[mutation]")'
        )
    SCRATCH_DIR.mkdir(parents=True, exist_ok=True)

    report: dict[str, object] = {}
    for site in sites:
        site_out: dict[str, object] = {}
        for module_rel, qualnames in site.targets:
            _require_clean(module_rel, allow_dirty)
            ranges = function_ranges(REPO_ROOT / module_rel, qualnames)
            slug = f"{site.key}__{Path(module_rel).stem}"
            config = SCRATCH_DIR / f"{slug}.toml"
            session = SCRATCH_DIR / f"{slug}.sqlite"
            session.unlink(missing_ok=True)

            # time the unmutated suite once: green-baseline check + timeout basis
            cmd = test_command(site.tests)
            print(f"[mutation] {slug}: baseline run: {cmd}")
            _purge_pycache()
            t0 = time.perf_counter()
            base = subprocess.run(
                cmd, cwd=REPO_ROOT, shell=True, capture_output=True, text=True,
                env=_NO_BYTECODE_ENV,
            )
            baseline_s = time.perf_counter() - t0
            if base.returncode != 0:
                raise SystemExit(
                    f"[mutation] {slug}: baseline test run FAILED — aborting.\n"
                    + base.stdout[-2000:] + base.stderr[-2000:]
                )
            timeout_s = max(_TIMEOUT_FLOOR_S, timeout_factor * baseline_s)

            _write_config(config, module_rel, site.tests, timeout_s)
            subprocess.run([cr, "init", str(config), str(session)], cwd=REPO_ROOT, check=True)
            ann_spans = annotation_spans(REPO_ROOT / module_rel)
            total, ann_excluded, kept = _prune_session(session, ranges, ann_spans, max_mutants)
            print(f"[mutation] {slug}: {total} mutants in module, {kept} in scope "
                  f"({ann_excluded} annotation-equivalent excluded; "
                  f"baseline {baseline_s:.1f}s, per-mutant timeout {timeout_s:.0f}s)")
            if kept:
                _purge_pycache()
                subprocess.run(
                    [cr, "exec", str(config), str(session)],
                    cwd=REPO_ROOT, check=True, env=_NO_BYTECODE_ENV,
                )

            source_lines = (REPO_ROOT / module_rel).read_text(encoding="utf-8").splitlines()
            outcome = _read_outcomes(session, ranges, source_lines)
            site_out[module_rel] = {
                "functions": {q: {"lines": [lo, hi]} for q, (lo, hi) in ranges.items()},
                "test_command": cmd,
                "baseline_suite_s": round(baseline_s, 2),
                "per_mutant_timeout_s": round(timeout_s, 1),
                "mutants_in_module": total,
                "annotation_equivalent_excluded": ann_excluded,
                "mutants_in_scope": kept,
                **outcome,
            }
            if not keep_sessions:
                session.unlink(missing_ok=True)
                config.unlink(missing_ok=True)
        report[site.key] = site_out
    return report


def _auto_class(source: str) -> str:
    """Hint for manual classification; the CSV's ``classification`` stays empty"""
    if "logger." in source or "self._logger" in source:
        return "log-only"
    return ""


def write_survivor_csv(mutation: dict[str, object], path: Path) -> int:
    """One row per surviving mutant, for manual classification (R2-1)"""
    rows = 0
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["site", "module", "function", "line", "operator",
                         "source", "diff", "auto_hint", "classification", "note"])
        for site_key, modules in mutation.items():
            for module_rel, mut in modules.items():  # type: ignore[attr-defined]
                for sv in mut["surviving_mutants"]:
                    writer.writerow([
                        site_key, module_rel, sv["function"], sv["line"],
                        sv["operator"], sv["source"], sv["diff"],
                        _auto_class(str(sv["source"])), "", "",
                    ])
                    rows += 1
    return rows


# --------------------------------------------------------------------------
# entry point
# --------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mode", choices=("coverage", "mutation", "both"), default="both")
    parser.add_argument("--sites", default=None,
                        help="comma-separated site keys (default: all)")
    parser.add_argument("--coverage-json", default=None,
                        help="path for/of the coverage.py JSON report")
    parser.add_argument("--skip-suite", action="store_true",
                        help="reuse an existing coverage JSON instead of re-running pytest")
    parser.add_argument("--max-mutants", type=int, default=None,
                        help="cap in-scope mutants per module (smoke testing only)")
    parser.add_argument("--timeout-factor", type=float, default=_TIMEOUT_FACTOR,
                        help="per-mutant timeout as a multiple of the baseline suite time")
    parser.add_argument("--allow-dirty", action="store_true",
                        help="run mutation mode even if a target module has uncommitted changes")
    parser.add_argument("--keep-sessions", action="store_true",
                        help="keep cosmic-ray session sqlite files for auditing")
    args = parser.parse_args()

    keys = {s.key for s in SITES}
    selected = list(SITES)
    if args.sites:
        wanted = [k.strip() for k in args.sites.split(",")]
        unknown = [k for k in wanted if k not in keys]
        if unknown:
            raise SystemExit(f"unknown site key(s): {', '.join(unknown)} "
                             f"(valid: {', '.join(sorted(keys))})")
        selected = [s for s in SITES if s.key in wanted]

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    coverage_json = (Path(args.coverage_json) if args.coverage_json
                     else SCRATCH_DIR / "coverage.json")
    coverage_json.parent.mkdir(parents=True, exist_ok=True)

    doc: dict[str, object] = {
        "benchmark": "safety_site_mutation",
        "schema_version": 1,
        "generated_utc": _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds"),
        "platform": f"{sys.platform} / CPython {platform.python_version()}",
        "hostname": platform.node(),
        "git_describe": git_describe(),
        "sites": {s.key: {"property": s.prop,
                          "targets": {m: list(q) for m, q in s.targets},
                          "tests": list(s.tests),
                          "note": s.note} for s in selected},
    }
    if args.mode in ("coverage", "both"):
        doc["coverage"] = run_coverage(selected, coverage_json, args.skip_suite)
    if args.mode in ("mutation", "both"):
        doc["mutation"] = run_mutation(selected, args.max_mutants, args.timeout_factor,
                                       args.allow_dirty, args.keep_sessions)

    stamp = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = RESULTS_DIR / f"safety_site_mutation_{stamp}.json"
    out_file.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
    if "mutation" in doc:
        csv_file = RESULTS_DIR / f"safety_site_survivors_{stamp}.csv"
        n_rows = write_survivor_csv(doc["mutation"], csv_file)  # type: ignore[arg-type]
        print(f"[mutation] {n_rows} surviving mutants written to {csv_file}")

    # human summary
    print("\n=== safety-site summary ===")
    for site in selected:
        line = f"{site.prop:<28}"
        if "coverage" in doc:
            for target, cov in doc["coverage"][site.key].items():  # type: ignore[index]
                lp = cov["lines"]["pct"]
                bp = cov["branches"]["pct"]
                bp_txt = "n/a" if bp is None else f"{bp}%"
                print(f"{line} {target}: lines {lp}%  branches {bp_txt}")
                line = " " * 28
        if "mutation" in doc:
            for module_rel, mut in doc["mutation"][site.key].items():  # type: ignore[index]
                print(f"{'':28} {module_rel}: score {mut['mutation_score_pct']}% "
                      f"({mut['killed']}+{mut['killed_by_timeout']}t killed / "
                      f"{mut['survived']} survived / {mut['incompetent']} incompetent "
                      f"of {mut['mutants_in_scope']})")
    print(f"\nResults saved to {out_file}")


if __name__ == "__main__":
    main()
