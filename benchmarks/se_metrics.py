"""
benchmarks/se_metrics.py
~~~~~~~~~~~~~~~~~~~~~~~~
Software-engineering metrics for the four task encodings (revision
experiment E8).

Compares the same pick-and-place task across Arachnite, py_trees, Jason
and ROS 2 BT on static, tool-measurable dimensions:

  loc                 non-blank, non-comment lines (baselines.compare.count_loc,
                      identical to the paper's existing LoC table)
  cyclomatic          per-function cyclomatic complexity via ``lizard``
                      (sum / mean / max / function count). lizard parses the
                      Python and Java sources; AgentSpeak (.asl), YAML and XML
                      files carry no lizard-measurable functions and are
                      reported in ``unanalyzed_files``.
  cognitive           cognitive complexity via ``complexipy`` (Python files
                      only — no tooling exists for Java or AgentSpeak; those
                      frameworks report null and the paper table footnotes it).
  api_symbols         distinct framework identifiers imported by the task
                      encoding — an auditable proxy for "concepts a developer
                      must know". Counted from ``import``/``from`` statements
                      (Python, filtered to the framework's own packages) and
                      ``import`` statements (Java, filtered to ``jason.*``).
                      AgentSpeak has no imports; the Jason count is therefore a
                      lower bound and the paper says so.

Deliberately NOT here: any runtime measurement (that is compare.py's job) and
any subjective "concept count" — the automated ``api_symbols`` metric replaces
it with something a reviewer can recompute.

Requires: pip install lizard complexipy
Usage:    python benchmarks/se_metrics.py [--output-dir benchmarks/results]
"""

from __future__ import annotations

import argparse
import json
import platform
import re
import time
from pathlib import Path
from typing import Any

BASE = Path(__file__).resolve().parent.parent

# Framework package prefixes whose imports count as framework API surface.
_API_PREFIXES: dict[str, tuple[str, ...]] = {
    "Arachnite": ("arachnite",),
    "py_trees": ("py_trees",),
    "Jason": ("jason",),
    "ROS 2 BT": ("rclpy", "py_trees", "std_msgs", "geometry_msgs", "rcl_interfaces"),
}

_JAVA_IMPORT_RE = re.compile(r"^\s*import\s+([\w.]+(?:\.\*)?)\s*;")


def _framework_files(name: str) -> list[Path]:
    from baselines.compare import FRAMEWORK_FILES
    return [BASE / rel for rel in FRAMEWORK_FILES[name]]


def _py_symbols(source: str, prefixes: tuple[str, ...]) -> set[str]:
    """Framework symbols in one Python source: from-imports resolved via ast,
    module-style imports counted by their distinct attribute chains in use."""
    import ast
    symbols: set[str] = set()
    module_imports: set[str] = set()
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            if node.module.split(".")[0] in prefixes:
                for alias in node.names:
                    symbols.add(f"{node.module}.{alias.name}")
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split(".")[0] in prefixes:
                    module_imports.add(alias.asname or alias.name)
    # For `import py_trees`-style usage, each distinct attribute chain the
    # encoding touches is one API surface item (py_trees.composites.Selector).
    for mod in module_imports:
        for m in re.finditer(rf"\b{re.escape(mod)}(?:\.\w+)+", source):
            symbols.add(m.group(0))
    return symbols


def _api_symbols(name: str, files: list[Path]) -> dict[str, Any]:
    """Distinct framework identifiers referenced by the task encoding."""
    prefixes = _API_PREFIXES[name]
    symbols: set[str] = set()
    for path in files:
        if not path.exists() or path.suffix not in (".py", ".java"):
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        if path.suffix == ".py":
            symbols |= _py_symbols(text, prefixes)
        else:
            for line in text.splitlines():
                m = _JAVA_IMPORT_RE.match(line)
                if m and m.group(1).split(".")[0] in prefixes:
                    symbols.add(m.group(1))
    return {"count": len(symbols), "symbols": sorted(symbols)}


def _cyclomatic(files: list[Path]) -> dict[str, Any]:
    """Aggregate per-function cyclomatic complexity via lizard."""
    import lizard
    ccns: list[int] = []
    nloc = 0
    unanalyzed: list[str] = []
    for path in files:
        if not path.exists():
            continue
        if path.suffix not in (".py", ".java"):
            unanalyzed.append(path.name)
            continue
        analysis = lizard.analyze_file(str(path))
        ccns.extend(f.cyclomatic_complexity for f in analysis.function_list)
        nloc += analysis.nloc
    return {
        "functions": len(ccns),
        "nloc": nloc,
        "ccn_sum": sum(ccns),
        "ccn_mean": round(sum(ccns) / len(ccns), 2) if ccns else None,
        "ccn_max": max(ccns) if ccns else None,
        "unanalyzed_files": unanalyzed,
    }


def _cognitive(files: list[Path]) -> dict[str, Any] | None:
    """Aggregate cognitive complexity via complexipy (Python files only)."""
    try:
        from complexipy import file_complexity
    except ImportError:
        return None
    scores: list[int] = []
    for path in files:
        if path.suffix != ".py" or not path.exists():
            continue
        result = file_complexity(str(path))
        scores.extend(f.complexity for f in result.functions)
    if not scores:
        return None
    return {
        "functions": len(scores),
        "cognitive_sum": sum(scores),
        "cognitive_mean": round(sum(scores) / len(scores), 2),
        "cognitive_max": max(scores),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=str, default="benchmarks/results")
    args = parser.parse_args()

    from baselines.compare import FRAMEWORK_FILES, count_loc

    report: dict[str, Any] = {
        "benchmark": "se_metrics",
        "platform": f"{platform.system()} / CPython {platform.python_version()}",
        "frameworks": {},
    }
    print("\n  SE metrics — same task, four frameworks (E8)")
    print("  " + "-" * 72)
    header = (
        f"  {'framework':12s} {'LoC':>5s} {'funcs':>6s} {'CCN sum':>8s}"
        f" {'CCN mean':>9s} {'CCN max':>8s} {'cog sum':>8s} {'API syms':>9s}"
    )
    print(header)
    for name in FRAMEWORK_FILES:
        files = _framework_files(name)
        loc = count_loc([str(p) for p in files])
        cyc = _cyclomatic(files)
        cog = _cognitive(files)
        api = _api_symbols(name, files)
        report["frameworks"][name] = {
            "files": [p.name for p in files],
            "loc": loc,
            "cyclomatic": cyc,
            "cognitive": cog,
            "api_symbols": api,
        }
        cog_sum = cog["cognitive_sum"] if cog else "-"
        print(
            f"  {name:12s} {loc:5d} {cyc['functions']:6d} {cyc['ccn_sum']:8d}"
            f" {cyc['ccn_mean'] if cyc['ccn_mean'] is not None else '-':>9}"
            f" {cyc['ccn_max'] if cyc['ccn_max'] is not None else '-':>8}"
            f" {cog_sum:>8} {api['count']:9d}"
        )

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    out_file = out_dir / f"se_metrics_{stamp}.json"
    out_file.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"\n  Results saved to {out_file}")


if __name__ == "__main__":
    main()
