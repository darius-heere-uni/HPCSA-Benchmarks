#!/usr/bin/env python3
"""
Aggregate Benchmark 1 (tiny relay latency) results from both stacks.

Expected tree:
<results_root>/
  k8s/
    bench-b1-k8s-r01/
      part_00/
      part_01/
    ...
  ksi/
    bench-b1-ksi-r01/
      part_00/
      part_01/
    ...

Reads:
- part_summary.json          -> partition-level summary
- job_latency.jsonl         -> per-Job warm latency
- out_*.json                -> in-container function timing

Writes:
- benchmark1_run_summary.csv      (main file; one row per run)
- benchmark1_part_summary.csv     (one row per partition)
- benchmark1_job_latencies.csv    (one row per invocation/job)
- benchmark1_function_times.csv   (one row per invocation/function body)
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from pathlib import Path
from statistics import mean
from typing import Any, Dict, Iterable, List, Optional, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aggregate Benchmark 1 results.")
    parser.add_argument(
        "--results-root",
        default="/nfs/bench/faas_tiny_relay_v1/results",
        help="Root directory containing k8s/ and ksi/ result folders.",
    )
    parser.add_argument(
        "--out-dir",
        default="/nfs/bench/faas_tiny_relay_v1/aggregated",
        help="Directory where CSV outputs will be written.",
    )
    return parser.parse_args()


def read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSONL in {path} at line {line_no}: {e}") from e
    return rows


def safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, str):
        if value.strip().lower() in {"", "null", "none", "nan"}:
            return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def percentile(values: List[float], p: float) -> Optional[float]:
    if not values:
        return None
    if p <= 0:
        return min(values)
    if p >= 100:
        return max(values)
    xs = sorted(values)
    k = (len(xs) - 1) * (p / 100.0)
    lo = math.floor(k)
    hi = math.ceil(k)
    if lo == hi:
        return xs[lo]
    frac = k - lo
    return xs[lo] * (1 - frac) + xs[hi] * frac


def stats(values: Iterable[Any], prefix: str) -> Dict[str, Any]:
    xs = [safe_float(v) for v in values]
    xs = [x for x in xs if x is not None]
    if not xs:
        return {
            f"{prefix}_count": 0,
            f"{prefix}_min": None,
            f"{prefix}_max": None,
            f"{prefix}_mean": None,
            f"{prefix}_p50": None,
            f"{prefix}_p95": None,
        }
    return {
        f"{prefix}_count": len(xs),
        f"{prefix}_min": min(xs),
        f"{prefix}_max": max(xs),
        f"{prefix}_mean": mean(xs),
        f"{prefix}_p50": percentile(xs, 50),
        f"{prefix}_p95": percentile(xs, 95),
    }


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        with path.open("w", newline="", encoding="utf-8") as f:
            f.write("")
        return

    # Stable column order: union of all keys in first-seen order.
    fieldnames: List[str] = []
    seen = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def collect_rows(results_root: Path) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    part_rows: List[Dict[str, Any]] = []
    job_rows: List[Dict[str, Any]] = []
    func_rows: List[Dict[str, Any]] = []

    if not results_root.exists():
        raise FileNotFoundError(f"Results root does not exist: {results_root}")

    for stack_dir in sorted(p for p in results_root.iterdir() if p.is_dir()):
        stack = stack_dir.name

        for run_dir in sorted(p for p in stack_dir.iterdir() if p.is_dir()):
            run_id = run_dir.name

            for part_dir in sorted(p for p in run_dir.iterdir() if p.is_dir() and p.name.startswith("part_")):
                summary_path = part_dir / "part_summary.json"
                if summary_path.exists():
                    row = read_json(summary_path)
                    row.setdefault("stack", stack)
                    row.setdefault("run_id", run_id)
                    row["part_dir"] = str(part_dir)
                    part_rows.append(row)

                job_path = part_dir / "job_latency.jsonl"
                for row in read_jsonl(job_path):
                    row.setdefault("stack", stack)
                    row.setdefault("run_id", run_id)
                    row["part_dir"] = str(part_dir)
                    job_rows.append(row)

                for out_path in sorted(part_dir.glob("out_*.json")):
                    row = read_json(out_path)
                    row.setdefault("stack", stack)
                    row.setdefault("run_id", run_id)
                    row["part_dir"] = str(part_dir)
                    row["result_file"] = str(out_path)
                    func_rows.append(row)

    return part_rows, job_rows, func_rows


def build_run_summary(
    part_rows: List[Dict[str, Any]],
    job_rows: List[Dict[str, Any]],
    func_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    parts_by_run: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    jobs_by_run: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    funcs_by_run: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)

    for row in part_rows:
        parts_by_run[(row.get("stack"), row.get("run_id"))].append(row)
    for row in job_rows:
        jobs_by_run[(row.get("stack"), row.get("run_id"))].append(row)
    for row in func_rows:
        funcs_by_run[(row.get("stack"), row.get("run_id"))].append(row)

    keys = sorted(set(parts_by_run) | set(jobs_by_run) | set(funcs_by_run))
    run_rows: List[Dict[str, Any]] = []

    for stack, run_id in keys:
        parts = sorted(parts_by_run.get((stack, run_id), []), key=lambda r: int(r.get("partition_id", 999)))
        jobs = jobs_by_run.get((stack, run_id), [])
        funcs = funcs_by_run.get((stack, run_id), [])

        row: Dict[str, Any] = {
            "benchmark": "faas_tiny_relay_v1",
            "stack": stack,
            "run_id": run_id,
            "parts_present": len(parts),
            "total_assigned_invocations": sum(int(r.get("assigned_invocations", 0) or 0) for r in parts),
            "total_ok": sum(int(r.get("ok", 0) or 0) for r in parts),
            "total_failed": sum(int(r.get("failed", 0) or 0) for r in parts),
        }

        startup_vals = [safe_float(r.get("startup_setup_ms")) for r in parts]
        startup_vals = [x for x in startup_vals if x is not None]
        warm_vals = [safe_float(r.get("warm_makespan_ms")) for r in parts]
        warm_vals = [x for x in warm_vals if x is not None]

        row["platform_startup_setup_ms"] = max(startup_vals) if startup_vals else None
        row["cluster_warm_makespan_ms"] = max(warm_vals) if warm_vals else None

        for part in parts:
            pid = int(part.get("partition_id"))
            row[f"part_{pid:02d}_node_name"] = part.get("node_name")
            row[f"part_{pid:02d}_assigned_invocations"] = part.get("assigned_invocations")
            row[f"part_{pid:02d}_ok"] = part.get("ok")
            row[f"part_{pid:02d}_failed"] = part.get("failed")
            row[f"part_{pid:02d}_startup_setup_ms"] = safe_float(part.get("startup_setup_ms"))
            row[f"part_{pid:02d}_warm_makespan_ms"] = safe_float(part.get("warm_makespan_ms"))

        row.update(stats((r.get("job_wall_ms") for r in jobs), "job_wall_ms"))
        row.update(stats((r.get("duration_ms") for r in funcs), "function_duration_ms"))

        # Sanity checks.
        job_invocations = {int(r["invocation_id"]) for r in jobs if r.get("invocation_id") is not None}
        func_invocations = {int(r["invocation_id"]) for r in funcs if r.get("invocation_id") is not None}
        row["job_invocation_rows"] = len(job_invocations)
        row["function_invocation_rows"] = len(func_invocations)
        row["job_function_invocation_match"] = (job_invocations == func_invocations) if jobs or funcs else None

        run_rows.append(row)

    return run_rows


def main() -> None:
    args = parse_args()
    results_root = Path(args.results_root)
    out_dir = Path(args.out_dir)

    part_rows, job_rows, func_rows = collect_rows(results_root)
    run_rows = build_run_summary(part_rows, job_rows, func_rows)

    write_csv(out_dir / "benchmark1_run_summary.csv", run_rows)
    write_csv(out_dir / "benchmark1_part_summary.csv", part_rows)
    write_csv(out_dir / "benchmark1_job_latencies.csv", job_rows)
    write_csv(out_dir / "benchmark1_function_times.csv", func_rows)

    print(f"Wrote {len(run_rows)} rows to {out_dir / 'benchmark1_run_summary.csv'}")
    print(f"Wrote {len(part_rows)} rows to {out_dir / 'benchmark1_part_summary.csv'}")
    print(f"Wrote {len(job_rows)} rows to {out_dir / 'benchmark1_job_latencies.csv'}")
    print(f"Wrote {len(func_rows)} rows to {out_dir / 'benchmark1_function_times.csv'}")


if __name__ == "__main__":
    main()
