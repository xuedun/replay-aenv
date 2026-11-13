#!/usr/bin/env python3
"""Summarize agent-benchmark (agent_replay suite) run outputs.

Usage:
    python scripts/summarize_agent_benchmark.py <results-dir> [<results-dir> ...] [--out DIR]

Computes pooled avg/P50/P95/P99 of per-VM execution time (sum of per-step
command_sec, the actual command execution cost) and wall time from
`tasks/<task>/replay-result.json` (run layout or `ratio-*/repeat-*` trial
layout). No per-VM rows: writes exec_summary.csv with one row per statistic.

Also plots host CPU and memory utilization over time from
`observability/metrics_1s.jsonl` as two subplots in one PNG per metrics file.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

STATS = ("avg", 50, 95, 99)
TASK_GLOBS = (
    "tasks/*/replay-result.json",
    "ratio-*/repeat-*/tasks/*/replay-result.json",
    "*/ratio-*/repeat-*/tasks/*/replay-result.json",
)
METRICS_GLOBS = (
    "observability/metrics_1s.jsonl",
    "ratio-*/repeat-*/observability/metrics_1s.jsonl",
    "*/ratio-*/repeat-*/observability/metrics_1s.jsonl",
)


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    pos = (len(ordered) - 1) * q / 100.0
    lo = int(pos)
    hi = min(lo + 1, len(ordered) - 1)
    frac = pos - lo
    return ordered[lo] * (1.0 - frac) + ordered[hi] * frac


def stat_value(values: list[float], stat) -> float | None:
    if not values:
        return None
    if stat == "avg":
        return sum(values) / len(values)
    return percentile(values, stat)


def discover(root: Path, globs: tuple[str, ...]) -> list[Path]:
    seen: set[Path] = set()
    out: list[Path] = []
    for pattern in globs:
        for path in sorted(root.glob(pattern)):
            if path not in seen:
                seen.add(path)
                out.append(path)
    return out


def task_label(path: Path, root: Path) -> str:
    try:
        rel = path.parent.relative_to(root)
    except ValueError:
        return "."
    parts = rel.parts[:-2] if len(rel.parts) >= 2 else ()
    return "/".join(parts) or "."


def load_vm(path: Path) -> dict | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"  WARN: unreadable {path}: {exc}", file=sys.stderr)
        return None
    steps = data.get("steps") or []
    return {
        "exec_sec": round(sum(s.get("command_sec") or 0.0 for s in steps), 4),
        "wall_sec": data.get("elapsed_sec"),
        "steps": len(steps),
        "succeeded": bool(data.get("succeeded")),
        "trajectory": Path(str(data.get("trajectory_path") or "unknown")).name,
    }


def exec_summary(vms: list[dict], out_dir: Path) -> None:
    exec_vals = [v["exec_sec"] for v in vms if isinstance(v.get("exec_sec"), (int, float))]
    wall_vals = [v["wall_sec"] for v in vms if isinstance(v.get("wall_sec"), (int, float))]
    step_vals = [float(v["steps"]) for v in vms if v.get("steps")]
    rows = []
    for stat in STATS:
        name = stat if isinstance(stat, str) else f"P{stat}"
        row = {"stat": name, "vms": len(vms)}
        for label, vals in (("exec_sec", exec_vals), ("wall_sec", wall_vals), ("steps", step_vals)):
            v = stat_value(vals, stat)
            row[label] = round(v, 4) if v is not None else ""
        row["succeeded"] = f"{sum(1 for v in vms if v['succeeded'])}/{len(vms)}"
        rows.append(row)
    path = out_dir / "exec_summary.csv"
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["stat", "vms", "exec_sec", "wall_sec", "steps", "succeeded"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"  exec stats ({len(vms)} VMs): " + " | ".join(
        f"{r['stat']} exec={r['exec_sec']}s wall={r['wall_sec']}s" for r in rows))
    print(f"  wrote: {path}")


def parse_elapsed(records: list[dict]) -> list[float]:
    times: list[datetime] = []
    for rec in records:
        ts = rec.get("ts")
        if not ts:
            continue
        try:
            times.append(datetime.fromisoformat(ts))
        except ValueError:
            continue
    if not times:
        return []
    t0 = times[0]
    return [(t - t0).total_seconds() for t in times]


def metrics_plot(records: list[dict], label: str, out_path: Path, n_vms: int) -> bool:
    if not records:
        return False
    xs = parse_elapsed(records)
    if not xs:
        print(f"  WARN: no parseable ts in {label}", file=sys.stderr)
        return False

    def series(key: str) -> tuple[list[float], list[float]]:
        pts = [(x, rec[key]) for x, rec in zip(xs, records)
               if isinstance(rec.get(key), (int, float))]
        return ([p[0] for p in pts], [p[1] for p in pts])

    fig, (ax_cpu, ax_mem) = plt.subplots(2, 1, figsize=(14, 8), sharex=True)

    for key, label_txt in (("cpu_pct", "cpu_pct"), ("cpu_iowait_pct", "iowait_pct")):
        xs_v, ys_v = series(key)
        if ys_v:
            ax_cpu.plot(xs_v, ys_v, linewidth=1.2, label=label_txt)
    ax_cpu.set_ylabel("CPU (%)")
    ax_cpu.set_ylim(0, 105)
    ax_cpu.set_title(f"Host CPU & memory utilization - {label}" + (f" ({n_vms} VMs)" if n_vms else ""))
    ax_cpu.legend(loc="upper right")
    ax_cpu.grid(alpha=0.3)

    for key, label_txt in (("mem_used_gb", "mem_used_gb"), ("fc_rss_gb", "fc_rss_gb"),
                           ("mem_cached_gb", "mem_cached_gb")):
        xs_v, ys_v = series(key)
        if ys_v:
            ax_mem.plot(xs_v, ys_v, linewidth=1.2, label=label_txt)
    ax_mem.set_ylabel("Memory (GB)")
    ax_mem.set_xlabel("elapsed (s)")
    ax_mem.legend(loc="upper right")
    ax_mem.grid(alpha=0.3)

    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return True


def process_root(root: Path, out_dir: Path) -> None:
    task_files = discover(root, TASK_GLOBS)
    vms = [vm for f in task_files if (vm := load_vm(f)) is not None]
    if not vms:
        print(f"[{root}] no task results found, skipping")
        return
    print(f"[{root}] {len(vms)} VM task results")
    out_dir.mkdir(parents=True, exist_ok=True)
    exec_summary(vms, out_dir)

    metrics_files = discover(root, METRICS_GLOBS)
    if not metrics_files:
        print("  no metrics_1s.jsonl found, skipping plots")
        return
    plotted = 0
    for mf in metrics_files:
        try:
            records = [json.loads(line) for line in mf.read_text(encoding="utf-8").splitlines() if line.strip()]
        except (OSError, json.JSONDecodeError) as exc:
            print(f"  WARN: unreadable {mf}: {exc}", file=sys.stderr)
            continue
        label = task_label(mf, root)
        safe = label.replace("/", "_") if label not in (".", "") else "run"
        png = out_dir / f"metrics_{safe}.png"
        if metrics_plot(records, label, png, len(vms)):
            plotted += 1
            print(f"  wrote: {png} ({len(records)} samples)")
    if plotted == 0:
        print("  WARN: no metrics plots produced")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("roots", nargs="+", type=Path, help="run output dir or bench experiment root")
    parser.add_argument("--out", type=Path, default=None, help="output dir (default: <root>/processed)")
    args = parser.parse_args()
    for root in args.roots:
        root = root.expanduser().resolve()
        if not root.is_dir():
            print(f"ERROR: {root} is not a directory", file=sys.stderr)
            return 1
        out_dir = (args.out or root / "processed").expanduser().resolve()
        process_root(root, out_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
