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
VM_METRICS_GLOBS = (
    "tasks/*/sandbox-metrics.json",
    "ratio-*/repeat-*/tasks/*/sandbox-metrics.json",
    "*/ratio-*/repeat-*/tasks/*/sandbox-metrics.json",
)
GB = 1024.0 ** 3


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


def load_vm_mem(path: Path) -> dict | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"  WARN: unreadable {path}: {exc}", file=sys.stderr)
        return None
    samples = data.get("samples") or []
    pts = [(s["timestampUnix"], s["memUsed"], s["memCache"])
           for s in samples
           if isinstance(s.get("timestampUnix"), (int, float))
           and isinstance(s.get("memUsed"), (int, float))
           and isinstance(s.get("memCache"), (int, float))]
    if not pts:
        return None
    return {
        "task": path.parent.name,
        "sandbox_id": data.get("sandbox_id") or "",
        "points": pts,
    }


def vm_mem_section(vm_mems: list[dict], out_dir: Path, per_vm: bool = False) -> None:
    rows = []
    for vm in vm_mems:
        used = [p[1] / GB for p in vm["points"]]
        cache = [p[2] / GB for p in vm["points"]]
        rows.append({
            "trial": vm.get("trial", ""),
            "task": vm["task"],
            "sandbox_id": vm["sandbox_id"],
            "samples": len(vm["points"]),
            "mem_used_avg_gb": round(sum(used) / len(used), 3),
            "mem_used_peak_gb": round(max(used), 3),
            "mem_cache_avg_gb": round(sum(cache) / len(cache), 3),
            "mem_cache_peak_gb": round(max(cache), 3),
        })
    csv_path = out_dir / "vm_mem_summary.csv"
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "trial", "task", "sandbox_id", "samples",
            "mem_used_avg_gb", "mem_used_peak_gb", "mem_cache_avg_gb", "mem_cache_peak_gb"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"  vm mem: {len(vm_mems)} VMs, wrote: {csv_path}")

    t0 = min(p[0] for vm in vm_mems for p in vm["points"])
    fig, (ax_used, ax_cache) = plt.subplots(2, 1, figsize=(14, 8), sharex=True)
    for vm in vm_mems:
        xs = [p[0] - t0 for p in vm["points"]]
        ax_used.plot(xs, [p[1] / GB for p in vm["points"]], linewidth=0.9, alpha=0.65)
        ax_cache.plot(xs, [p[2] / GB for p in vm["points"]], linewidth=0.9, alpha=0.65)
    ax_used.set_ylabel("VM memUsed (GB)")
    ax_used.set_title(f"Per-VM guest memory over time ({len(vm_mems)} VMs, server-sampled)")
    ax_used.grid(alpha=0.3)
    ax_cache.set_ylabel("VM memCache (GB)")
    ax_cache.set_xlabel("elapsed since first sample (s)")
    ax_cache.grid(alpha=0.3)
    fig.tight_layout()
    png_path = out_dir / "vm_mem_curves.png"
    fig.savefig(png_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote: {png_path}")

    if per_vm:
        plots_dir = out_dir / "vm_mem_plots"
        plots_dir.mkdir(parents=True, exist_ok=True)
        for vm in vm_mems:
            xs = [p[0] - vm["points"][0][0] for p in vm["points"]]
            fig, (ax_used, ax_cache) = plt.subplots(2, 1, figsize=(10, 6), sharex=True)
            ax_used.plot(xs, [p[1] / GB for p in vm["points"]], linewidth=1.2)
            ax_used.set_ylabel("memUsed (GB)")
            ax_used.set_title(
                f"VM {vm['task']} sandbox={vm['sandbox_id'] or 'n/a'}"
                + (f" trial={vm.get('trial', '')}" if vm.get("trial", ".") != "." else ""))
            ax_used.grid(alpha=0.3)
            ax_cache.plot(xs, [p[2] / GB for p in vm["points"]], linewidth=1.2, color="tab:orange")
            ax_cache.set_ylabel("memCache (GB)")
            ax_cache.set_xlabel("elapsed since VM first sample (s)")
            ax_cache.grid(alpha=0.3)
            fig.tight_layout()
            trial = vm.get("trial", ".")
            prefix = trial.replace("/", "_") + "_" if trial not in (".", "") else ""
            fig.savefig(plots_dir / f"vm_mem_{prefix}{vm['task']}.png", dpi=130, bbox_inches="tight")
            plt.close(fig)
        print(f"  wrote {len(vm_mems)} per-VM PNGs: {plots_dir}/")


def process_root(root: Path, out_dir: Path, per_vm: bool = False) -> None:
    task_files = discover(root, TASK_GLOBS)
    vms = [vm for f in task_files if (vm := load_vm(f)) is not None]
    out_dir.mkdir(parents=True, exist_ok=True)
    if vms:
        print(f"[{root}] {len(vms)} VM task results")
        exec_summary(vms, out_dir)
    else:
        print(f"[{root}] no task results; plotting metrics curves only")

    metrics_files = discover(root, METRICS_GLOBS)
    if not metrics_files:
        print("  no metrics_1s.jsonl found, skipping plots")
    else:
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

    vm_mem_files = discover(root, VM_METRICS_GLOBS)
    vm_mems = []
    for f in vm_mem_files:
        vm = load_vm_mem(f)
        if vm is not None:
            vm["trial"] = task_label(f, root)
            vm_mems.append(vm)
    if vm_mems:
        vm_mem_section(vm_mems, out_dir, per_vm=per_vm)
    else:
        print("  no per-VM sandbox-metrics.json found, skipping vm mem curves")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("roots", nargs="+", type=Path, help="run output dir or bench experiment root")
    parser.add_argument("--out", type=Path, default=None, help="output dir (default: <root>/processed)")
    parser.add_argument("--per-vm", action="store_true",
                        help="also write one PNG per VM under <out>/vm_mem_plots/")
    args = parser.parse_args()
    for root in args.roots:
        root = root.expanduser().resolve()
        if not root.is_dir():
            print(f"ERROR: {root} is not a directory", file=sys.stderr)
            return 1
        out_dir = (args.out or root / "processed").expanduser().resolve()
        process_root(root, out_dir, per_vm=args.per_vm)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
