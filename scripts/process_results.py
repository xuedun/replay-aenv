#!/usr/bin/env python3
"""Process benchmark results: per-VM timing CSV + cgroup CPU/mem plots.

Usage:
  python3 process_results.py <results_dir> <output_dir>

  results_dir  - runs/pause-resume-oversub-N8-xxx/ (from benchmark driver)
  output_dir   - where to write CSVs and PNGs

Outputs:
  output_dir/
  ├── vm_timings/
  │   ├── vm_0001.csv          # per-action: wait, exec, pause, resume
  │   ├── vm_0002.csv
  │   └── ...
  ├── vm_summary.csv           # per-VM averages
  └── observability/
      ├── cgroup_cpu_mem_ratio01_repeat01.png
      └── cgroup_cpu_mem_ratio02_repeat01.png
"""
import csv
import json
import os
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


# ── Part 1: Per-VM timing CSV ───────────────────────────────


def find_replay_results(results_dir: Path) -> list[Path]:
    """Find all replay-result.json files under results_dir."""
    return sorted(results_dir.rglob("replay-result.json"))


def detect_scenario(result_json: dict) -> str:
    """Detect whether a replay-result is pause-resume or no-lifecycle (COW)."""
    steps = result_json.get("steps", [])
    if not steps:
        return "unknown"
    first = steps[0]
    if "paused_sec" in first or "pause_sec" in first:
        return "pause-resume"
    if first.get("sandbox_lifecycle_enabled") is False:
        return "cow"
    if "pause_sec" in first or "resume_sec" in first:
        return "pause-resume"
    return "cow"


def extract_step_timings(result_json: dict) -> tuple[list[dict], str]:
    """Extract per-step timing from a replay-result.json.

    Returns (rows, scenario) where scenario is 'pause-resume' or 'cow'.
    """
    steps = result_json.get("steps", [])
    scenario = detect_scenario(result_json)
    rows = []
    for i, step in enumerate(steps, 1):
        if scenario == "pause-resume":
            wait_sec = step.get("paused_sec", 0.0)
            pause_sec = step.get("pause_sec", 0.0)
            resume_sec = step.get("resume_total_sec", step.get("resume_sec", 0.0))
            post_pause_sec = step.get("post_command_pause_sec", 0.0)
            capacity_wait = step.get("capacity_wait_sec", 0.0)
            run_queue_wait = step.get("run_queue_wait_sec", 0.0)
        else:
            # COW: no pause/resume, "wait" is the delay between actions
            wait_sec = step.get("delay_wait_sec", 0.0)
            pause_sec = 0.0
            resume_sec = 0.0
            post_pause_sec = 0.0
            capacity_wait = 0.0
            run_queue_wait = 0.0

        rows.append({
            "action": i,
            "scenario": scenario,
            "wait_sec": round(wait_sec, 4),
            "exec_sec": round(step.get("command_sec", step.get("elapsed_sec", 0.0)), 4),
            "pause_sec": round(pause_sec, 4),
            "resume_sec": round(resume_sec, 4),
            "delay_wait_sec": round(step.get("delay_wait_sec", 0.0), 4),
            "capacity_wait_sec": round(capacity_wait, 4),
            "run_queue_wait_sec": round(run_queue_wait, 4),
            "interaction_total_sec": round(step.get("interaction_total_sec", 0.0), 4),
            "post_pause_sec": round(post_pause_sec, 4),
            "succeeded": step.get("succeeded", False),
            "exit_code": step.get("exit_code", ""),
        })
    return rows, scenario


def process_vm_timings(results_dir: Path, output_dir: Path):
    """Generate per-VM CSV files and summary CSV."""
    vm_dir = output_dir / "vm_timings"
    vm_dir.mkdir(parents=True, exist_ok=True)

    result_files = find_replay_results(results_dir)
    if not result_files:
        print(f"[WARN] No replay-result.json found under {results_dir}")
        return

    print(f"[vm_timings] Found {len(result_files)} replay results")

    timing_fields = [
        "action", "scenario", "wait_sec", "exec_sec", "pause_sec", "resume_sec",
        "delay_wait_sec", "capacity_wait_sec", "run_queue_wait_sec",
        "interaction_total_sec", "post_pause_sec", "succeeded", "exit_code",
    ]

    summary_rows = []
    for idx, result_path in enumerate(result_files, 1):
        try:
            result = json.loads(result_path.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"  [WARN] Cannot parse {result_path}: {e}")
            continue

        steps, scenario = extract_step_timings(result)
        if not steps:
            continue

        vm_name = f"vm_{idx:04d}"
        # Try to get a meaningful name from the directory
        parent_dir = result_path.parent.name
        vm_label = parent_dir if parent_dir else vm_name

        csv_path = vm_dir / f"{vm_name}.csv"
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=timing_fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(steps)

        # Compute averages
        n = len(steps)
        avg_wait = sum(s["wait_sec"] for s in steps) / n if n else 0
        avg_exec = sum(s["exec_sec"] for s in steps) / n if n else 0
        avg_pause = sum(s["pause_sec"] for s in steps) / n if n else 0
        avg_resume = sum(s["resume_sec"] for s in steps) / n if n else 0
        avg_total = sum(s["interaction_total_sec"] for s in steps) / n if n else 0

        sandbox_id = result.get("sandbox_id", "")
        succeeded = result.get("succeeded", False)

        summary_rows.append({
            "vm": vm_name,
            "label": vm_label,
            "scenario": scenario,
            "sandbox_id": sandbox_id,
            "actions": n,
            "avg_wait_sec": round(avg_wait, 4),
            "avg_exec_sec": round(avg_exec, 4),
            "avg_pause_sec": round(avg_pause, 4),
            "avg_resume_sec": round(avg_resume, 4),
            "avg_interaction_sec": round(avg_total, 4),
            "total_wait_sec": round(sum(s["wait_sec"] for s in steps), 4),
            "total_exec_sec": round(sum(s["exec_sec"] for s in steps), 4),
            "total_pause_sec": round(sum(s["pause_sec"] for s in steps), 4),
            "total_resume_sec": round(sum(s["resume_sec"] for s in steps), 4),
            "succeeded": succeeded,
        })

    summary_path = output_dir / "vm_summary.csv"
    summary_fields = [
        "vm", "label", "scenario", "sandbox_id", "actions",
        "avg_wait_sec", "avg_exec_sec", "avg_pause_sec", "avg_resume_sec",
        "avg_interaction_sec",
        "total_wait_sec", "total_exec_sec", "total_pause_sec", "total_resume_sec",
        "succeeded",
    ]
    with open(summary_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=summary_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(summary_rows)

    print(f"[vm_timings] Per-VM CSVs: {vm_dir}/ ({len(summary_rows)} VMs)")
    print(f"[vm_timings] Summary: {summary_path}")


# ── Part 2: Cgroup CPU/Mem from observability ───────────────


def find_metrics_files(results_dir: Path) -> list[Path]:
    """Find all metrics_1s.jsonl files under results_dir."""
    return sorted(results_dir.rglob("metrics_1s.jsonl"))


def load_metrics(metrics_path: Path) -> list[dict]:
    """Load JSONL metrics file into a list of dicts."""
    rows = []
    with open(metrics_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def extract_cgroup_metrics(metrics: list[dict]) -> dict:
    """Extract cgroup CPU and memory time series from metrics."""
    timestamps = []
    cg_cpu = []
    cg_cpu_user = []
    cg_cpu_sys = []
    cg_mem_usage = []
    cg_mem_rss = []
    cg_mem_cache = []
    host_cpu_pct = []
    host_mem_used = []
    host_mem_cached = []
    elapsed = []

    for m in metrics:
        ts = m.get("ts", "")
        t = m.get("interval_s", 0)
        e = m.get("elapsed_s", 0) if "elapsed_s" in m else t

        # Prioritize cgroup fields; fall back to host fields
        cpu = m.get("cgroup_cpu_cores", m.get("cpu_pct", 0))
        mem_u = m.get("cgroup_mem_usage_gb", m.get("mem_used_gb", 0))
        mem_r = m.get("cgroup_mem_rss_gb", 0)
        mem_c = m.get("cgroup_mem_cache_gb", m.get("mem_cached_gb", 0))

        timestamps.append(ts)
        elapsed.append(e)
        cg_cpu.append(float(cpu) if cpu is not None else 0)
        cg_cpu_user.append(float(m.get("cgroup_cpu_user_cores", 0)))
        cg_cpu_sys.append(float(m.get("cgroup_cpu_sys_cores", 0)))
        cg_mem_usage.append(float(mem_u) if mem_u is not None else 0)
        cg_mem_rss.append(float(mem_r) if mem_r is not None else 0)
        cg_mem_cache.append(float(mem_c) if mem_c is not None else 0)
        host_cpu_pct.append(float(m.get("cpu_pct", 0)))
        host_mem_used.append(float(m.get("mem_used_gb", 0)))
        host_mem_cached.append(float(m.get("mem_cached_gb", 0)))

    return {
        "timestamps": timestamps,
        "elapsed": elapsed,
        "cg_cpu": cg_cpu,
        "cg_cpu_user": cg_cpu_user,
        "cg_cpu_sys": cg_cpu_sys,
        "cg_mem_usage": cg_mem_usage,
        "cg_mem_rss": cg_mem_rss,
        "cg_mem_cache": cg_mem_cache,
        "host_cpu_pct": host_cpu_pct,
        "host_mem_used": host_mem_used,
        "host_mem_cached": host_mem_cached,
    }


def plot_cgroup_metrics(data: dict, output_path: Path, title: str = ""):
    """Plot cgroup CPU and memory curves side by side."""
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), sharex=True)

    elapsed = data["elapsed"]
    has_cgroup = any(v > 0 for v in data["cg_cpu"])

    # --- CPU plot ---
    if has_cgroup:
        ax1.plot(elapsed, data["cg_cpu"], "b-", linewidth=1, label="cgroup CPU (cores)")
        ax1.plot(elapsed, data["cg_cpu_user"], "g-", linewidth=0.5, alpha=0.7, label="cgroup user")
        ax1.plot(elapsed, data["cg_cpu_sys"], "r-", linewidth=0.5, alpha=0.7, label="cgroup sys")
        ax1.set_ylabel("cgroup CPU (cores)")
    else:
        ax1.plot(elapsed, data["host_cpu_pct"], "b-", linewidth=1, label="Host CPU (%)")
        ax1.set_ylabel("Host CPU (%)")

    ax1.set_title(title or "CPU Utilization Over Time")
    ax1.legend(loc="upper right", fontsize=8)
    ax1.grid(True, alpha=0.3)

    # --- Memory plot ---
    if has_cgroup:
        ax2.fill_between(elapsed, 0, data["cg_mem_rss"], alpha=0.4, color="red", label="cgroup RSS (anon)")
        ax2.fill_between(elapsed, data["cg_mem_rss"],
                         [r + c for r, c in zip(data["cg_mem_rss"], data["cg_mem_cache"])],
                         alpha=0.4, color="blue", label="cgroup cache (file)")
        ax2.plot(elapsed, data["cg_mem_usage"], "k-", linewidth=1.5, label="cgroup total usage")
    else:
        ax2.fill_between(elapsed, 0, data["host_mem_used"], alpha=0.4, color="red", label="Host used (GB)")
        ax2.fill_between(elapsed, data["host_mem_used"],
                         [u + c for u, c in zip(data["host_mem_used"], data["host_mem_cached"])],
                         alpha=0.4, color="blue", label="Host cached (GB)")

    ax2.set_xlabel("Elapsed (s)")
    ax2.set_ylabel("Memory (GB)")
    ax2.set_title("Memory Utilization Over Time" if not title else f"{title} - Memory")
    ax2.legend(loc="upper left", fontsize=8)
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  [plot] {output_path}")


def write_cgroup_csv(data: dict, csv_path: Path):
    """Write cgroup metrics to CSV."""
    fields = [
        "timestamp", "elapsed_s",
        "cgroup_cpu_cores", "cgroup_cpu_user_cores", "cgroup_cpu_sys_cores",
        "cgroup_mem_usage_gb", "cgroup_mem_rss_gb", "cgroup_mem_cache_gb",
        "host_cpu_pct", "host_mem_used_gb", "host_mem_cached_gb",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for i in range(len(data["elapsed"])):
            writer.writerow({
                "timestamp": data["timestamps"][i],
                "elapsed_s": data["elapsed"][i],
                "cgroup_cpu_cores": data["cg_cpu"][i],
                "cgroup_cpu_user_cores": data["cg_cpu_user"][i],
                "cgroup_cpu_sys_cores": data["cg_cpu_sys"][i],
                "cgroup_mem_usage_gb": data["cg_mem_usage"][i],
                "cgroup_mem_rss_gb": data["cg_mem_rss"][i],
                "cgroup_mem_cache_gb": data["cg_mem_cache"][i],
                "host_cpu_pct": data["host_cpu_pct"][i],
                "host_mem_used_gb": data["host_mem_used"][i],
                "host_mem_cached_gb": data["host_mem_cached"][i],
            })


def process_observability(results_dir: Path, output_dir: Path):
    """Extract cgroup metrics and plot CPU/mem curves."""
    obs_dir = output_dir / "observability"
    obs_dir.mkdir(parents=True, exist_ok=True)

    metrics_files = find_metrics_files(results_dir)
    if not metrics_files:
        print(f"[observability] No metrics_1s.jsonl found under {results_dir}")
        return

    print(f"[observability] Found {len(metrics_files)} metrics files")

    for metrics_path in metrics_files:
        # Derive a label from the path: ratio-XX/repeat-YY
        parts = metrics_path.parts
        label_parts = []
        for p in parts:
            if p.startswith("ratio-") or p.startswith("repeat-"):
                label_parts.append(p)
        label = "_".join(label_parts) if label_parts else metrics_path.parent.name

        metrics = load_metrics(metrics_path)
        if not metrics:
            continue

        data = extract_cgroup_metrics(metrics)

        # Write CSV
        csv_path = obs_dir / f"cgroup_{label}.csv"
        write_cgroup_csv(data, csv_path)
        print(f"  [csv] {csv_path}")

        # Plot
        png_path = obs_dir / f"cgroup_cpu_mem_{label}.png"
        plot_cgroup_metrics(data, png_path, title=f"cgroup CPU/Mem ({label})")

    print(f"[observability] Done: {obs_dir}/")


# ── Main ─────────────────────────────────────────────────────


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <results_dir> <output_dir>")
        print(f"  results_dir  - runs/pause-resume-oversub-N8-xxx/")
        print(f"  output_dir   - where to write CSVs and PNGs")
        return 1

    results_dir = Path(sys.argv[1]).resolve()
    output_dir = Path(sys.argv[2]).resolve()

    if not results_dir.is_dir():
        print(f"ERROR: results dir not found: {results_dir}")
        return 1

    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Results:  {results_dir}")
    print(f"Output:   {output_dir}")
    print()

    process_vm_timings(results_dir, output_dir)
    print()
    process_observability(results_dir, output_dir)
    print()
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
