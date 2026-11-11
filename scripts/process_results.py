#!/usr/bin/env python3
"""Process benchmark results: per-VM timing CSV + cgroup CPU/mem plots.

Usage:
  python3 process_results.py <results_dir> [output_dir]

  results_dir  - runs/pause-resume-oversub-N8-xxx/ (from benchmark driver)
  output_dir   - where to write CSVs and PNGs; omitted -> <results_dir>/processed

Outputs:
  output_dir/
  ├── vm_timings/
  │   ├── vm_0001.csv          # per-action: wait, exec, pause, resume
  │   ├── vm_0002.csv
  │   └── ...
  ├── vm_summary.csv           # per-VM averages
  ├── case_summary.csv         # per-case wall/exec stats (avg/P50/P95/P99, real VMs only)
  ├── mpki_summary.csv         # global L3 MPKI stats per metrics file (when collected)
  └── observability/
      ├── cgroup_cpu_mem_ratio01_repeat01.png
      ├── mpki_ratio01_repeat01.png          # global L3 MPKI over time (when collected)
      └── cgroup_cpu_mem_ratio02_repeat01.png
"""
import csv
import json
import math
import os
import re
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
    case_rows = []
    noise_vm_count = 0
    for idx, result_path in enumerate(result_files, 1):
        try:
            result = json.loads(result_path.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"  [WARN] Cannot parse {result_path}: {e}")
            continue

        steps, scenario = extract_step_timings(result)
        if not steps:
            continue

        is_noise = is_noise_vm(result_path)
        if is_noise:
            noise_vm_count += 1

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

        if not is_noise:
            # Case aggregation only considers real VMs (noise-fill excluded).
            case_rows.append({
                "case": extract_case_name(result, result_path),
                "scenario": scenario,
                "wall_sec": (
                    float(result["elapsed_sec"])
                    if result.get("elapsed_sec") is not None
                    else None
                ),
                "total_exec_sec": sum(s["exec_sec"] for s in steps),
                # wait semantics per scenario: COW = measured delay sleep
                # (includes scheduler overshoot), pause-resume = paused_sec.
                "total_wait_sec": sum(s["wait_sec"] for s in steps),
                "actions": len(steps),
                "succeeded": bool(succeeded),
            })

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
    if noise_vm_count:
        print(f"[vm_timings] Noise-fill VMs excluded from case summary: {noise_vm_count}")

    if case_rows:
        write_case_summary(case_rows, output_dir)


# ── Part 1b: Case summary (aggregate VMs of the same case) ───


def extract_case_name(result_json: dict, result_path: Path) -> str:
    """Identify the replayed case (trajectory) for one VM run.

    Priority: trajectory_path stem > instance_id > parent dir name.
    """
    trajectory_path = result_json.get("trajectory_path") or ""
    if trajectory_path:
        stem = Path(trajectory_path).name
        for suffix in (".replay.json", ".json", ".traj"):
            if stem.endswith(suffix):
                stem = stem[: -len(suffix)]
                break
        if stem:
            return stem
    instance_id = result_json.get("instance_id") or ""
    if instance_id:
        return str(instance_id)
    return result_path.parent.name


def is_noise_vm(result_path: Path) -> bool:
    """Noise-fill runs use rollout-*-noise-* output dirs; real ones use *-real-*.

    Only the immediate parent directory name is checked, so a results root or
    trajectory name containing "noise" does not cause false positives.
    """
    return bool(re.match(r"rollout-\d+-noise", result_path.parent.name))


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    n = len(ordered)
    if n % 2 == 1:
        return ordered[n // 2]
    return (ordered[n // 2 - 1] + ordered[n // 2]) / 2


def _percentile(values: list[float], q: float) -> float:
    """Linear-interpolation percentile (numpy 'linear' method)."""
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    pos = (len(ordered) - 1) * q / 100.0
    lo = int(pos)
    hi = min(lo + 1, len(ordered) - 1)
    frac = pos - lo
    return ordered[lo] * (1.0 - frac) + ordered[hi] * frac


def write_case_summary(
    case_rows: list[dict],
    output_dir: Path,
) -> None:
    """Aggregate per-VM wall times per case (real VMs only) into case_summary.csv."""
    cases: dict[str, list[dict]] = {}
    for row in case_rows:
        cases.setdefault(row["case"], []).append(row)

    summary_rows = []
    for case in sorted(cases, key=lambda c: (len(c), c)):
        runs = cases[case]
        wall = [r["wall_sec"] for r in runs if r["wall_sec"] is not None]
        execs = [r["total_exec_sec"] for r in runs]
        waits = [r["total_wait_sec"] for r in runs]
        # Estimated run time without create/teardown/inter-step gaps:
        # per-VM exec + wait. Lower bound vs wall (wall also carries
        # sandbox_create_sec + inter-step gaps + delete_cleanup_sec).
        ests = [e + w for e, w in zip(execs, waits)]
        n = len(runs)
        succeeded = sum(1 for r in runs if r["succeeded"])
        summary_rows.append({
            "case": case,
            "scenario": runs[0]["scenario"],
            "vms": n,
            "succeeded": succeeded,
            "succeeded_ratio": round(succeeded / n, 4) if n else 0,
            # wall: avg / P50 / P95 / P99 (+ min/max)
            "avg_wall_sec": round(sum(wall) / len(wall), 4) if wall else "",
            "median_wall_sec": round(_median(wall), 4) if wall else "",
            "wall_p95_sec": round(_percentile(wall, 95), 4) if wall else "",
            "wall_p99_sec": round(_percentile(wall, 99), 4) if wall else "",
            "min_wall_sec": round(min(wall), 4) if wall else "",
            "max_wall_sec": round(max(wall), 4) if wall else "",
            # exec (sum of per-step command times): avg / P50 / P95 / P99
            "avg_total_exec_sec": round(sum(execs) / n, 4) if n else 0,
            "exec_p50_sec": round(_percentile(execs, 50), 4) if execs else "",
            "exec_p95_sec": round(_percentile(execs, 95), 4) if execs else "",
            "exec_p99_sec": round(_percentile(execs, 99), 4) if execs else "",
            # wait totals (same stat set as exec)
            "avg_total_wait_sec": round(sum(waits) / n, 4) if n else 0,
            "wait_p50_sec": round(_percentile(waits, 50), 4) if waits else "",
            "wait_p95_sec": round(_percentile(waits, 95), 4) if waits else "",
            "wait_p99_sec": round(_percentile(waits, 99), 4) if waits else "",
            # exec + wait: the startup-free run-time estimate
            "exec_wait_avg_sec": round(sum(ests) / n, 4) if n else 0,
            "exec_wait_p50_sec": round(_percentile(ests, 50), 4) if ests else "",
            "exec_wait_p95_sec": round(_percentile(ests, 95), 4) if ests else "",
            "exec_wait_p99_sec": round(_percentile(ests, 99), 4) if ests else "",
            "avg_actions": round(sum(r["actions"] for r in runs) / n, 2) if n else 0,
        })

    # ALL row: the P50 TRAJECTORY, not pooled percentiles. Cases are ranked
    # by their own wall P50; the case whose P50 sits at the median of the
    # case-level P50s becomes the representative -- the ALL row carries that
    # trajectory's full stat set (vms, wall/exec/wait/exec_wait), so it reads
    # as "a typical trajectory" instead of a blend across different cases.
    if summary_rows:
        candidates = [
            (row, row["median_wall_sec"])
            for row in summary_rows
            if row["median_wall_sec"] not in ("", None)
        ]
        if candidates:
            p50_values = sorted(v for _r, v in candidates)
            p50_median = _median(p50_values)
            chosen_row, _ = min(
                candidates,
                key=lambda c: (abs(c[1] - p50_median), c[1]),
            )
        else:
            chosen_row = summary_rows[0]
        all_row = dict(chosen_row)
        all_row["case"] = "ALL"
        all_row["source_case"] = chosen_row["case"]
        summary_rows.append(all_row)

    summary_path = output_dir / "case_summary.csv"
    fields = [
        "case", "source_case", "scenario", "vms", "succeeded", "succeeded_ratio",
        "avg_wall_sec", "median_wall_sec", "wall_p95_sec", "wall_p99_sec",
        "min_wall_sec", "max_wall_sec",
        "avg_total_exec_sec", "exec_p50_sec", "exec_p95_sec", "exec_p99_sec",
        "avg_total_wait_sec", "wait_p50_sec", "wait_p95_sec", "wait_p99_sec",
        "exec_wait_avg_sec", "exec_wait_p50_sec", "exec_wait_p95_sec", "exec_wait_p99_sec",
        "avg_actions",
    ]
    with open(summary_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(summary_rows)

    print(f"[case_summary] Real VMs per case: {summary_path}")
    for row in summary_rows:
        tag = "ALL" if row["case"] == "ALL" else "case"
        src = f" (source={row['source_case']})" if row.get("source_case") else ""
        print(
            f"  {tag}={row['case']}{src} vms={row['vms']} ok={row['succeeded']}/{row['vms']} "
            f"wall avg={row['avg_wall_sec']}s p50={row['median_wall_sec']}s "
            f"p95={row['wall_p95_sec']}s p99={row['wall_p99_sec']}s "
            f"exec avg={row['avg_total_exec_sec']}s p95={row['exec_p95_sec']}s "
            f"wait avg={row['avg_total_wait_sec']}s "
            f"exec+wait p50={row['exec_wait_p50_sec']}s p95={row['exec_wait_p95_sec']}s"
        )


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
    mpki_global = []
    elapsed = []

    # Accumulate per-sample interval_s into a monotonic elapsed axis.
    # The old code fell back to the raw interval_s value (always 1.0) when
    # "elapsed_s" was absent from the JSONL, which collapsed every x-axis
    # point onto 1.0 and squashed the plotted curves.
    elapsed_running = 0.0

    for m in metrics:
        ts = m.get("ts", "")
        t = m.get("interval_s", 0)
        elapsed_running += float(t) if t else 0.0

        # Prioritize cgroup fields; fall back to host fields
        cpu = m.get("cgroup_cpu_cores", m.get("cpu_pct", 0))
        mem_u = m.get("cgroup_mem_usage_gb", m.get("mem_used_gb", 0))
        mem_r = m.get("cgroup_mem_rss_gb", 0)
        mem_c = m.get("cgroup_mem_cache_gb", m.get("mem_cached_gb", 0))

        timestamps.append(ts)
        elapsed.append(round(elapsed_running, 3))
        cg_cpu.append(float(cpu) if cpu is not None else 0)
        cg_cpu_user.append(float(m.get("cgroup_cpu_user_cores", 0)))
        cg_cpu_sys.append(float(m.get("cgroup_cpu_sys_cores", 0)))
        cg_mem_usage.append(float(mem_u) if mem_u is not None else 0)
        cg_mem_rss.append(float(mem_r) if mem_r is not None else 0)
        cg_mem_cache.append(float(mem_c) if mem_c is not None else 0)
        host_cpu_pct.append(float(m.get("cpu_pct", 0)))
        host_mem_used.append(float(m.get("mem_used_gb", 0)))
        host_mem_cached.append(float(m.get("mem_cached_gb", 0)))
        # Global instruction-weighted L3 MPKI (None when the sample carries
        # no l3_* fields, e.g. perf warm-up or MPKI disabled).
        l3_misses = m.get("l3_misses_s") or {}
        l3_instr = m.get("l3_instr_s") or {}
        m_sum = sum(v for v in l3_misses.values() if isinstance(v, (int, float)))
        i_sum = sum(v for v in l3_instr.values() if isinstance(v, (int, float)))
        mpki_global.append(m_sum / i_sum * 1000.0 if i_sum > 0 else None)

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
        "mpki_global": mpki_global,
    }


def plot_cgroup_metrics(
    data: dict,
    output_path: Path,
    title: str = "",
    cpu_limit: int = 384,
):
    """Plot cgroup CPU and memory curves side by side."""
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), sharex=True)

    elapsed = data["elapsed"]
    has_cgroup = any(v > 0 for v in data["cg_cpu"])

    # --- CPU plot ---
    if has_cgroup:
        ax1.plot(elapsed, data["cg_cpu"], "b-", linewidth=1, label="cgroup CPU (cores)")
        ax1.plot(elapsed, data["cg_cpu_user"], "g-", linewidth=0.5, alpha=0.7, label="cgroup user")
        ax1.plot(elapsed, data["cg_cpu_sys"], "r-", linewidth=0.5, alpha=0.7, label="cgroup sys")
        ax1.axhline(y=cpu_limit, color="red", linestyle="--", linewidth=2,
                     alpha=0.8, label=f"CPU limit ({cpu_limit} cores)")
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


def plot_mpki_curve(
    data: dict,
    output_path: Path,
    title: str = "",
):
    """Plot the global instruction-weighted L3 MPKI over time (one curve).

    Uses the same elapsed axis as the cgroup CPU/mem plots and lives in the
    same observability directory. None samples (perf warm-up / MPKI off)
    become NaN and are simply skipped by the line.
    """
    elapsed = data["elapsed"]
    values = [
        float("nan") if v is None else float(v)
        for v in data["mpki_global"]
    ]
    if not any(not math.isnan(v) for v in values):
        return
    fig, ax = plt.subplots(figsize=(14, 4.5))
    ax.plot(
        elapsed,
        values,
        color="#c084fc",
        linewidth=1.2,
        label="global L3 MPKI (instruction-weighted)",
    )
    ax.set_xlabel("Elapsed (s)")
    ax.set_ylabel("L3 MPKI (misses / kilo instructions)")
    ax.set_title(title or "Global L3 MPKI Over Time")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="upper right", fontsize=9)
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
        "l3_mpki_global",
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
                "l3_mpki_global": (
                    round(data["mpki_global"][i], 3)
                    if data["mpki_global"][i] is not None
                    else ""
                ),
            })


def aggregate_l3_mpki(metrics: list[dict]) -> dict | None:
    """Global per-run L3 MPKI statistics from per-domain per-sample l3_* fields.

    The headline number is the instruction-weighted global MPKI:
    sum(l3_misses_s) / sum(l3_instr_s) * 1000 -- the physically meaningful
    aggregate (a simple mean of per-domain MPKI values would re-weight idle
    domains equally and dilute hot ones). The simple mean and the hottest
    domain are reported alongside for reference.
    """
    m_sum = i_sum = 0.0
    mpki_vals: list[float] = []
    domain_means: dict[int, list[float]] = {}
    samples = 0
    for m in metrics:
        misses = m.get("l3_misses_s") or {}
        instr = m.get("l3_instr_s") or {}
        mpki = m.get("l3_mpki") or {}
        if not misses and not mpki:
            continue
        samples += 1
        for v in misses.values():
            if isinstance(v, (int, float)):
                m_sum += float(v)
        for v in instr.values():
            if isinstance(v, (int, float)):
                i_sum += float(v)
        for d, v in mpki.items():
            if isinstance(v, (int, float)):
                mpki_vals.append(float(v))
                domain_means.setdefault(int(d), []).append(float(v))
    if samples == 0:
        return None
    dom_means = {d: sum(vs) / len(vs) for d, vs in domain_means.items()}
    hottest = max(dom_means, key=dom_means.get) if dom_means else None
    return {
        "mpki_samples": samples,
        "domains": len(dom_means),
        "mpki_weighted_global": round(m_sum / i_sum * 1000.0, 3) if i_sum > 0 else "",
        "mpki_mean": round(sum(mpki_vals) / len(mpki_vals), 3) if mpki_vals else "",
        "mpki_max_domain_mean": round(max(dom_means.values()), 3) if dom_means else "",
        "hottest_domain": hottest if hottest is not None else "",
        "hottest_domain_mean": round(dom_means[hottest], 3) if hottest is not None else "",
    }


def process_observability(results_dir: Path, output_dir: Path, cpu_limit: int = 384):
    """Extract cgroup metrics and plot CPU/mem curves."""
    obs_dir = output_dir / "observability"
    obs_dir.mkdir(parents=True, exist_ok=True)

    metrics_files = find_metrics_files(results_dir)
    if not metrics_files:
        print(f"[observability] No metrics_1s.jsonl found under {results_dir}")
        return

    print(f"[observability] Found {len(metrics_files)} metrics files")

    mpki_rows = []
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

        # Global L3 MPKI statistics (only when the monitor collected them)
        mpki_stats = aggregate_l3_mpki(metrics)
        if mpki_stats is not None:
            mpki_rows.append({"label": label, **mpki_stats})

        # Write CSV
        csv_path = obs_dir / f"cgroup_{label}.csv"
        write_cgroup_csv(data, csv_path)
        print(f"  [csv] {csv_path}")

        # Plot
        png_path = obs_dir / f"cgroup_cpu_mem_{label}.png"
        plot_cgroup_metrics(data, png_path, title=f"cgroup CPU/Mem ({label})", cpu_limit=cpu_limit)
        mpki_png = obs_dir / f"mpki_{label}.png"
        plot_mpki_curve(data, mpki_png, title=f"global L3 MPKI ({label})")

    if mpki_rows:
        mpki_path = output_dir / "mpki_summary.csv"
        mpki_fields = [
            "label", "mpki_weighted_global", "mpki_mean", "mpki_max_domain_mean",
            "hottest_domain", "hottest_domain_mean", "domains", "mpki_samples",
        ]
        with open(mpki_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=mpki_fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(mpki_rows)
        print(f"[mpki] L3 MPKI global stats: {mpki_path}")
        for row in mpki_rows:
            print(
                f"  {row['label']}: weighted_global={row['mpki_weighted_global']} "
                f"mean={row['mpki_mean']} hottest=domain{row['hottest_domain']}"
                f"({row['hottest_domain_mean']}) over {row['domains']} domains"
            )

    print(f"[observability] Done: {obs_dir}/")


# ── Main ─────────────────────────────────────────────────────


def main():
    args = sys.argv[1:]
    cpu_limit = 384
    if "--cpu-limit" in args:
        idx = args.index("--cpu-limit")
        if idx + 1 < len(args):
            try:
                cpu_limit = int(args[idx + 1])
                args = args[:idx] + args[idx + 2:]
            except ValueError:
                print(f"ERROR: --cpu-limit must be an integer, got: {args[idx + 1]}")
                return 1

    if not args:
        print(f"Usage: {sys.argv[0]} <results_dir> [output_dir] [--cpu-limit N]")
        print(f"  results_dir  - runs/pause-resume-oversub-N8-xxx/")
        print(f"  output_dir   - where to write CSVs and PNGs (default: <results_dir>/processed)")
        print(f"  --cpu-limit  - total CPU cores for reference line (default: 384)")
        return 1

    results_dir = Path(args[0]).resolve()
    if len(args) > 1 and str(args[1]).strip():
        output_dir = Path(args[1]).resolve()
    else:
        # No output_dir given: create one inside the results dir so the
        # artifacts travel with the run.
        output_dir = results_dir / "processed"

    if not results_dir.is_dir():
        print(f"ERROR: results dir not found: {results_dir}")
        return 1

    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Results:  {results_dir}")
    print(f"Output:   {output_dir}")
    print()

    process_vm_timings(results_dir, output_dir)
    print()
    process_observability(results_dir, output_dir, cpu_limit=cpu_limit)
    print()
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
