#!/usr/bin/env python3
"""Summarize per-VM proxy-read benchmark results into one CSV.

Input : a run directory of the form runs/proxy-read-oversub-N1-<timestamp>
        (any pause-resume/proxy-read oversub run layout: ratio-XX/repeat-YY/slot-ZZZZ/<vm>/job.log)
Output: <run_dir>/vm-summary.csv with one row per VM:

    vm, avg_exec_sec, avg_pause_sec, avg_resume_sec, avg_proxy_read_sec,
    avg_e2e_sec, pause_count, resume_count, proxy_read_count, delay_sec

Metrics come from each VM's job.log:
  - "Agent interaction timing" lines: pause / resume / command / total (and
    transport=proxy_read + server_exec for proxied steps)
  - delay: the trajectory think time between steps (sum + per-step average)

Usage:
    python3 summarize_vm_runs.py runs/proxy-read-oversub-N1-20261028-103548
    python3 summarize_vm_runs.py <run_dir> -o /tmp/vm-summary.csv
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path

TIMING_RE = re.compile(
    r"Agent interaction timing step=(\d+)/\d+ "
    r"pause=([\d.]+)s delay=([\d.]+)s capacity_wait=([\d.]+)s "
    r"resume=([\d.]+)s command=([\d.]+)s total=([\d.]+)s"
    r"(?: transport=(\w+))?"
    r"(?: server_exec=(\d+)ms)?"
    r"(?: binding_generation=(\d+))?"
)
TRANSPORT_RE = re.compile(r"transport=(\w+)")
RESULT_RE = re.compile(
    r"\[(\d+)/\d+\] RESULT exit_code=(-?\d+) succeeded=(\w+) elapsed=([\d.]+)s"
)
E2E_RE = re.compile(r"Replay completed.*?elapsed=([\d.]+)s")


def avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def summarize_vm(vm_dir: Path) -> dict | None:
    job_log = vm_dir / "job.log"
    if not job_log.is_file():
        return None
    text = job_log.read_text(encoding="utf-8", errors="replace")

    pauses, resumes, commands, totals, delays = [], [], [], [], []
    proxy_pauses, proxy_totals, proxy_cmds, proxy_server_ms = [], [], [], []
    exec_secs, proxy_exec_secs = [], []
    for line in text.splitlines():
        m = TIMING_RE.search(line)
        if m:
            pause, delay, _wait, resume, command, total = (
                float(m.group(i)) for i in range(2, 8)
            )
            transport = m.group(8)
            server_ms = int(m.group(9)) if m.group(9) else None
            pauses.append(pause)
            delays.append(delay)
            commands.append(command)
            totals.append(total)
            if transport == "proxy_read":
                proxy_pauses.append(pause)
                proxy_totals.append(total)
                proxy_cmds.append(command)
                if server_ms is not None:
                    proxy_server_ms.append(server_ms)
                proxy_exec_secs.append(command)
            else:
                resumes.append(resume)
                exec_secs.append(command)
            continue

    e2e = None
    m = E2E_RE.search(text)
    if m:
        e2e = float(m.group(1))

    if not totals:
        return None

    return {
        "vm": vm_dir.name,
        "avg_exec_sec": round(avg(exec_secs + proxy_exec_secs), 4),
        "avg_exec_resume_sec": round(avg(exec_secs), 4),
        "avg_exec_proxy_read_sec": round(avg(proxy_exec_secs), 4),
        "avg_pause_sec": round(avg(pauses), 4),
        "avg_pause_resume_sec": round(avg(
            [p for p, t in zip(pauses, _transports(text)) if t != "proxy_read"]
        ), 4),
        "avg_pause_proxy_read_sec": round(avg(proxy_pauses), 4),
        "avg_resume_sec": round(avg(resumes), 4),
        "avg_e2e_sec": round(e2e if e2e is not None else avg(totals), 4),
        "avg_step_e2e_sec": round(avg(totals), 4),
        "pause_count": len(pauses),
        "resume_count": len(resumes),
        "proxy_read_count": len(proxy_exec_secs),
        "delay_sec": round(sum(delays), 3),
        "proxy_server_exec_ms_avg": round(avg(proxy_server_ms), 1),
    }


def _transports(text: str) -> list[str]:
    """Transport per timing line, aligned with the pause list."""
    out = []
    for line in text.splitlines():
        m = TIMING_RE.search(line)
        if m:
            out.append(m.group(8) or "resume")
    return out


COLUMNS = [
    "vm",
    "avg_exec_sec",
    "avg_exec_resume_sec",
    "avg_exec_proxy_read_sec",
    "avg_pause_sec",
    "avg_pause_resume_sec",
    "avg_pause_proxy_read_sec",
    "avg_resume_sec",
    "avg_e2e_sec",
    "avg_step_e2e_sec",
    "pause_count",
    "resume_count",
    "proxy_read_count",
    "delay_sec",
    "proxy_server_exec_ms_avg",
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Summarize per-VM proxy-read benchmark results into one CSV."
    )
    parser.add_argument("run_dir", type=Path, help="run directory (runs/...-N1-...)")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="output CSV path (default: <run_dir>/vm-summary.csv)",
    )
    args = parser.parse_args()

    run_dir = args.run_dir.expanduser().resolve()
    if not run_dir.is_dir():
        print(f"error: {run_dir} is not a directory", file=sys.stderr)
        return 2

    vm_dirs = sorted(
        p
        for p in run_dir.glob("ratio-*/repeat-*/slot-*/*")
        if p.is_dir() and (p / "job.log").is_file()
    )
    if not vm_dirs:
        print(f"error: no VM job.log found under {run_dir}", file=sys.stderr)
        return 1

    rows = [row for vm_dir in vm_dirs if (row := summarize_vm(vm_dir))]
    if not rows:
        print("error: no parsable VM timings found", file=sys.stderr)
        return 1

    output = args.output or run_dir / "vm-summary.csv"
    with output.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    print(f"summarized {len(rows)} VM(s) -> {output}")
    for row in rows:
        print(
            f"  {row['vm'][:40]:<42} "
            f"exec(all)={row['avg_exec_sec']:.3f} "
            f"exec(resume)={row['avg_exec_resume_sec']:.3f} "
            f"exec(proxy)={row['avg_exec_proxy_read_sec']:.3f} "
            f"pause={row['avg_pause_sec']:.3f} "
            f"resume={row['avg_resume_sec']:.3f} "
            f"e2e={row['avg_e2e_sec']:.3f} "
            f"pause×{row['pause_count']} resume×{row['resume_count']} "
            f"proxy×{row['proxy_read_count']} delay={row['delay_sec']}s "
            f"server_exec={row['proxy_server_exec_ms_avg']}ms"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
