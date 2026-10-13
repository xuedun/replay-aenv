#!/usr/bin/env python3
"""Run k concurrent tasks for each k and measure time degradation.

For every test level k this driver invokes replay-aenv with:

    target-count = k
    concurrency = k
    running-concurrency = 1

The median k=1 completion time is the baseline.  The report computes all-task
completion-time degradation and throughput gain for every k, then generates
CSV, JSON, and dependency-free SVG charts.
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime
from html import escape
import json
import os
from pathlib import Path
import re
import shlex
import statistics
import subprocess
import sys
import time
from typing import Any


DEFAULT_REPO_DIR = Path(__file__).parent.parent
DEFAULT_TRAJECTORY_DIR = DEFAULT_REPO_DIR / "single_data"
DEFAULT_TEMPLATE = "django-money-task-v2"
DEFAULT_LEVELS = "1,2,3,4,5,6,7,8,9,10,11,12"
DEFAULT_CONTROL_PLANE_QPS = 1000.0
DEFAULT_CONTROL_PLANE_MAX_INFLIGHT = 1024
SVG_TEXT_REPLACEMENTS = {
    "font-family:Arial,sans-serif": (
        'font-family:"Noto Sans CJK SC","Microsoft YaHei",'
        '"SimHei","PingFang SC",Arial,sans-serif'
    ),
    "k concurrent tasks / 1 RUNNING slot": (
        "超分比（k 个并发任务 : 1 个 RUNNING 槽）"
    ),
    "Oversubscription ratio (k concurrent tasks : 1 RUNNING slot)": (
        "超分比（k 个并发任务 : 1 个 RUNNING 槽）"
    ),
    "Oversubscription decision: latency cost vs throughput gain": (
        "时间劣化与吞吐提升"
    ),
    "Time degradation (left axis)": "时间劣化率（左轴）",
    "Throughput gain (right axis)": "吞吐提升率（右轴）",
    "Time degradation (%)": "时间劣化率（%）",
    "Throughput gain (%)": "吞吐提升率（%）",
    "incomplete": "结果不完整",
}


def localize_svg(path: Path) -> None:
    """中文化 SVG，并从点标签中去掉“成功次数/期望次数”。"""

    text = path.read_text(encoding="utf-8")
    for source, target in SVG_TEXT_REPLACEMENTS.items():
        text = text.replace(source, target)
    text = re.sub(
        r"selected\s+(\d+):1\s+\|\s+degradation\s+"
        r"(-?[\d.]+)%\s+\|\s+throughput gain\s+(-?[\d.]+)%",
        r"推荐超分比 \1:1｜时间劣化 \2%｜吞吐提升 \3%",
        text,
    )
    text = re.sub(
        r"allowed degradation\s+(-?[\d.]+)%",
        r"允许的时间劣化阈值 \1%",
        text,
    )
    text = re.sub(
        r"threshold\s+(-?[\d.]+)%",
        r"时间劣化阈值 \1%",
        text,
    )
    text = re.sub(
        r"(-?[\d.]+)\s+\(\d+/\d+\)(?=</text>)",
        r"\1",
        text,
    )
    path.write_text(text, encoding="utf-8")


def parse_levels(value: str) -> list[int]:
    levels: set[int] = set()
    for raw in value.split(","):
        raw = raw.strip()
        if not raw:
            continue
        level = int(raw)
        if level < 1:
            raise argparse.ArgumentTypeError("concurrency levels must be positive")
        levels.add(level)
    if not levels:
        raise argparse.ArgumentTypeError("at least one concurrency level is required")
    if 1 not in levels:
        raise argparse.ArgumentTypeError("level 1 is required as the baseline")
    # Always start from the baseline and increase pressure gradually.
    return sorted(levels)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "For each k, run k concurrent tasks over one RUNNING slot and "
            "measure completion-time degradation relative to k=1."
        )
    )
    parser.add_argument("--repo-dir", type=Path, default=DEFAULT_REPO_DIR)
    parser.add_argument(
        "--trajectory-dir", type=Path, default=DEFAULT_TRAJECTORY_DIR
    )
    parser.add_argument("--template", default=DEFAULT_TEMPLATE)
    parser.add_argument(
        "--concurrencies",
        "--ks",
        dest="levels",
        type=parse_levels,
        default=parse_levels(DEFAULT_LEVELS),
        help=(
            "Comma-separated k values; k=1 is mandatory and values are run in "
            f"ascending order (default: {DEFAULT_LEVELS})"
        ),
    )
    parser.add_argument(
        "--repeats", type=int, default=3, help="Repeats per k (default: 3)"
    )
    parser.add_argument(
        "--max-degradation-pct",
        type=float,
        default=None,
        help=(
            "Optional SLA threshold. When set, select the largest zero-failure "
            "k whose median completion-time degradation does not exceed it; "
            "the default is no threshold and no automatic selection"
        ),
    )
    parser.add_argument(
        "--control-plane-qps",
        type=float,
        default=DEFAULT_CONTROL_PLANE_QPS,
        help=f"Control-plane QPS (default: {DEFAULT_CONTROL_PLANE_QPS:g})",
    )
    parser.add_argument(
        "--control-plane-max-inflight",
        type=int,
        default=DEFAULT_CONTROL_PLANE_MAX_INFLIGHT,
        help=(
            "Control-plane in-flight cap "
            f"(default: {DEFAULT_CONTROL_PLANE_MAX_INFLIGHT})"
        ),
    )
    parser.add_argument(
        "--cooldown-sec",
        type=float,
        default=30.0,
        help="Delay between trials (default: 30)",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        help="Output directory (default: runs/k-degradation-<timestamp>)",
    )
    parser.add_argument(
        "--reuse",
        action="store_true",
        help="Reuse an existing valid batch-summary.json",
    )
    parser.add_argument(
        "--stop-on-failure",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Stop after the first failed trial (default: true)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands without running replay jobs",
    )
    return parser


def validate_args(args: argparse.Namespace) -> None:
    if args.repeats < 1:
        raise ValueError("repeats must be positive")
    if args.max_degradation_pct is not None and args.max_degradation_pct < 0:
        raise ValueError("max-degradation-pct must not be negative")
    if args.control_plane_qps <= 0:
        raise ValueError("control-plane-qps must be positive")
    if args.control_plane_max_inflight < 1:
        raise ValueError("control-plane-max-inflight must be positive")
    if args.cooldown_sec < 0:
        raise ValueError("cooldown-sec must not be negative")

    repo_dir = args.repo_dir.expanduser().resolve()
    runner = repo_dir / ".venv/bin/replay-aenv"
    if not runner.is_file():
        raise FileNotFoundError(f"replay-aenv entry point not found: {runner}")

    trajectory_dir = args.trajectory_dir.expanduser().resolve()
    if not trajectory_dir.is_dir():
        raise FileNotFoundError(f"trajectory directory not found: {trajectory_dir}")
    trajectories = [
        path
        for path in trajectory_dir.iterdir()
        if path.is_file() and path.name.endswith((".json", ".traj"))
    ]
    if not trajectories:
        raise ValueError(f"no .json or .traj files found in {trajectory_dir}")
    if len(trajectories) != 1:
        raise ValueError(
            "this degradation test requires exactly one source trajectory so "
            f"every k has identical work; found {len(trajectories)} in {trajectory_dir}"
        )


def command_for_trial(
    args: argparse.Namespace, *, k: int, output_dir: Path
) -> list[str]:
    runner = args.repo_dir.expanduser().resolve() / ".venv/bin/replay-aenv"
    return [
        str(runner),
        "batch",
        "--trajectory-dir",
        str(args.trajectory_dir.expanduser().resolve()),
        "--template",
        args.template,
        "--target-count",
        str(k),
        "--concurrency",
        str(k),
        "--running-concurrency",
        "1",
        "--control-plane-qps",
        str(args.control_plane_qps),
        "--control-plane-max-inflight",
        str(args.control_plane_max_inflight),
        "--output-dir",
        str(output_dir),
    ]


def read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"expected a JSON object in {path}")
    return value


def trial_from_summary(
    *,
    k: int,
    repeat: int,
    output_dir: Path,
    return_code: int | None,
    reused: bool,
) -> dict[str, Any]:
    summary_path = output_dir / "batch-summary.json"
    record: dict[str, Any] = {
        "k": k,
        "repeat": repeat,
        "target_count": k,
        "concurrency": k,
        "running_concurrency": 1,
        "valid": False,
        "elapsed_sec": None,
        "throughput_tasks_per_sec": None,
        "total": None,
        "succeeded": None,
        "failed": None,
        "running_peak": None,
        "run_queue_wait_avg_sec": None,
        "control_wait_avg_sec": None,
        "control_wait_max_sec": None,
        "return_code": return_code,
        "reused": reused,
        "error": None,
        "output_dir": str(output_dir),
        "summary_path": str(summary_path),
    }
    if not summary_path.is_file():
        record["error"] = "batch-summary.json was not produced"
        return record

    try:
        summary = read_json(summary_path)
        total = int(summary.get("total", 0))
        succeeded = int(summary.get("succeeded", 0))
        failed = int(summary.get("failed", 0))
        elapsed_sec = float(summary["elapsed_sec"])
        running = summary.get("running_slots") or {}
        control = summary.get("control_plane") or {}
        record.update(
            {
                "elapsed_sec": elapsed_sec,
                "throughput_tasks_per_sec": (
                    total / elapsed_sec if elapsed_sec > 0 else None
                ),
                "total": total,
                "succeeded": succeeded,
                "failed": failed,
                "running_peak": running.get("peak_active"),
                "run_queue_wait_avg_sec": running.get(
                    "average_queue_wait_sec"
                ),
                "control_wait_avg_sec": control.get("average_wait_sec"),
                "control_wait_max_sec": control.get("max_wait_sec"),
            }
        )
        record["valid"] = (
            total == k
            and succeeded == k
            and failed == 0
            and (return_code in (None, 0))
        )
        if not record["valid"]:
            item_errors = [
                str(item.get("error"))
                for item in summary.get("items", [])
                if item.get("error")
            ]
            record["error"] = (
                item_errors[0]
                if item_errors
                else f"incomplete batch: succeeded={succeeded}, failed={failed}"
            )
    except Exception as exc:
        record["error"] = f"cannot parse summary: {type(exc).__name__}: {exc}"
    return record


def run_trial(
    args: argparse.Namespace, *, k: int, repeat: int, output_dir: Path
) -> dict[str, Any]:
    summary_path = output_dir / "batch-summary.json"
    if args.reuse and summary_path.is_file():
        print(f"[reuse] k={k} repeat={repeat}: {summary_path}", flush=True)
        return trial_from_summary(
            k=k,
            repeat=repeat,
            output_dir=output_dir,
            return_code=None,
            reused=True,
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    command = command_for_trial(args, k=k, output_dir=output_dir)
    print(
        f"\n[start] k={k} tasks, concurrency={k}, RUNNING=1, "
        f"repeat={repeat}/{args.repeats}\n        {shlex.join(command)}",
        flush=True,
    )
    environment = os.environ.copy()
    environment["PYTHONUNBUFFERED"] = "1"
    log_path = output_dir / "benchmark-driver.log"
    with log_path.open("w", encoding="utf-8") as log_handle:
        process = subprocess.Popen(
            command,
            cwd=args.repo_dir.expanduser().resolve(),
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        assert process.stdout is not None
        try:
            for line in process.stdout:
                sys.stdout.write(line)
                sys.stdout.flush()
                log_handle.write(line)
                log_handle.flush()
            return_code = process.wait()
        except KeyboardInterrupt:
            process.terminate()
            try:
                return_code = process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                return_code = process.wait()
            raise

    record = trial_from_summary(
        k=k,
        repeat=repeat,
        output_dir=output_dir,
        return_code=return_code,
        reused=False,
    )
    state = "valid" if record["valid"] else "FAILED"
    print(
        f"[done] k={k} repeat={repeat} state={state} "
        f"elapsed_sec={record['elapsed_sec']} failed={record['failed']}",
        flush=True,
    )
    return record


def aggregate_trials(
    levels: list[int], records: list[dict[str, Any]], expected_repeats: int
) -> list[dict[str, Any]]:
    raw: list[dict[str, Any]] = []
    for k in levels:
        attempted = [record for record in records if record["k"] == k]
        valid = [record for record in attempted if record["valid"]]
        elapsed = [float(record["elapsed_sec"]) for record in valid]
        median_elapsed = statistics.median(elapsed) if elapsed else None
        raw.append(
            {
                "k": k,
                "attempted": len(attempted),
                "successful": len(valid),
                "expected_repeats": expected_repeats,
                "all_repeats_successful": (
                    len(attempted) == expected_repeats
                    and len(valid) == expected_repeats
                ),
                "median_elapsed_sec": median_elapsed,
                "min_elapsed_sec": min(elapsed) if elapsed else None,
                "max_elapsed_sec": max(elapsed) if elapsed else None,
                "median_throughput_tasks_per_sec": (
                    k / median_elapsed if median_elapsed else None
                ),
                "time_degradation_pct": None,
                "throughput_gain_pct": None,
            }
        )

    baseline_item = next((item for item in raw if item["k"] == 1), None)
    baseline = (
        float(baseline_item["median_elapsed_sec"])
        if baseline_item and baseline_item["median_elapsed_sec"] is not None
        else None
    )
    if baseline is not None:
        baseline_throughput = 1.0 / baseline
        for item in raw:
            median_elapsed = item["median_elapsed_sec"]
            if median_elapsed is None:
                continue
            elapsed_value = float(median_elapsed)
            throughput = int(item["k"]) / elapsed_value
            item["time_degradation_pct"] = (elapsed_value / baseline - 1.0) * 100.0
            item["throughput_gain_pct"] = (
                throughput / baseline_throughput - 1.0
            ) * 100.0
    return raw


def select_best_k(
    aggregates: list[dict[str, Any]], max_degradation_pct: float | None
) -> dict[str, Any] | None:
    if max_degradation_pct is None:
        return None
    eligible = [
        item
        for item in aggregates
        if item["all_repeats_successful"]
        and item["time_degradation_pct"] is not None
        and float(item["time_degradation_pct"]) <= max_degradation_pct
    ]
    if not eligible:
        return None
    selected = max(eligible, key=lambda item: int(item["k"]))
    return {
        "k": selected["k"],
        "oversubscription_ratio": f"{selected['k']}:1",
        "median_elapsed_sec": selected["median_elapsed_sec"],
        "time_degradation_pct": selected["time_degradation_pct"],
        "throughput_gain_pct": selected["throughput_gain_pct"],
        "max_allowed_degradation_pct": max_degradation_pct,
        "rule": (
            "largest zero-failure k whose median all-task completion-time "
            "degradation is within the configured threshold"
        ),
    }


def write_csv(path: Path, records: list[dict[str, Any]]) -> None:
    columns = [
        "k",
        "repeat",
        "target_count",
        "concurrency",
        "running_concurrency",
        "valid",
        "elapsed_sec",
        "throughput_tasks_per_sec",
        "total",
        "succeeded",
        "failed",
        "running_peak",
        "run_queue_wait_avg_sec",
        "control_wait_avg_sec",
        "control_wait_max_sec",
        "return_code",
        "reused",
        "error",
        "output_dir",
        "summary_path",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)


def write_aggregate_csv(path: Path, aggregates: list[dict[str, Any]]) -> None:
    columns = [
        "k",
        "attempted",
        "successful",
        "expected_repeats",
        "all_repeats_successful",
        "median_elapsed_sec",
        "min_elapsed_sec",
        "max_elapsed_sec",
        "time_degradation_pct",
        "median_throughput_tasks_per_sec",
        "throughput_gain_pct",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(aggregates)


def write_line_svg(
    path: Path,
    *,
    aggregates: list[dict[str, Any]],
    value_key: str,
    title: str,
    y_label: str,
    threshold: float | None = None,
    selected_k: int | None = None,
) -> None:
    width, height = 1000, 620
    left, right, top, bottom = 105, 45, 70, 95
    plot_width = width - left - right
    plot_height = height - top - bottom
    levels = [int(item["k"]) for item in aggregates]
    points = [
        (int(item["k"]), float(item[value_key]), item)
        for item in aggregates
        if item[value_key] is not None
    ]
    values = [value for _, value, _ in points]
    values_for_scale = values + ([threshold] if threshold is not None else [])
    y_min = min(0.0, min(values_for_scale, default=0.0))
    y_max = max(values_for_scale, default=1.0)
    if y_max == y_min:
        y_max = y_min + 1.0
    padding = (y_max - y_min) * 0.12
    y_min -= padding
    y_max += padding
    x_min, x_max = min(levels, default=1), max(levels, default=1)

    def x_pos(k: int) -> float:
        if x_max == x_min:
            return left + plot_width / 2
        return left + (k - x_min) * plot_width / (x_max - x_min)

    def y_pos(value: float) -> float:
        return top + (y_max - value) * plot_height / (y_max - y_min)

    elements = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        '<style>text{font-family:Arial,sans-serif;fill:#263238}.grid{stroke:#dfe5e8;stroke-width:1}.axis{stroke:#263238;stroke-width:1.5}.curve{fill:none;stroke:#1565c0;stroke-width:3}.error{stroke:#78909c;stroke-width:1.5}.fail{stroke:#c62828;stroke-width:3}</style>',
        f'<text x="{width / 2}" y="34" text-anchor="middle" font-size="22" font-weight="bold">{escape(title)}</text>',
    ]

    for index in range(6):
        value = y_min + (y_max - y_min) * index / 5
        y = y_pos(value)
        elements.extend(
            [
                f'<line class="grid" x1="{left}" y1="{y:.1f}" x2="{width - right}" y2="{y:.1f}"/>',
                f'<text x="{left - 12}" y="{y + 5:.1f}" text-anchor="end" font-size="13">{value:.2f}</text>',
            ]
        )

    elements.extend(
        [
            f'<line class="axis" x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_height}"/>',
            f'<line class="axis" x1="{left}" y1="{top + plot_height}" x2="{width - right}" y2="{top + plot_height}"/>',
            f'<text x="{left + plot_width / 2}" y="{height - 34}" text-anchor="middle" font-size="16">k concurrent tasks / 1 RUNNING slot</text>',
            f'<text x="25" y="{top + plot_height / 2}" transform="rotate(-90 25 {top + plot_height / 2})" text-anchor="middle" font-size="16">{escape(y_label)}</text>',
        ]
    )

    for k in levels:
        x = x_pos(k)
        elements.extend(
            [
                f'<line class="axis" x1="{x:.1f}" y1="{top + plot_height}" x2="{x:.1f}" y2="{top + plot_height + 6}"/>',
                f'<text x="{x:.1f}" y="{top + plot_height + 25}" text-anchor="middle" font-size="13">{k}:1</text>',
            ]
        )

    if threshold is not None:
        y = y_pos(threshold)
        elements.extend(
            [
                f'<line x1="{left}" y1="{y:.1f}" x2="{width - right}" y2="{y:.1f}" stroke="#ef6c00" stroke-width="2" stroke-dasharray="7 5"/>',
                f'<text x="{width - right - 5}" y="{y - 7:.1f}" text-anchor="end" font-size="12" fill="#ef6c00">threshold {threshold:.1f}%</text>',
            ]
        )

    if len(points) >= 2:
        coordinates = " ".join(
            f"{x_pos(k):.1f},{y_pos(value):.1f}" for k, value, _ in points
        )
        elements.append(f'<polyline class="curve" points="{coordinates}"/>')

    for k, value, item in points:
        x, y = x_pos(k), y_pos(value)
        color = "#2e7d32" if item["all_repeats_successful"] else "#ef6c00"
        radius = 8 if k == selected_k else 6
        elements.extend(
            [
                f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{radius}" fill="{color}" stroke="white" stroke-width="2"/>',
                f'<text x="{x:.1f}" y="{y - 13:.1f}" text-anchor="middle" font-size="12">{value:.2f} ({item["successful"]}/{item["expected_repeats"]})</text>',
            ]
        )

    for item in aggregates:
        if item["attempted"] and not item["all_repeats_successful"]:
            x, y = x_pos(int(item["k"])), top + 18
            elements.extend(
                [
                    f'<line class="fail" x1="{x - 6:.1f}" y1="{y - 6:.1f}" x2="{x + 6:.1f}" y2="{y + 6:.1f}"/>',
                    f'<line class="fail" x1="{x + 6:.1f}" y1="{y - 6:.1f}" x2="{x - 6:.1f}" y2="{y + 6:.1f}"/>',
                ]
            )

    elements.append("</svg>")
    path.write_text("\n".join(elements) + "\n", encoding="utf-8")


def write_decision_svg(
    path: Path,
    *,
    aggregates: list[dict[str, Any]],
    threshold: float | None,
    selected: dict[str, Any] | None,
) -> None:
    """Draw degradation and throughput gain together with separate Y axes."""

    width, height = 1100, 650
    left, right, top, bottom = 105, 105, 85, 100
    plot_width = width - left - right
    plot_height = height - top - bottom
    levels = [int(item["k"]) for item in aggregates]
    points = [
        item
        for item in aggregates
        if item["time_degradation_pct"] is not None
        and item["throughput_gain_pct"] is not None
    ]
    x_min, x_max = min(levels, default=1), max(levels, default=1)
    degradation_values = [
        float(item["time_degradation_pct"]) for item in points
    ]
    throughput_values = [float(item["throughput_gain_pct"]) for item in points]
    degradation_scale_values = [0.0] + degradation_values
    if threshold is not None:
        degradation_scale_values.append(threshold)
    degradation_max = max(degradation_scale_values)
    throughput_max = max([0.0] + throughput_values)
    degradation_max = max(1.0, degradation_max * 1.15)
    throughput_max = max(1.0, throughput_max * 1.15)

    def x_pos(k: int) -> float:
        if x_max == x_min:
            return left + plot_width / 2
        return left + (k - x_min) * plot_width / (x_max - x_min)

    def degradation_y(value: float) -> float:
        return top + plot_height - value * plot_height / degradation_max

    def throughput_y(value: float) -> float:
        return top + plot_height - value * plot_height / throughput_max

    elements = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        '<style>text{font-family:Arial,sans-serif;fill:#263238}.grid{stroke:#dfe5e8;stroke-width:1}.axis{stroke:#263238;stroke-width:1.5}.degradation{fill:none;stroke:#ef6c00;stroke-width:3}.throughput{fill:none;stroke:#1565c0;stroke-width:3}.fail{stroke:#c62828;stroke-width:3}</style>',
        f'<text x="{width / 2}" y="34" text-anchor="middle" font-size="22" font-weight="bold">Oversubscription decision: latency cost vs throughput gain</text>',
        f'<line x1="{left + 80}" y1="58" x2="{left + 125}" y2="58" stroke="#ef6c00" stroke-width="3"/>',
        f'<text x="{left + 133}" y="63" font-size="13">Time degradation (left axis)</text>',
        f'<line x1="{left + 380}" y1="58" x2="{left + 425}" y2="58" stroke="#1565c0" stroke-width="3"/>',
        f'<text x="{left + 433}" y="63" font-size="13">Throughput gain (right axis)</text>',
    ]

    for index in range(6):
        fraction = index / 5
        y = top + plot_height - fraction * plot_height
        degradation_tick = degradation_max * fraction
        throughput_tick = throughput_max * fraction
        elements.extend(
            [
                f'<line class="grid" x1="{left}" y1="{y:.1f}" x2="{width - right}" y2="{y:.1f}"/>',
                f'<text x="{left - 12}" y="{y + 5:.1f}" text-anchor="end" font-size="13" fill="#ef6c00">{degradation_tick:.1f}%</text>',
                f'<text x="{width - right + 12}" y="{y + 5:.1f}" font-size="13" fill="#1565c0">{throughput_tick:.1f}%</text>',
            ]
        )

    elements.extend(
        [
            f'<line class="axis" x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_height}"/>',
            f'<line class="axis" x1="{width - right}" y1="{top}" x2="{width - right}" y2="{top + plot_height}"/>',
            f'<line class="axis" x1="{left}" y1="{top + plot_height}" x2="{width - right}" y2="{top + plot_height}"/>',
            f'<text x="24" y="{top + plot_height / 2}" transform="rotate(-90 24 {top + plot_height / 2})" text-anchor="middle" font-size="16" fill="#ef6c00">Time degradation (%)</text>',
            f'<text x="{width - 24}" y="{top + plot_height / 2}" transform="rotate(90 {width - 24} {top + plot_height / 2})" text-anchor="middle" font-size="16" fill="#1565c0">Throughput gain (%)</text>',
            f'<text x="{left + plot_width / 2}" y="{height - 35}" text-anchor="middle" font-size="16">Oversubscription ratio (k concurrent tasks : 1 RUNNING slot)</text>',
        ]
    )

    for k in levels:
        x = x_pos(k)
        elements.extend(
            [
                f'<line class="axis" x1="{x:.1f}" y1="{top + plot_height}" x2="{x:.1f}" y2="{top + plot_height + 6}"/>',
                f'<text x="{x:.1f}" y="{top + plot_height + 25}" text-anchor="middle" font-size="13">{k}:1</text>',
            ]
        )

    if threshold is not None:
        threshold_y = degradation_y(threshold)
        elements.extend(
            [
                f'<line x1="{left}" y1="{threshold_y:.1f}" x2="{width - right}" y2="{threshold_y:.1f}" stroke="#ef6c00" stroke-width="2" stroke-dasharray="7 5" opacity="0.7"/>',
                f'<text x="{left + 6}" y="{threshold_y - 7:.1f}" font-size="12" fill="#ef6c00">allowed degradation {threshold:.1f}%</text>',
            ]
        )

    if len(points) >= 2:
        degradation_coordinates = " ".join(
            f"{x_pos(int(item['k'])):.1f},{degradation_y(float(item['time_degradation_pct'])):.1f}"
            for item in points
        )
        throughput_coordinates = " ".join(
            f"{x_pos(int(item['k'])):.1f},{throughput_y(float(item['throughput_gain_pct'])):.1f}"
            for item in points
        )
        elements.extend(
            [
                f'<polyline class="degradation" points="{degradation_coordinates}"/>',
                f'<polyline class="throughput" points="{throughput_coordinates}"/>',
            ]
        )

    for item in points:
        k = int(item["k"])
        x = x_pos(k)
        degradation = float(item["time_degradation_pct"])
        throughput = float(item["throughput_gain_pct"])
        elements.extend(
            [
                f'<circle cx="{x:.1f}" cy="{degradation_y(degradation):.1f}" r="5" fill="#ef6c00" stroke="white" stroke-width="1.5"/>',
                f'<circle cx="{x:.1f}" cy="{throughput_y(throughput):.1f}" r="5" fill="#1565c0" stroke="white" stroke-width="1.5"/>',
            ]
        )

    if selected is not None:
        selected_k = int(selected["k"])
        x = x_pos(selected_k)
        label = (
            f"selected {selected_k}:1 | degradation "
            f"{float(selected['time_degradation_pct']):.2f}% | throughput gain "
            f"{float(selected['throughput_gain_pct']):.2f}%"
        )
        elements.extend(
            [
                f'<line x1="{x:.1f}" y1="{top}" x2="{x:.1f}" y2="{top + plot_height}" stroke="#2e7d32" stroke-width="3" stroke-dasharray="5 4"/>',
                f'<rect x="{left + 8}" y="{top + 8}" width="520" height="30" rx="5" fill="#e8f5e9" stroke="#2e7d32"/>',
                f'<text x="{left + 20}" y="{top + 29}" font-size="14" font-weight="bold" fill="#2e7d32">{escape(label)}</text>',
            ]
        )

    for item in aggregates:
        if item["attempted"] and not item["all_repeats_successful"]:
            x, y = x_pos(int(item["k"])), top + plot_height - 12
            elements.extend(
                [
                    f'<line class="fail" x1="{x - 6:.1f}" y1="{y - 6:.1f}" x2="{x + 6:.1f}" y2="{y + 6:.1f}"/>',
                    f'<line class="fail" x1="{x + 6:.1f}" y1="{y - 6:.1f}" x2="{x - 6:.1f}" y2="{y + 6:.1f}"/>',
                ]
            )

    elements.append("</svg>")
    path.write_text("\n".join(elements) + "\n", encoding="utf-8")


def add_decision_point_labels(
    path: Path,
    aggregates: list[dict[str, Any]],
    threshold: float | None,
) -> None:
    """为合并图中的每个劣化点和吞吐点添加具体数值。"""

    width, height = 1100, 650
    left, right, top, bottom = 105, 105, 85, 100
    plot_width = width - left - right
    plot_height = height - top - bottom
    levels = [int(item["k"]) for item in aggregates]
    points = [
        item
        for item in aggregates
        if item["time_degradation_pct"] is not None
        and item["throughput_gain_pct"] is not None
    ]
    if not levels or not points:
        return

    x_min, x_max = min(levels), max(levels)
    degradation_values = [
        float(item["time_degradation_pct"]) for item in points
    ]
    throughput_values = [float(item["throughput_gain_pct"]) for item in points]
    degradation_scale_values = [0.0] + degradation_values
    if threshold is not None:
        degradation_scale_values.append(threshold)
    degradation_max = max(1.0, max(degradation_scale_values) * 1.15)
    throughput_max = max(1.0, max([0.0] + throughput_values) * 1.15)

    def x_pos(k: int) -> float:
        if x_max == x_min:
            return left + plot_width / 2
        return left + (k - x_min) * plot_width / (x_max - x_min)

    def degradation_y(value: float) -> float:
        return top + plot_height - value * plot_height / degradation_max

    def throughput_y(value: float) -> float:
        return top + plot_height - value * plot_height / throughput_max

    labels = ['<g id="point-value-labels">']
    for item in points:
        k = int(item["k"])
        degradation = float(item["time_degradation_pct"])
        throughput = float(item["throughput_gain_pct"])
        x = x_pos(k)
        degradation_label_y = max(top + 14, degradation_y(degradation) - 12)
        throughput_label_y = min(
            top + plot_height - 5,
            throughput_y(throughput) + 19,
        )
        labels.extend(
            [
                (
                    f'<text x="{x:.1f}" y="{degradation_label_y:.1f}" '
                    'text-anchor="middle" font-size="12" font-weight="bold" '
                    'style="fill:#ef6c00;paint-order:stroke;stroke:white;'
                    'stroke-width:3px;stroke-linejoin:round">'
                    f'{degradation:.2f}%</text>'
                ),
                (
                    f'<text x="{x:.1f}" y="{throughput_label_y:.1f}" '
                    'text-anchor="middle" font-size="12" font-weight="bold" '
                    'style="fill:#1565c0;paint-order:stroke;stroke:white;'
                    'stroke-width:3px;stroke-linejoin:round">'
                    f'{throughput:.2f}%</text>'
                ),
            ]
        )
    labels.append("</g>")

    text = path.read_text(encoding="utf-8")
    text = text.replace("</svg>", "\n".join(labels) + "\n</svg>")
    path.write_text(text, encoding="utf-8")


def write_outputs(
    output_root: Path,
    *,
    args: argparse.Namespace,
    records: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    aggregates = aggregate_trials(args.levels, records, args.repeats)
    best = select_best_k(aggregates, args.max_degradation_pct)
    write_csv(output_root / "trials.csv", records)
    write_aggregate_csv(output_root / "aggregates.csv", aggregates)
    report = {
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "configuration": {
            "repo_dir": str(args.repo_dir.expanduser().resolve()),
            "trajectory_dir": str(args.trajectory_dir.expanduser().resolve()),
            "template": args.template,
            "levels": args.levels,
            "repeats": args.repeats,
            "running_concurrency": 1,
            "target_count_rule": "target_count = k",
            "concurrency_rule": "concurrency = k",
            "max_degradation_pct": args.max_degradation_pct,
            "control_plane_qps": args.control_plane_qps,
            "control_plane_max_inflight": args.control_plane_max_inflight,
            "cooldown_sec": args.cooldown_sec,
        },
        "trials": records,
        "aggregates": aggregates,
        "selected_k": best,
    }
    (output_root / "benchmark-summary.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    selected_k = int(best["k"]) if best else None
    write_line_svg(
        output_root / "completion-time-curve.svg",
        aggregates=aggregates,
        value_key="median_elapsed_sec",
        title="k 个并发任务的全部任务完成总时长",
        y_label="全部任务完成总时长（秒）",
        selected_k=selected_k,
    )
    write_line_svg(
        output_root / "time-degradation-curve.svg",
        aggregates=aggregates,
        value_key="time_degradation_pct",
        title="相对单任务基线（k=1）的总时长劣化",
        y_label="时间劣化率（%）",
        threshold=args.max_degradation_pct,
        selected_k=selected_k,
    )
    write_line_svg(
        output_root / "throughput-gain-curve.svg",
        aggregates=aggregates,
        value_key="throughput_gain_pct",
        title="相对单任务基线（k=1）的吞吐提升",
        y_label="吞吐提升率（%）",
        selected_k=selected_k,
    )
    write_decision_svg(
        output_root / "oversubscription-decision-curve.svg",
        aggregates=aggregates,
        threshold=args.max_degradation_pct,
        selected=best,
    )
    for chart_name in (
        "completion-time-curve.svg",
        "time-degradation-curve.svg",
        "throughput-gain-curve.svg",
        "oversubscription-decision-curve.svg",
    ):
        localize_svg(output_root / chart_name)
    add_decision_point_labels(
        output_root / "oversubscription-decision-curve.svg",
        aggregates,
        args.max_degradation_pct,
    )
    return aggregates, best


def main() -> int:
    args = build_parser().parse_args()
    try:
        validate_args(args)
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    output_root = (
        args.output_root.expanduser().resolve()
        if args.output_root
        else (args.repo_dir / "runs" / f"k-degradation-{timestamp}").resolve()
    )

    if args.dry_run:
        degradation_text = (
            f"{args.max_degradation_pct:g}%"
            if args.max_degradation_pct is not None
            else "not set (measurement only)"
        )
        print(
            f"Levels (ascending): {args.levels}\n"
            f"Repeats: {args.repeats}\n"
            "For each k: target-count=k, concurrency=k, RUNNING=1\n"
            f"Maximum accepted degradation: {degradation_text}\n"
            f"Control-plane QPS: {args.control_plane_qps:g}\n"
            f"Control-plane max in-flight: {args.control_plane_max_inflight}\n"
            f"Output root: {output_root}\n"
        )
        for k in args.levels:
            for repeat in range(1, args.repeats + 1):
                trial_dir = output_root / f"k-{k:03d}" / f"repeat-{repeat:02d}"
                print(shlex.join(command_for_trial(args, k=k, output_dir=trial_dir)))
        return 0

    output_root.mkdir(parents=True, exist_ok=True)
    records: list[dict[str, Any]] = []
    stopped_early = False
    total_trials = len(args.levels) * args.repeats
    trial_number = 0
    try:
        for k in args.levels:
            for repeat in range(1, args.repeats + 1):
                trial_number += 1
                if trial_number > 1 and args.cooldown_sec:
                    print(
                        f"[cooldown] sleeping {args.cooldown_sec:.1f}s before "
                        f"trial {trial_number}/{total_trials}",
                        flush=True,
                    )
                    time.sleep(args.cooldown_sec)
                trial_dir = output_root / f"k-{k:03d}" / f"repeat-{repeat:02d}"
                record = run_trial(
                    args, k=k, repeat=repeat, output_dir=trial_dir
                )
                records.append(record)
                write_outputs(output_root, args=args, records=records)
                if not record["valid"] and args.stop_on_failure:
                    print(
                        "[stop] failed trial detected; stopping to avoid backend "
                        "carry-over from uncertain sandboxes",
                        file=sys.stderr,
                        flush=True,
                    )
                    stopped_early = True
                    break
            if stopped_early:
                break
    except KeyboardInterrupt:
        print("\n[interrupted] writing partial results", file=sys.stderr)
        write_outputs(output_root, args=args, records=records)
        return 130

    aggregates, best = write_outputs(output_root, args=args, records=records)
    print(f"\nResults:    {output_root}")
    print(f"Trials:    {output_root / 'trials.csv'}")
    print(f"Aggregates:{output_root / 'aggregates.csv'}")
    print(f"JSON:      {output_root / 'benchmark-summary.json'}")
    print(f"Duration:  {output_root / 'completion-time-curve.svg'}")
    print(f"Degrade:   {output_root / 'time-degradation-curve.svg'}")
    print(f"Throughput:{output_root / 'throughput-gain-curve.svg'}")
    print(f"Decision:  {output_root / 'oversubscription-decision-curve.svg'}")
    if best:
        print(
            f"Selected k={best['k']} ({best['oversubscription_ratio']}), "
            f"degradation={best['time_degradation_pct']:.3f}%, "
            f"throughput_gain={best['throughput_gain_pct']:.3f}%"
        )
    else:
        completed = sum(1 for item in aggregates if item["all_repeats_successful"])
        if args.max_degradation_pct is None:
            print(
                "No k selected because no degradation SLA was configured; "
                "the report contains measurement curves only."
            )
        else:
            print(
                "No k selected: baseline or complete zero-failure results are "
                f"missing ({completed}/{len(args.levels)} levels complete)."
            )
    return 1 if stopped_early or any(not item["valid"] for item in records) else 0


if __name__ == "__main__":
    raise SystemExit(main())
