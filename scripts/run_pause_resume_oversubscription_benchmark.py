#!/usr/bin/env python3
"""Run the N-running pause/resume oversubscription benchmark.

For every ratio k (1..12 by default), execute k*N replay tasks with k*N
logical workers and exactly N RUNNING slots.  replay-aenv owns the lifecycle:

    create -> initialize -> pause
    FIFO acquire -> resume -> command -> pause -> release

Source trajectories are selected by natural filename order.  The first N are
used when enough files exist; otherwise all files are cycled until each trial
contains k*N tasks.  Every task is an independent sandbox execution.

The driver writes restart-safe JSON/CSV data after every trial.  A separate
artifact-tool exporter converts these files to the final XLSX workbook.  Each
executed trial also runs the observability collector and generates its README
reports (XLSX, Markdown, Gantt PNG, and pause/resume PNG) after collection stops.
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime
import json
import os
from pathlib import Path
import re
import shlex
import signal
import statistics
import subprocess
import sys
import time
from typing import Any, Iterable, TextIO


DEFAULT_REPO_DIR = Path(__file__).parent.parent
DEFAULT_TRAJECTORY_DIR = DEFAULT_REPO_DIR / "delay_time_trajectories"
DEFAULT_RATIOS = "1,2,3,4,5,6,7,8,9,10,11,12"
DEFAULT_REPEATS = 2
DEFAULT_API_URL = "http://127.0.0.1:8000"
DEFAULT_API_KEY_FILE = Path("/var/lib/aenv/secrets/api-key")
DEFAULT_COMMAND_TRANSPORT = "aenv-proxy"
DEFAULT_ENVD_PORT = 49_983
DEFAULT_CONTROL_PLANE_QPS = 1000.0
DEFAULT_CONTROL_PLANE_MAX_INFLIGHT = 1024
DEFAULT_OBSERVABILITY_INTERVAL_SEC = 1.0
OBSERVABILITY_STARTUP_TIMEOUT_SEC = 30.0
OBSERVABILITY_STOP_TIMEOUT_SEC = 30.0


def cleanup_all_sandboxes(args: argparse.Namespace) -> int:
    """Kill every sandbox on the AgentENV server via the aenv CLI."""
    configure_api_key(args)
    try:
        result = subprocess.run(
            ["aenv", "list", "--output", "json"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            print(f"[cleanup] aenv list failed: {result.stderr.strip()[:300]}", file=sys.stderr, flush=True)
            return 0
        sandboxes = json.loads(result.stdout)
    except Exception as exc:
        print(f"[cleanup] Error listing sandboxes: {exc}", file=sys.stderr, flush=True)
        return 0
    if not sandboxes:
        print("[cleanup] No sandboxes to clean up", flush=True)
        return 0
    killed = 0
    print(f"[cleanup] Killing {len(sandboxes)} sandbox(es)", flush=True)
    for sb in sandboxes:
        sid = sb.get("sandboxID") or sb.get("sandbox_id")
        if not sid:
            continue
        try:
            subprocess.run(["aenv", "delete", sid], capture_output=True, timeout=30)
            killed += 1
        except Exception:
            pass
    print(f"[cleanup] Cleaned up {killed}/{len(sandboxes)} sandbox(es)", flush=True)
    return killed


def configure_api_key(args: argparse.Namespace) -> str:
    """Ensure replay subprocesses inherit an API key without logging its value."""
    if os.environ.get("E2B_API_KEY"):
        return "environment:E2B_API_KEY"
    key_path = args.api_key_file.expanduser().resolve()
    if not key_path.is_file():
        raise FileNotFoundError(
            "E2B_API_KEY is unset and API key file was not found: "
            f"{key_path}"
        )
    key = key_path.read_text(encoding="utf-8").strip()
    if not key:
        raise ValueError(f"API key file is empty: {key_path}")
    os.environ["E2B_API_KEY"] = key
    return f"file:{key_path}"

TRIAL_COLUMNS = [
    "ratio",
    "repeat",
    "oversubscription_ratio",
    "running_concurrency",
    "target_count",
    "concurrency",
    "valid",
    "batch_elapsed_sec",
    "driver_wall_sec",
    "throughput_tasks_per_sec",
    "total",
    "succeeded",
    "failed",
    "running_peak",
    "running_granted",
    "run_queue_wait_avg_sec",
    "control_wait_avg_sec",
    "control_wait_max_sec",
    "return_code",
    "reused",
    "started_at",
    "completed_at",
    "error",
    "output_dir",
    "summary_path",
    "observability_dir",
    "observability_return_code",
    "observability_error",
    "observability_report_return_code",
    "observability_report_error",
]

TASK_COLUMNS = [
    "ratio",
    "repeat",
    "oversubscription_ratio",
    "task_index",
    "source_index",
    "cycle",
    "occurrence",
    "source_trajectory",
    "staged_trajectory",
    "instance_id",
    "template",
    "sandbox_id",
    "started_at",
    "completed_at",
    "elapsed_sec",
    "replay_completed",
    "succeeded",
    "submitted",
    "commands",
    "succeeded_commands",
    "failed_commands",
    "requested_delay_sec",
    "actual_paused_sec",
    "pause_sec_sum",
    "paused_sec_sum",
    "capacity_wait_sec_sum",
    "run_queue_wait_sec_sum",
    "resume_sec_sum",
    "resume_total_sec_sum",
    "command_sec_sum",
    "running_slot_held_sec_sum",
    "interaction_total_sec_sum",
    "post_command_pause_sec_sum",
    "resume_attempts_sum",
    "step_count",
    "error",
    "output_dir",
    "result_path",
]

STEP_COLUMNS = [
    "ratio",
    "repeat",
    "oversubscription_ratio",
    "task_index",
    "source_trajectory",
    "instance_id",
    "sandbox_id",
    "step_index",
    "action",
    "started_at",
    "completed_at",
    "elapsed_sec",
    "succeeded",
    "exit_code",
    "pause_started_at",
    "pause_completed_at",
    "pause_sec",
    "pause_scheduler_wait_sec",
    "pause_api_sec",
    "paused_sec",
    "trajectory_delay_sec",
    "configured_extra_delay_sec",
    "requested_delay_sec",
    "run_queue_entered_at",
    "run_slot_acquired_at",
    "run_queue_wait_sec",
    "run_slot_contention_wait_sec",
    "capacity_wait_sec",
    "resume_started_at",
    "resume_completed_at",
    "resume_sec",
    "resume_total_sec",
    "resume_attempts",
    "resume_scheduler_wait_sec",
    "resume_api_sec",
    "command_sec",
    "running_slot_held_sec",
    "interaction_total_sec",
    "post_command_pause_started_at",
    "post_command_pause_completed_at",
    "post_command_pause_sec",
    "post_command_pause_scheduler_wait_sec",
    "post_command_pause_api_sec",
    "infrastructure_error",
]

RATIO_COLUMNS = [
    "ratio",
    "oversubscription_ratio",
    "running_concurrency",
    "target_count",
    "expected_repeats",
    "attempted",
    "successful",
    "all_repeats_successful",
    "median_batch_elapsed_sec",
    "average_batch_elapsed_sec",
    "min_batch_elapsed_sec",
    "max_batch_elapsed_sec",
    "median_throughput_tasks_per_sec",
    "time_degradation_vs_1_1_pct",
    "throughput_gain_vs_1_1_pct",
]


def natural_key(value: str) -> list[tuple[int, int | str]]:
    """Return a deterministic case-insensitive natural-sort key."""

    parts: list[tuple[int, int | str]] = []
    for part in re.split(r"(\d+)", value.casefold()):
        if part.isdigit():
            parts.append((0, int(part)))
        else:
            parts.append((1, part))
    return parts


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return parsed


def nonnegative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be a non-negative integer")
    return parsed


def nonnegative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_ratios(value: str) -> list[int]:
    ratios = sorted({positive_int(raw.strip()) for raw in value.split(",") if raw.strip()})
    if not ratios:
        raise argparse.ArgumentTypeError("at least one ratio is required")
    return ratios


def build_parser() -> argparse.ArgumentParser:
    env_n = os.environ.get("N_RUNNING")
    parser = argparse.ArgumentParser(
        description=(
            "Run k*N independent replay tasks over N RUNNING slots for each k."
        )
    )
    parser.add_argument("--repo-dir", type=Path, default=DEFAULT_REPO_DIR)
    parser.add_argument("--trajectory-dir", type=Path, default=DEFAULT_TRAJECTORY_DIR)
    parser.add_argument(
        "--api-url",
        default=os.environ.get("E2B_API_URL", DEFAULT_API_URL),
        help=(
            "AgentENV E2B-compatible lifecycle API URL "
            f"(default: E2B_API_URL or {DEFAULT_API_URL})"
        ),
    )
    parser.add_argument(
        "--api-key-file",
        type=Path,
        default=Path(
            os.environ.get("E2B_API_KEY_FILE", str(DEFAULT_API_KEY_FILE))
        ),
        help=(
            "Read E2B_API_KEY from this file when it is absent from the "
            "environment (default: E2B_API_KEY_FILE or "
            f"{DEFAULT_API_KEY_FILE})"
        ),
    )
    parser.add_argument(
        "--image-map",
        type=Path,
        help=(
            "JSON/CSV mapping from trajectory filename/path to its image/template. "
            "When omitted, read template/image from trajectory metadata."
        ),
    )
    parser.add_argument(
        "--workdir",
        default="/testbed",
        help="Working directory inside each sandbox (default: /testbed)",
    )
    parser.add_argument(
        "--command-transport",
        choices=("direct-envd", "aenv-proxy"),
        default=DEFAULT_COMMAND_TRANSPORT,
        help=(
            "Command and file data-plane transport. direct-envd resolves the "
            "route from local Firecracker MMDS/network state and bypasses the "
            "AgentENV command proxy; "
            "aenv-proxy uses the standard AgentENV proxy path (default: aenv-proxy)"
        ),
    )
    parser.add_argument(
        "--direct-envd-port",
        type=positive_int,
        default=DEFAULT_ENVD_PORT,
        help=f"envd port for direct-envd transport (default: {DEFAULT_ENVD_PORT})",
    )
    parser.add_argument(
        "--running-concurrency",
        "--n-running",
        dest="running_concurrency",
        type=positive_int,
        default=positive_int(env_n) if env_n else None,
        help="N RUNNING slots; alternatively set environment variable N_RUNNING",
    )
    parser.add_argument(
        "--ratios",
        type=parse_ratios,
        default=parse_ratios(DEFAULT_RATIOS),
        help=f"Comma-separated k values (default: {DEFAULT_RATIOS})",
    )
    parser.add_argument("--repeats", type=positive_int, default=DEFAULT_REPEATS)
    parser.add_argument(
        "--lifecycle-timeout-sec",
        "--request-timeout-sec",
        dest="lifecycle_timeout_sec",
        type=positive_int,
        default=300,
        help=(
            "Timeout for create, pause, resume, and SDK API requests "
            "passed to replay_agent.py (default: 300)"
        ),
    )
    parser.add_argument(
        "--command-timeout-sec",
        type=nonnegative_int,
        default=300,
        help=(
            "Timeout for each trajectory command passed to replay_agent.py "
            "(default: 300; 0 disables)"
        ),
    )
    parser.add_argument(
        "--command-timeout-continue",
        action="store_true",
        help=(
            "Continue each replay after a trajectory command timeout; the "
            "timed-out command is recorded as failed"
        ),
    )
    parser.add_argument(
        "--control-plane-qps",
        type=float,
        default=DEFAULT_CONTROL_PLANE_QPS,
    )
    parser.add_argument(
        "--control-plane-max-inflight",
        type=positive_int,
        default=DEFAULT_CONTROL_PLANE_MAX_INFLIGHT,
    )
    parser.add_argument("--launch-interval-sec", type=nonnegative_float, default=0.0)
    parser.add_argument("--pause-duration-sec", type=nonnegative_float, default=0.0)
    parser.add_argument(
        "--extra-delay-sec",
        type=nonnegative_float,
        default=0.0,
        help="Additional running wait before every trajectory command (default: 0)",
    )
    parser.add_argument(
        "--delay",
        type=nonnegative_float,
        default=None,
        help="Override all trajectory delay_time with this fixed value in seconds; "
        "when omitted, use each step's recorded delay_time",
    )
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Stop a trajectory after a command returns nonzero (default: continue)",
    )
    parser.add_argument(
        "--noise-fill",
        action="store_true",
        default=False,
        help=(
            "Start noise VMs to maintain constant concurrency when real tasks "
            "finish early. Uses replay_agent_noise.py instead of "
            "replay_agent.py."
        ),
    )
    parser.add_argument(
        "--rollout",
        type=int,
        default=1,
        help=(
            "Number of rollout rounds (each worker repeats its assigned "
            "trajectory). Only effective with --noise-fill. (default: 1)"
        ),
    )
    parser.add_argument(
        "--ready-probe-timeout-sec",
        type=float,
        default=10.0,
        help="Timeout per envd ready probe attempt (default: 10)",
    )
    parser.add_argument(
        "--ready-probe-attempts",
        type=int,
        default=5,
        help="Number of envd ready probe attempts (default: 5)",
    )
    parser.add_argument("--cooldown-sec", type=nonnegative_float, default=30.0)
    parser.add_argument(
        "--observability",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Run scripts/observability/spotbox_monitor.py for every executed trial "
            "(default: true; disable with --no-observability)"
        ),
    )
    parser.add_argument(
        "--observability-interval-sec",
        type=positive_float,
        default=DEFAULT_OBSERVABILITY_INTERVAL_SEC,
        help=(
            "Host metrics sampling interval for each trial monitor "
            f"(default: {DEFAULT_OBSERVABILITY_INTERVAL_SEC})"
        ),
    )
    parser.add_argument(
        "--observability-reports",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Generate README observability XLSX/Markdown/PNG reports after each "
            "executed trial (default: true; disable with --no-observability-reports)"
        ),
    )
    parser.add_argument(
        "--observability-report-python",
        default="python3",
        help="Python executable used by observability report scripts (default: python3)",
    )
    parser.add_argument(
        "--cgroup-name",
        default=None,
        help=(
            "Pre-created cgroup name for per-cgroup CPU/memory sampling "
            "(e.g. 'aenv'). When set, spotbox_monitor.py adds "
            "cgroup_cpu_cores / cgroup_mem_* fields to metrics_1s.jsonl. "
            "The cgroup must already exist under "
            "/sys/fs/cgroup/cpu,cpuacct/ and /sys/fs/cgroup/memory/. "
            "(default: none)"
        ),
    )
    parser.add_argument(
        "--cgroup-version",
        choices=("auto", "v1", "v2"),
        default="auto",
        help="Cgroup version to use with --cgroup-name (default: auto-detect)",
    )
    parser.add_argument(
        "--cpu-range",
        default=None,
        help=(
            "CPU range for per-core /proc/stat aggregation (e.g. '0-127'). "
            "When set, host CPU metrics (user/sys/guest) are aggregated only "
            "for the specified cores, giving cgroup-scoped accuracy at O(1) "
            "file read cost. Pair with cpuset cgroup binding. (default: none)"
        ),
    )
    parser.add_argument("--output-root", type=Path)
    parser.add_argument(
        "--reuse",
        action="store_true",
        help="Reuse trial directories containing batch-summary.json",
    )
    parser.add_argument(
        "--stop-on-failure",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Stop after the first invalid trial (default: false)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Prepare the natural-order selection and print all commands only",
    )
    return parser


def read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"expected a JSON object in {path}")
    return value


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_csv(path: Path, columns: list[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def find_trajectories(directory: Path) -> list[Path]:
    if not directory.is_dir():
        raise NotADirectoryError(f"trajectory directory not found: {directory}")
    trajectories = sorted(
        (
            path.resolve()
            for path in directory.iterdir()
            if path.is_file() and path.name.endswith((".json", ".traj"))
        ),
        key=lambda path: natural_key(path.name),
    )
    if not trajectories:
        raise ValueError(f"no .json or .traj files found in {directory}")
    return trajectories


def load_image_map(path: Path | None) -> dict[str, str]:
    if path is None:
        return {}
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"image map not found: {resolved}")
    mapping: dict[str, str] = {}
    if resolved.suffix.casefold() == ".csv":
        with resolved.open("r", encoding="utf-8-sig", newline="") as handle:
            for row_number, row in enumerate(csv.DictReader(handle), start=2):
                key = str(
                    row.get("trajectory")
                    or row.get("trajectory_path")
                    or row.get("filename")
                    or ""
                ).strip()
                image = str(row.get("image") or row.get("template") or "").strip()
                if not key or not image:
                    raise ValueError(
                        f"image map CSV row {row_number} requires trajectory and image"
                    )
                mapping[key] = image
    else:
        raw = json.loads(resolved.read_text(encoding="utf-8"))
        if isinstance(raw, dict) and isinstance(raw.get("images"), dict):
            raw = raw["images"]
        if isinstance(raw, dict):
            for key, value in raw.items():
                image = (
                    value.get("image") or value.get("template")
                    if isinstance(value, dict)
                    else value
                )
                if not isinstance(image, str) or not image.strip():
                    raise ValueError(f"invalid image mapping for {key!r}")
                mapping[str(key)] = image.strip()
        elif isinstance(raw, list):
            for index, item in enumerate(raw, start=1):
                if not isinstance(item, dict):
                    raise ValueError(f"image map item {index} must be an object")
                key = item.get("trajectory") or item.get("trajectory_path") or item.get("filename")
                image = item.get("image") or item.get("template")
                if not isinstance(key, str) or not isinstance(image, str) or not key.strip() or not image.strip():
                    raise ValueError(
                        f"image map item {index} requires trajectory and image"
                    )
                mapping[key.strip()] = image.strip()
        else:
            raise ValueError("image map JSON must be an object or array")
    return mapping


def image_from_trajectory(path: Path, mapping: dict[str, str]) -> str | None:
    for key in (str(path), path.name, path.stem):
        if key in mapping:
            return mapping[key]
    raw = read_json(path)
    candidates = [
        raw.get("image"),
        raw.get("template"),
        (raw.get("replay_config") or {}).get("image")
        if isinstance(raw.get("replay_config"), dict)
        else None,
        (raw.get("replay_config") or {}).get("template")
        if isinstance(raw.get("replay_config"), dict)
        else None,
        (raw.get("info") or {}).get("image")
        if isinstance(raw.get("info"), dict)
        else None,
        (raw.get("info") or {}).get("template")
        if isinstance(raw.get("info"), dict)
        else None,
    ]
    return next(
        (value.strip() for value in candidates if isinstance(value, str) and value.strip()),
        None,
    )


def validate_args(
    args: argparse.Namespace,
) -> tuple[Path, Path, list[Path], dict[str, str]]:
    if args.running_concurrency is None:
        raise ValueError(
            "N is required: pass --running-concurrency N or set N_RUNNING=N"
        )
    if args.control_plane_qps <= 0:
        raise ValueError("control-plane-qps must be positive")
    if not 1 <= args.direct_envd_port <= 65_535:
        raise ValueError("direct-envd-port must be between 1 and 65535")
    configure_api_key(args)
    repo_dir = args.repo_dir.expanduser().resolve()
    trajectory_dir = args.trajectory_dir.expanduser().resolve()
    agent_script_name = (
        "replay_agent_noise.py"
        if getattr(args, "noise_fill", False)
        else "replay_agent.py"
    )
    runner = repo_dir / "agent" / agent_script_name
    if not runner.is_file():
        raise FileNotFoundError(f"replay agent script not found: {runner}")
    return repo_dir, trajectory_dir, find_trajectories(trajectory_dir), load_image_map(args.image_map)


def selection_records(
    all_trajectories: list[Path],
    running_concurrency: int,
    image_map: dict[str, str],
) -> list[dict[str, Any]]:
    source_pool = all_trajectories[: min(len(all_trajectories), running_concurrency)]
    records: list[dict[str, Any]] = []
    missing: list[str] = []
    for offset in range(running_concurrency):
        path = source_pool[offset % len(source_pool)]
        image = image_from_trajectory(path, image_map)
        if image is None:
            missing.append(path.name)
        records.append(
            {
                "selection_index": offset + 1,
                "source_trajectory": str(path),
                "source_filename": path.name,
                "image": image,
                "source_cycle": (offset // len(source_pool)) + 1,
            }
        )
    if missing:
        names = ", ".join(sorted(set(missing), key=natural_key)[:10])
        suffix = " ..." if len(set(missing)) > 10 else ""
        raise ValueError(
            "no image/template mapping for selected trajectories: " + names + suffix
        )
    return records


def prepare_selection(
    output_root: Path,
    records: list[dict[str, Any]],
) -> tuple[Path, dict[str, str]]:
    selection_dir = output_root / "selected-trajectories"
    manifest_path = output_root / "trajectory-selection.json"
    output_root.mkdir(parents=True, exist_ok=True)
    selection_dir.mkdir(parents=True, exist_ok=True)
    staged_to_source: dict[str, str] = {}
    for record in records:
        source = Path(str(record["source_trajectory"]))
        slot_dir = selection_dir / f"slot-{int(record['selection_index']):04d}"
        slot_dir.mkdir(parents=True, exist_ok=True)
        staged_name = f"{int(record['selection_index']):04d}__{source.name}"
        staged = slot_dir / staged_name
        if staged.exists() or staged.is_symlink():
            if not staged.is_symlink() or staged.resolve() != source:
                raise ValueError(f"unexpected existing staged trajectory: {staged}")
        else:
            staged.symlink_to(source)
        record["staged_trajectory"] = str(staged)
        record["trajectory_dir"] = str(slot_dir)
        staged_to_source[staged_name] = str(source)

    expected_manifest = {"natural_sort": True, "selected": records}
    if manifest_path.exists() and read_json(manifest_path) != expected_manifest:
        raise ValueError(
            f"existing trajectory selection differs from requested run: {manifest_path}"
        )
    write_json(manifest_path, expected_manifest)
    write_csv(
        output_root / "trajectory-selection.csv",
        [
            "selection_index",
            "source_filename",
            "source_trajectory",
            "staged_trajectory",
            "image",
            "source_cycle",
            "trajectory_dir",
        ],
        records,
    )
    return selection_dir, staged_to_source


def command_for_trial(
    args: argparse.Namespace,
    *,
    repo_dir: Path,
    mix_config_path: Path,
    ratio: int,
    output_dir: Path,
) -> list[str]:
    target_count = ratio * args.running_concurrency
    agent_script_name = (
        "replay_agent_noise.py"
        if getattr(args, "noise_fill", False)
        else "replay_agent.py"
    )
    command = [
        sys.executable,
        str(repo_dir / "agent" / agent_script_name),
        "--api-url",
        args.api_url,
        "mix",
        "--config",
        str(mix_config_path),
        "--concurrency",
        str(target_count),
        "--running-concurrency",
        str(args.running_concurrency),
        "--control-plane-qps",
        str(args.control_plane_qps),
        "--control-plane-max-inflight",
        str(args.control_plane_max_inflight),
        "--launch-interval-sec",
        str(args.launch_interval_sec),
        "--pause-duration-sec",
        str(args.pause_duration_sec),
        "--workdir",
        args.workdir,
        "--lifecycle-timeout-sec",
        str(args.lifecycle_timeout_sec),
        "--command-timeout-sec",
        str(args.command_timeout_sec),
        "--command-transport",
        args.command_transport,
        "--direct-envd-port",
        str(args.direct_envd_port),
        "--skip-instance-patch",
        "--output-dir",
        str(output_dir),
    ]
    if args.stop_on_error:
        command.append("--stop-on-error")
    if args.command_timeout_continue:
        command.append("--command-timeout-continue")
    if args.delay is not None:
        command.extend(["--delay", str(args.delay)])
    if getattr(args, "extra_delay_sec", 0.0) > 0:
        command.extend(["--extra-delay-sec", str(args.extra_delay_sec)])
    if getattr(args, "noise_fill", False):
        command.append("--noise-fill")
    if getattr(args, "rollout", 1) != 1:
        command.extend(["--rollout", str(args.rollout)])
    if getattr(args, "ready_probe_timeout_sec", 10.0) != 10.0:
        command.extend(["--ready-probe-timeout-sec", str(args.ready_probe_timeout_sec)])
    if getattr(args, "ready_probe_attempts", 5) != 5:
        command.extend(["--ready-probe-attempts", str(args.ready_probe_attempts)])
    return command


def write_mix_config(
    path: Path,
    *,
    selection: list[dict[str, Any]],
    ratio: int,
    concurrency: int,
) -> None:
    config = {
        "concurrency": concurrency,
        "workloads": [
            {
                "name": f"slot-{int(record['selection_index']):04d}",
                "template": record["image"],
                "trajectory_dir": record["trajectory_dir"],
                "vm_count": ratio,
            }
            for record in selection
        ],
    }
    write_json(path, config)


def log_tail(path: Path, max_chars: int = 2000) -> str:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    return text[-max_chars:].strip()


def stop_observability(
    monitor: tuple[subprocess.Popen[Any], TextIO, Path] | None,
) -> dict[str, Any]:
    if monitor is None:
        return {"return_code": None, "error": None}

    process, log_handle, log_path = monitor
    error: str | None = None
    was_running = process.poll() is None
    try:
        if was_running:
            process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=OBSERVABILITY_STOP_TIMEOUT_SEC)
            except subprocess.TimeoutExpired:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                error = "observability monitor did not stop gracefully"
    finally:
        log_handle.close()

    if not was_running:
        error = "observability monitor exited before trial completed"
    if process.returncode not in (0, None):
        tail = log_tail(log_path)
        exit_error = f"observability monitor exited with code {process.returncode}"
        if tail:
            exit_error = f"{exit_error}: {tail}"
        error = f"{error}; {exit_error}" if error else exit_error
    return {"return_code": process.returncode, "error": error}


def start_observability(
    args: argparse.Namespace,
    *,
    repo_dir: Path,
    output_dir: Path,
    environment: dict[str, str],
) -> tuple[subprocess.Popen[Any], TextIO, Path] | None:
    if not args.observability:
        return None

    monitor_script = repo_dir / "scripts" / "observability" / "spotbox_monitor.py"
    if not monitor_script.is_file():
        raise FileNotFoundError(f"observability monitor not found: {monitor_script}")

    observability_dir = output_dir / "observability"
    observability_dir.mkdir(parents=True, exist_ok=True)
    log_path = observability_dir / "monitor.log"
    command = [
        sys.executable,
        str(monitor_script),
        str(observability_dir),
        str(args.observability_interval_sec),
    ]
    if getattr(args, "cgroup_name", None):
        command.append(args.cgroup_name)
        if getattr(args, "cgroup_version", "auto") != "auto":
            command.append(args.cgroup_version)
    if getattr(args, "cpu_range", None):
        if not getattr(args, "cgroup_name", None):
            command.append("")  # empty cgroup_name placeholder
        command.append(getattr(args, "cgroup_version", "auto"))
        command.append(args.cpu_range)
    log_handle = log_path.open("w", encoding="utf-8")
    try:
        process = subprocess.Popen(
            command,
            cwd=repo_dir,
            env=environment,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            start_new_session=True,
        )
    except Exception:
        log_handle.close()
        raise

    monitor = (process, log_handle, log_path)
    events_path = observability_dir / "sandbox_events.jsonl"
    metrics_path = observability_dir / "metrics_1s.jsonl"
    deadline = time.monotonic() + OBSERVABILITY_STARTUP_TIMEOUT_SEC
    while time.monotonic() < deadline:
        return_code = process.poll()
        if return_code is not None:
            status = stop_observability(monitor)
            detail = status["error"] or log_tail(log_path)
            raise RuntimeError(
                f"observability monitor exited during startup with code {return_code}"
                + (f": {detail}" if detail else "")
            )
        try:
            if events_path.stat().st_size > 0 and metrics_path.stat().st_size > 0:
                print(
                    f"[observability] started pid={process.pid} dir={observability_dir}",
                    flush=True,
                )
                return monitor
        except FileNotFoundError:
            pass
        time.sleep(0.1)

    status = stop_observability(monitor)
    detail = status["error"] or log_tail(log_path)
    raise RuntimeError(
        f"observability monitor did not become ready within "
        f"{OBSERVABILITY_STARTUP_TIMEOUT_SEC:.0f}s"
        + (f": {detail}" if detail else "")
    )


def generate_observability_reports(
    args: argparse.Namespace,
    *,
    repo_dir: Path,
    output_dir: Path,
    environment: dict[str, str],
) -> dict[str, Any]:
    if not args.observability or not args.observability_reports:
        return {"return_code": None, "error": None}

    observability_dir = output_dir / "observability"
    report_log_path = observability_dir / "report-generation.log"
    report_scripts = repo_dir / "scripts" / "observability"
    result_dirs: list[Path] = []
    summary_path = output_dir / "batch-summary.json"
    if summary_path.is_file():
        summary = read_json(summary_path)
        result_dirs = sorted(
            {
                candidate
                for item in summary.get("items", [])
                if isinstance(item, dict) and item.get("output_dir")
                for candidate in [Path(str(item["output_dir"])).expanduser().resolve()]
                if (candidate / "replay-result.json").is_file()
            },
            key=lambda path: natural_key(str(path)),
        )

    commands: list[tuple[str, list[str]]] = []
    errors: list[str] = []
    if result_dirs:
        result_args = [str(path) for path in result_dirs]
        commands.extend(
            [
                (
                    "xlsx",
                    [
                        args.observability_report_python,
                        str(report_scripts / "make_obs_xlsx.py"),
                        str(observability_dir),
                        *result_args,
                    ],
                ),
                (
                    "markdown",
                    [
                        args.observability_report_python,
                        str(report_scripts / "make_obs_report.py"),
                        str(observability_dir),
                        *result_args,
                    ],
                ),
            ]
        )
    else:
        errors.append("no replay-result.json files found for XLSX/Markdown reports")

    aggregate_log = output_dir / "job.log"
    if aggregate_log.is_file():
        commands.append(
            (
                "pause-resume-plot",
                [
                    args.observability_report_python,
                    str(report_scripts / "analyze_pause_resume.py"),
                    str(observability_dir),
                    "--runlog",
                    str(aggregate_log),
                    "--plot",
                ],
            )
        )
    else:
        errors.append(f"aggregate job.log not found: {aggregate_log}")

    with report_log_path.open("w", encoding="utf-8") as log_handle:
        for label, command in commands:
            log_handle.write(f"[{label}] {shlex.join(command)}\n")
            log_handle.flush()
            try:
                completed = subprocess.run(
                    command,
                    cwd=repo_dir,
                    env=environment,
                    stdout=log_handle,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    check=False,
                )
            except OSError as exc:
                errors.append(f"{label} report could not start: {exc}")
                continue
            if completed.returncode != 0:
                errors.append(f"{label} report exited with code {completed.returncode}")

    expected = {
        "xlsx": observability_dir / "report.xlsx",
        "markdown": observability_dir / "report.md",
        "gantt": observability_dir / "gantt.png",
    }
    for label, path in expected.items():
        if result_dirs and not path.is_file():
            errors.append(f"{label} output was not produced: {path}")
    pause_resume_plots = [
        observability_dir / "pause_resume_multi.png",
        observability_dir / "pause_resume_cycle.png",
    ]
    if aggregate_log.is_file() and not any(path.is_file() for path in pause_resume_plots):
        errors.append("pause/resume PNG output was not produced")

    error = "; ".join(errors) if errors else None
    return {"return_code": 1 if errors else 0, "error": error}


def numeric_sum(steps: list[dict[str, Any]], key: str) -> float:
    return sum(float(step.get(key) or 0.0) for step in steps)


def collect_task_and_step_records(
    summary: dict[str, Any],
    *,
    ratio: int,
    repeat: int,
    staged_to_source: dict[str, str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    task_rows: list[dict[str, Any]] = []
    step_rows: list[dict[str, Any]] = []
    for item in summary.get("items", []):
        task_index = item.get("index")
        staged_path = Path(str(item.get("trajectory") or ""))
        source_path = staged_to_source.get(staged_path.name, str(staged_path))
        result_path = Path(str(item.get("output_dir") or "")) / "replay-result.json"
        result: dict[str, Any] = {}
        result_error: str | None = None
        if result_path.is_file():
            try:
                result = read_json(result_path)
            except Exception as exc:
                result_error = f"cannot parse replay-result.json: {type(exc).__name__}: {exc}"
        else:
            result_error = "replay-result.json was not produced"

        steps = [step for step in result.get("steps", []) if isinstance(step, dict)]
        task_error = item.get("error") or result.get("error") or result_error
        task_row = {
            "ratio": ratio,
            "repeat": repeat,
            "oversubscription_ratio": f"1:{ratio}",
            "task_index": task_index,
            "source_index": item.get("source_index"),
            "cycle": item.get("cycle"),
            "occurrence": item.get("occurrence"),
            "source_trajectory": source_path,
            "staged_trajectory": str(staged_path),
            "instance_id": result.get("instance_id"),
            "template": result.get("template") or item.get("template"),
            "sandbox_id": result.get("sandbox_id"),
            "started_at": result.get("started_at"),
            "completed_at": result.get("completed_at"),
            "elapsed_sec": result.get("elapsed_sec"),
            "replay_completed": result.get("replay_completed", item.get("replay_completed")),
            "succeeded": result.get("succeeded", item.get("succeeded")),
            "submitted": result.get("submitted", item.get("submitted")),
            "commands": result.get("commands", item.get("commands")),
            "succeeded_commands": result.get(
                "succeeded_commands", item.get("succeeded_commands")
            ),
            "failed_commands": result.get("failed_commands", item.get("failed_commands")),
            "requested_delay_sec": result.get("requested_delay_sec"),
            "actual_paused_sec": result.get("actual_paused_sec"),
            "pause_sec_sum": numeric_sum(steps, "pause_sec"),
            "paused_sec_sum": numeric_sum(steps, "paused_sec"),
            "capacity_wait_sec_sum": numeric_sum(steps, "capacity_wait_sec"),
            "run_queue_wait_sec_sum": numeric_sum(steps, "run_queue_wait_sec"),
            "resume_sec_sum": numeric_sum(steps, "resume_sec"),
            "resume_total_sec_sum": numeric_sum(steps, "resume_total_sec"),
            "command_sec_sum": numeric_sum(steps, "command_sec"),
            "running_slot_held_sec_sum": numeric_sum(steps, "running_slot_held_sec"),
            "interaction_total_sec_sum": numeric_sum(steps, "interaction_total_sec"),
            "post_command_pause_sec_sum": numeric_sum(steps, "post_command_pause_sec"),
            "resume_attempts_sum": numeric_sum(steps, "resume_attempts"),
            "step_count": len(steps),
            "error": task_error,
            "output_dir": item.get("output_dir"),
            "result_path": str(result_path),
        }
        task_rows.append(task_row)

        for position, step in enumerate(steps, start=1):
            row = {
                "ratio": ratio,
                "repeat": repeat,
                "oversubscription_ratio": f"1:{ratio}",
                "task_index": task_index,
                "source_trajectory": source_path,
                "instance_id": result.get("instance_id"),
                "sandbox_id": result.get("sandbox_id"),
                "step_index": step.get("index", position),
            }
            for column in STEP_COLUMNS:
                if column not in row:
                    row[column] = step.get(column)
            if isinstance(row.get("infrastructure_error"), (dict, list)):
                row["infrastructure_error"] = json.dumps(
                    row["infrastructure_error"], ensure_ascii=False
                )
            step_rows.append(row)
    return task_rows, step_rows


def trial_from_summary(
    args: argparse.Namespace,
    *,
    ratio: int,
    repeat: int,
    output_dir: Path,
    return_code: int | None,
    reused: bool,
    driver_wall_sec: float | None,
    staged_to_source: dict[str, str],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    target_count = ratio * args.running_concurrency
    summary_path = output_dir / "batch-summary.json"
    trial = {
        "ratio": ratio,
        "repeat": repeat,
        "oversubscription_ratio": f"1:{ratio}",
        "running_concurrency": args.running_concurrency,
        "target_count": target_count,
        "concurrency": target_count,
        "valid": False,
        "batch_elapsed_sec": None,
        "driver_wall_sec": driver_wall_sec,
        "throughput_tasks_per_sec": None,
        "total": None,
        "succeeded": None,
        "failed": None,
        "running_peak": None,
        "running_granted": None,
        "run_queue_wait_avg_sec": None,
        "control_wait_avg_sec": None,
        "control_wait_max_sec": None,
        "return_code": return_code,
        "reused": reused,
        "started_at": None,
        "completed_at": None,
        "error": None,
        "output_dir": str(output_dir),
        "summary_path": str(summary_path),
        "observability_dir": (
            str(output_dir / "observability") if args.observability else None
        ),
        "observability_return_code": None,
        "observability_error": None,
        "observability_report_return_code": None,
        "observability_report_error": None,
    }
    if not summary_path.is_file():
        trial["error"] = "batch-summary.json was not produced"
        return trial, [], []

    try:
        summary = read_json(summary_path)
        total = int(summary.get("total", 0))
        succeeded = int(summary.get("succeeded", 0))
        failed = int(summary.get("failed", 0))
        elapsed = float(summary["elapsed_sec"])
        running = summary.get("running_slots") or {}
        control = summary.get("control_plane") or {}
        trial.update(
            {
                "batch_elapsed_sec": elapsed,
                "throughput_tasks_per_sec": total / elapsed if elapsed > 0 else None,
                "total": total,
                "succeeded": succeeded,
                "failed": failed,
                "running_peak": running.get("peak_active"),
                "running_granted": running.get("granted"),
                "run_queue_wait_avg_sec": running.get("average_queue_wait_sec"),
                "control_wait_avg_sec": control.get("average_wait_sec"),
                "control_wait_max_sec": control.get("max_wait_sec"),
                "started_at": summary.get("started_at"),
                "completed_at": summary.get("completed_at"),
            }
        )
        trial["valid"] = (
            total == target_count
            and succeeded == target_count
            and failed == 0
            and running.get("maximum") == args.running_concurrency
            and int(running.get("peak_active") or 0) <= args.running_concurrency
            and return_code in (None, 0)
        )
        if not trial["valid"]:
            errors = [str(item.get("error")) for item in summary.get("items", []) if item.get("error")]
            trial["error"] = errors[0] if errors else (
                f"invalid batch total={total} succeeded={succeeded} failed={failed} "
                f"running_peak={running.get('peak_active')}"
            )
        tasks, steps = collect_task_and_step_records(
            summary,
            ratio=ratio,
            repeat=repeat,
            staged_to_source=staged_to_source,
        )
        return trial, tasks, steps
    except Exception as exc:
        trial["error"] = f"cannot parse summary: {type(exc).__name__}: {exc}"
        return trial, [], []


def run_trial(
    args: argparse.Namespace,
    *,
    repo_dir: Path,
    selection: list[dict[str, Any]],
    ratio: int,
    repeat: int,
    output_dir: Path,
    staged_to_source: dict[str, str],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    summary_path = output_dir / "batch-summary.json"
    if args.reuse and summary_path.is_file():
        print(f"[reuse] ratio=1:{ratio} repeat={repeat}: {summary_path}", flush=True)
        result = trial_from_summary(
            args,
            ratio=ratio,
            repeat=repeat,
            output_dir=output_dir,
            return_code=None,
            reused=True,
            driver_wall_sec=None,
            staged_to_source=staged_to_source,
        )
        environment = os.environ.copy()
        environment["PYTHONUNBUFFERED"] = "1"
        try:
            report_status = generate_observability_reports(
                args,
                repo_dir=repo_dir,
                output_dir=output_dir,
                environment=environment,
            )
        except Exception as exc:
            report_status = {
                "return_code": 1,
                "error": f"unexpected report generation error: {type(exc).__name__}: {exc}",
            }
        trial = result[0]
        trial["observability_report_return_code"] = report_status["return_code"]
        trial["observability_report_error"] = report_status["error"]
        if report_status["return_code"] is not None:
            print(
                f"[observability] reports rc={report_status['return_code']} "
                f"dir={output_dir / 'observability'}",
                flush=True,
            )
        if report_status["error"]:
            print(
                f"[observability] report warning: {report_status['error']}",
                file=sys.stderr,
                flush=True,
            )
        return result

    output_dir.mkdir(parents=True, exist_ok=True)
    mix_config_path = output_dir / "mix-config.json"
    write_mix_config(
        mix_config_path,
        selection=selection,
        ratio=ratio,
        concurrency=ratio * args.running_concurrency,
    )
    command = command_for_trial(
        args,
        repo_dir=repo_dir,
        mix_config_path=mix_config_path,
        ratio=ratio,
        output_dir=output_dir,
    )
    print(
        f"\n[start] ratio=1:{ratio} N={args.running_concurrency} "
        f"tasks={ratio * args.running_concurrency} repeat={repeat}/{args.repeats}\n"
        f"        {shlex.join(command)}",
        flush=True,
    )
    log_path = output_dir / "benchmark-driver.log"
    environment = os.environ.copy()
    environment["PYTHONUNBUFFERED"] = "1"
    try:
        monitor = start_observability(
            args,
            repo_dir=repo_dir,
            output_dir=output_dir,
            environment=environment,
        )
    except Exception as exc:
        result = trial_from_summary(
            args,
            ratio=ratio,
            repeat=repeat,
            output_dir=output_dir,
            return_code=None,
            reused=False,
            driver_wall_sec=None,
            staged_to_source=staged_to_source,
        )
        trial = result[0]
        monitor_error = f"cannot start observability monitor: {type(exc).__name__}: {exc}"
        trial["observability_error"] = monitor_error
        trial["error"] = monitor_error
        print(f"[observability] ERROR {monitor_error}", file=sys.stderr, flush=True)
        return result

    started = time.perf_counter()
    monitor_status: dict[str, Any]
    try:
        with log_path.open("w", encoding="utf-8") as log_handle:
            process = subprocess.Popen(
                command,
                cwd=repo_dir,
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
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                raise
    finally:
        monitor_status = stop_observability(monitor)
        if monitor is not None:
            print(
                f"[observability] stopped rc={monitor_status['return_code']} "
                f"dir={output_dir / 'observability'}",
                flush=True,
            )
    driver_wall_sec = time.perf_counter() - started
    result = trial_from_summary(
        args,
        ratio=ratio,
        repeat=repeat,
        output_dir=output_dir,
        return_code=return_code,
        reused=False,
        driver_wall_sec=driver_wall_sec,
        staged_to_source=staged_to_source,
    )
    trial = result[0]
    trial["observability_return_code"] = monitor_status["return_code"]
    trial["observability_error"] = monitor_status["error"]
    if monitor_status["error"]:
        trial["valid"] = False
        trial["error"] = (
            f"{trial['error']}; {monitor_status['error']}"
            if trial["error"]
            else monitor_status["error"]
        )
    try:
        report_status = generate_observability_reports(
            args,
            repo_dir=repo_dir,
            output_dir=output_dir,
            environment=environment,
        )
    except Exception as exc:
        report_status = {
            "return_code": 1,
            "error": f"unexpected report generation error: {type(exc).__name__}: {exc}",
        }
    trial["observability_report_return_code"] = report_status["return_code"]
    trial["observability_report_error"] = report_status["error"]
    if report_status["return_code"] is not None:
        print(
            f"[observability] reports rc={report_status['return_code']} "
            f"dir={output_dir / 'observability'}",
            flush=True,
        )
    if report_status["error"]:
        print(
            f"[observability] report warning: {report_status['error']}",
            file=sys.stderr,
            flush=True,
        )
    print(
        f"[done] ratio=1:{ratio} repeat={repeat} valid={trial['valid']} "
        f"elapsed={trial['batch_elapsed_sec']} failed={trial['failed']}",
        flush=True,
    )
    return result


def aggregate_ratios(
    args: argparse.Namespace, trials: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for ratio in args.ratios:
        attempted = [trial for trial in trials if trial["ratio"] == ratio]
        valid = [trial for trial in attempted if trial["valid"]]
        elapsed = [float(trial["batch_elapsed_sec"]) for trial in valid]
        throughput = [float(trial["throughput_tasks_per_sec"]) for trial in valid]
        rows.append(
            {
                "ratio": ratio,
                "oversubscription_ratio": f"1:{ratio}",
                "running_concurrency": args.running_concurrency,
                "target_count": ratio * args.running_concurrency,
                "expected_repeats": args.repeats,
                "attempted": len(attempted),
                "successful": len(valid),
                "all_repeats_successful": len(valid) == args.repeats,
                "median_batch_elapsed_sec": statistics.median(elapsed) if elapsed else None,
                "average_batch_elapsed_sec": statistics.fmean(elapsed) if elapsed else None,
                "min_batch_elapsed_sec": min(elapsed) if elapsed else None,
                "max_batch_elapsed_sec": max(elapsed) if elapsed else None,
                "median_throughput_tasks_per_sec": statistics.median(throughput) if throughput else None,
                "time_degradation_vs_1_1_pct": None,
                "throughput_gain_vs_1_1_pct": None,
            }
        )

    baseline = next((row for row in rows if row["ratio"] == 1), None)
    if baseline and baseline["median_batch_elapsed_sec"] is not None:
        baseline_elapsed = float(baseline["median_batch_elapsed_sec"])
        baseline_throughput = float(baseline["median_throughput_tasks_per_sec"])
        for row in rows:
            if row["median_batch_elapsed_sec"] is not None:
                row["time_degradation_vs_1_1_pct"] = (
                    float(row["median_batch_elapsed_sec"]) / baseline_elapsed - 1.0
                ) * 100.0
            if row["median_throughput_tasks_per_sec"] is not None:
                row["throughput_gain_vs_1_1_pct"] = (
                    float(row["median_throughput_tasks_per_sec"]) / baseline_throughput - 1.0
                ) * 100.0
    return rows


def write_outputs(
    output_root: Path,
    *,
    args: argparse.Namespace,
    repo_dir: Path,
    trajectory_dir: Path,
    selection: list[dict[str, Any]],
    trials: list[dict[str, Any]],
    tasks: list[dict[str, Any]],
    steps: list[dict[str, Any]],
) -> None:
    ratios = aggregate_ratios(args, trials)
    write_csv(output_root / "trial-summary.csv", TRIAL_COLUMNS, trials)
    write_csv(output_root / "ratio-summary.csv", RATIO_COLUMNS, ratios)
    write_csv(output_root / "trajectory-detail.csv", TASK_COLUMNS, tasks)
    write_csv(output_root / "step-detail.csv", STEP_COLUMNS, steps)
    report = {
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "configuration": {
            "repo_dir": str(repo_dir),
            "trajectory_dir": str(trajectory_dir),
            "image_map": str(args.image_map.expanduser().resolve()) if args.image_map else None,
            "workdir": args.workdir,
            "skip_instance_patch": True,
            "api_url": args.api_url,
            "command_transport": args.command_transport,
            "direct_envd_port": args.direct_envd_port,
            "running_concurrency": args.running_concurrency,
            "ratios": args.ratios,
            "repeats": args.repeats,
            "command_timeout_sec": args.command_timeout_sec,
            "command_timeout_continue": args.command_timeout_continue,
            "stop_on_error": args.stop_on_error,
            "control_plane_qps": args.control_plane_qps,
            "control_plane_max_inflight": args.control_plane_max_inflight,
            "launch_interval_sec": args.launch_interval_sec,
            "pause_duration_sec": args.pause_duration_sec,
            "extra_delay_sec": getattr(args, "extra_delay_sec", 0.0),
            "delay_override": args.delay,
            "cooldown_sec": args.cooldown_sec,
            "observability": args.observability,
            "observability_interval_sec": args.observability_interval_sec,
            "cgroup_name": getattr(args, "cgroup_name", None),
            "cgroup_version": getattr(args, "cgroup_version", "auto"),
            "cpu_range": getattr(args, "cpu_range", None),
            "observability_output_rule": "<trial_dir>/observability",
            "observability_reports": args.observability_reports,
            "observability_report_python": args.observability_report_python,
            "observability_report_outputs": [
                "report.xlsx",
                "report.md",
                "gantt.png",
                "pause_resume_multi.png or pause_resume_cycle.png",
            ],
            "selection_rule": "natural filename order; first N, or cycle all when count < N",
            "task_rule": "ratio k runs k*N tasks with k*N workers over N RUNNING slots",
            "lifecycle": "create/init/pause; FIFO resume/command/pause; release after pause",
        },
        "trajectory_selection": selection,
        "trials": trials,
        "ratio_summary": ratios,
        "trajectory_details": tasks,
        "step_details": steps,
    }
    write_json(output_root / "benchmark-report.json", report)


def main() -> int:
    args = build_parser().parse_args()
    try:
        repo_dir, trajectory_dir, all_trajectories, image_map = validate_args(args)
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    output_root = (
        args.output_root.expanduser().resolve()
        if args.output_root
        else (repo_dir / "runs" / f"pause-resume-oversub-N{args.running_concurrency}-{timestamp}").resolve()
    )
    try:
        selection = selection_records(
            all_trajectories, args.running_concurrency, image_map
        )
        selection_dir, staged_to_source = prepare_selection(output_root, selection)
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if args.dry_run:
        print(
            f"N_RUNNING={args.running_concurrency}\n"
            f"api_url={args.api_url}\n"
            f"command_transport={args.command_transport}\n"
            f"noise_fill={getattr(args, 'noise_fill', False)}\n"
            f"rollout={getattr(args, 'rollout', 1)}\n"
            f"cgroup_name={getattr(args, 'cgroup_name', None)}\n"
            f"cpu_range={getattr(args, 'cpu_range', None)}\n"
            f"trajectory_dir={trajectory_dir}\n"
            f"available_trajectories={len(all_trajectories)}\n"
            f"selected_sources={len(selection)}\n"
            f"ratios={args.ratios}\n"
            f"repeats={args.repeats}\n"
            f"observability={args.observability}\n"
            f"observability_interval_sec={args.observability_interval_sec}\n"
            f"observability_reports={args.observability_reports}\n"
            f"observability_report_python={args.observability_report_python}\n"
            f"output_root={output_root}\n"
        )
        for ratio in args.ratios:
            for repeat in range(1, args.repeats + 1):
                trial_dir = output_root / f"ratio-{ratio:02d}" / f"repeat-{repeat:02d}"
                trial_dir.mkdir(parents=True, exist_ok=True)
                mix_config_path = trial_dir / "mix-config.json"
                write_mix_config(
                    mix_config_path,
                    selection=selection,
                    ratio=ratio,
                    concurrency=ratio * args.running_concurrency,
                )
                print(
                    shlex.join(
                        command_for_trial(
                            args,
                            repo_dir=repo_dir,
                            mix_config_path=mix_config_path,
                            ratio=ratio,
                            output_dir=trial_dir,
                        )
                    )
                )
        write_outputs(
            output_root,
            args=args,
            repo_dir=repo_dir,
            trajectory_dir=trajectory_dir,
            selection=selection,
            trials=[],
            tasks=[],
            steps=[],
        )
        return 0

    trials: list[dict[str, Any]] = []
    tasks: list[dict[str, Any]] = []
    steps: list[dict[str, Any]] = []
    stopped_early = False
    trial_number = 0
    total_trials = len(args.ratios) * args.repeats
    try:
        for ratio in args.ratios:
            for repeat in range(1, args.repeats + 1):
                trial_number += 1
                if trial_number > 1 and args.cooldown_sec:
                    print(
                        f"[cooldown] {args.cooldown_sec:.1f}s before "
                        f"trial {trial_number}/{total_trials}",
                        flush=True,
                    )
                    time.sleep(args.cooldown_sec)
                trial_dir = output_root / f"ratio-{ratio:02d}" / f"repeat-{repeat:02d}"
                trial, trial_tasks, trial_steps = run_trial(
                    args,
                    repo_dir=repo_dir,
                    selection=selection,
                    ratio=ratio,
                    repeat=repeat,
                    output_dir=trial_dir,
                    staged_to_source=staged_to_source,
                )
                trials.append(trial)
                tasks.extend(trial_tasks)
                steps.extend(trial_steps)
                write_outputs(
                    output_root,
                    args=args,
                    repo_dir=repo_dir,
                    trajectory_dir=trajectory_dir,
                    selection=selection,
                    trials=trials,
                    tasks=tasks,
                    steps=steps,
                )
                if not trial["valid"] and args.stop_on_failure:
                    stopped_early = True
                    break
            if stopped_early:
                break
    except KeyboardInterrupt:
        print("\n[interrupted] partial results were preserved", file=sys.stderr)
        write_outputs(
            output_root,
            args=args,
            repo_dir=repo_dir,
            trajectory_dir=trajectory_dir,
            selection=selection,
            trials=trials,
            tasks=tasks,
            steps=steps,
        )
        return 130

    print(f"\nResults: {output_root}")
    print(f"JSON:    {output_root / 'benchmark-report.json'}")
    print(f"Trials:  {output_root / 'trial-summary.csv'}")
    print(f"Tasks:   {output_root / 'trajectory-detail.csv'}")
    print(f"Steps:   {output_root / 'step-detail.csv'}")
    return 1 if stopped_early else 0


if __name__ == "__main__":
    raise SystemExit(main())
