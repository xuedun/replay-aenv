#!/usr/bin/env python3
"""Extract the ready fc-vCPU count time series from benchmark replay logs.

Definition (per the oversubscription experiment):
  ready VM = a live, non-sleeping VM whose vCPUs want CPU time
  ready vCPUs = ready VMs * VCPUS_PER_VM (default 2)

Sleep windows are excluded:
  * COW / no-lifecycle runs: each step's
    delay_wait_started_at..delay_wait_completed_at
  * pause-resume runs: each paused window (pre-command pause plus the
    recorded delay, i.e. run_queue_entered_at..run_slot_acquired_at, and
    the post-command pause). The VM only becomes ready at
    resume_started_at and stays ready until the command completes.

The script scans a run root (e.g. runs/pause-resume-oversub-N1-...),
finds every ratio-*/repeat-* trial directory, and for each trial builds
a time series of the ready vCPU count:

  * default: event-based (one row at every interval boundary, count is
    constant between boundaries, exact for sub-second windows)
  * --interval S: fixed bins of S seconds; each bin reports the
    time-weighted average ready count over the bin plus the max

Outputs one CSV per trial under <trial>/observability/ and a combined
ready-vcpu-timeseries.csv at the run root.

Usage:
    python3 scripts/extract_ready_vcpu_timeseries.py runs/<run-dir>
    python3 scripts/extract_ready_vcpu_timeseries.py runs/<run-dir> --interval 1
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:
    plt = None

DEFAULT_VCPUS_PER_VM = 2

EVENT_COLUMNS = [
    "trial",
    "ratio",
    "repeat",
    "ts",
    "epoch_sec",
    "t_rel_sec",
    "ready_vms",
    "ready_vcpus",
    "cpus_per_core",
]

BIN_COLUMNS = [
    "trial",
    "ratio",
    "repeat",
    "bin_index",
    "bin_start_ts",
    "bin_end_ts",
    "t_rel_start_sec",
    "t_rel_end_sec",
    "ready_vms_avg",
    "ready_vcpus_avg",
    "ready_vms_max",
    "ready_vcpus_max",
    "cpus_per_core_avg",
    "cpus_per_core_max",
]


def parse_ts(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value)).timestamp()
    except ValueError:
        return None


def read_json(path: Path) -> dict[str, Any] | None:
    try:
        with path.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
        return value if isinstance(value, dict) else None
    except (OSError, ValueError):
        return None


def parse_n_from_path(path: Path) -> int | None:
    """Parse the N value from a run directory name like '...-N1-...'."""
    m = re.search(r"-N(\d+)-", path.name)
    if m:
        return int(m.group(1))
    for parent in path.parents:
        m = re.search(r"-N(\d+)-", parent.name)
        if m:
            return int(m.group(1))
    return None


def classify_mode(steps: list[dict[str, Any]]) -> str:
    for step in steps:
        if step.get("pause_started_at") or step.get("resume_started_at"):
            return "pause-resume"
        if step.get("delay_wait_started_at"):
            return "cow"
    return "cow"


def ready_intervals_pr(
    steps: list[dict[str, Any]],
) -> list[tuple[float, float]]:
    """pause-resume: ready from resume start to post-command pause start.

    The VM is resumed (becomes ready/wanting-CPU) at ``resume_started_at``
    and stays running until the post-command pause begins at
    ``post_command_pause_started_at``.  Using ``completed_at`` as the end
    boundary would miss the time between command completion and the
    subsequent pause — significant in noise-fill mode where VMs stay
    running longer between commands.  Falls back to ``completed_at`` when
    ``post_command_pause_started_at`` is absent (older runs).
    """
    intervals: list[tuple[float, float]] = []
    for step in steps:
        start = parse_ts(step.get("resume_started_at"))
        end = parse_ts(step.get("post_command_pause_started_at"))
        if end is None:
            end = parse_ts(step.get("completed_at"))
        if start is None:
            start = parse_ts(step.get("started_at"))
        if start is None or end is None or end <= start:
            continue
        intervals.append((start, end))
    return intervals


def ready_intervals_cow(
    result: dict[str, Any],
    steps: list[dict[str, Any]],
) -> list[tuple[float, float]]:
    """COW/no-lifecycle: VM running lifetime minus sleep (delay-wait) windows.

    The VM is running from ``sandbox_create_completed_at`` to
    ``sandbox_deleted_at`` (present in no-lifecycle replay results).
    Falls back to ``started_at``/``completed_at`` (replay start/end) when
    those fields are absent.  Delay-wait (asyncio.sleep) windows are
    excluded as sleep periods.
    """
    born = parse_ts(result.get("sandbox_create_completed_at"))
    if born is None:
        born = parse_ts(result.get("started_at"))
    died = parse_ts(result.get("sandbox_deleted_at"))
    if died is None:
        died = parse_ts(result.get("completed_at"))
    if born is None or died is None or died <= born:
        return []

    sleeps: list[tuple[float, float]] = []
    for step in steps:
        start = parse_ts(step.get("delay_wait_started_at"))
        end = parse_ts(step.get("delay_wait_completed_at"))
        if start is None or end is None or end <= start:
            continue
        sleeps.append((start, end))
    sleeps.sort()

    merged: list[tuple[float, float]] = []
    for start, end in sleeps:
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))

    intervals: list[tuple[float, float]] = []
    cursor = born
    for start, end in merged:
        start = max(start, born)
        end = min(end, died)
        if start > cursor:
            intervals.append((cursor, start))
        cursor = max(cursor, end)
    if cursor < died:
        intervals.append((cursor, died))
    return intervals


def collect_trial(trial_dir: Path, vcpus_per_vm: int) -> dict[str, Any] | None:
    result_paths = sorted(trial_dir.rglob("replay-result.json"))
    all_intervals: list[tuple[float, float]] = []
    mode = "cow"
    tasks = 0
    tasks_with_intervals = 0
    for path in result_paths:
        result = read_json(path)
        if result is None:
            continue
        steps = [
            step for step in result.get("steps", []) if isinstance(step, dict)
        ]
        if not steps:
            continue
        tasks += 1
        task_mode = classify_mode(steps)
        if task_mode == "pause-resume":
            mode = "pause-resume"
            intervals = ready_intervals_pr(steps)
        else:
            intervals = ready_intervals_cow(result, steps)
        if intervals:
            tasks_with_intervals += 1
            all_intervals.extend(intervals)

    if not all_intervals:
        return None

    all_intervals.sort()
    t0 = all_intervals[0][0]
    t1 = max(end for _, end in all_intervals)

    deltas: list[tuple[float, int]] = []
    for start, end in all_intervals:
        deltas.append((start, 1))
        deltas.append((end, -1))
    deltas.sort(key=lambda item: (item[0], item[1]))
    events: list[dict[str, Any]] = []
    active = 0
    for point, delta in deltas:
        active += delta
        events.append({"epoch_sec": point, "ready_vms": active})

    return {
        "mode": mode,
        "tasks": tasks,
        "tasks_with_intervals": tasks_with_intervals,
        "t0": t0,
        "t1": t1,
        "events": events,
        "intervals": all_intervals,
    }


def write_event_csv(
    path: Path,
    *,
    trial_label: str,
    ratio: str,
    repeat: str,
    t0: float,
    events: list[dict[str, Any]],
    vcpus_per_vm: int,
    total_cpus: int = 0,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=EVENT_COLUMNS)
        writer.writeheader()
        for event in events:
            point = event["epoch_sec"]
            ready_vcpus = event["ready_vms"] * vcpus_per_vm
            cpus_per_core = ready_vcpus / total_cpus if total_cpus > 0 else 0.0
            writer.writerow(
                {
                    "trial": trial_label,
                    "ratio": ratio,
                    "repeat": repeat,
                    "ts": datetime.fromtimestamp(point).astimezone().isoformat(
                        timespec="milliseconds"
                    ),
                    "epoch_sec": f"{point:.3f}",
                    "t_rel_sec": f"{point - t0:.3f}",
                    "ready_vms": event["ready_vms"],
                    "ready_vcpus": ready_vcpus,
                    "cpus_per_core": round(cpus_per_core, 4),
                }
            )


def weighted_series(
    events: list[dict[str, Any]], t0: float, t1: float
) -> list[tuple[float, float, int]]:
    """(segment_start, segment_end, ready_vms) covering [t0, t1)."""
    segments: list[tuple[float, float, int]] = []
    for index, event in enumerate(events):
        start = event["epoch_sec"]
        end = events[index + 1]["epoch_sec"] if index + 1 < len(events) else t1
        if end > start:
            segments.append((start, end, event["ready_vms"]))
    return segments


def write_bin_csv(
    path: Path,
    *,
    trial_label: str,
    ratio: str,
    repeat: str,
    t0: float,
    t1: float,
    segments: list[tuple[float, float, int]],
    interval: float,
    vcpus_per_vm: int,
    total_cpus: int = 0,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    n_bins = max(1, int((t1 - t0) / interval) + 1)
    seg_index = 0
    for bin_index in range(n_bins):
        bin_start = t0 + bin_index * interval
        bin_end = min(bin_start + interval, t1)
        if bin_end <= bin_start:
            break
        covered = 0.0
        weighted = 0.0
        peak = 0
        while seg_index < len(segments) and segments[seg_index][1] <= bin_start:
            seg_index += 1
        cursor = seg_index
        while cursor < len(segments) and segments[cursor][0] < bin_end:
            seg_start, seg_end, count = segments[cursor]
            overlap = min(seg_end, bin_end) - max(seg_start, bin_start)
            if overlap > 0:
                covered += overlap
                weighted += overlap * count
                peak = max(peak, count)
            cursor += 1
        avg = weighted / (bin_end - bin_start) if bin_end > bin_start else 0.0
        vcpus_avg = avg * vcpus_per_vm
        vcpus_max = peak * vcpus_per_vm
        rows.append(
            {
                "trial": trial_label,
                "ratio": ratio,
                "repeat": repeat,
                "bin_index": bin_index,
                "bin_start_ts": datetime.fromtimestamp(bin_start)
                .astimezone()
                .isoformat(timespec="milliseconds"),
                "bin_end_ts": datetime.fromtimestamp(bin_end)
                .astimezone()
                .isoformat(timespec="milliseconds"),
                "t_rel_start_sec": round(bin_start - t0, 3),
                "t_rel_end_sec": round(bin_end - t0, 3),
                "ready_vms_avg": round(avg, 3),
                "ready_vcpus_avg": round(vcpus_avg, 3),
                "ready_vms_max": peak,
                "ready_vcpus_max": vcpus_max,
                "cpus_per_core_avg": round(vcpus_avg / total_cpus, 4) if total_cpus > 0 else 0.0,
                "cpus_per_core_max": round(vcpus_max / total_cpus, 4) if total_cpus > 0 else 0.0,
                "_covered": covered,
            }
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = [c for c in BIN_COLUMNS]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=columns, extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerows(rows)
    return rows


def find_trials(root: Path) -> list[tuple[Path, str, str]]:
    trials: list[tuple[Path, str, str]] = []
    for ratio_dir in sorted(root.glob("ratio-*")):
        if not ratio_dir.is_dir():
            continue
        ratio = ratio_dir.name.replace("ratio-", "")
        for repeat_dir in sorted(ratio_dir.glob("repeat-*")):
            if not repeat_dir.is_dir():
                continue
            trials.append(
                (repeat_dir, ratio, repeat_dir.name.replace("repeat-", ""))
            )
    if not trials and any(root.rglob("replay-result.json")):
        trials.append((root, "?", "?"))
    return trials


def plot_ratios(
    combined_rows: list[dict[str, Any]],
    columns: list[str],
    output_path: Path,
    *,
    total_cpus: int,
) -> None:
    """Draw one line chart per ratio showing cpus_per_core over time."""
    if plt is None:
        print("        WARN matplotlib not available, skipping plots")
        return

    is_binned = "bin_index" in columns
    t_col = "t_rel_start_sec" if is_binned else "t_rel_sec"
    y_col = "cpus_per_core_avg" if is_binned else "cpus_per_core"

    ratios: dict[str, list[dict[str, Any]]] = {}
    for row in combined_rows:
        r = str(row.get("ratio", "?"))
        ratios.setdefault(r, []).append(row)

    for ratio in sorted(ratios, key=lambda x: (len(x), x) if x.isdigit() else (9999, x)):
        rows = ratios[ratio]
        if not rows:
            continue
        xs = [float(r.get(t_col, 0) or 0) for r in rows]
        ys = [float(r.get(y_col, 0) or 0) for r in rows]

        fig, ax = plt.subplots(figsize=(12, 5))
        ax.plot(xs, ys, linewidth=1.0, color="steelblue")
        ax.fill_between(xs, ys, alpha=0.15, color="steelblue")
        ax.set_xlabel("Time (s)")
        ax.set_ylabel("CPUs per core")
        ax.set_title(
            f"Ready vCPUs per Core over Time  (ratio={ratio}, "
            f"total_cpus={total_cpus})"
        )
        ax.axhline(1.0, color="red", linestyle="--", linewidth=0.8, alpha=0.7,
                    label="1.0 (full utilization)")
        ax.legend(loc="upper right")
        ax.grid(True, alpha=0.3)
        fig.tight_layout()

        png_path = output_path.parent / f"cpus-per-core-ratio-{ratio}.png"
        fig.savefig(png_path, dpi=150)
        plt.close(fig)
        print(f"        plot: {png_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", type=Path, help="Run root or trial directory")
    parser.add_argument(
        "--vcpus-per-vm",
        type=int,
        default=DEFAULT_VCPUS_PER_VM,
        help=f"vCPUs per FC VM (default: {DEFAULT_VCPUS_PER_VM})",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=None,
        help=(
            "Also emit fixed bins of this many seconds (time-weighted "
            "average + max ready count per bin)"
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Combined CSV path (default: <run_dir>/ready-vcpu-timeseries.csv)",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help=(
            "Directory for per-trial CSVs; mirrors the ratio/repeat layout "
            "(default: each trial's own observability/ directory)"
        ),
    )
    parser.add_argument(
        "--total-cpus",
        type=int,
        default=None,
        help=(
            "Total CPU count for cpus_per_core normalization. "
            "Default: parse N from run dir name (e.g. N1->1), "
            "then total_cpus = N * ratio * vcpus_per_vm"
        ),
    )
    parser.add_argument(
        "--no-plot",
        action="store_true",
        help="Skip generating per-ratio PNG plots",
    )
    args = parser.parse_args()

    root = args.run_dir.expanduser().resolve()
    if not root.is_dir():
        raise SystemExit(f"not a directory: {root}")
    trials = find_trials(root)
    if not trials:
        raise SystemExit(f"no trial directories with replay-result.json under {root}")

    n_value = parse_n_from_path(root)
    if n_value is not None:
        print(f"parsed N={n_value} from run dir name")
    elif args.total_cpus is not None:
        print(f"using --total-cpus={args.total_cpus}")
    else:
        print("WARN: cannot parse N from path and --total-cpus not set; "
              "cpus_per_core will be 0")

    out_dir = args.out_dir.expanduser().resolve() if args.out_dir else None
    if out_dir:
        out_dir.mkdir(parents=True, exist_ok=True)
    combined_path = args.output or (
        (out_dir or root) / "ready-vcpu-timeseries.csv"
    )
    combined_rows: list[dict[str, Any]] = []
    columns = EVENT_COLUMNS if args.interval is None else BIN_COLUMNS

    def trial_out_path(trial_dir: Path, name: str) -> Path:
        if out_dir:
            return out_dir / trial_dir.relative_to(root) / name
        return trial_dir / "observability" / name

    def safe_write(action, out_path: Path) -> bool:
        try:
            action()
            return True
        except OSError as exc:
            print(f"        WARN cannot write {out_path}: {exc}")
            return False

    for trial_dir, ratio, repeat in trials:
        label = f"ratio-{ratio}/repeat-{repeat}"
        data = collect_trial(trial_dir, args.vcpus_per_vm)
        if data is None:
            print(f"[skip] {label}: no usable replay-result.json intervals")
            continue
        t0, t1 = data["t0"], data["t1"]

        if args.total_cpus is not None:
            total_cpus = args.total_cpus
        elif n_value is not None:
            ratio_int = int(ratio) if ratio.isdigit() else 0
            total_cpus = n_value * ratio_int * args.vcpus_per_vm
        else:
            total_cpus = 0

        print(
            f"[trial] {label} mode={data['mode']} tasks={data['tasks']} "
            f"with_intervals={data['tasks_with_intervals']} "
            f"span={t1 - t0:.1f}s intervals={len(data['intervals'])} "
            f"total_cpus={total_cpus}"
        )

        if args.interval is None:
            out_path = trial_out_path(trial_dir, "ready-vcpu-events.csv")
            safe_write(
                lambda: write_event_csv(
                    out_path,
                    trial_label=label,
                    ratio=ratio,
                    repeat=repeat,
                    t0=t0,
                    events=data["events"],
                    vcpus_per_vm=args.vcpus_per_vm,
                    total_cpus=total_cpus,
                ),
                out_path,
            )
            for event in data["events"]:
                ready_vcpus = event["ready_vms"] * args.vcpus_per_vm
                combined_rows.append(
                    {
                        "trial": label,
                        "ratio": ratio,
                        "repeat": repeat,
                        "ts": datetime.fromtimestamp(event["epoch_sec"])
                        .astimezone()
                        .isoformat(timespec="milliseconds"),
                        "epoch_sec": f"{event['epoch_sec']:.3f}",
                        "t_rel_sec": f"{event['epoch_sec'] - t0:.3f}",
                        "ready_vms": event["ready_vms"],
                        "ready_vcpus": ready_vcpus,
                        "cpus_per_core": round(ready_vcpus / total_cpus, 4) if total_cpus > 0 else 0.0,
                    }
                )
            peak = max((e["ready_vms"] for e in data["events"]), default=0)
            print(
                f"        wrote {out_path} (events={len(data['events'])}, "
                f"peak_ready_vms={peak}, peak_ready_vcpus={peak * args.vcpus_per_vm})"
            )
        else:
            segments = weighted_series(data["events"], t0, t1)
            out_path = trial_out_path(trial_dir, "ready-vcpu-binned.csv")
            rows: list[dict[str, Any]] = []
            safe_write(
                lambda: rows.extend(
                    write_bin_csv(
                        out_path,
                        trial_label=label,
                        ratio=ratio,
                        repeat=repeat,
                        t0=t0,
                        t1=t1,
                        segments=segments,
                        interval=args.interval,
                        vcpus_per_vm=args.vcpus_per_vm,
                        total_cpus=total_cpus,
                    )
                ),
                out_path,
            )
            combined_rows.extend(rows)
            span = t1 - t0
            busy_vm_sec = sum(end - start for start, end in data["intervals"])
            mean_vms = busy_vm_sec / span if span > 0 else 0.0
            peak = max((r["ready_vms_max"] for r in rows), default=0)
            print(
                f"        wrote {out_path} (bins={len(rows)}, span={span:.1f}s, "
                f"time_weighted_mean_ready_vms={mean_vms:.2f}, "
                f"peak_ready_vms={peak}, "
                f"peak_ready_vcpus={peak * args.vcpus_per_vm})"
            )

    combined_path.parent.mkdir(parents=True, exist_ok=True)
    with combined_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=columns, extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerows(combined_rows)
    print(f"\ncombined: {combined_path} ({len(combined_rows)} rows)")

    if not args.no_plot and combined_rows:
        plot_total_cpus = args.total_cpus if args.total_cpus is not None else (
            n_value * max(
                (int(r) for r in {str(row.get("ratio", "0")) for row in combined_rows}
                 if r.isdigit()),
                default=0,
            ) * args.vcpus_per_vm
            if n_value is not None else 0
        )
        plot_ratios(
            combined_rows,
            columns,
            combined_path,
            total_cpus=plot_total_cpus,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
