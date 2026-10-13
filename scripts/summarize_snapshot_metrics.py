#!/usr/bin/env python3
"""Summarize snapshot/reload latency and optional S3 size from replay results."""

from __future__ import annotations

import argparse
import json
import math
import statistics
from pathlib import Path
from typing import Any


SNAPSHOT_METRICS = (
    ("snapshot_sec", "snapshot 总时延", "s"),
    ("snapshot_api_sec", "snapshot API 时延", "s"),
    ("snapshot_scheduler_wait_sec", "调度等待", "s"),
    ("snapshot_size_probe_sec", "大小探测时延", "s"),
    ("snapshot_s3_write_size_bytes", "S3 本次写入大小", "MB"),
    ("snapshot_size_bytes", "snapshot 逻辑大小", "MB"),
    ("snapshot_direct_size_bytes", "snapshot 独占对象大小", "MB"),
    ("snapshot_unique_layer_size_bytes", "唯一 layer 大小", "MB"),
)

RELOAD_METRICS = (
    ("reload_total_sec", "reload 总时延", "s"),
    ("reload_sec", "reload 创建时延", "s"),
    ("reload_api_sec", "reload API 时延", "s"),
    ("reload_scheduler_wait_sec", "reload 调度等待", "s"),
)


def percentile(values: list[float], quantile: float) -> float:
    """Linear percentile using index (n - 1) * q."""

    if not values:
        raise ValueError("percentile requires at least one value")
    ordered = sorted(values)
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def result_paths(input_path: Path) -> list[Path]:
    if input_path.is_file():
        return [input_path]
    if not input_path.is_dir():
        raise FileNotFoundError(input_path)
    return sorted(input_path.rglob("replay-result.json"))


def load_records(
    paths: list[Path],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    snapshots: list[dict[str, Any]] = []
    reloads: list[dict[str, Any]] = []
    errors: list[str] = []
    for path in paths:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            errors.append(f"{path}: {type(exc).__name__}: {exc}")
            continue
        rows = data.get("snapshots", []) if isinstance(data, dict) else []
        if not isinstance(rows, list):
            errors.append(f"{path}: snapshots is not a list")
            continue
        for row in rows:
            if isinstance(row, dict):
                snapshots.append(row)
        steps = data.get("steps", []) if isinstance(data, dict) else []
        if not isinstance(steps, list):
            errors.append(f"{path}: steps is not a list")
            continue
        for row in steps:
            if isinstance(row, dict) and isinstance(
                row.get("reload_total_sec"), (int, float)
            ):
                reloads.append(row)
    return snapshots, reloads, errors


def summarize(values: list[float]) -> dict[str, float | int]:
    return {
        "count": len(values),
        "mean": statistics.fmean(values),
        "p50": percentile(values, 0.50),
        "p95": percentile(values, 0.95),
        "min": min(values),
        "max": max(values),
    }


def collect_metric(rows: list[dict[str, Any]], field: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = row.get(field)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            values.append(float(value))
    return values


def render_value(value: float, unit: str) -> str:
    return f"{value:.3f} MB" if unit == "MB" else f"{value:.6f} s"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Recursively read replay-result.json and calculate mean/P50/P95 "
            "for snapshot/reload latency and available size fields."
        )
    )
    parser.add_argument("path", help="A replay-result.json file or a run directory")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    paths = result_paths(Path(args.path).expanduser().resolve())
    if not paths:
        raise SystemExit(f"没有找到 replay-result.json: {args.path}")
    snapshots, reloads, errors = load_records(paths)
    if not snapshots and not reloads:
        raise SystemExit("没有找到 snapshot 或 reload 记录")

    metric_summaries: dict[str, dict[str, Any]] = {}
    labels: dict[str, tuple[str, str]] = {}
    for field, label, unit in (*SNAPSHOT_METRICS, *RELOAD_METRICS):
        source_rows = reloads if field.startswith("reload_") else snapshots
        values = collect_metric(source_rows, field)
        if unit == "MB":
            values = [value / (1024 * 1024) for value in values]
        labels[field] = (label, unit)
        if values:
            metric_summaries[field] = {"unit": unit, **summarize(values)}

    output = {
        "result_file_count": len(paths),
        "snapshot_count": len(snapshots),
        "reload_count": len(reloads),
        "snapshot_with_size_count": len(
            collect_metric(snapshots, "snapshot_size_bytes")
        ),
        "snapshot_with_s3_write_size_count": len(
            collect_metric(snapshots, "snapshot_s3_write_size_bytes")
        ),
        "percentile_method": "linear interpolation at (n - 1) * q",
        "metrics": metric_summaries,
        "read_errors": errors,
    }
    if args.json:
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0

    print(f"结果文件数: {len(paths)}")
    print(f"snapshot 数: {len(snapshots)}")
    print(f"reload 数: {len(reloads)}")
    print(f"有大小数据: {output['snapshot_with_size_count']}/{len(snapshots)}")
    print(
        "有S3写入量数据: "
        f"{output['snapshot_with_s3_write_size_count']}/{len(snapshots)}"
    )
    print("百分位算法: linear interpolation, index=(n-1)*q")
    print()
    print(f"{'指标':<24} {'数量':>7} {'平均':>18} {'P50':>18} {'P95':>18}")
    print("-" * 91)
    for field, stats in metric_summaries.items():
        label, unit = labels[field]
        print(
            f"{label:<24} {stats['count']:>7} "
            f"{render_value(float(stats['mean']), unit):>18} "
            f"{render_value(float(stats['p50']), unit):>18} "
            f"{render_value(float(stats['p95']), unit):>18}"
        )
    if errors:
        print(f"\n读取失败文件数: {len(errors)}")
        for error in errors:
            print(f"  {error}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
