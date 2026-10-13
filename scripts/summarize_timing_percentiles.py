#!/usr/bin/env python3
"""统计 replay-agent 批次日志中的阶段耗时分位数。"""

from __future__ import annotations

import argparse
import math
import re
import statistics
from pathlib import Path


TIMING_RE = re.compile(
    r"Agent interaction timing\b.*?"
    r"pause=(?P<pause>\d+(?:\.\d+)?)s\s+"
    r"delay=(?P<delay>\d+(?:\.\d+)?)s\s+"
    r"capacity_wait=(?P<capacity_wait>\d+(?:\.\d+)?)s\s+"
    r"resume=(?P<resume>\d+(?:\.\d+)?)s\s+"
    r"command=(?P<command>\d+(?:\.\d+)?)s\s+"
    r"total=(?P<total>\d+(?:\.\d+)?)s"
)
METRICS = ("capacity_wait", "pause", "resume", "total")


def percentile(values: list[float], percent: float) -> float:
    """使用线性插值计算分位数，与 NumPy 默认算法一致。"""

    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * percent / 100.0
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def select_task_logs(run_dir: Path) -> list[Path]:
    """递归发现任务目录；每个任务优先读取 job.log，防止重复统计。"""

    logs: list[Path] = []
    task_dirs = sorted(
        {
            log_path.parent
            for filename in ("job.log", "run.log")
            for log_path in run_dir.rglob(filename)
            if log_path.parent != run_dir
        },
        key=lambda path: str(path),
    )
    for task_dir in task_dirs:
        job_log = task_dir / "job.log"
        run_log = task_dir / "run.log"
        if job_log.is_file():
            logs.append(job_log)
        elif run_log.is_file():
            logs.append(run_log)
    return logs


def parse_timings(log_paths: list[Path]) -> dict[str, list[float]]:
    values = {metric: [] for metric in METRICS}
    for log_path in log_paths:
        with log_path.open("r", encoding="utf-8", errors="replace") as stream:
            for line in stream:
                match = TIMING_RE.search(line)
                if match is None:
                    continue
                for metric in METRICS:
                    values[metric].append(float(match.group(metric)))
    return values


def main() -> int:
    parser = argparse.ArgumentParser(
        description="统计 replay-agent 批次的 capacity_wait/pause/resume/total 分位数"
    )
    parser.add_argument("run_dir", type=Path, help="批次结果目录")
    args = parser.parse_args()

    run_dir = args.run_dir.expanduser().resolve()
    if not run_dir.is_dir():
        parser.error(f"目录不存在：{run_dir}")

    log_paths = select_task_logs(run_dir)
    if not log_paths:
        parser.error("未找到任务子目录中的 job.log 或 run.log")

    values = parse_timings(log_paths)
    sample_count = len(values[METRICS[0]])
    if sample_count == 0:
        parser.error("日志中没有找到 Agent interaction timing 记录")

    batch_complete = (run_dir / "batch-summary.json").is_file()
    print(f"结果目录：{run_dir}")
    print(f"批次状态：{'已完成' if batch_complete else '运行中/未完整结束（当前快照）'}")
    print(f"已读取任务日志：{len(log_paths)}")
    print(f"Timing 样本数：{sample_count}")
    print()
    print(f"{'指标':<18}{'P50(s)':>12}{'P95(s)':>12}{'平均(s)':>12}{'最大(s)':>12}")
    print("-" * 66)
    for metric in METRICS:
        metric_values = values[metric]
        print(
            f"{metric:<18}"
            f"{percentile(metric_values, 50):>12.3f}"
            f"{percentile(metric_values, 95):>12.3f}"
            f"{statistics.fmean(metric_values):>12.3f}"
            f"{max(metric_values):>12.3f}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
