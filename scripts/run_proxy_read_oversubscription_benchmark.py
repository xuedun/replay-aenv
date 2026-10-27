#!/usr/bin/env python3
"""Run the N-running proxy-read oversubscription benchmark.

Identical methodology to run_pause_resume_oversubscription_benchmark.py —
k*N replay tasks over N RUNNING slots for every oversubscription ratio k —
except that read-only, environment-independent trajectory actions (ls, find,
cat, grep, ...) execute against the PAUSED sandbox through the AgentENV
resident proxy sandbox:

    POST /sandboxes/{id}/proxy-read

A proxy-read step acquires no running slot, performs no resume and no pause;
the target sandbox stays paused.  Every other action keeps the baseline
resume -> command -> pause slice, so the workload mix and lifecycle semantics
are preserved and the two benchmarks are directly comparable.

The driver reuses the pause/resume benchmark implementation wholesale and
only patches:
  * ``command_for_trial``    -> run agent/replay_agent_proxy.py
  * ``STEP_COLUMNS``/``TASK_COLUMNS`` -> transport / proxy metrics columns
  * ``collect_task_and_step_records`` -> task-level proxy summaries

Outputs are the same trial/task/step CSV+JSON files plus the proxy columns.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_DIR = SCRIPT_DIR.parent
BASE_DRIVER_PATH = SCRIPT_DIR / "run_pause_resume_oversubscription_benchmark.py"

#: Columns appended to the step CSV describing how each step was executed.
PROXY_STEP_COLUMNS = [
    "transport",
    "proxy_eligible",
    "proxy_skip_reason",
    "proxy_fallback_reason",
    "proxy_duration_ms",
    "binding_generation",
]

#: Columns appended to the task CSV summarising the proxy/resume mix.
PROXY_TASK_COLUMNS = [
    "proxy_read_steps",
    "resume_steps",
    "resume_fallback_steps",
    "proxy_read_command_sec_sum",
    "running_slot_saved_steps",
    "proxy_read_ratio",
]


def load_base_driver():
    spec = importlib.util.spec_from_file_location(
        "pause_resume_benchmark_base", BASE_DRIVER_PATH
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def install_patches(base: Any) -> None:
    """Redirect the base benchmark onto the proxy agent and extend outputs."""
    import json

    base.STEP_COLUMNS.extend(PROXY_STEP_COLUMNS)
    base.TASK_COLUMNS.extend(PROXY_TASK_COLUMNS)

    base_command_for_trial = base.command_for_trial

    def proxy_command_for_trial(args, **kwargs):
        command = base_command_for_trial(args, **kwargs)
        if getattr(args, "noise_fill", False):
            print(
                "[proxy] warning: --noise-fill has no proxy variant; "
                "running the proxy agent without noise fill",
                file=sys.stderr,
            )
        # The base driver invokes `python agent/replay_agent.py mix ...`;
        # swap in the proxy agent (the script path is argv[1]).
        repo_dir = Path(kwargs["repo_dir"])
        command[1] = str(repo_dir / "agent" / "replay_agent_proxy.py")
        if getattr(args, "proxy_read_disable", False):
            command.append("--proxy-read-disable")
        if getattr(args, "proxy_read_no_fallback", False):
            command.append("--proxy-read-no-fallback")
        if getattr(args, "proxy_read_retry_timeout_sec", None) is not None:
            command.extend(
                [
                    "--proxy-read-retry-timeout-sec",
                    str(args.proxy_read_retry_timeout_sec),
                ]
            )
        return command

    base.command_for_trial = proxy_command_for_trial

    base_collect = base.collect_task_and_step_records

    def proxy_collect_task_and_step_records(
        summary, *, ratio, repeat, staged_to_source
    ):
        task_rows, step_rows = base_collect(
            summary,
            ratio=ratio,
            repeat=repeat,
            staged_to_source=staged_to_source,
        )
        for row in task_rows:
            row.update(_proxy_task_summary(row.get("result_path")))
        return task_rows, step_rows

    base.collect_task_and_step_records = proxy_collect_task_and_step_records

    base_build_parser = base.build_parser

    def build_proxy_parser():
        parser = base_build_parser()
        group = parser.add_argument_group("proxy read")
        group.add_argument(
            "--proxy-read-disable",
            action="store_true",
            help="Route every step through the resume path (baseline mode)",
        )
        group.add_argument(
            "--proxy-read-no-fallback",
            action="store_true",
            help="Fail eligible steps on proxy errors instead of falling back",
        )
        group.add_argument(
            "--proxy-read-retry-timeout-sec",
            type=float,
            default=30.0,
            help="Retry window for 503 rebind_in_progress (default: 30)",
        )
        return parser

    base.build_parser = build_proxy_parser


def _proxy_task_summary(result_path: Any) -> dict[str, Any]:
    import json

    summary = {
        "proxy_read_steps": 0,
        "resume_steps": 0,
        "resume_fallback_steps": 0,
        "proxy_read_command_sec_sum": 0.0,
        "running_slot_saved_steps": 0,
        "proxy_read_ratio": 0.0,
    }
    if not result_path:
        return summary
    try:
        result = json.loads(Path(result_path).read_text(encoding="utf-8"))
    except Exception:
        return summary
    steps = [s for s in result.get("steps", []) if isinstance(s, dict)]
    if not steps:
        return summary
    for step in steps:
        transport = step.get("transport")
        if transport == "proxy_read":
            summary["proxy_read_steps"] += 1
            summary["proxy_read_command_sec_sum"] += float(
                step.get("command_sec") or 0.0
            )
        elif transport == "resume_fallback":
            summary["resume_fallback_steps"] += 1
        elif transport == "resume":
            summary["resume_steps"] += 1
    summary["running_slot_saved_steps"] = summary["proxy_read_steps"]
    summary["proxy_read_ratio"] = round(
        summary["proxy_read_steps"] / len(steps), 4
    )
    return summary


def main() -> int:
    base = load_base_driver()
    install_patches(base)
    return base.main()


if __name__ == "__main__":
    raise SystemExit(main())
