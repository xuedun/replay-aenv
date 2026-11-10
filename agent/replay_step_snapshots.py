#!/usr/bin/env python3
"""Replay one trajectory and save a named snapshot after every step.

输入 instance id，自动定位轨迹文件并解析模板，逐步重放：

    chain 模式（默认）:
        create(template) → exec(action_i) → snapshot(i) → kill
        → create(snapshot_i) → exec(action_i+1) → ...

    checkpoint 模式（--checkpoint-mode，AgentENV 快照不停止沙箱）:
        create(template) → exec(action_i) → snapshot(i) → exec(action_i+1) → ...

每步快照命名 stepsnap-<instance>-<runid>-<step>，结束后输出
replay-result.json（含每步 action 结果与 snapshot_id 列表）。

用法:
    python3 agent/replay_step_snapshots.py django-money__django-money.835c1ab8.combine_file__i1ay87eb
    python3 agent/replay_step_snapshots.py hipparchus-math__hipparchus-266.neutral --max-steps 5
    python3 agent/replay_step_snapshots.py <instance_id> --template django-money-task-v2 --checkpoint-mode
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import shlex
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger("replay-step-snapshots")

DEFAULT_API_URL = "http://127.0.0.1:8002"
DEFAULT_API_KEY = "e2b_" + "0" * 40


def configure_aenv_environment(api_url: str) -> None:
    """Point the e2b SDK at an AgentENV server (self-contained)."""
    os.environ["E2B_API_URL"] = api_url
    os.environ["E2B_SANDBOX_URL"] = api_url
    os.environ.setdefault("E2B_API_KEY", DEFAULT_API_KEY)
    os.environ.setdefault("E2B_ACCESS_TOKEN", "dummy")


def patch_api_key_validation() -> None:
    """AgentENV managed keys are raw hex; skip the SDK's e2b_ prefix check."""
    try:
        from e2b import api as e2b_api
    except ImportError:
        return
    if hasattr(e2b_api, "validate_api_key"):
        e2b_api.validate_api_key = lambda api_key: None
DEFAULT_API_KEY_FILE = Path("/var/lib/aenv/secrets/api-key")
DEFAULT_WORKDIR = "/testbed"
REPO_ROOT = Path(__file__).resolve().parents[1]

TRAJECTORY_SEARCH_DIRS = [
    Path("/home/cxd/swe-python/replay"),
    Path("/home/cxd/data/replay"),
    Path("/tmp/p2-trajectories/swesmith"),
    Path("/tmp/p2-trajectories/controls"),
    Path("/tmp/p2-trajectories/mix"),
    REPO_ROOT,
    REPO_ROOT / "other_replay" / "pdf",
    REPO_ROOT / "other_replay" / "browser",
    REPO_ROOT / "dockerfiles" / "django-money_task",
]

IMAGE_MAP_FILES = [
    Path("/home/cxd/swe-python/trajectory-templates.json"),
    Path("/home/cxd/data/trajectory-templates.json"),
    REPO_ROOT / "scripts" / "delay_time_trajectory_image_map.json",
    REPO_ROOT / "image-map.json",
]

MANIFEST_FILE = Path("/home/cxd/data/manifest.csv")

TRAJECTORY_SUFFIXES = (".replay.json", ".json", ".traj")


def absolute_timestamp() -> str:
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


def slugify(value: str, limit: int = 48) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower())
    return slug.strip("-_")[:limit].rstrip("-_") or "instance"


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def find_trajectory(instance_id: str, extra_dirs: list[Path]) -> Path:
    """Locate `<instance_id>.replay.json` under the search roots."""
    dirs = [d.expanduser().resolve() for d in extra_dirs] + TRAJECTORY_SEARCH_DIRS
    for directory in dirs:
        if not directory.is_dir():
            continue
        exact = directory / f"{instance_id}.replay.json"
        if exact.is_file():
            return exact
        for candidate in sorted(directory.iterdir()):
            if not candidate.is_file():
                continue
            if not any(candidate.name.endswith(sfx) for sfx in TRAJECTORY_SUFFIXES):
                continue
            stem = candidate.name
            for suffix in TRAJECTORY_SUFFIXES:
                if stem.endswith(suffix):
                    stem = stem[: -len(suffix)]
                    break
            if stem == instance_id or stem.startswith(instance_id + "."):
                return candidate
    searched = ", ".join(str(d) for d in dirs)
    raise FileNotFoundError(
        f"trajectory not found for instance {instance_id!r}; searched: {searched}"
    )


def load_image_map() -> dict[str, str]:
    """Merge image maps; earlier files win (they are more specific/local)."""
    mapping: dict[str, str] = {}
    for path in IMAGE_MAP_FILES:
        if not path.is_file():
            continue
        try:
            data = load_json(path)
        except (OSError, json.JSONDecodeError) as exc:
            LOGGER.warning("cannot read image map %s: %s", path, exc)
            continue
        if isinstance(data, dict) and isinstance(data.get("images"), dict):
            data = data["images"]
        pairs: list[tuple[str, str]] = []
        if isinstance(data, dict):
            pairs = [(str(key), str(value)) for key, value in data.items() if value]
        elif isinstance(data, list):
            for item in data:
                if not isinstance(item, dict):
                    continue
                key = item.get("trajectory") or item.get("filename")
                value = item.get("image") or item.get("template")
                if isinstance(key, str) and isinstance(value, str):
                    pairs.append((key, value))
        for key, value in pairs:
            mapping.setdefault(key, value)
    return mapping


def resolve_template_from_manifest(instance_id: str) -> str | None:
    """Derive the template name from manifest.csv exactly like build_template.py.

    Rule: image slug (minus swerebench-<arch>- prefix) -> task, tag first
    segment -> num, then sanitize all non [A-Za-z0-9_-] runs to single '-'.
    """
    if not MANIFEST_FILE.is_file():
        return None
    import csv

    with MANIFEST_FILE.open(encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row.get("instance_id") != instance_id:
                continue
            for col in ("image_arm64", "image_x86"):
                remote = (row.get(col) or "").strip()
                if not remote or ":" not in remote:
                    continue
                path, tag = remote.rsplit(":", 1)
                slug = path.split("/")[-1]
                for prefix in ("swerebench-arm64-", "swerebench-x86-"):
                    if slug.startswith(prefix):
                        slug = slug[len(prefix) :]
                        break
                return re.sub(r"[^A-Za-z0-9_-]+", "-", f"swb-{slug}-{tag.split('-')[0]}")
    return None


def resolve_template(
    trajectory_path: Path,
    instance_id: str,
    override: str | None,
) -> str:
    if override:
        return override

    mapping = load_image_map()
    for key in (trajectory_path.name, trajectory_path.stem, instance_id):
        if key in mapping:
            return mapping[key]

    manifest_template = resolve_template_from_manifest(instance_id)
    if manifest_template:
        return manifest_template

    try:
        data = load_json(trajectory_path)
    except (OSError, json.JSONDecodeError):
        data = {}
    candidates = []
    for section in (data, data.get("info") or {}, data.get("replay_config") or {}):
        if isinstance(section, dict):
            candidates.extend(
                [section.get("image"), section.get("template")]
            )
    for candidate in candidates:
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()

    # Last resort: sanitize like build_template.py's tpl_name (dots and
    # '__' runs collapse to single '-'), e.g. validatorjs__validator.js-1306
    # -> swb-validatorjs-validator-js-1306. Only strip known control
    # trajectory suffixes; repo names may contain dots (validator.js).
    base = instance_id
    for suffix in (".neutral", ".paced"):
        if base.endswith(suffix):
            base = base[: -len(suffix)]
    return re.sub(r"[^A-Za-z0-9_-]+", "-", f"swb-{base}")

    raise ValueError(
        f"cannot resolve template for {instance_id!r}; "
        "pass --template explicitly"
    )


def load_steps(trajectory_path: Path) -> list[dict[str, Any]]:
    data = load_json(trajectory_path)
    if not isinstance(data, dict) or not isinstance(data.get("trajectory"), list):
        raise ValueError(f"invalid trajectory JSON: {trajectory_path}")
    steps = []
    for raw in data["trajectory"]:
        if not isinstance(raw, dict):
            continue
        action = raw.get("action")
        if not isinstance(action, str) or not action.strip():
            continue
        delay = raw.get("delay_time", 0.0)
        if not isinstance(delay, (int, float)) or isinstance(delay, bool) or delay < 0:
            delay = 0.0
        steps.append({
            "action": action.strip(),
            "delay_time_sec": float(delay),
            # Recorded ground-truth output from the original trajectory;
            # used to verify snapshot restore fidelity (may be absent).
            "response": raw.get("response") or "",
            "thought": raw.get("thought") or "",
        })
    if not steps:
        raise ValueError(f"no executable actions in {trajectory_path}")
    return steps


def wrap_action(action: str, workdir: str) -> str:
    commands = [
        "set +o pipefail",
        "export PAGER=cat",
        "export MANPAGER=cat",
        "export GIT_PAGER=cat",
        "export LESS=-FRX",
        f"cd {workdir}",
        action,
    ]
    import shlex

    return f"bash -lc {shlex.quote('; '.join(commands))}"


@dataclass
class StepOutcome:
    index: int
    action: str
    exit_code: int | None
    stdout: str
    stderr: str
    succeeded: bool
    skipped: bool
    snapshot_id: str | None
    snapshot_name: str | None
    started_at: str
    completed_at: str
    exec_sec: float
    snapshot_sec: float
    create_sec: float
    delete_sec: float
    # Recorded trajectory response for this step (ground-truth reference),
    # saved verbatim alongside the live output; no comparison is performed.
    recorded_response: str = ""


def truncate(text: str, limit: int = 4000) -> str:
    return text if len(text) <= limit else text[:limit] + f"... [{len(text)} bytes total]"


async def run_command(sandbox: Any, command: str, timeout_sec: int) -> dict[str, Any]:
    handle = await sandbox.commands.run(
        cmd=command,
        background=True,
        timeout=timeout_sec or 0,
        request_timeout=timeout_sec or 0,
        user="root",
    )
    result = await handle.wait()
    return {
        "exit_code": getattr(result, "exit_code", None),
        "stdout": getattr(result, "stdout", "") or "",
        "stderr": getattr(result, "stderr", "") or "",
    }


async def create_snapshot(sandbox: Any, name: str, timeout_sec: int) -> Any:
    return await sandbox.create_snapshot(
        name=name,
        request_timeout=timeout_sec,
    )


async def run_replay(args: argparse.Namespace) -> int:
    trajectory_path = (
        Path(args.trajectory).expanduser().resolve()
        if args.trajectory
        else find_trajectory(args.instance_id, args.trajectory_dir)
    )
    if args.trajectory and not trajectory_path.is_file():
        raise FileNotFoundError(f"trajectory file not found: {trajectory_path}")
    if not args.instance_id:
        try:
            data = json.loads(trajectory_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            data = {}
        args.instance_id = str(
            data.get("instance_id") or trajectory_path.stem.removesuffix(".replay")
        ).strip()

    instance_slug = slugify(args.instance_id)
    run_id = uuid.uuid4().hex[:8]
    output_dir = (
        Path(args.output_dir).expanduser().resolve()
        if args.output_dir
        else Path.cwd() / "runs" / "step-snapshots" / f"{instance_slug}-{run_id}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = output_dir / "run.log"

    def log(message: str) -> None:
        line = f"[{absolute_timestamp()}] {message}"
        print(line, flush=True)
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")

    template = resolve_template(trajectory_path, args.instance_id, args.template)
    steps = load_steps(trajectory_path)
    if args.max_steps > 0:
        steps = steps[: args.max_steps]

    log(
        f"instance={args.instance_id} trajectory={trajectory_path} "
        f"template={template} steps={len(steps)} mode="
        f"{'checkpoint' if args.checkpoint_mode else 'chain'}"
    )

    from e2b import AsyncSandbox

    outcomes: list[StepOutcome] = []
    sandbox: Any = None
    source_template = template
    failure: str | None = None

    try:
        for index, step in enumerate(steps):
            started_at = absolute_timestamp()
            action = step["action"]
            create_sec = 0.0
            delete_sec = 0.0

            if args.checkpoint_mode and sandbox is None:
                t0 = time.perf_counter()
                sandbox = await AsyncSandbox.create(
                    template=source_template,
                    timeout=args.sandbox_timeout_sec,
                    metadata={
                        "runner": "replay-step-snapshots",
                        "instance_id": args.instance_id,
                        "run_id": run_id,
                    },
                )
                create_sec = time.perf_counter() - t0
            elif not args.checkpoint_mode:
                t0 = time.perf_counter()
                sandbox = await AsyncSandbox.create(
                    template=source_template,
                    timeout=args.sandbox_timeout_sec,
                    metadata={
                        "runner": "replay-step-snapshots",
                        "instance_id": args.instance_id,
                        "run_id": run_id,
                        "step_index": str(index),
                    },
                )
                create_sec = time.perf_counter() - t0

            log(f"[{index + 1}/{len(steps)}] exec: {truncate(action, 200)}")
            expected_response = step.get("response") or ""
            t0 = time.perf_counter()
            try:
                result = await run_command(sandbox, wrap_action(action, args.workdir), args.timeout_sec)
                exec_sec = time.perf_counter() - t0
                exit_code = result["exit_code"]
                succeeded = exit_code in (None, 0)
                log(
                    f"[{index + 1}/{len(steps)}] rc={exit_code} "
                    f"elapsed={exec_sec:.3f}s stdout={len(result['stdout'])}B "
                    f"stderr={len(result['stderr'])}B"
                )
                if args.show_response:
                    log(
                        f"[{index + 1}/{len(steps)}] response: "
                        f"{truncate(result['stdout'], args.response_chars)}"
                    )
            except Exception as exc:
                exec_sec = time.perf_counter() - t0
                exit_code = None
                succeeded = False
                result = {"stdout": "", "stderr": f"{type(exc).__name__}: {exc}"}
                log(f"[{index + 1}/{len(steps)}] exec failed: {exc!r}")
                if args.stop_on_error:
                    outcomes.append(
                        StepOutcome(
                            index=index,
                            action=action,
                            exit_code=exit_code,
                            stdout="",
                            stderr=result["stderr"],
                            succeeded=False,
                            skipped=False,
                            snapshot_id=None,
                            snapshot_name=None,
                            started_at=started_at,
                            completed_at=absolute_timestamp(),
                            exec_sec=exec_sec,
                            snapshot_sec=0.0,
                            create_sec=create_sec,
                            delete_sec=delete_sec,
                            recorded_response=expected_response,
                        )
                    )
                    failure = f"step {index} failed: {exc!r}"
                    break

            snapshot_name = f"stepsnap-{instance_slug}-{run_id}-{index:03d}"
            snapshot_id: str | None = None
            snapshot_sec = 0.0
            try:
                t0 = time.perf_counter()
                info = await create_snapshot(sandbox, snapshot_name, args.lifecycle_timeout_sec)
                snapshot_sec = time.perf_counter() - t0
                snapshot_id = info.snapshot_id
                log(
                    f"[{index + 1}/{len(steps)}] snapshot name={snapshot_name} "
                    f"id={snapshot_id} elapsed={snapshot_sec:.3f}s"
                )
            except Exception as exc:
                log(f"[{index + 1}/{len(steps)}] snapshot failed: {exc!r}")
                failure = f"step {index} snapshot failed: {exc!r}"
                outcomes.append(
                    StepOutcome(
                        index=index,
                        action=action,
                        exit_code=exit_code,
                        stdout=result["stdout"],
                        stderr=result["stderr"],
                        succeeded=succeeded,
                        skipped=False,
                        snapshot_id=None,
                        snapshot_name=snapshot_name,
                        started_at=started_at,
                        completed_at=absolute_timestamp(),
                        exec_sec=exec_sec,
                        snapshot_sec=snapshot_sec,
                        create_sec=create_sec,
                        delete_sec=delete_sec,
                        recorded_response=expected_response,
                    )
                )
                break

            outcomes.append(
                StepOutcome(
                    index=index,
                    action=action,
                    exit_code=exit_code,
                    stdout=result["stdout"],
                    stderr=result["stderr"],
                    succeeded=succeeded,
                    skipped=False,
                    snapshot_id=snapshot_id,
                    snapshot_name=snapshot_name,
                    started_at=started_at,
                    completed_at=absolute_timestamp(),
                    exec_sec=exec_sec,
                    snapshot_sec=snapshot_sec,
                    create_sec=create_sec,
                    delete_sec=delete_sec,
                    recorded_response=expected_response,
                )
            )

            if not args.checkpoint_mode:
                t0 = time.perf_counter()
                await sandbox.kill()
                delete_sec = time.perf_counter() - t0
                outcomes[-1].delete_sec = delete_sec
                sandbox = None
                source_template = snapshot_id

            delay_sec = step["delay_time_sec"] * args.delay_scale
            if delay_sec > 0 and index < len(steps) - 1:
                log(f"[{index + 1}/{len(steps)}] wait {delay_sec:.2f}s")
                await asyncio.sleep(delay_sec)
    except Exception as exc:
        failure = f"{type(exc).__name__}: {exc}"
        log(f"replay aborted: {failure}")
    finally:
        if sandbox is not None and not args.keep_final_sandbox:
            try:
                await sandbox.kill()
                log(f"deleted sandbox {sandbox.sandbox_id}")
            except Exception as exc:
                log(f"failed to delete sandbox: {exc!r}")

    result = {
        "instance_id": args.instance_id,
        "trajectory": str(trajectory_path),
        "template": template,
        "run_id": run_id,
        "mode": "checkpoint" if args.checkpoint_mode else "chain",
        "started_at": outcomes[0].started_at if outcomes else None,
        "completed_at": absolute_timestamp(),
        "total_steps": len(steps),
        "executed_steps": len(outcomes),
        "succeeded_steps": sum(1 for o in outcomes if o.succeeded),
        "snapshots": [
            {"step": o.index, "snapshot_id": o.snapshot_id, "snapshot_name": o.snapshot_name}
            for o in outcomes
            if o.snapshot_id
        ],
        "error": failure,
        "steps": [vars(o) for o in outcomes],
    }
    result_path = output_dir / "replay-result.json"
    result_path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    log(f"result written to {result_path}")
    log(
        f"done: {result['executed_steps']}/{result['total_steps']} steps, "
        f"{len(result['snapshots'])} snapshots"
    )
    return 0 if failure is None else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="replay-step-snapshots",
        description="Replay one trajectory and snapshot after every step.",
    )
    parser.add_argument(
        "instance_id",
        nargs="?",
        help=(
            "Instance id, e.g. "
            "django-money__django-money.835c1ab8.combine_file__i1ay87eb "
            "(can be omitted when --trajectory is given)"
        ),
    )
    parser.add_argument(
        "--trajectory",
        type=Path,
        help="Direct path to a replay JSON file (skips instance-id lookup)",
    )
    parser.add_argument("--template", help="Override the resolved template")
    parser.add_argument(
        "--trajectory-dir",
        action="append",
        default=[],
        type=Path,
        help="Extra trajectory search directory (repeatable)",
    )
    parser.add_argument("--api-url", default=os.getenv("E2B_API_URL", DEFAULT_API_URL))
    parser.add_argument("--workdir", default=DEFAULT_WORKDIR)
    parser.add_argument("--timeout-sec", type=int, default=300, help="Per-command timeout")
    parser.add_argument("--sandbox-timeout-sec", type=int, default=86_400)
    parser.add_argument("--lifecycle-timeout-sec", type=int, default=300, help="Snapshot API timeout")
    parser.add_argument("--max-steps", type=int, default=0, help="Only replay the first N steps")
    parser.add_argument(
        "--delay-scale",
        type=float,
        default=0.0,
        help="Sleep delay_time * scale between steps (default 0 = no wait)",
    )
    parser.add_argument(
        "--checkpoint-mode",
        action="store_true",
        help="Keep one sandbox running and snapshot after each action "
        "(default: chain mode, reload from the previous snapshot each step)",
    )
    parser.add_argument("--stop-on-error", action="store_true")
    parser.add_argument("--keep-final-sandbox", action="store_true")
    parser.add_argument(
        "--show-response",
        action="store_true",
        help="Print each step's command response to the console",
    )
    parser.add_argument(
        "--response-chars",
        type=int,
        default=1200,
        help="Max characters printed per response block (default: 1200)",
    )
    parser.add_argument("--output-dir", type=Path)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    for logger_name in ("httpx", "httpcore", "e2b"):
        logging.getLogger(logger_name).setLevel(logging.WARNING)

    if not os.environ.get("E2B_API_KEY"):
        if DEFAULT_API_KEY_FILE.is_file():
            key = DEFAULT_API_KEY_FILE.read_text(encoding="utf-8").strip()
            if key:
                os.environ["E2B_API_KEY"] = key

    configure_aenv_environment(args.api_url)
    patch_api_key_validation()
    try:
        return asyncio.run(run_replay(args))
    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
