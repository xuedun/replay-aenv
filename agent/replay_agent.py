from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import re
import shlex
import sys
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Any

if __package__:
    from .control_plane_scheduler import (
        ControlPlaneConfig,
        ControlPlaneScheduler,
        is_transient_sandbox_error,
    )
    from .run_slot_scheduler import RunningLease, RunningSlotScheduler
else:
    from control_plane_scheduler import (
        ControlPlaneConfig,
        ControlPlaneScheduler,
        is_transient_sandbox_error,
    )
    from run_slot_scheduler import RunningLease, RunningSlotScheduler


LOGGER = logging.getLogger("replay-aenv")
SUBMIT_ACTIONS = {"submit", "finish", "done"}
DEFAULT_API_URL = "http://127.0.0.1:8000"
DEFAULT_TEMPLATE = "django-money-replay-v2"
DEFAULT_API_KEY = "e2b_" + "0" * 40
DEFAULT_INSTANCE_OVERLAY_DIR = "instance_overlays"
WRITE_MODES = ("buffered", "tmpfs", "directio")


class AbsoluteTimeFormatter(logging.Formatter):
    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        del datefmt
        return datetime.fromtimestamp(record.created).astimezone().isoformat(
            timespec="milliseconds"
        )


def absolute_timestamp() -> str:
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


def is_timeout_error(exc: BaseException) -> bool:
    """Return whether an exception represents a timeout."""

    text = f"{type(exc).__name__}: {exc}".lower()
    return "timeout" in text or "timed out" in text


def is_create_timeout_error(exc: BaseException) -> bool:
    """Return whether a create failure is ambiguous and must not be retried."""

    return is_timeout_error(exc)


@dataclass(slots=True, frozen=True)
class ReplayStep:
    index: int
    action: str
    delay_time_sec: float


@dataclass(slots=True, frozen=True)
class ScheduledTrajectory:
    """One independently executed occurrence of a source trajectory."""

    path: Path
    source_index: int
    cycle: int
    occurrence: int
    workload_name: str = "default"
    template: str | None = None
    workload_occurrence: int = 0


@dataclass(slots=True, frozen=True)
class WorkloadSpec:
    """One template paired with a trajectory set and a total VM count."""

    name: str
    template: str
    trajectory_dir: Path
    trajectories: tuple[Path, ...]
    vm_count: int


@dataclass(slots=True, frozen=True)
class MixedReplayConfig:
    concurrency: int | None
    workloads: tuple[WorkloadSpec, ...]


@dataclass(slots=True)
class ReplayOptions:
    template: str = DEFAULT_TEMPLATE
    workdir: str = "/testbed"
    timeout_sec: int | None = 300
    command_timeout_sec: int | None = 300
    command_timeout_continue: bool = False
    pause_duration_sec: float = 0.0
    delay_override_sec: float | None = None
    stop_on_error: bool = False
    setup_command: str | None = None
    write_mode: str = "buffered"
    sandbox_timeout_sec: int = 86_400
    create_retries: int = 1
    lifecycle_timeout_sec: int = 120
    lifecycle_retries: int = 2
    ready_probe_enabled: bool = False
    ready_probe_attempts: int = 5
    ready_probe_timeout_sec: int = 10
    keep_sandbox_on_failure: bool = False
    instance_overlay_dir: Path | None = None
    command_transport: str = "aenv-proxy"
    direct_envd_port: int = 49_983
    extra_delay_sec: float = 0.0


class SandboxInfrastructureError(RuntimeError):
    """Sandbox transport/readiness failure that must stop the current replay."""

    def __init__(self, message: str, details: dict[str, Any]):
        super().__init__(message)
        self.details = details


class CommandTimeoutError(RuntimeError):
    """A trajectory command timed out while continuation was disabled."""


def configure_aenv_environment(api_url: str) -> None:
    os.environ["E2B_API_URL"] = api_url
    os.environ["E2B_SANDBOX_URL"] = api_url
    os.environ.setdefault("E2B_API_KEY", DEFAULT_API_KEY)
    os.environ.setdefault("E2B_ACCESS_TOKEN", "dummy")


async def cleanup_all_sandboxes(
    *, label: str = "cleanup", log: logging.Logger | None = None
) -> int:
    """List and kill every sandbox on the configured AgentENV server."""
    logger = log or LOGGER
    from e2b import AsyncSandbox
    from e2b.sandbox.sandbox_api import SandboxQuery

    killed = 0
    try:
        paginator = AsyncSandbox.list(query=SandboxQuery(), limit=200)
        all_sandboxes: list[Any] = []
        while paginator.has_next:
            all_sandboxes.extend(await paginator.next_items())
    except Exception as exc:
        logger.warning("[%s] Failed to list sandboxes: %s: %r", label, type(exc).__name__, exc)
        return 0

    if not all_sandboxes:
        logger.info("[%s] No sandboxes to clean up", label)
        return 0

    logger.info("[%s] Killing %d sandbox(es)", label, len(all_sandboxes))
    for sandbox in all_sandboxes:
        sid = getattr(sandbox, "sandbox_id", str(sandbox))
        try:
            await AsyncSandbox.kill(sid)
            killed += 1
            logger.info("[%s] Killed sandbox %s", label, sid)
        except Exception as exc:
            logger.warning(
                "[%s] Failed to kill sandbox %s: %s: %r",
                label, sid, type(exc).__name__, exc,
            )
    logger.info("[%s] Cleaned up %d/%d sandbox(es)", label, killed, len(all_sandboxes))
    return killed


def load_trajectory(path: str | Path) -> tuple[str, str, list[ReplayStep]]:
    trajectory_path = Path(path).expanduser().resolve()
    if not trajectory_path.is_file():
        raise FileNotFoundError(f"Trajectory not found: {trajectory_path}")

    try:
        data: Any = json.loads(trajectory_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid trajectory JSON: {trajectory_path}: {exc}") from exc

    if not isinstance(data, dict):
        raise ValueError("Trajectory root must be a JSON object")

    environment_name = data.get("environment", "main")
    if not isinstance(environment_name, str):
        environment_name = "main"

    # 处理后的 replay JSON 通常没有单独的 instance_id 字段，因此优先读字段，
    # 缺失时再从 `<instance_id>.replay.json` 文件名恢复完整任务 ID。
    instance_id = data.get("instance_id")
    if not isinstance(instance_id, str) or not instance_id.strip():
        instance_id = trajectory_path.name
        for suffix in (".replay.json", ".json", ".traj"):
            if instance_id.endswith(suffix):
                instance_id = instance_id[: -len(suffix)]
                break
    instance_id = instance_id.strip()
    if not instance_id:
        raise ValueError(f"Cannot determine instance_id from {trajectory_path}")

    raw_steps = data.get("trajectory")
    if not isinstance(raw_steps, list):
        raise ValueError("trajectory must be a JSON array")

    steps: list[ReplayStep] = []
    for raw_step in raw_steps:
        if not isinstance(raw_step, dict):
            continue
        action = raw_step.get("action")
        if not isinstance(action, str) or not action.strip():
            continue

        raw_delay = raw_step.get("delay_time", 0.0)
        if (
            isinstance(raw_delay, bool)
            or not isinstance(raw_delay, (int, float))
            or raw_delay < 0
        ):
            LOGGER.warning(
                "Invalid delay_time=%r for action %r; using 0 seconds",
                raw_delay,
                action,
            )
            delay_time_sec = 0.0
        else:
            delay_time_sec = float(raw_delay)

        steps.append(
            ReplayStep(
                index=len(steps),
                action=action.strip(),
                delay_time_sec=delay_time_sec,
            )
        )

    if not steps:
        raise ValueError(f"No executable actions found in {trajectory_path}")
    return environment_name, instance_id, steps


def _remove_one_boundary_newline(value: str) -> str:
    """Remove one presentation newline from each edge of a tool argument."""

    if value.startswith("\r\n"):
        value = value[2:]
    elif value.startswith("\n"):
        value = value[1:]

    if value.endswith("\r\n"):
        value = value[:-2]
    elif value.endswith("\n"):
        value = value[:-1]
    return value


def normalize_str_replace_editor_action(action: str) -> str:
    """Match the original SWE structured-tool handling for multiline edits.

    Recorded ``str_replace_editor`` actions render multiline ``old_str`` and
    ``new_str`` values with one presentation newline at each edge.  The
    original structured tool call removes those edge newlines before matching,
    while invoking the rendered CLI command directly preserves them.  Rebuild
    only ``str_replace`` commands with the same boundary normalization; all
    other actions remain byte-for-byte unchanged.
    """

    try:
        arguments = shlex.split(action)
    except ValueError:
        return action

    if (
        len(arguments) < 3
        or arguments[0] != "str_replace_editor"
        or arguments[1] != "str_replace"
    ):
        return action

    flag_indexes: dict[str, int] = {}
    for flag in ("--old_str", "--new_str"):
        try:
            index = arguments.index(flag)
        except ValueError:
            return action
        if index + 1 >= len(arguments):
            return action
        flag_indexes[flag] = index + 1

    normalized_old_str = _remove_one_boundary_newline(
        arguments[flag_indexes["--old_str"]]
    )
    if not normalized_old_str:
        return action

    arguments[flag_indexes["--old_str"]] = normalized_old_str
    arguments[flag_indexes["--new_str"]] = _remove_one_boundary_newline(
        arguments[flag_indexes["--new_str"]]
    )
    return shlex.join(arguments)


def rewrite_write_txt_action(action: str, write_mode: str, workdir: str) -> str:
    """Apply a batch-level I/O mode to an exact ``writeTxt N`` action.

    Unrelated trajectory actions are returned byte-for-byte unchanged.  The
    tmpfs and direct-I/O commands intentionally match the payload writers used
    by ``scripts/measure_pause_snapshot_growth.py`` so replay and standalone
    measurements exercise the same data paths.
    """

    if write_mode not in WRITE_MODES:
        raise ValueError(f"unsupported write mode: {write_mode}")
    try:
        arguments = shlex.split(action)
    except ValueError:
        return action
    if len(arguments) != 2 or arguments[0] != "writeTxt":
        return action
    try:
        size_mib = int(arguments[1])
    except ValueError:
        return action
    if size_mib < 0:
        return action
    if write_mode == "buffered":
        return action

    if write_mode == "tmpfs":
        target = "/dev/shm/pause-growth-tmpfs.bin"
        return (
            'test "$(findmnt -n -o FSTYPE -T /dev/shm)" = tmpfs && '
            f"dd if=/dev/urandom of={shlex.quote(target)} bs=1048576 "
            f"count={size_mib} status=none"
        )
    else:
        target = f"{workdir.rstrip('/')}/pause-growth-directio.bin"
    return (
        f"fstype=$(findmnt -n -o FSTYPE -T {shlex.quote(workdir)}) && "
        'case "$fstype" in tmpfs|ramfs) exit 1;; esac && '
        f"dd if=/dev/urandom of={shlex.quote(target)} bs=1048576 "
        f"count={size_mib} oflag=direct conv=fsync status=none"
    )


def wrap_action(action: str, workdir: str, setup_command: str | None) -> str:
    commands = [
        # The original SWE bash tool reports the status of the final pipeline
        # command.  Keep the replay shell consistent with that behavior.
        "set +o pipefail",
        "export PAGER=cat",
        "export MANPAGER=cat",
        "export GIT_PAGER=cat",
        "export LESS=-FRX",
        f"cd {shlex.quote(workdir)}",
    ]
    if setup_command:
        commands.append(setup_command)
    commands.append(action)
    return f"bash -lc {shlex.quote('; '.join(commands))}"


def slugify(value: str, limit: int = 72) -> str:
    slug = re.sub(r"[^a-z0-9.-]+", "-", value.lower().replace("_", "-"))
    return slug.strip("-.")[:limit] or "replay"


def default_run_dir(trajectory_path: Path) -> Path:
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    stem = trajectory_path.name
    for suffix in (".replay.json", ".json", ".traj"):
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
            break
    return Path.cwd() / "runs" / f"{stamp}-{slugify(stem)}"


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def result_fields(raw_result: Any) -> tuple[str, str, int | None]:
    stdout = getattr(raw_result, "stdout", "")
    stderr = getattr(raw_result, "stderr", "")
    return_code = None
    for name in ("exit_code", "return_code", "exit_status", "code"):
        value = getattr(raw_result, name, None)
        if isinstance(value, int):
            return_code = value
            break
    return str(stdout or ""), str(stderr or ""), return_code


def append_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as output_file:
        output_file.write(text.rstrip() + "\n")


def replay_outcome(result: dict[str, Any]) -> tuple[str, str]:
    if result.get("succeeded"):
        status = "success"
        failed_commands = int(result.get("failed_commands", 0))
        if failed_commands:
            completion_note = (
                "已到达终止 action"
                if result.get("submitted")
                else "已完整执行全部可回放命令"
            )
            summary = (
                f"{completion_note}；{failed_commands}/{result.get('commands', 0)} "
                "条命令返回非零，已按命令结果记录，不影响回放完成状态。"
            )
        else:
            summary = "全部已执行命令成功。"
    elif result.get("stopped_early"):
        status = "stopped_early"
        summary = "遇到失败命令后提前停止，尚未到达终止 action。"
    elif result.get("failed_commands", 0):
        status = "completed_with_errors"
        terminal_note = "已到达终止 action" if result.get("submitted") else "未遇到终止 action"
        summary = (
            f"{terminal_note}，但 {result['failed_commands']}/{result['commands']} "
            "条命令返回非零；请查看 replay-result.json 中的 stderr。"
        )
    else:
        status = "failed"
        summary = str(result.get("error", "回放未成功完成。"))
    return status, summary


def job_log_line(level: str, message: str, *, timestamp: str | None = None) -> str:
    safe_message = message.replace("\r", "\\r").replace("\n", "\\n")
    return f"{timestamp or absolute_timestamp()} {level.upper()} {safe_message}"


def replay_summary_text(result: dict[str, Any], result_path: Path) -> str:
    status, summary = replay_outcome(result)

    return "\n".join(
        [
            "",
            f"[{result.get('completed_at', absolute_timestamp())}] [REPLAY RESULT]",
            f"started_at: {result.get('started_at')}",
            f"finished_at: {result.get('completed_at')}",
            f"status: {status}",
            f"commands: {result.get('commands', 0)}",
            f"succeeded: {result.get('succeeded_commands', 0)}",
            f"failed: {result.get('failed_commands', 0)}",
            f"submitted: {str(bool(result.get('submitted'))).lower()}",
            f"stopped_early: {str(bool(result.get('stopped_early'))).lower()}",
            f"elapsed: {float(result.get('elapsed_sec', 0.0)):.3f}s",
            f"summary: {summary}",
            f"result: {result_path.resolve()}",
        ]
    )


def batch_summary_text(summary: dict[str, Any], result_path: Path) -> str:
    status = "success" if summary["failed"] == 0 else "completed_with_errors"
    explanation = (
        "全部轨迹成功。"
        if summary["failed"] == 0
        else f"{summary['failed']}/{summary['total']} 条轨迹失败，请查看对应任务目录。"
    )
    return "\n".join(
        [
            "",
            f"[{summary['completed_at']}] [BATCH RESULT]",
            f"started_at: {summary['started_at']}",
            f"finished_at: {summary['completed_at']}",
            f"status: {status}",
            f"total: {summary['total']}",
            f"succeeded: {summary['succeeded']}",
            f"failed: {summary['failed']}",
            f"concurrency: {summary['concurrency']}",
            f"summary: {explanation}",
            f"result: {result_path.resolve()}",
        ]
    )


class ReplayRunner:
    def __init__(
        self,
        trajectory_path: Path,
        output_dir: Path,
        options: ReplayOptions,
        *,
        console_output: bool = True,
        control_plane_scheduler: ControlPlaneScheduler | None = None,
        running_slot_scheduler: RunningSlotScheduler | None = None,
    ):
        self.trajectory_path = trajectory_path.expanduser().resolve()
        self.output_dir = output_dir.expanduser().resolve()
        self.options = options
        self.console_output = console_output
        # batch 模式为所有 ReplayRunner 注入同一个 scheduler；单任务模式使用
        # 固定容量 1。调度算法集中在 control_plane_scheduler.py。
        self.control_plane_scheduler = (
            control_plane_scheduler or ControlPlaneScheduler.single_task()
        )
        self.running_slot_scheduler = (
            running_slot_scheduler or RunningSlotScheduler.single_task()
        )
        self.run_slot_task_id = f"{self.output_dir.name}:{self.trajectory_path.name}"
        self.job_log_path = self.output_dir / "job.log"
        self.sandbox: Any | None = None
        self.sandbox_id: str | None = None
        # One key is generated per ReplayRunner and reused by all create retries.
        # AgentEnv does not expose a native idempotency key for create(), so the
        # key is stored in metadata and used to reconcile an ambiguous timeout.
        self.sandbox_create_key = uuid.uuid4().hex
        # A timed-out create can still complete server-side after the client
        # returns. While that outcome is unresolved, its running slot must not
        # be returned to the pool or the configured hard cap could be exceeded.
        self._create_state_uncertain = False
        self._paused = False
        self._running_lease: RunningLease | None = None

    def _event(self, level: str, message: str) -> None:
        timestamp = absolute_timestamp()
        safe_message = message.replace("\r", "\\r").replace("\n", "\\n")
        line = f"{timestamp} {level.upper()} {safe_message}"
        append_text(self.output_dir / "run.log", line)
        self._job_event(level, message, timestamp=timestamp)
        if self.console_output:
            print(line, flush=True)

    def _job_event(
        self,
        level: str,
        message: str,
        *,
        timestamp: str | None = None,
    ) -> None:
        append_text(
            self.job_log_path,
            job_log_line(level, message, timestamp=timestamp),
        )

    def _log_retry_queued(
        self,
        *,
        stage: str,
        attempt: int,
        max_attempts: int | str,
        error: BaseException,
    ) -> None:
        """Record a transient failure that will re-enter the global QPS queue."""

        self._event(
            "warning",
            f"Retry stage={stage} attempt={attempt}/{max_attempts} "
            "retryable=true retry_mode=rate_limit requeued=true "
            f"error_type={type(error).__name__} error={error!r}",
        )

    def _log_retry_recovered(
        self,
        *,
        stage: str,
        attempts: int,
    ) -> None:
        """Record that a previously failed operation recovered on retry."""

        self._event(
            "info",
            f"Retry recovered stage={stage} attempts={attempts} "
            "retry_mode=rate_limit",
        )

    def _log_retry_exhausted(
        self,
        *,
        stage: str,
        attempts: int,
        retryable: bool,
        error: BaseException,
    ) -> None:
        """Record the final error when an operation cannot be retried again."""

        self._event(
            "error",
            f"Retry exhausted stage={stage} attempts={attempts} "
            f"retryable={str(retryable).lower()} "
            f"error_type={type(error).__name__} error={error!r}",
        )

    def _block(self, text: str) -> None:
        append_text(self.output_dir / "run.log", text)
        if self.console_output:
            print(text, flush=True)

    async def _find_sandboxes_by_create_key(self) -> list[Any]:
        """Return every live sandbox created for this replay task."""
        from e2b import AsyncSandbox
        from e2b.sandbox.sandbox_api import SandboxQuery

        paginator = AsyncSandbox.list(
            query=SandboxQuery(
                metadata={"replay_create_key": self.sandbox_create_key}
            ),
            limit=100,
        )
        matches: list[Any] = []
        while paginator.has_next:
            matches.extend(await paginator.next_items())
        return matches

    async def _reconcile_ambiguous_create(self) -> bool:
        """Adopt a sandbox whose create response was lost.

        A create ReadTimeout is ambiguous: AgentEnv may have committed the
        sandbox even though the client did not receive its ID.  Require two
        successful, spaced metadata queries to report no match before allowing
        the caller to issue another non-idempotent create request.  A failed
        query never counts as proof that the sandbox does not exist.
        """
        from e2b import AsyncSandbox

        successful_empty_checks = 0
        last_lookup_error: Exception | None = None
        max_checks = 3

        for check in range(1, max_checks + 1):
            try:
                # Reconciliation attempts use the same global QPS queue as all
                # other infrastructure calls instead of an independent timer.
                async with self.control_plane_scheduler.create_slot():
                    matches = await self._find_sandboxes_by_create_key()
            except Exception as lookup_exc:
                last_lookup_error = lookup_exc
                self._event(
                    "warning",
                    "Sandbox create reconciliation query failed; "
                    f"check={check}/{max_checks} "
                    f"create_key={self.sandbox_create_key} "
                    f"error_type={type(lookup_exc).__name__} "
                    f"error={lookup_exc!r}",
                )
                continue

            if matches:
                # The key should identify exactly one sandbox.  If an older
                # client already created duplicates, consistently adopt the
                # earliest one and report the condition instead of creating yet
                # another sandbox.
                matches.sort(
                    key=lambda item: (
                        str(getattr(item, "started_at", "")),
                        item.sandbox_id,
                    )
                )
                selected = matches[0]
                if len(matches) > 1:
                    self._event(
                        "warning",
                        "Multiple sandboxes matched create key; "
                        f"create_key={self.sandbox_create_key} "
                        f"selected={selected.sandbox_id} "
                        f"matches={','.join(item.sandbox_id for item in matches)}",
                    )
                try:
                    async with self.control_plane_scheduler.create_slot():
                        self.sandbox = await AsyncSandbox.connect(
                            selected.sandbox_id,
                            timeout=self.options.sandbox_timeout_sec,
                        )
                except Exception as connect_exc:
                    last_lookup_error = connect_exc
                    self._event(
                        "warning",
                        "Sandbox create reconciliation connect failed; "
                        f"check={check}/{max_checks} "
                        f"create_key={self.sandbox_create_key} "
                        f"sandbox_id={selected.sandbox_id} "
                        f"error_type={type(connect_exc).__name__} "
                        f"error={connect_exc!r}",
                    )
                    continue
                self.sandbox_id = selected.sandbox_id
                self._event(
                    "info",
                    "Recovered sandbox after ambiguous create failure; "
                    f"sandbox_id={self.sandbox_id} "
                    f"create_key={self.sandbox_create_key} check={check}",
                )
                return True

            successful_empty_checks += 1
            self._event(
                "info",
                "Sandbox create reconciliation found no match; "
                f"check={check}/{max_checks} "
                f"successful_empty_checks={successful_empty_checks}/2 "
                f"create_key={self.sandbox_create_key}",
            )
            if successful_empty_checks >= 2:
                return False

        # Do not turn an unavailable list API into a duplicate create.  The
        # task fails safely and logs its create key for later reconciliation.
        raise RuntimeError(
            "Cannot confirm that timed-out sandbox create did not succeed; "
            f"refusing duplicate create for key {self.sandbox_create_key}"
        ) from last_lookup_error

    async def _create_sandbox(self) -> None:
        from e2b import AsyncSandbox

        # Create bypasses the QPS scheduler, but run() reserves a shared running
        # slot before calling this method because create returns a RUNNING sandbox.
        create_attempts = 0
        create_had_failure = False
        for attempt in range(1, self.options.create_retries + 2):
            create_attempts = attempt
            try:
                self.sandbox = await AsyncSandbox.create(
                    template=self.options.template,
                    metadata={
                        "runner": "replay-aenv",
                        "trajectory": self.trajectory_path.name,
                        "replay_create_key": self.sandbox_create_key,
                    },
                    timeout=self.options.sandbox_timeout_sec,
                    request_timeout=self.options.lifecycle_timeout_sec,
                )
                self._create_state_uncertain = False
                break
            except Exception as exc:
                create_had_failure = True
                retryable = is_transient_sandbox_error(exc)
                wait_ready_failure = re.search(
                    r"\b500: sandbox ([0-9a-fA-F-]{36}) operation WaitReady failed:",
                    str(exc),
                )
                if wait_ready_failure:
                    # A server-side WaitReady failure identifies the failed VM.
                    # Keep its RUNNING lease until deletion returns successfully.
                    self._create_state_uncertain = True
                    failed_sandbox_id = wait_ready_failure.group(1)
                    self._event(
                        "warning",
                        f"Cleaning up sandbox after WaitReady failure: {failed_sandbox_id}",
                    )
                    async with self.control_plane_scheduler.cleanup_slot():
                        await AsyncSandbox.kill(failed_sandbox_id)
                    self._create_state_uncertain = False
                    if attempt > self.options.create_retries:
                        self._log_retry_exhausted(
                            stage="create",
                            attempts=attempt,
                            retryable=True,
                            error=exc,
                        )
                        raise
                    self._log_retry_queued(
                        stage="create",
                        attempt=attempt,
                        max_attempts=self.options.create_retries + 1,
                        error=exc,
                    )
                    await asyncio.sleep(2)
                    continue
                if is_create_timeout_error(exc):
                    self._create_state_uncertain = True
                    self._event(
                        "warning",
                        "Sandbox create timed out; "
                        f"attempt={attempt} create_key={self.sandbox_create_key} "
                        "reconciling_without_retry=true "
                        f"error_type={type(exc).__name__} error={exc!r}",
                    )
                    # A timeout is ambiguous: the server may still finish the
                    # original create after the client stops waiting. Adopt a
                    # visible match, but never issue a second create request.
                    if await self._reconcile_ambiguous_create():
                        self._create_state_uncertain = False
                        break
                    self._log_retry_exhausted(
                        stage="create",
                        attempts=attempt,
                        retryable=False,
                        error=exc,
                    )
                    raise RuntimeError(
                        "Sandbox create timed out and no matching sandbox is "
                        "visible yet; refusing a duplicate create for key "
                        f"{self.sandbox_create_key}"
                    ) from exc
                if not retryable or attempt > self.options.create_retries:
                    self._log_retry_exhausted(
                        stage="create",
                        attempts=attempt,
                        retryable=retryable,
                        error=exc,
                    )
                    raise
                self._event(
                    "warning",
                    "Sandbox create failed; "
                    f"attempt={attempt}/{self.options.create_retries + 1} "
                    f"create_key={self.sandbox_create_key} "
                    "reconciling_before_retry=true "
                    f"error_type={type(exc).__name__} error={exc!r}",
                )
                # create() is not idempotent.  A timeout may only mean that its
                # response was lost, so query by the per-task key and adopt the
                # server-side sandbox before considering another create call.
                self._create_state_uncertain = True
                if await self._reconcile_ambiguous_create():
                    self._create_state_uncertain = False
                    break
                self._create_state_uncertain = False

                self._log_retry_queued(
                    stage="create",
                    attempt=attempt,
                    max_attempts=self.options.create_retries + 1,
                    error=exc,
                )
        if create_had_failure:
            self._log_retry_recovered(
                stage="create",
                attempts=create_attempts,
            )
        self.sandbox_id = getattr(self.sandbox, "sandbox_id", None)
        self._event(
            "info",
            f"Created sandbox {self.sandbox_id} create_attempts={create_attempts}",
        )

    async def _dispatch_command(self, command: str, timeout_sec: int | None) -> Any:
        if self.sandbox is None:
            raise RuntimeError("Sandbox is not available")

        # Only serialize stream establishment.  Releasing the command slot as
        # soon as the handle is returned keeps long-running sandbox commands
        # concurrent while preventing thousands of simultaneous stream opens.
        async with self.control_plane_scheduler.command_start_slot():
            handle = await self.sandbox.commands.run(
                cmd=command,
                background=True,
                timeout=timeout_sec or 0,
                request_timeout=timeout_sec or 0,
                user="root",
            )
        try:
            return await handle.wait()
        except Exception as exc:
            if any(hasattr(exc, name) for name in ("exit_code", "stdout", "stderr")):
                return exc
            raise

    async def _validate_workdir(self) -> None:
        command = f"bash -lc {shlex.quote(f'test -d {shlex.quote(self.options.workdir)}')}"
        raw_result = await self._dispatch_command(
            command,
            min(self.options.timeout_sec or 30, 30),
        )
        _, stderr, return_code = result_fields(raw_result)
        if return_code not in (None, 0):
            raise RuntimeError(
                f"Replay workdir does not exist in sandbox: {self.options.workdir}; "
                f"stderr={stderr!r}"
            )
        self._event("info", f"Validated workdir {self.options.workdir}")

    async def _probe_sandbox_ready(self) -> dict[str, Any]:
        """Run an idempotent command after resume until the command plane is ready.

        Resume success only proves that the sandbox lifecycle API has returned; at
        high concurrency the command service can become reachable slightly later.
        Retrying `true` is safe because it does not mutate the task workspace.
        """

        started_at = absolute_timestamp()
        started = time.perf_counter()
        errors: list[dict[str, Any]] = []
        total_wait_sec = 0.0

        for attempt in range(1, self.options.ready_probe_attempts + 1):
            attempt_started = time.perf_counter()
            try:
                raw_result = await self._dispatch_command(
                    "true",
                    self.options.ready_probe_timeout_sec,
                )
                stdout, stderr, return_code = result_fields(raw_result)
                if return_code in (None, 0):
                    completed_at = absolute_timestamp()
                    elapsed_sec = time.perf_counter() - started
                    result = {
                        "command": "true",
                        "started_at": started_at,
                        "completed_at": completed_at,
                        "attempts": attempt,
                        "succeeded": True,
                        "elapsed_sec": elapsed_sec,
                        "total_wait_sec": total_wait_sec,
                        "errors": errors,
                    }
                    if attempt > 1:
                        self._log_retry_recovered(
                            stage="ready_probe",
                            attempts=attempt,
                        )
                    return result

                error_text = (
                    "Readiness command returned an unexpected result: "
                    f"exit_code={return_code!r} stdout={stdout!r} stderr={stderr!r}"
                )
                errors.append(
                    {
                        "attempt": attempt,
                        "elapsed_sec": time.perf_counter() - attempt_started,
                        "error": error_text,
                        "retryable": False,
                    }
                )
                details = {
                    "stage": "ready_probe",
                    "command": "true",
                    "started_at": started_at,
                    "completed_at": absolute_timestamp(),
                    "attempts": attempt,
                    "succeeded": False,
                    "elapsed_sec": time.perf_counter() - started,
                    "total_wait_sec": total_wait_sec,
                    "errors": errors,
                }
                raise SandboxInfrastructureError(error_text, details)
            except SandboxInfrastructureError:
                raise
            except Exception as exc:
                error_text = f"{type(exc).__name__}: {exc}".rstrip()
                retryable = is_transient_sandbox_error(exc)
                error_entry = {
                    "attempt": attempt,
                    "elapsed_sec": time.perf_counter() - attempt_started,
                    "error": error_text,
                    "retryable": retryable,
                }
                errors.append(error_entry)

                exhausted = attempt >= self.options.ready_probe_attempts
                if exhausted or not retryable:
                    completed_at = absolute_timestamp()
                    details = {
                        "stage": "ready_probe",
                        "command": "true",
                        "started_at": started_at,
                        "completed_at": completed_at,
                        "attempts": attempt,
                        "succeeded": False,
                        "elapsed_sec": time.perf_counter() - started,
                        "total_wait_sec": total_wait_sec,
                        "errors": errors,
                    }
                    self._log_retry_exhausted(
                        stage="ready_probe",
                        attempts=attempt,
                        retryable=retryable,
                        error=exc,
                    )
                    raise SandboxInfrastructureError(
                        "Sandbox command service did not become ready after "
                        f"{attempt} attempt(s): {error_text}",
                        details,
                    ) from exc

                # The next attempt re-enters the global QPS queue. There is no
                # per-task sleep and retry traffic shares the same rate budget.
                error_entry["retry_mode"] = "rate_limit"
                self._log_retry_queued(
                    stage="ready_probe",
                    attempt=attempt,
                    max_attempts=self.options.ready_probe_attempts,
                    error=exc,
                )

        raise AssertionError("ready probe loop ended unexpectedly")

    async def _initialize_instance_state(self, instance_id: str) -> dict[str, Any]:
        """在第一条轨迹命令前恢复 SWE-smith 任务的缺陷初始状态。

        manifest 和 initial.patch 保存在宿主机；补丁经 E2B 文件接口上传到
        sandbox，再从 manifest 指定的基础提交开始正向应用。任一校验失败都会
        抛出异常，使任务在执行轨迹前失败，避免在错误代码状态上继续回放。
        """
        overlay_root = self.options.instance_overlay_dir
        if overlay_root is None:
            self._event("info", "Instance patch initialization skipped")
            return {"status": "skipped", "instance_id": instance_id}

        overlay_root = overlay_root.expanduser().resolve()
        trajectory_instance_id = instance_id
        instance_dir = (overlay_root / instance_id).resolve()
        if not instance_dir.is_relative_to(overlay_root):
            raise ValueError(f"Unsafe instance_id path: {instance_id!r}")

        manifest_path = instance_dir / "manifest.json"
        if not manifest_path.is_file():
            # 压测轨迹可能添加 copy_<编号>_ 等前缀或其他后缀。只要轨迹 ID
            # 包含一个完整的 overlay 任务 ID，就复用该任务的初始状态补丁。
            matches = sorted(
                candidate
                for candidate in overlay_root.iterdir()
                if candidate.is_dir()
                and candidate.name in trajectory_instance_id
                and (candidate / "manifest.json").is_file()
            )
            if not matches:
                raise FileNotFoundError(
                    "Instance patch manifest not found for trajectory instance_id "
                    f"{trajectory_instance_id!r} under {overlay_root}"
                )
            if len(matches) > 1:
                matched_ids = ", ".join(candidate.name for candidate in matches)
                raise ValueError(
                    "Ambiguous instance patch match for trajectory instance_id "
                    f"{trajectory_instance_id!r}: {matched_ids}"
                )

            instance_dir = matches[0].resolve()
            instance_id = instance_dir.name
            manifest_path = instance_dir / "manifest.json"
            self._event(
                "info",
                "Matched instance patch by contained task ID "
                f"trajectory_instance_id={trajectory_instance_id} "
                f"instance_id={instance_id}",
            )
        try:
            manifest: Any = json.loads(manifest_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid instance patch manifest: {manifest_path}") from exc
        if not isinstance(manifest, dict):
            raise ValueError(f"Instance patch manifest must be an object: {manifest_path}")
        if manifest.get("instance_id") != instance_id:
            raise ValueError(
                "Instance patch manifest mismatch: "
                f"expected {instance_id!r}, got {manifest.get('instance_id')!r}"
            )
        if manifest.get("apply_direction") != "forward":
            raise ValueError(
                f"Unsupported patch direction for {instance_id}: "
                f"{manifest.get('apply_direction')!r}"
            )

        base_commit = manifest.get("base_commit")
        if not isinstance(base_commit, str) or not re.fullmatch(
            r"[0-9a-fA-F]{7,40}", base_commit
        ):
            raise ValueError(f"Invalid base_commit for {instance_id}: {base_commit!r}")

        patch_file = manifest.get("patch_file", "initial.patch")
        if not isinstance(patch_file, str) or Path(patch_file).name != patch_file:
            raise ValueError(f"Invalid patch_file for {instance_id}: {patch_file!r}")
        patch_path = instance_dir / patch_file
        if not patch_path.is_file():
            raise FileNotFoundError(f"Instance patch not found: {patch_path}")

        patch_bytes = patch_path.read_bytes()
        patch_sha256 = hashlib.sha256(patch_bytes).hexdigest()
        expected_sha256 = manifest.get("patch_sha256")
        if expected_sha256 != patch_sha256:
            raise ValueError(
                f"Instance patch SHA-256 mismatch for {instance_id}: "
                f"expected {expected_sha256!r}, got {patch_sha256!r}"
            )

        expected_changed_files = manifest.get("changed_files")
        if not isinstance(expected_changed_files, list) or not all(
            isinstance(path, str) and path for path in expected_changed_files
        ):
            raise ValueError(f"Invalid changed_files for {instance_id}")
        if self.sandbox is None:
            raise RuntimeError("Sandbox is not available")

        remote_patch_path = (
            f"/tmp/replay-aenv/{slugify(instance_id, limit=120)}/{patch_file}"
        )
        started_at = absolute_timestamp()
        started = time.perf_counter()
        self._event(
            "info",
            f"Initializing instance state instance_id={instance_id} "
            f"base_commit={base_commit}",
        )
        # 通过文件接口上传，避免把可能很大的补丁编码进 shell 命令行。
        await self.sandbox.files.write(
            remote_patch_path,
            patch_bytes,
            user="root",
            request_timeout=self.options.lifecycle_timeout_sec,
            gzip=True,
        )

        workdir = shlex.quote(self.options.workdir)
        remote_patch = shlex.quote(remote_patch_path)
        expected_hash = shlex.quote(patch_sha256)
        base_revision = shlex.quote(f"{base_commit}^{{commit}}")
        # sandbox 是一次性的，可以安全清理工作区；补丁放在 /tmp，避免被
        # `git clean -fdx` 删除。先检查补丁，再应用到干净的基础提交。
        command_body = "; ".join(
            [
                "set -euo pipefail",
                f"test -d {workdir}",
                f"test \"$(sha256sum {remote_patch} | awk '{{print $1}}')\" = {expected_hash}",
                f"resolved_base=$(git -C {workdir} rev-parse {base_revision})",
                f"git -C {workdir} reset --hard \"$resolved_base\"",
                f"git -C {workdir} clean -fdx",
                f"git -C {workdir} apply --check {remote_patch}",
                f"git -C {workdir} apply {remote_patch}",
                "printf '%s\\n' REPLAY_CHANGED_FILES_BEGIN",
                f"git -C {workdir} diff --name-only --",
                "printf '%s\\n' REPLAY_CHANGED_FILES_END",
            ]
        )
        raw_result = await self._dispatch_command(
            f"bash -lc {shlex.quote(command_body)}",
            self.options.timeout_sec,
        )
        stdout, stderr, return_code = result_fields(raw_result)
        if return_code not in (None, 0):
            raise RuntimeError(
                f"Could not apply instance patch for {instance_id}; "
                f"exit_code={return_code}; stdout={stdout!r}; stderr={stderr!r}"
            )

        # 应用成功后仍要核对变更文件集合，防止补丁内容或目标仓库状态漂移。
        changed_files: list[str] = []
        collecting = False
        for line in stdout.splitlines():
            if line == "REPLAY_CHANGED_FILES_BEGIN":
                collecting = True
                continue
            if line == "REPLAY_CHANGED_FILES_END":
                collecting = False
                break
            if collecting and line:
                changed_files.append(line)
        if sorted(changed_files) != sorted(expected_changed_files):
            raise RuntimeError(
                f"Changed files mismatch after applying patch for {instance_id}; "
                f"expected={expected_changed_files!r}, actual={changed_files!r}"
            )

        completed_at = absolute_timestamp()
        elapsed_sec = time.perf_counter() - started
        self._event(
            "info",
            f"Initialized instance state instance_id={instance_id} "
            f"changed_files={len(changed_files)} elapsed={elapsed_sec:.3f}s",
        )
        return {
            "status": "applied",
            "instance_id": instance_id,
            "trajectory_instance_id": trajectory_instance_id,
            "base_commit": base_commit,
            "patch_sha256": patch_sha256,
            "changed_files": changed_files,
            "started_at": started_at,
            "completed_at": completed_at,
            "elapsed_sec": elapsed_sec,
        }

    async def _pause_sandbox_strict(self, *, reason: str) -> dict[str, Any]:
        """Pause and keep the running lease held unless the API succeeds."""

        if self.sandbox is None:
            raise RuntimeError("Sandbox is not available")
        started_at = absolute_timestamp()
        scheduler_wait_sec = 0.0
        api_sec = 0.0
        last_error: Exception | None = None
        self._paused = False
        attempt = 0
        while True:
            attempt += 1
            slot_started = time.perf_counter()
            try:
                async with self.control_plane_scheduler.pause_slot():
                    scheduler_wait_sec += time.perf_counter() - slot_started
                    api_started = time.perf_counter()
                    try:
                        await self.sandbox.beta_pause(
                            request_timeout=self.options.lifecycle_timeout_sec,
                        )
                    finally:
                        api_sec = time.perf_counter() - api_started
                last_error = None
                break
            except Exception as exc:
                last_error = exc
                retryable = is_transient_sandbox_error(exc)
                if not retryable:
                    self._log_retry_exhausted(
                        stage="pause",
                        attempts=attempt,
                        retryable=retryable,
                        error=exc,
                    )
                    break
                self._log_retry_queued(
                    stage="pause",
                    attempt=attempt,
                    max_attempts="∞",
                    error=exc,
                )
                await asyncio.sleep(min(2 ** min(attempt, 6), 60))
        if last_error is not None:
            raise last_error
        self._paused = True
        completed_at = absolute_timestamp()
        completed_monotonic = time.monotonic()
        return {
            "pause_started_at": started_at,
            "pause_completed_at": completed_at,
            "pause_completed_monotonic": completed_monotonic,
            "pause_sec": api_sec,
            "pause_scheduler_wait_sec": scheduler_wait_sec,
            "pause_api_sec": api_sec,
            "pause_reason": reason,
        }

    async def _resume_sandbox_strict(self) -> dict[str, Any]:
        """Resume only after a FIFO running slot has been reserved."""

        if self.sandbox is None:
            raise RuntimeError("Sandbox is not available")
        started_at = absolute_timestamp()
        attempts = 0
        scheduler_wait_sec = 0.0
        api_sec = 0.0
        last_error: Exception | None = None
        # A timeout is ambiguous: the backend may already be RUNNING. Keep the
        # lease reserved until pause or delete is confirmed.
        self._paused = False
        attempt = 0
        while True:
            attempt += 1
            attempts = attempt
            slot_started = time.perf_counter()
            try:
                async with self.control_plane_scheduler.resume_slot():
                    scheduler_wait_sec += time.perf_counter() - slot_started
                    api_started = time.perf_counter()
                    try:
                        self.sandbox = await self.sandbox.connect(
                            timeout=self.options.sandbox_timeout_sec,
                            request_timeout=self.options.lifecycle_timeout_sec,
                        )
                    finally:
                        api_sec += time.perf_counter() - api_started
                last_error = None
                break
            except Exception as exc:
                last_error = exc
                wait_ready_failure = re.search(
                    r"\b500: sandbox ([0-9a-fA-F-]{36}) operation WaitReady failed:",
                    str(exc),
                )
                # Resume rollback preserves the paused snapshot. Retry the same
                # sandbox through the scheduler while retaining its RUNNING lease;
                # deleting and recreating here would lose prior trajectory state.
                retryable = is_transient_sandbox_error(exc) or bool(
                    wait_ready_failure
                    and wait_ready_failure.group(1).lower() == str(self.sandbox_id).lower()
                )
                if not retryable:
                    self._log_retry_exhausted(
                        stage="resume",
                        attempts=attempt,
                        retryable=retryable,
                        error=exc,
                    )
                    break
                self._log_retry_queued(
                    stage="resume",
                    attempt=attempt,
                    max_attempts="∞",
                    error=exc,
                )
                await asyncio.sleep(min(2 ** min(attempt, 6), 60))
        if last_error is not None:
            raise RuntimeError(
                "Sandbox lifecycle reconnect failed after "
                f"{attempts} attempt(s): {last_error!r}"
            ) from last_error
        if attempts > 1:
            self._log_retry_recovered(stage="resume", attempts=attempts)

        completed_at = absolute_timestamp()
        ready_probe = (
            await self._probe_sandbox_ready()
            if self.options.ready_probe_enabled
            else None
        )
        return {
            "resume_started_at": started_at,
            "resume_completed_at": completed_at,
            # Report cumulative API latency; keep local rate-limit wait separate.
            "resume_sec": api_sec,
            "resume_attempts": attempts,
            "resume_scheduler_wait_sec": scheduler_wait_sec,
            "resume_api_sec": api_sec,
            "ready_probe": ready_probe,
        }

    async def _force_stop_for_running_release(self, *, reason: str) -> None:
        """Delete an uncertain sandbox before releasing its running slot."""

        if self.sandbox is None:
            return
        try:
            async with self.control_plane_scheduler.cleanup_slot():
                await self.sandbox.kill()
        except Exception as exc:
            self._event(
                "error",
                "Could not stop sandbox while holding running slot; "
                f"reason={reason} sandbox_id={self.sandbox_id} error={exc!r}",
            )
            await self.running_slot_scheduler.close()
            raise RuntimeError(
                "Cannot prove sandbox stopped; running admission has been closed "
                f"for sandbox {self.sandbox_id}"
            ) from exc
        self._event(
            "warning",
            f"Deleted sandbox {self.sandbox_id} to release running slot reason={reason}",
        )
        self.sandbox = None
        self._paused = False

    async def _run_slice(
        self,
        step: ReplayStep,
        total_steps: int,
        job_total_steps: int,
        preceding_pause: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """FIFO-admit one complete resume -> command -> pause slice."""

        requested_delay_sec = (self.options.delay_override_sec if self.options.delay_override_sec is not None else step.delay_time_sec) + self.options.pause_duration_sec + self.options.extra_delay_sec
        ready_at = preceding_pause["pause_completed_monotonic"] + requested_delay_sec
        queue_entered_at = absolute_timestamp()
        lease = await self.running_slot_scheduler.acquire(
            self.run_slot_task_id,
            ready_at=ready_at,
        )
        self._running_lease = lease
        slot_acquired_at = absolute_timestamp()
        paused_sec = max(
            0.0,
            lease.acquired_at - preceding_pause["pause_completed_monotonic"],
        )
        capacity_wait_sec = max(0.0, paused_sec - requested_delay_sec)
        self._job_event(
            "info",
            f"Run slot acquired step={step.index + 1}/{job_total_steps}",
            timestamp=slot_acquired_at,
        )

        step_result: dict[str, Any] | None = None
        next_pause: dict[str, Any] | None = None
        resume_info: dict[str, Any] | None = None
        resume_total_sec: float | None = None
        interaction_total_sec: float | None = None
        primary_error: BaseException | None = None
        try:
            resume_info = await self._resume_sandbox_strict()
            step_result = await self._execute_step(
                step,
                total_steps,
                job_total_steps,
            )
            ready_probe_sec = (
                resume_info["ready_probe"]["elapsed_sec"]
                if resume_info["ready_probe"] is not None
                else 0.0
            )
            resume_total_sec = resume_info["resume_sec"] + ready_probe_sec
            interaction_total_sec = (
                preceding_pause["pause_sec"]
                + requested_delay_sec
                + capacity_wait_sec
                + resume_total_sec
                + step_result["elapsed_sec"]
            )
            timing_message = (
                f"Agent interaction timing step={step.index + 1}/{job_total_steps} "
                f"pause={preceding_pause['pause_sec']:.3f}s "
                f"delay={requested_delay_sec:.3f}s "
                f"capacity_wait={capacity_wait_sec:.3f}s "
                f"resume={resume_total_sec:.3f}s "
                f"command={step_result['elapsed_sec']:.3f}s "
                f"total={interaction_total_sec:.3f}s"
            )
            self._job_event(
                "info",
                timing_message,
                timestamp=step_result["completed_at"],
            )
            self._block(timing_message)
        except BaseException as exc:
            primary_error = exc
        finally:
            if self.sandbox is not None and not self._paused:
                try:
                    next_pause = await self._pause_sandbox_strict(
                        reason="run_slice_complete"
                    )
                except BaseException as pause_exc:
                    try:
                        await self._force_stop_for_running_release(
                            reason=f"pause_failed:{type(pause_exc).__name__}"
                        )
                    except BaseException as stop_exc:
                        if primary_error is None:
                            primary_error = stop_exc
                    else:
                        if primary_error is None:
                            primary_error = pause_exc

            if self._running_lease is not None and (
                self._paused or self.sandbox is None
            ):
                await self._running_lease.release()
                self._running_lease = None

        if primary_error is not None:
            raise primary_error
        if (
            step_result is None
            or next_pause is None
            or resume_info is None
            or resume_total_sec is None
            or interaction_total_sec is None
        ):
            raise RuntimeError("Run slice completed without result, resume, and pause")

        running_slot_held_sec = max(
            0.0,
            next_pause["pause_completed_monotonic"] - lease.acquired_at,
        )
        step_result.update(
            {
                "pause_started_at": preceding_pause["pause_started_at"],
                "pause_completed_at": preceding_pause["pause_completed_at"],
                "resume_started_at": resume_info["resume_started_at"],
                "resume_completed_at": resume_info["resume_completed_at"],
                "trajectory_delay_sec": step.delay_time_sec,
                "configured_extra_delay_sec": self.options.pause_duration_sec,
                "requested_delay_sec": requested_delay_sec,
                "pause_sec": preceding_pause["pause_sec"],
                "paused_sec": paused_sec,
                "pause_scheduler_wait_sec": preceding_pause[
                    "pause_scheduler_wait_sec"
                ],
                "pause_api_sec": preceding_pause["pause_api_sec"],
                "resume_sec": resume_info["resume_sec"],
                "resume_attempts": resume_info["resume_attempts"],
                "resume_scheduler_wait_sec": resume_info[
                    "resume_scheduler_wait_sec"
                ],
                "resume_api_sec": resume_info["resume_api_sec"],
                "resume_retry_wait_sec": 0.0,
                "pause_error": None,
                "ready_probe": resume_info["ready_probe"],
                "control_plane_mode": "rate",
                "run_queue_entered_at": queue_entered_at,
                "run_slot_acquired_at": slot_acquired_at,
                "run_queue_wait_sec": lease.queue_wait_sec,
                "run_slot_contention_wait_sec": capacity_wait_sec,
                "capacity_wait_sec": capacity_wait_sec,
                "running_slot_held_sec": running_slot_held_sec,
                "resume_total_sec": resume_total_sec,
                "interaction_pause_sec": preceding_pause["pause_sec"],
                "inference_delay_sec": requested_delay_sec,
                "command_sec": step_result["elapsed_sec"],
                "interaction_total_sec": interaction_total_sec,
                "post_command_pause_started_at": next_pause["pause_started_at"],
                "post_command_pause_completed_at": next_pause["pause_completed_at"],
                "post_command_pause_sec": next_pause["pause_sec"],
                "post_command_pause_scheduler_wait_sec": next_pause[
                    "pause_scheduler_wait_sec"
                ],
                "post_command_pause_api_sec": next_pause["pause_api_sec"],
            }
        )
        return step_result, next_pause

    async def _execute_step(
        self,
        step: ReplayStep,
        total_steps: int,
        job_total_steps: int,
    ) -> dict[str, Any]:
        normalized_action = normalize_str_replace_editor_action(step.action)
        execution_action = rewrite_write_txt_action(
            normalized_action,
            self.options.write_mode,
            self.options.workdir,
        )
        command = wrap_action(
            execution_action,
            self.options.workdir,
            self.options.setup_command,
        )
        started_at = absolute_timestamp()
        self._job_event(
            "info",
            f"[{step.index + 1}/{job_total_steps}] COMMAND {step.action}",
            timestamp=started_at,
        )
        if normalized_action != step.action:
            self._job_event(
                "info",
                f"[{step.index + 1}/{job_total_steps}] "
                "Normalized str_replace_editor boundary newlines",
                timestamp=started_at,
            )
        if execution_action != normalized_action:
            self._job_event(
                "info",
                f"[{step.index + 1}/{job_total_steps}] "
                f"Rewrote writeTxt action write_mode={self.options.write_mode}",
                timestamp=started_at,
            )
        self._block(
            "\n"
            f"[{started_at}] [{step.index + 1}/{total_steps}] COMMAND START\n"
            f"{step.action}"
        )
        started = time.perf_counter()
        infrastructure_error = False
        timed_out = False
        try:
            raw_result = await self._dispatch_command(
                command,
                self.options.command_timeout_sec,
            )
            stdout, stderr, return_code = result_fields(raw_result)
            succeeded = return_code in (None, 0)
            timed_out = isinstance(raw_result, BaseException) and is_timeout_error(
                raw_result
            )
            if timed_out:
                succeeded = False
        except Exception as exc:
            stdout = ""
            stderr = f"{type(exc).__name__}: {exc}"
            return_code = None
            succeeded = False
            timed_out = is_timeout_error(exc)
            infrastructure_error = (
                not timed_out and is_transient_sandbox_error(exc)
            )
        if timed_out:
            outcome = (
                "ignored; continuing with subsequent commands"
                if self.options.command_timeout_continue
                else "stopping replay because continuation is disabled"
            )
            self._job_event(
                "warning",
                f"[{step.index + 1}/{job_total_steps}] COMMAND TIMEOUT {outcome}",
            )

        completed_at = absolute_timestamp()
        elapsed_sec = time.perf_counter() - started
        self._job_event(
            "info",
            f"[{step.index + 1}/{job_total_steps}] RESULT "
            f"exit_code={return_code} succeeded={succeeded} elapsed={elapsed_sec:.3f}s",
            timestamp=completed_at,
        )
        self._block(
            "\n"
            f"[{completed_at}] [{step.index + 1}/{total_steps}] COMMAND END\n"
            f"exit_code: {return_code}\n"
            f"elapsed: {elapsed_sec:.3f}s\n"
            "stdout:\n"
            f"{stdout.rstrip() if stdout else '<empty>'}\n\n"
            "stderr:\n"
            f"{stderr.rstrip() if stderr else '<empty>'}"
        )
        result = {
            "index": step.index,
            "action": step.action,
            "execution_action": execution_action,
            "write_mode": self.options.write_mode,
            "started_at": started_at,
            "completed_at": completed_at,
            "stdout": stdout,
            "stderr": stderr,
            "return_code": return_code,
            "elapsed_sec": elapsed_sec,
            "succeeded": succeeded,
            "timed_out": timed_out,
            "infrastructure_error": infrastructure_error,
        }
        return result

    async def _cleanup(self) -> None:
        if self.sandbox is None:
            return
        # Delete a paused sandbox directly. Reconnecting here would create an
        # unaccounted RUNNING sandbox outside the FIFO running-slot admission.
        try:
            async with self.control_plane_scheduler.cleanup_slot():
                await self.sandbox.kill()
            self._event("info", f"Deleted sandbox {self.sandbox_id}")
            self.sandbox = None
            self._paused = False
            if self._running_lease is not None:
                await self._running_lease.release()
                self._running_lease = None
        except Exception as exc:
            self._event(
                "warning",
                f"Direct delete failed for sandbox {self.sandbox_id}: {exc}; "
                "retrying cleanup under a running slot",
            )
            if not self._paused:
                if self._running_lease is not None:
                    await self.running_slot_scheduler.close()
                return

            self._running_lease = await self.running_slot_scheduler.acquire(
                f"{self.run_slot_task_id}:cleanup"
            )
            try:
                await self._resume_sandbox_strict()
                async with self.control_plane_scheduler.cleanup_slot():
                    await self.sandbox.kill()
                self._event("info", f"Deleted sandbox {self.sandbox_id} after resume")
                self.sandbox = None
                self._paused = False
            except Exception as cleanup_exc:
                await self._force_stop_for_running_release(
                    reason=f"cleanup_failed:{type(cleanup_exc).__name__}"
                )
            finally:
                if self._running_lease is not None and self.sandbox is None:
                    await self._running_lease.release()
                    self._running_lease = None

    async def run(self) -> dict[str, Any]:
        environment_name, instance_id, steps = load_trajectory(self.trajectory_path)
        terminal_step_position = next(
            (
                position
                for position, step in enumerate(steps, start=1)
                if step.action.lower() in SUBMIT_ACTIONS
            ),
            None,
        )
        executable_steps = (
            steps[: terminal_step_position - 1]
            if terminal_step_position is not None
            else steps
        )
        self.output_dir.mkdir(parents=True, exist_ok=True)
        replay_started_at = absolute_timestamp()
        replay_started = time.perf_counter()
        step_results: list[dict[str, Any]] = []
        submitted = False
        stopped_early = False
        result: dict[str, Any] | None = None
        instance_patch_result: dict[str, Any] | None = None
        preceding_pause: dict[str, Any] | None = None

        try:
            self._event("info", f"Replay started trajectory={self.trajectory_path}")
            if terminal_step_position is not None:
                self._event(
                    "info",
                    f"Terminal action at position {terminal_step_position}; "
                    f"executable commands={len(executable_steps)} "
                    f"trajectory actions={len(steps)}",
                )
            # create() returns a RUNNING sandbox. Reserve capacity before create
            # and release it only after initialization is confirmed paused.
            self._running_lease = await self.running_slot_scheduler.acquire(
                f"{self.run_slot_task_id}:prepare"
            )
            try:
                await self._create_sandbox()
                await self._probe_sandbox_ready()
                await self._validate_workdir()
                instance_patch_result = await self._initialize_instance_state(instance_id)
                preceding_pause = await self._pause_sandbox_strict(
                    reason="initialization_complete"
                )
            except BaseException:
                if self.sandbox is not None and not self._paused:
                    await self._force_stop_for_running_release(
                        reason="initialization_failed"
                    )
                elif self._create_state_uncertain:
                    self._event(
                        "error",
                        "Sandbox create outcome is uncertain; closing running-slot "
                        "admission and retaining the reserved slot to preserve the "
                        "configured hard running cap",
                    )
                    await self.running_slot_scheduler.close()
                raise
            finally:
                if self._running_lease is not None and (
                    self._paused
                    or (self.sandbox is None and not self._create_state_uncertain)
                ):
                    await self._running_lease.release()
                    self._running_lease = None

            for step in executable_steps:
                if preceding_pause is None:
                    raise RuntimeError("Sandbox is not paused before run-slice admission")
                step_result, preceding_pause = await self._run_slice(
                    step,
                    len(executable_steps),
                    len(steps),
                    preceding_pause,
                )
                step_results.append(step_result)
                if step_result["infrastructure_error"]:
                    details = {
                        "stage": "command",
                        "step_index": step.index,
                        "action": step.action,
                        "error": step_result["stderr"],
                    }
                    raise SandboxInfrastructureError(
                        "Sandbox infrastructure failed while dispatching command; "
                        "stopping current replay: " + step_result["stderr"],
                        details,
                    )
                if (
                    step_result["timed_out"]
                    and not self.options.command_timeout_continue
                ):
                    raise CommandTimeoutError(
                        f"Command timed out at step {step.index + 1}; "
                        "stopping replay because --command-timeout-continue "
                        "was not enabled"
                    )
                if (
                    self.options.stop_on_error
                    and not step_result["succeeded"]
                    and not step_result["timed_out"]
                ):
                    stopped_early = True
                    break

            submitted = terminal_step_position is not None and not stopped_early

            succeeded_count = sum(1 for item in step_results if item["succeeded"])
            failed_count = len(step_results) - succeeded_count
            replay_completed = not stopped_early
            result = {
                "version": "0.2.0",
                "trajectory_path": str(self.trajectory_path),
                "instance_id": instance_id,
                "template": self.options.template,
                "write_mode": self.options.write_mode,
                "command_timeout_sec": self.options.command_timeout_sec,
                "command_timeout_continue": self.options.command_timeout_continue,
                "sandbox_id": self.sandbox_id,
                "environment": environment_name,
                "started_at": replay_started_at,
                "completed_at": absolute_timestamp(),
                "trajectory_actions": len(steps),
                "executable_actions": len(executable_steps),
                "terminal_action_position": terminal_step_position,
                "submitted": submitted,
                "stopped_early": stopped_early,
                # Command exit codes are replay observations. A nonzero exit may
                # be intentional in the recorded trajectory, so it does not make
                # a fully replayed task fail unless --stop-on-error stopped it.
                "replay_completed": replay_completed,
                "succeeded": replay_completed,
                "commands": len(step_results),
                "succeeded_commands": succeeded_count,
                "failed_commands": failed_count,
                "elapsed_sec": time.perf_counter() - replay_started,
                "requested_delay_sec": sum(
                    item["requested_delay_sec"] for item in step_results
                ),
                "actual_paused_sec": sum(item["paused_sec"] for item in step_results),
                "instance_patch": instance_patch_result,
                "steps": step_results,
            }
            write_json(self.output_dir / "replay-result.json", result)
            return result
        except BaseException as exc:
            error_detail = f"{type(exc).__name__}: {exc!r}"
            self._event("error", f"Replay failed: {error_detail}")
            error_result = {
                "version": "0.2.0",
                "trajectory_path": str(self.trajectory_path),
                "instance_id": instance_id,
                "template": self.options.template,
                "command_timeout_sec": self.options.command_timeout_sec,
                "command_timeout_continue": self.options.command_timeout_continue,
                "sandbox_id": self.sandbox_id,
                "replay_completed": False,
                "succeeded": False,
                "started_at": replay_started_at,
                "completed_at": absolute_timestamp(),
                "error": error_detail,
                "elapsed_sec": time.perf_counter() - replay_started,
                "instance_patch": instance_patch_result,
                "steps": step_results,
            }
            if isinstance(exc, SandboxInfrastructureError):
                error_result["infrastructure_error"] = exc.details
            result = error_result
            write_json(self.output_dir / "replay-result.json", error_result)
            raise
        finally:
            should_keep = (
                self.options.keep_sandbox_on_failure
                and (result is None or not result.get("succeeded", False))
                and self._paused
            )
            if should_keep:
                self._event(
                    "warning", f"Keeping failed sandbox {self.sandbox_id} for debugging"
                )
            else:
                cleanup_succeeded = False
                try:
                    await self._cleanup()
                    cleanup_succeeded = True
                finally:
                    if result is not None:
                        # Match no-lifecycle: final elapsed time includes cleanup.
                        result["completed_at"] = absolute_timestamp()
                        result["elapsed_sec"] = time.perf_counter() - replay_started
                        write_json(self.output_dir / "replay-result.json", result)
                        if cleanup_succeeded and "error" not in result:
                            self._job_event(
                                "info",
                                "Replay completed "
                                f"succeeded={result['succeeded']} commands={result['commands']} "
                                f"failed_commands={result['failed_commands']} "
                                f"elapsed={result['elapsed_sec']:.3f}s",
                                timestamp=result["completed_at"],
                            )


def replay_options_from_args(args: argparse.Namespace) -> ReplayOptions:
    if args.pause_duration_sec < 0:
        raise ValueError("pause-duration-sec must be non-negative")
    if args.timeout_sec < 0:
        raise ValueError("timeout-sec must be non-negative")
    if args.command_timeout_sec < 0:
        raise ValueError("command-timeout-sec must be non-negative")
    if args.create_retries < 0:
        raise ValueError("create-retries must be non-negative")
    if args.lifecycle_timeout_sec <= 0:
        raise ValueError("lifecycle-timeout-sec must be positive")
    if args.lifecycle_retries < 0:
        raise ValueError("lifecycle-retries must be non-negative")
    if args.ready_probe_attempts <= 0:
        raise ValueError("ready-probe-attempts must be positive")
    if args.ready_probe_timeout_sec <= 0:
        raise ValueError("ready-probe-timeout-sec must be positive")
    return ReplayOptions(
        template=args.template,
        workdir=args.workdir,
        timeout_sec=args.timeout_sec,
        command_timeout_sec=args.command_timeout_sec,
        command_timeout_continue=args.command_timeout_continue,
        pause_duration_sec=args.pause_duration_sec,
        delay_override_sec=getattr(args, "delay", None),
        stop_on_error=args.stop_on_error,
        setup_command=args.setup_command,
        write_mode=args.write_mode,
        sandbox_timeout_sec=args.sandbox_timeout_sec,
        create_retries=args.create_retries,
        lifecycle_timeout_sec=args.lifecycle_timeout_sec,
        lifecycle_retries=args.lifecycle_retries,
        ready_probe_enabled=args.ready_probe,
        ready_probe_attempts=args.ready_probe_attempts,
        ready_probe_timeout_sec=args.ready_probe_timeout_sec,
        keep_sandbox_on_failure=args.keep_sandbox_on_failure,
        instance_overlay_dir=(
            None
            if args.skip_instance_patch
            else Path(args.instance_overlay_dir).expanduser().resolve()
        ),
        command_transport=args.command_transport,
        direct_envd_port=args.direct_envd_port,
        extra_delay_sec=getattr(args, "extra_delay_sec", 0.0),
    )


async def run_single(args: argparse.Namespace) -> int:
    trajectory_path = Path(args.trajectory)
    output_dir = Path(args.output_dir) if args.output_dir else default_run_dir(trajectory_path)
    output_dir = output_dir.expanduser().resolve()
    result_path = output_dir / "replay-result.json"
    runner = ReplayRunner(
        trajectory_path=trajectory_path,
        output_dir=output_dir,
        options=replay_options_from_args(args),
    )
    try:
        result = await runner.run()
    finally:
        await runner.running_slot_scheduler.close()
    summary_text = replay_summary_text(result, result_path)
    append_text(output_dir / "run.log", summary_text)
    print(summary_text, flush=True)
    return 0 if result["succeeded"] else 1


def find_trajectories(directory: Path) -> list[Path]:
    if not directory.is_dir():
        raise NotADirectoryError(f"Trajectory directory not found: {directory}")
    return sorted(
        path
        for path in directory.iterdir()
        if path.is_file() and (path.name.endswith(".json") or path.name.endswith(".traj"))
    )


def build_trajectory_schedule(
    trajectories: list[Path], target_count: int
) -> list[ScheduledTrajectory]:
    """Cycle over sorted source trajectories until target_count jobs exist.

    A target_count of zero preserves the original behavior and schedules every
    source trajectory exactly once. Failures still count as executions, so a
    broken backend cannot turn a finite batch into an infinite retry loop.
    """

    if target_count < 0:
        raise ValueError("target-count must be non-negative")
    if not trajectories:
        return []

    scheduled_count = target_count or len(trajectories)
    source_count = len(trajectories)
    return [
        ScheduledTrajectory(
            path=trajectories[offset % source_count],
            source_index=(offset % source_count) + 1,
            cycle=(offset // source_count) + 1,
            occurrence=(offset // source_count) + 1,
        )
        for offset in range(scheduled_count)
    ]


def load_mix_config(path: str | Path) -> MixedReplayConfig:
    """Load a mixed workload config and resolve all trajectory directories."""

    config_path = Path(path).expanduser().resolve()
    if not config_path.is_file():
        raise FileNotFoundError(f"Mix config not found: {config_path}")
    try:
        raw: Any = json.loads(config_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid mix config JSON: {config_path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ValueError("Mix config root must be a JSON object")

    concurrency = raw.get("concurrency")
    if concurrency is not None:
        if isinstance(concurrency, bool) or not isinstance(concurrency, int):
            raise ValueError("concurrency must be a positive integer")
        if concurrency < 1:
            raise ValueError("concurrency must be a positive integer")

    raw_workloads = raw.get("workloads")
    if not isinstance(raw_workloads, list) or not raw_workloads:
        raise ValueError("workloads must be a non-empty JSON array")

    workloads: list[WorkloadSpec] = []
    names: set[str] = set()
    for index, raw_workload in enumerate(raw_workloads, start=1):
        field = f"workloads[{index - 1}]"
        if not isinstance(raw_workload, dict):
            raise ValueError(f"{field} must be a JSON object")

        template = raw_workload.get("template")
        if not isinstance(template, str) or not template.strip():
            raise ValueError(f"{field}.template must be a non-empty string")
        template = template.strip()

        raw_name = raw_workload.get("name", template)
        if not isinstance(raw_name, str) or not raw_name.strip():
            raise ValueError(f"{field}.name must be a non-empty string")
        name = slugify(raw_name)
        if name in names:
            raise ValueError(f"Duplicate workload name: {name}")
        names.add(name)

        raw_vm_count = raw_workload.get("vm_count")
        if isinstance(raw_vm_count, bool) or not isinstance(raw_vm_count, int):
            raise ValueError(f"{field}.vm_count must be a positive integer")
        if raw_vm_count < 1:
            raise ValueError(f"{field}.vm_count must be a positive integer")

        raw_trajectory_dir = raw_workload.get("trajectory_dir")
        if not isinstance(raw_trajectory_dir, str) or not raw_trajectory_dir.strip():
            raise ValueError(f"{field}.trajectory_dir must be a non-empty string")
        trajectory_dir = Path(raw_trajectory_dir).expanduser()
        if not trajectory_dir.is_absolute():
            trajectory_dir = config_path.parent / trajectory_dir
        trajectory_dir = trajectory_dir.resolve()
        trajectories = find_trajectories(trajectory_dir)
        if not trajectories:
            raise ValueError(f"No .json or .traj files found in {trajectory_dir}")

        workloads.append(
            WorkloadSpec(
                name=name,
                template=template,
                trajectory_dir=trajectory_dir,
                trajectories=tuple(trajectories),
                vm_count=raw_vm_count,
            )
        )
    return MixedReplayConfig(concurrency=concurrency, workloads=tuple(workloads))


def build_mixed_schedule(config: MixedReplayConfig) -> list[ScheduledTrajectory]:
    """Interleave workload VM executions using smooth weighted round robin."""

    remaining = {workload.name: workload.vm_count for workload in config.workloads}
    current = {workload.name: 0 for workload in config.workloads}
    emitted = {workload.name: 0 for workload in config.workloads}
    total = sum(remaining.values())
    schedule: list[ScheduledTrajectory] = []

    while len(schedule) < total:
        active = [workload for workload in config.workloads if remaining[workload.name]]
        active_weight = sum(workload.vm_count for workload in active)
        for workload in active:
            current[workload.name] += workload.vm_count
        selected = max(active, key=lambda workload: current[workload.name])
        current[selected.name] -= active_weight

        workload_offset = emitted[selected.name]
        source_count = len(selected.trajectories)
        source_offset = workload_offset % source_count
        schedule.append(
            ScheduledTrajectory(
                path=selected.trajectories[source_offset],
                source_index=source_offset + 1,
                cycle=(workload_offset // source_count) + 1,
                occurrence=(workload_offset // source_count) + 1,
                workload_name=selected.name,
                template=selected.template,
                workload_occurrence=workload_offset + 1,
            )
        )
        emitted[selected.name] += 1
        remaining[selected.name] -= 1
    return schedule


async def run_batch(args: argparse.Namespace) -> int:
    mixed_config: MixedReplayConfig | None = getattr(args, "mixed_config", None)
    if mixed_config is None:
        trajectories = find_trajectories(Path(args.trajectory_dir).expanduser().resolve())
        if args.limit:
            trajectories = trajectories[: args.limit]
        if not trajectories:
            raise ValueError("No .json or .traj files found")
        scheduled_trajectories = build_trajectory_schedule(
            trajectories, args.target_count
        )
    else:
        trajectories = [
            path
            for workload in mixed_config.workloads
            for path in workload.trajectories
        ]
        scheduled_trajectories = build_mixed_schedule(mixed_config)
    if args.concurrency < 1:
        raise ValueError("concurrency must be a positive integer")
    running_concurrency = args.running_concurrency or args.concurrency
    if not 1 <= running_concurrency <= args.concurrency:
        raise ValueError(
            "running-concurrency requires 1 <= value <= concurrency"
        )
    if args.control_plane_qps <= 0:
        raise ValueError("control-plane-qps must be positive")
    if args.control_plane_max_inflight < 1:
        raise ValueError("control-plane-max-inflight must be a positive integer")
    if args.launch_interval_sec < 0:
        raise ValueError("launch-interval-sec must be non-negative")
    batch_started_at = absolute_timestamp()
    batch_started = time.perf_counter()
    batch_id = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    batch_dir = (
        Path(args.output_dir).expanduser().resolve()
        if args.output_dir
        else (Path.cwd() / "runs" / batch_id).resolve()
    )
    batch_dir.mkdir(parents=True, exist_ok=True)
    job_log_path = batch_dir / "job.log"
    job_log_lock = asyncio.Lock()
    semaphore = asyncio.Semaphore(args.concurrency)
    options = replay_options_from_args(args)
    # Keep a separate in-flight fuse in addition to QPS: QPS limits how fast
    # requests start, while this cap prevents slow backend calls accumulating.
    control_plane_config = ControlPlaneConfig(
        qps=args.control_plane_qps,
        inflight_cap=min(args.control_plane_max_inflight, args.concurrency),
    )
    control_plane_scheduler = ControlPlaneScheduler(control_plane_config)
    running_slot_scheduler = RunningSlotScheduler(running_concurrency)
    launch_lock = asyncio.Lock()
    next_launch_at = time.perf_counter()

    async def wait_for_launch_turn() -> None:
        """Space actual replay starts without allowing catch-up bursts."""

        nonlocal next_launch_at
        if args.launch_interval_sec <= 0:
            return
        async with launch_lock:
            now = time.perf_counter()
            wait_sec = max(0.0, next_launch_at - now)
            if wait_sec > 0:
                await asyncio.sleep(wait_sec)
            launched_at = time.perf_counter()
            next_launch_at = launched_at + args.launch_interval_sec

    async def write_job_event(
        level: str,
        message: str,
        *,
        timestamp: str | None = None,
    ) -> None:
        async with job_log_lock:
            append_text(
                job_log_path,
                job_log_line(level, message, timestamp=timestamp),
            )

    await write_job_event(
        "info",
        f"Batch job started trajectories={len(scheduled_trajectories)} "
        f"source_trajectories={len(trajectories)} "
        f"target_count={len(scheduled_trajectories)} "
        f"concurrency={args.concurrency} "
        f"running_concurrency={running_concurrency} "
        f"create_admission=running_slots "
        f"control_plane_mode=rate "
        f"control_plane_qps={args.control_plane_qps:.3f} "
        f"control_plane_inflight_cap={control_plane_config.inflight_cap} "
        f"launch_interval_sec={args.launch_interval_sec:.3f} "
        f"write_mode={args.write_mode} "
        f"template={args.template if mixed_config is None else 'mixed'} "
        f"trajectory_source={Path(args.trajectory_dir).expanduser().resolve()} "
        f"output_dir={batch_dir}",
        timestamp=batch_started_at,
    )

    async def run_one(
        index: int, scheduled: ScheduledTrajectory
    ) -> dict[str, Any]:
        path = scheduled.path
        stem = path.name
        for suffix in (".replay.json", ".json", ".traj"):
            if stem.endswith(suffix):
                stem = stem[: -len(suffix)]
                break
        repeat_suffix = (
            f".repeat-{scheduled.occurrence:03d}" if args.target_count else ""
        )
        workload_dir = (
            batch_dir
            if mixed_config is None
            else batch_dir / scheduled.workload_name
        )
        run_dir = workload_dir / f"{index:03d}-{slugify(stem)}{repeat_suffix}"
        async with semaphore:
            await wait_for_launch_turn()
            LOGGER.info(
                "Starting %d/%d: %s cycle=%d",
                index,
                len(scheduled_trajectories),
                path.name,
                scheduled.cycle,
            )
            await write_job_event(
                "info",
                f"[{index}/{len(scheduled_trajectories)}] Replay job started "
                f"workload={scheduled.workload_name} "
                f"template={scheduled.template or options.template} "
                f"trajectory={path} source_index={scheduled.source_index} "
                f"cycle={scheduled.cycle} occurrence={scheduled.occurrence} "
                f"output_dir={run_dir}",
            )
            try:
                result = await ReplayRunner(
                    path,
                    run_dir,
                    replace(options, template=scheduled.template or options.template),
                    console_output=False,
                    control_plane_scheduler=control_plane_scheduler,
                    running_slot_scheduler=running_slot_scheduler,
                ).run()
                append_text(
                    run_dir / "run.log",
                    replay_summary_text(result, run_dir / "replay-result.json"),
                )
                LOGGER.info(
                    "Completed %d/%d: %s succeeded=%s commands=%s failed=%s",
                    index,
                    len(scheduled_trajectories),
                    path.name,
                    result.get("succeeded"),
                    result.get("commands"),
                    result.get("failed_commands"),
                )
                status, explanation = replay_outcome(result)
                await write_job_event(
                    "info" if result.get("succeeded") else "warning",
                    f"[{index}/{len(scheduled_trajectories)}] Replay job completed "
                    f"status={status} commands={result.get('commands', 0)} "
                    f"succeeded={result.get('succeeded_commands', 0)} "
                    f"failed={result.get('failed_commands', 0)} "
                    f"submitted={str(bool(result.get('submitted'))).lower()} "
                    f"replay_completed={str(bool(result.get('replay_completed'))).lower()} "
                    f"summary={explanation} result={run_dir / 'replay-result.json'}",
                    timestamp=result.get("completed_at"),
                )
                return {
                    "index": index,
                    "workload": scheduled.workload_name,
                    "template": scheduled.template or options.template,
                    "trajectory": str(path),
                    "source_index": scheduled.source_index,
                    "cycle": scheduled.cycle,
                    "occurrence": scheduled.occurrence,
                    "output_dir": str(run_dir),
                    "replay_completed": bool(result.get("replay_completed")),
                    "succeeded": bool(result.get("succeeded")),
                    "commands": result.get("commands"),
                    "succeeded_commands": result.get("succeeded_commands"),
                    "failed_commands": result.get("failed_commands"),
                    "submitted": result.get("submitted"),
                    "error": result.get("error"),
                }
            except Exception as exc:
                # repr 和异常类型不能省略；ReadTimeout 等异常的 str(exc)
                # 可能为空，否则批次控制台只会显示一个没有原因的 ERROR。
                LOGGER.error(
                    "Replay failed for %s: %s: %r",
                    path.name,
                    type(exc).__name__,
                    exc,
                )
                await write_job_event(
                    "error",
                    f"[{index}/{len(scheduled_trajectories)}] Replay job failed "
                    f"trajectory={path} error={type(exc).__name__}: {exc!r} "
                    f"result={run_dir / 'replay-result.json'}",
                )
                return {
                    "index": index,
                    "workload": scheduled.workload_name,
                    "template": scheduled.template or options.template,
                    "trajectory": str(path),
                    "source_index": scheduled.source_index,
                    "cycle": scheduled.cycle,
                    "occurrence": scheduled.occurrence,
                    "output_dir": str(run_dir),
                    "succeeded": False,
                    "error": f"{type(exc).__name__}: {exc}",
                }

    # Queue only creates concurrency worker tasks, rather than one coroutine per
    # requested replay. This keeps memory stable when target-count is very large.
    work_queue: asyncio.Queue[tuple[int, ScheduledTrajectory] | None] = asyncio.Queue()
    for index, scheduled in enumerate(scheduled_trajectories, start=1):
        work_queue.put_nowait((index, scheduled))

    items_by_index: list[dict[str, Any] | None] = [
        None
    ] * len(scheduled_trajectories)

    async def worker() -> None:
        while True:
            work_item = await work_queue.get()
            try:
                if work_item is None:
                    return
                index, scheduled = work_item
                items_by_index[index - 1] = await run_one(index, scheduled)
            finally:
                work_queue.task_done()

    worker_count = min(args.concurrency, len(scheduled_trajectories))
    workers = [asyncio.create_task(worker()) for _ in range(worker_count)]
    await work_queue.join()
    for _ in workers:
        work_queue.put_nowait(None)
    await asyncio.gather(*workers)
    running_snapshot = running_slot_scheduler.snapshot()
    await running_slot_scheduler.close()

    if any(item is None for item in items_by_index):
        raise RuntimeError("Batch worker finished without producing every result")
    items = [item for item in items_by_index if item is not None]
    succeeded = sum(1 for item in items if item["succeeded"])
    workload_summaries = []
    if mixed_config is not None:
        for workload in mixed_config.workloads:
            workload_items = [
                item for item in items if item["workload"] == workload.name
            ]
            workload_succeeded = sum(
                1 for item in workload_items if item["succeeded"]
            )
            workload_summaries.append(
                {
                    "name": workload.name,
                    "template": workload.template,
                    "trajectory_dir": str(workload.trajectory_dir),
                    "source_trajectory_count": len(workload.trajectories),
                    "vm_count": workload.vm_count,
                    "succeeded": workload_succeeded,
                    "failed": len(workload_items) - workload_succeeded,
                }
            )
    summary = {
        "batch_id": batch_id,
        "started_at": batch_started_at,
        "completed_at": absolute_timestamp(),
        "trajectory_dir": (
            str(Path(args.trajectory_dir).expanduser().resolve())
            if mixed_config is None
            else None
        ),
        "mix_config": (
            None
            if mixed_config is None
            else str(Path(args.trajectory_dir).expanduser().resolve())
        ),
        "source_trajectory_count": len(trajectories),
        "requested_target_count": (
            args.target_count or None
            if mixed_config is None
            else len(scheduled_trajectories)
        ),
        "template": args.template if mixed_config is None else None,
        "mode": "batch" if mixed_config is None else "mix",
        "write_mode": options.write_mode,
        "timeout_sec": options.timeout_sec,
        "command_timeout_sec": options.command_timeout_sec,
        "command_timeout_continue": options.command_timeout_continue,
        "workloads": workload_summaries,
        "concurrency": args.concurrency,
        "running_concurrency": running_concurrency,
        "launch_interval_sec": args.launch_interval_sec,
        "create": {
            "admission": "running_slots",
            "retry_on_timeout": False,
            "request_timeout_sec": options.lifecycle_timeout_sec,
        },
        "ready_probe": {
            "enabled": options.ready_probe_enabled,
            "attempts": options.ready_probe_attempts,
            "timeout_sec": options.ready_probe_timeout_sec,
        },
        "total": len(items),
        "succeeded": succeeded,
        "failed": len(items) - succeeded,
        "elapsed_sec": time.perf_counter() - batch_started,
        "control_plane": control_plane_scheduler.snapshot(),
        "running_slots": running_snapshot,
        "items": items,
    }
    result_path = batch_dir / "batch-summary.json"
    write_json(result_path, summary)
    await write_job_event(
        "info" if summary["failed"] == 0 else "warning",
        "Batch job completed "
        f"status={'success' if summary['failed'] == 0 else 'completed_with_errors'} "
        f"total={summary['total']} succeeded={summary['succeeded']} "
        f"failed={summary['failed']} elapsed={summary['elapsed_sec']:.3f}s "
        f"result={result_path}",
        timestamp=summary["completed_at"],
    )
    print(batch_summary_text(summary, result_path), flush=True)
    return 0 if summary["failed"] == 0 else 1


async def run_mix(args: argparse.Namespace) -> int:
    config = load_mix_config(args.config)
    concurrency = args.concurrency or config.concurrency
    if concurrency is None or concurrency < 1:
        raise ValueError(
            "concurrency must be provided by --concurrency or mix config"
        )
    schedule = build_mixed_schedule(config)
    allocation = [
        {
            "name": workload.name,
            "template": workload.template,
            "trajectory_dir": str(workload.trajectory_dir),
            "source_trajectory_count": len(workload.trajectories),
            "vm_count": workload.vm_count,
        }
        for workload in config.workloads
    ]
    if args.dry_run:
        result = {
            "mode": "mix",
            "config": str(Path(args.config).expanduser().resolve()),
            "concurrency": concurrency,
            "total_vm_count": len(schedule),
            "workloads": allocation,
            "schedule_preview": [
                {
                    "index": index,
                    "workload": scheduled.workload_name,
                    "template": scheduled.template,
                    "trajectory": str(scheduled.path),
                }
                for index, scheduled in enumerate(schedule[:20], start=1)
            ],
        }
        print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
        return 0

    args.mixed_config = config
    args.trajectory_dir = str(Path(args.config).expanduser().resolve())
    args.concurrency = concurrency
    return await run_batch(args)


async def check_aenv(args: argparse.Namespace) -> int:
    health_url = f"{args.api_url.rstrip('/')}/health"

    def fetch_health() -> tuple[int, str]:
        try:
            with urllib.request.urlopen(health_url, timeout=5) as response:
                return response.status, response.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read().decode("utf-8", errors="replace")

    status, body = await asyncio.to_thread(fetch_health)
    from e2b import AsyncTemplate

    template_exists = await AsyncTemplate.alias_exists(args.template)
    result = {
        "api_url": args.api_url,
        "health_status": status,
        "health_body": body,
        "template": args.template,
        "template_exists": template_exists,
        "succeeded": 200 <= status < 300 and template_exists,
    }
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["succeeded"] else 1


def add_replay_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--template", default=DEFAULT_TEMPLATE)
    parser.add_argument("--workdir", default="/testbed")
    parser.add_argument(
        "--timeout-sec",
        type=int,
        default=300,
        help=(
            "Timeout for setup and instance initialization commands "
            "(default: 300; 0 disables)"
        ),
    )
    parser.add_argument(
        "--command-timeout-sec",
        type=int,
        default=300,
        help=(
            "Timeout for each trajectory command; timed-out commands are "
            "recorded as failed steps and replay continues "
            "(default: 300; 0 disables)"
        ),
    )
    parser.add_argument(
        "--command-timeout-continue",
        action="store_true",
        help=(
            "When a trajectory command times out, record only that command as "
            "failed and continue the replay (default: stop the replay)"
        ),
    )
    parser.add_argument("--sandbox-timeout-sec", type=int, default=86_400)
    parser.add_argument(
        "--create-retries",
        type=int,
        default=1,
        help="Retries after a transient sandbox create failure (default: 1)",
    )
    parser.add_argument("--lifecycle-timeout-sec", type=int, default=120)
    parser.add_argument("--lifecycle-retries", type=int, default=2)
    parser.add_argument(
        "--ready-probe",
        action="store_true",
        help="Run an opt-in `true` command after each resume (disabled by default)",
    )
    parser.add_argument(
        "--ready-probe-attempts",
        type=int,
        default=5,
        help="Maximum attempts for the post-create/post-resume `true` readiness probe",
    )
    parser.add_argument(
        "--ready-probe-timeout-sec",
        type=int,
        default=10,
        help="Timeout for each post-create/post-resume readiness probe",
    )
    parser.add_argument("--pause-duration-sec", type=float, default=0.0)
    parser.add_argument(
        "--delay",
        type=float,
        default=None,
        help="Override all trajectory delay_time with this fixed value in seconds; "
        "when omitted, use each step's recorded delay_time",
    )
    parser.add_argument(
        "--command-transport",
        choices=("direct-envd", "aenv-proxy"),
        default="aenv-proxy",
        help=(
            "Command and file data-plane transport. direct-envd resolves the "
            "route from local Firecracker MMDS/network state and bypasses the "
            "AgentENV command proxy; "
            "aenv-proxy uses the standard AgentENV proxy path (default: aenv-proxy)"
        ),
    )
    parser.add_argument(
        "--direct-envd-port",
        type=int,
        default=49_983,
        help="envd port for direct-envd transport (default: 49983)",
    )
    parser.add_argument(
        "--extra-delay-sec",
        type=float,
        default=0.0,
        help="Additional running wait before every trajectory command (default: 0)",
    )
    parser.add_argument("--setup-command")
    parser.add_argument(
        "--write-mode",
        choices=WRITE_MODES,
        default="buffered",
        help=(
            "Rewrite exact `writeTxt N` trajectory actions: buffered keeps "
            "writeTxt, tmpfs writes /dev/shm, directio uses O_DIRECT under "
            "--workdir (default: buffered)"
        ),
    )
    parser.add_argument("--stop-on-error", action="store_true")
    parser.add_argument("--keep-sandbox-on-failure", action="store_true")
    parser.add_argument(
        "--instance-overlay-dir",
        default=DEFAULT_INSTANCE_OVERLAY_DIR,
        help="Per-instance patch directory (default: instance_overlays)",
    )
    parser.add_argument(
        "--skip-instance-patch",
        action="store_true",
        help="Replay without applying an initial-state patch",
    )
    parser.add_argument("--output-dir")


def add_batch_runtime_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--launch-interval-sec",
        type=float,
        default=0.0,
        help=(
            "Minimum interval in seconds between actual replay job starts; "
            "0 starts jobs without pacing (default: 0)"
        ),
    )
    parser.add_argument(
        "--running-concurrency",
        type=int,
        default=0,
        help=(
            "Maximum reserved RUNNING sandboxes across create and complete "
            "resume/command/pause slices; 0 uses --concurrency"
        ),
    )
    parser.add_argument(
        "--control-plane-qps",
        type=float,
        default=20.0,
        help="Maximum combined non-create AgentENV infrastructure requests per second",
    )
    parser.add_argument(
        "--control-plane-max-inflight",
        type=int,
        default=1024,
        help="Maximum AgentENV infrastructure requests awaiting responses",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="replay-aenv",
        description="Replay recorded shell trajectories directly on AgentENV.",
    )
    parser.add_argument(
        "--api-url",
        default=os.getenv("E2B_API_URL", DEFAULT_API_URL),
        help="AgentENV E2B-compatible API URL",
    )
    parser.add_argument("--verbose", action="store_true")
    subparsers = parser.add_subparsers(dest="command", required=True)

    check_parser = subparsers.add_parser("check", help="Check AgentENV and template")
    check_parser.add_argument("--template", default=DEFAULT_TEMPLATE)
    check_parser.set_defaults(handler=check_aenv)

    run_parser = subparsers.add_parser("run", help="Replay one trajectory")
    run_parser.add_argument("--trajectory", required=True)
    add_replay_arguments(run_parser)
    run_parser.set_defaults(handler=run_single)

    batch_parser = subparsers.add_parser("batch", help="Replay a trajectory directory")
    batch_parser.add_argument("--trajectory-dir", required=True)
    batch_parser.add_argument("--concurrency", type=int, default=4)
    add_batch_runtime_arguments(batch_parser)
    batch_parser.add_argument("--limit", type=int, default=0)
    batch_parser.add_argument(
        "--target-count",
        type=int,
        default=0,
        help=(
            "Total replay executions; cycle over the selected trajectory files "
            "when the value exceeds their count (default: run each file once)"
        ),
    )
    add_replay_arguments(batch_parser)
    batch_parser.set_defaults(handler=run_batch)

    mix_parser = subparsers.add_parser(
        "mix", help="Replay multiple template/trajectory workloads"
    )
    mix_parser.add_argument("--config", required=True)
    mix_parser.add_argument(
        "--concurrency",
        type=int,
        default=0,
        help="Global replay concurrency; overrides concurrency in the config",
    )
    mix_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate config and print the allocation without creating VMs",
    )
    add_batch_runtime_arguments(mix_parser)
    add_replay_arguments(mix_parser)
    mix_parser.set_defaults(
        handler=run_mix,
        concurrency=0,
        limit=0,
        target_count=0,
        mixed_config=None,
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    handler = logging.StreamHandler()
    handler.setFormatter(AbsoluteTimeFormatter("%(asctime)s %(levelname)s %(message)s"))
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        handlers=[handler],
    )
    if not args.verbose:
        for logger_name in ("httpx", "httpcore", "e2b"):
            logging.getLogger(logger_name).setLevel(logging.WARNING)
    configure_aenv_environment(args.api_url)
    try:
        return asyncio.run(args.handler(args))
    except KeyboardInterrupt:
        LOGGER.warning("Interrupted — cleaning up all sandboxes")
        try:
            asyncio.run(cleanup_all_sandboxes(label="interrupt"))
        except Exception as exc:
            LOGGER.error("Cleanup failed: %s: %s", type(exc).__name__, exc)
        return 130
    except Exception as exc:
        LOGGER.error("%s: %s", type(exc).__name__, exc)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
