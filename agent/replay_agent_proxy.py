#!/usr/bin/env python3
"""Replay agent variant that executes read-only steps against PAUSED sandboxes.

Read-only, environment-independent actions (ls / find / cat / grep / ...) are
dispatched to the AgentENV resident proxy sandbox via
``POST /sandboxes/{id}/proxy-read`` instead of the resume -> command -> pause
slice.  A proxy step therefore holds NO running slot, performs NO resume and
NO pause: the target sandbox stays paused the whole time.

Everything else (writes, env-var dependent commands, command substitution,
redirections, non-whitelisted binaries) keeps the original
``_run_slice`` path unchanged.  When a proxy read fails at the infrastructure
level (binding missing, proxy unavailable, command not found inside the
chroot) the step transparently falls back to the resume path, so a server
without the proxy feature enabled degrades to the baseline benchmark.

The module reuses replay_agent.py wholesale: the only overridden behaviour is
``ReplayRunner._run_slice``.  Run with the same CLI as replay_agent.py plus
the proxy flags below.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

if __package__:
    from . import replay_agent
else:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import replay_agent

LOGGER = logging.getLogger("replay-aenv-proxy")

# ── Command classification ───────────────────────────────────────────────────
#
# The proxy executes `busybox chroot <target-rootfs> /bin/sh -c '<command>'`:
# the target image's own /bin/sh and binaries run with full flag
# compatibility.  Classification is intentionally conservative — a mis-routed
# write would silently diverge from the source trajectory, while a mis-routed
# read only costs one extra resume slice.

#: Read-only commands safe to execute against the paused rootfs.  Everything
#: here must be side-effect free on the filesystem.
READONLY_COMMANDS = frozenset(
    {
        "ls", "find", "cat", "head", "tail", "grep", "egrep", "fgrep",
        "stat", "wc", "du", "df", "which", "whereis", "basename", "dirname",
        "readlink", "realpath", "diff", "cmp", "sort", "uniq", "cut", "tr",
        "nl", "tac", "rev", "md5sum", "sha1sum", "sha256sum", "cksum",
        "sum", "pwd", "echo", "cd", "true", "false", "type",
    }
)

#: find primaries with filesystem side effects.
FIND_MUTATING_PRIMARIES = (
    "-delete", "-exec", "-execdir", "-ok", "-okdir",
    "-fprint", "-fprintf", "-fls",
)

#: Segment separators inside a shell command line.
_SEGMENT_SPLIT = re.compile(r"&&|\|\||\||;|\n")


def proxy_read_eligible(action: str | None) -> tuple[bool, str]:
    """Return (eligible, reason) for executing `action` via proxy-read."""
    if not action or not action.strip():
        return False, "empty"
    text = action
    if "$" in text:
        # Environment/positional variables resolve against the proxy's
        # environment, not the paused sandbox's shell state.
        return False, "env-var"
    if "`" in text or "$(" in text:
        return False, "command-substitution"
    if ">" in text or "<" in text:
        # Redirections, heredocs and process substitution.  Conservative:
        # even `2>/dev/null` is routed to the resume path.
        return False, "redirection"
    for segment in _SEGMENT_SPLIT.split(text):
        segment = segment.strip()
        if not segment:
            continue
        try:
            tokens = segment.split()
        except ValueError:
            return False, "unparsable"
        if not tokens:
            continue
        head = tokens[0]
        if head not in READONLY_COMMANDS:
            return False, f"command:{head}"
        if head == "find":
            for primary in FIND_MUTATING_PRIMARIES:
                if any(token == primary or token.startswith(primary) for token in tokens[1:]):
                    return False, f"find:{primary}"
    return True, "ok"


# ── Proxy-read HTTP client ────────────────────────────────────────────────────

#: Module-level configuration, populated once from CLI args in main().
PROXY_READ_ENABLED = True
PROXY_READ_FALLBACK = True
PROXY_READ_RETRY_TIMEOUT_SEC = 30.0
PROXY_READ_HTTP_TIMEOUT_SEC = 120.0


class ProxyReadError(RuntimeError):
    """Infrastructure-level proxy-read failure (fallback eligible)."""

    def __init__(self, message: str, *, status: int | None = None):
        super().__init__(message)
        self.status = status


def _proxy_read_http(
    api_url: str,
    api_key: str,
    sandbox_id: str,
    command: str,
    cwd: str,
    timeout_sec: float,
) -> dict[str, Any]:
    url = f"{api_url.rstrip('/')}/sandboxes/{sandbox_id}/proxy-read"
    body = json.dumps({"command": command, "cwd": cwd}).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "X-API-Key": api_key,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = ""
        try:
            detail = exc.read().decode("utf-8", errors="replace")[:300]
        except Exception:
            pass
        raise ProxyReadError(
            f"proxy-read HTTP {exc.code}: {detail}", status=exc.code
        ) from exc
    except Exception as exc:
        raise ProxyReadError(f"proxy-read request failed: {exc!r}") from exc


async def proxy_read(
    sandbox_id: str,
    command: str,
    cwd: str,
) -> dict[str, Any]:
    """Execute one proxy-read with rebind_in_progress retries.

    Raises ProxyReadError on infrastructure failures; callers fall back to
    the resume path when configured.
    """
    api_url = os.environ.get("E2B_API_URL", "http://127.0.0.1:8000")
    api_key = os.environ.get("E2B_API_KEY", "")
    loop = asyncio.get_running_loop()
    deadline = time.monotonic() + PROXY_READ_RETRY_TIMEOUT_SEC
    attempt = 0
    while True:
        attempt += 1
        try:
            return await loop.run_in_executor(
                None,
                lambda: _proxy_read_http(
                    api_url,
                    api_key,
                    sandbox_id,
                    command,
                    cwd,
                    PROXY_READ_HTTP_TIMEOUT_SEC,
                ),
            )
        except ProxyReadError as exc:
            retryable = exc.status == 503  # rebind_in_progress / proxy_unavailable
            if retryable and time.monotonic() < deadline:
                await asyncio.sleep(min(0.5 * attempt, 5.0))
                continue
            raise


# ── Runner ────────────────────────────────────────────────────────────────────


class ProxyReplayRunner(replay_agent.ReplayRunner):
    """ReplayRunner whose read-only slices never leave the paused state."""

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        # The very first action always takes the resume path: the initial
        # pause-triggered BIND may still be in flight, and the target rootfs
        # is not guaranteed to be mounted in the proxy sandbox yet.
        self._proxy_slices_started = False

    async def _run_slice(
        self,
        step: replay_agent.ReplayStep,
        total_steps: int,
        job_total_steps: int,
        preceding_pause: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        first_slice = not self._proxy_slices_started
        self._proxy_slices_started = True
        if first_slice:
            eligible, reason = False, "first-action"
        elif PROXY_READ_ENABLED:
            eligible, reason = proxy_read_eligible(step.action)
        else:
            eligible, reason = False, "proxy-disabled"
        if not eligible:
            step_result, next_pause = await super()._run_slice(
                step, total_steps, job_total_steps, preceding_pause
            )
            step_result["transport"] = "resume"
            step_result["proxy_eligible"] = False
            step_result["proxy_skip_reason"] = reason
            return step_result, next_pause

        started_at = replay_agent.absolute_timestamp()
        self._job_event(
            "info",
            f"[{step.index + 1}/{job_total_steps}] PROXY-READ {step.action}",
            timestamp=started_at,
        )
        try:
            step_result, next_pause = await self._proxy_read_slice(
                step, total_steps, job_total_steps, preceding_pause
            )
            step_result["transport"] = "proxy_read"
            step_result["proxy_eligible"] = True
            step_result["proxy_skip_reason"] = None
            return step_result, next_pause
        except ProxyReadError as exc:
            if not PROXY_READ_FALLBACK:
                raise
            self._job_event(
                "warning",
                f"[{step.index + 1}/{job_total_steps}] Proxy read failed; "
                f"falling back to resume slice: {exc}",
            )
            step_result, next_pause = await super()._run_slice(
                step, total_steps, job_total_steps, preceding_pause
            )
            step_result["transport"] = "resume_fallback"
            step_result["proxy_eligible"] = True
            step_result["proxy_skip_reason"] = None
            step_result["proxy_fallback_reason"] = str(exc)
            return step_result, next_pause

    async def _proxy_read_slice(
        self,
        step: replay_agent.ReplayStep,
        total_steps: int,
        job_total_steps: int,
        preceding_pause: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Read one step against the paused sandbox; no slot, no resume."""

        options = self.options
        requested_delay_sec = (
            options.delay_override_sec
            if options.delay_override_sec is not None
            else step.delay_time_sec
        ) + options.pause_duration_sec + options.extra_delay_sec
        ready_at = preceding_pause["pause_completed_monotonic"] + requested_delay_sec
        remaining = ready_at - time.monotonic()
        if remaining > 0:
            await asyncio.sleep(remaining)

        if self.sandbox is None:
            raise ProxyReadError("sandbox is not available")
        sandbox_id = self.sandbox.sandbox_id

        started = time.perf_counter()
        started_at = replay_agent.absolute_timestamp()
        payload = await proxy_read(
            sandbox_id, step.action, options.workdir or "/"
        )
        elapsed_sec = time.perf_counter() - started
        completed_at = replay_agent.absolute_timestamp()
        completed_monotonic = time.monotonic()

        exit_code = payload.get("exitCode")
        stdout = payload.get("stdout") or ""
        stderr = payload.get("stderr") or ""
        succeeded = exit_code == 0
        if exit_code == 127:
            # /bin/sh could not resolve the command inside the chroot (e.g.
            # a busybox-only proxy or an unusual shell).  Re-execute through
            # the resume path for fidelity; read-only commands are idempotent.
            raise ProxyReadError(
                f"command not found inside proxy chroot: {step.action.split()[0]!r}"
            )

        paused_sec = max(0.0, completed_monotonic - preceding_pause["pause_completed_monotonic"])
        self._job_event(
            "info",
            f"[{step.index + 1}/{job_total_steps}] RESULT "
            f"exit_code={exit_code} succeeded={succeeded} "
            f"elapsed={elapsed_sec:.3f}s transport=proxy_read "
            f"generation={payload.get('bindingGeneration')}",
            timestamp=completed_at,
        )
        self._block(
            "\n"
            f"[{completed_at}] [{step.index + 1}/{total_steps}] PROXY-READ END\n"
            f"exit_code: {exit_code}\n"
            f"elapsed: {elapsed_sec:.3f}s\n"
            "stdout:\n"
            f"{stdout.rstrip() if stdout else '<empty>'}\n\n"
            "stderr:\n"
            f"{stderr.rstrip() if stderr else '<empty>'}"
        )
        interaction_total_sec = (
            preceding_pause["pause_sec"]
            + requested_delay_sec
            + elapsed_sec
        )
        step_result = {
            "index": step.index,
            "action": step.action,
            "execution_action": step.action,
            "write_mode": options.write_mode,
            "started_at": started_at,
            "completed_at": completed_at,
            "stdout": stdout,
            "stderr": stderr,
            "return_code": exit_code,
            "elapsed_sec": elapsed_sec,
            "succeeded": succeeded,
            "timed_out": False,
            "infrastructure_error": False,
            # Timing fields mirror _run_slice so downstream collectors work;
            # the proxy path performs no pause/resume of its own.
            "pause_started_at": preceding_pause["pause_started_at"],
            "pause_completed_at": preceding_pause["pause_completed_at"],
            "resume_started_at": None,
            "resume_completed_at": None,
            "trajectory_delay_sec": step.delay_time_sec,
            "configured_extra_delay_sec": options.pause_duration_sec,
            "requested_delay_sec": requested_delay_sec,
            "pause_sec": preceding_pause["pause_sec"],
            "paused_sec": paused_sec,
            "pause_scheduler_wait_sec": preceding_pause[
                "pause_scheduler_wait_sec"
            ],
            "pause_api_sec": preceding_pause["pause_api_sec"],
            "resume_sec": 0.0,
            "resume_attempts": 0,
            "resume_scheduler_wait_sec": 0.0,
            "resume_api_sec": 0.0,
            "resume_retry_wait_sec": 0.0,
            "pause_error": None,
            "ready_probe": None,
            "control_plane_mode": "rate",
            "run_queue_entered_at": started_at,
            "run_slot_acquired_at": None,
            "run_queue_wait_sec": 0.0,
            "run_slot_contention_wait_sec": 0.0,
            "capacity_wait_sec": 0.0,
            "running_slot_held_sec": 0.0,
            "resume_total_sec": 0.0,
            "interaction_pause_sec": preceding_pause["pause_sec"],
            "inference_delay_sec": requested_delay_sec,
            "command_sec": elapsed_sec,
            "interaction_total_sec": interaction_total_sec,
            "post_command_pause_started_at": None,
            "post_command_pause_completed_at": completed_at,
            "post_command_pause_sec": 0.0,
            "post_command_pause_scheduler_wait_sec": 0.0,
            "post_command_pause_api_sec": 0.0,
            # Proxy-specific fields.
            "proxy_duration_ms": payload.get("durationMs"),
            "binding_generation": payload.get("bindingGeneration"),
            "proxy_fallback_reason": None,
        }
        # The sandbox never left the paused state; a synthetic pause record
        # keeps delay chaining correct for the next step.
        next_pause = {
            "pause_started_at": started_at,
            "pause_completed_at": completed_at,
            "pause_completed_monotonic": completed_monotonic,
            "pause_sec": 0.0,
            "pause_scheduler_wait_sec": 0.0,
            "pause_api_sec": 0.0,
            "pause_reason": "proxy_read_noop",
        }
        return step_result, next_pause


# ── CLI ───────────────────────────────────────────────────────────────────────


def add_proxy_arguments(parser: argparse.ArgumentParser) -> None:
    group = parser.add_argument_group("proxy read")
    group.add_argument(
        "--proxy-read-disable",
        action="store_true",
        help="Route every step through the resume path (baseline mode)",
    )
    group.add_argument(
        "--proxy-read-no-fallback",
        action="store_true",
        help=(
            "Fail the replay when an eligible proxy read fails instead of "
            "falling back to the resume path"
        ),
    )
    group.add_argument(
        "--proxy-read-retry-timeout-sec",
        type=float,
        default=30.0,
        help="Retry window for 503 rebind_in_progress responses (default: 30)",
    )


def add_proxy_arguments_all_levels(parser: argparse.ArgumentParser) -> None:
    """Add the proxy flags to the top level AND every subcommand parser.

    The benchmark driver appends them after the subcommand (`... mix --config
    ... --proxy-read-retry-timeout-sec 30`), so each subparser must accept
    them too; defining them on both levels keeps direct invocations working
    regardless of position.
    """
    add_proxy_arguments(parser)
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            for subparser in action.choices.values():
                add_proxy_arguments(subparser)


def main() -> int:
    parser = replay_agent.build_parser()
    add_proxy_arguments_all_levels(parser)
    args = parser.parse_args()

    global PROXY_READ_ENABLED, PROXY_READ_FALLBACK, PROXY_READ_RETRY_TIMEOUT_SEC
    PROXY_READ_ENABLED = not args.proxy_read_disable
    PROXY_READ_FALLBACK = not args.proxy_read_no_fallback
    PROXY_READ_RETRY_TIMEOUT_SEC = args.proxy_read_retry_timeout_sec

    replay_agent.ReplayRunner = ProxyReplayRunner

    handler = logging.StreamHandler()
    handler.setFormatter(
        replay_agent.AbsoluteTimeFormatter(
            "%(asctime)s %(levelname)s %(message)s"
        )
    )
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        handlers=[handler],
    )
    if not args.verbose:
        for logger_name in ("httpx", "httpcore", "e2b"):
            logging.getLogger(logger_name).setLevel(logging.WARNING)
    replay_agent.configure_aenv_environment(args.api_url)
    try:
        return asyncio.run(args.handler(args))
    except KeyboardInterrupt:
        LOGGER.warning("Interrupted — cleaning up all sandboxes")
        try:
            asyncio.run(
                replay_agent.cleanup_all_sandboxes(label="interrupt")
            )
        except Exception as exc:
            LOGGER.error("Cleanup failed: %s: %s", type(exc).__name__, exc)
        return 130
    except Exception as exc:
        LOGGER.error("%s: %s", type(exc).__name__, exc)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
