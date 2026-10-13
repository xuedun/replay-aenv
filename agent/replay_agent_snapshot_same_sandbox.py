from __future__ import annotations

import asyncio
import time
from typing import Any

if __package__:
    from . import replay_agent_snapshot as base
else:
    import replay_agent_snapshot as base


class SameSandboxReplayRunner(base.ReplayRunner):
    """Replay a trajectory while repeatedly snapshotting one live sandbox."""

    async def _run_same_sandbox_step(
        self,
        step: base.ReplayStep,
        total_steps: int,
        job_total_steps: int,
    ) -> dict[str, Any]:
        requested_delay_sec = step.delay_time_sec + self.options.extra_delay_sec
        delay_started = time.monotonic()
        if requested_delay_sec:
            await asyncio.sleep(requested_delay_sec)
        actual_delay_sec = time.monotonic() - delay_started

        step_started = time.perf_counter()
        step_result: dict[str, Any] | None = None
        snapshot: dict[str, Any] | None = None
        primary_error: BaseException | None = None
        try:
            step_result = await self._execute_step(step, total_steps, job_total_steps)
        except BaseException as exc:
            primary_error = exc
        finally:
            if self.sandbox is not None:
                try:
                    snapshot = await self._create_snapshot_strict(
                        reason="same_sandbox_step_complete",
                        label=f"step-{step.index + 1:04d}",
                    )
                except BaseException as snapshot_exc:
                    if primary_error is None:
                        primary_error = snapshot_exc

        if primary_error is not None:
            raise primary_error
        if step_result is None or snapshot is None:
            raise RuntimeError("Same-sandbox step completed without command or snapshot")

        interaction_total_sec = time.perf_counter() - step_started + actual_delay_sec
        s3_write_mb = snapshot.get("snapshot_s3_write_size_mb")
        s3_write_text = (
            f"{s3_write_mb:.3f}" if isinstance(s3_write_mb, (int, float)) else "unavailable"
        )
        timing_message = (
            f"Same-sandbox interaction timing "
            f"step={step.index + 1}/{job_total_steps} "
            f"sandbox_id={self.sandbox_id} "
            f"delay={requested_delay_sec:.3f}s "
            f"command={step_result['elapsed_sec']:.3f}s "
            f"snapshot={snapshot['snapshot_sec']:.3f}s "
            f"snapshot_api={snapshot['snapshot_api_sec']:.3f}s "
            f"s3_write_mb={s3_write_text} "
            f"total={interaction_total_sec:.3f}s"
        )
        self._job_event(
            "info", timing_message, timestamp=snapshot["snapshot_completed_at"]
        )
        self._block(timing_message)
        step_result.update(
            {
                "trajectory_delay_sec": step.delay_time_sec,
                "configured_extra_delay_sec": self.options.extra_delay_sec,
                "requested_delay_sec": requested_delay_sec,
                "actual_delay_sec": actual_delay_sec,
                "inactive_sec": actual_delay_sec,
                "command_sec": step_result["elapsed_sec"],
                "interaction_total_sec": interaction_total_sec,
                "same_sandbox_id": self.sandbox_id,
                "post_command_snapshot_name": snapshot["snapshot_name"],
                "post_command_snapshot_id": snapshot["snapshot_id"],
                "post_command_snapshot_started_at": snapshot[
                    "snapshot_started_at"
                ],
                "post_command_snapshot_completed_at": snapshot[
                    "snapshot_completed_at"
                ],
                "post_command_snapshot_sec": snapshot["snapshot_sec"],
                "post_command_snapshot_scheduler_wait_sec": snapshot[
                    "snapshot_scheduler_wait_sec"
                ],
                "post_command_snapshot_api_sec": snapshot["snapshot_api_sec"],
                "post_command_snapshot_size_mb": snapshot.get("snapshot_size_mb"),
                "post_command_snapshot_s3_write_size_mb": snapshot.get(
                    "snapshot_s3_write_size_mb"
                ),
            }
        )
        return step_result

    def _snapshot_aggregates(self) -> dict[str, Any]:
        logical_sizes = [
            item["snapshot_size_bytes"]
            for item in self.created_snapshots
            if isinstance(item.get("snapshot_size_bytes"), int)
        ]
        write_sizes = [
            item["snapshot_s3_write_size_bytes"]
            for item in self.created_snapshots
            if isinstance(item.get("snapshot_s3_write_size_bytes"), int)
        ]
        return {
            "snapshot_count": len(self.created_snapshots),
            "snapshot_total_sec": sum(
                item["snapshot_sec"] for item in self.created_snapshots
            ),
            "snapshot_api_total_sec": sum(
                item["snapshot_api_sec"] for item in self.created_snapshots
            ),
            "snapshot_scheduler_wait_total_sec": sum(
                item["snapshot_scheduler_wait_sec"]
                for item in self.created_snapshots
            ),
            "snapshot_size_count": len(logical_sizes),
            "snapshot_size_total_bytes": sum(logical_sizes),
            "snapshot_s3_write_size_count": len(write_sizes),
            "snapshot_s3_write_size_total_bytes": sum(write_sizes),
        }

    async def run(self) -> dict[str, Any]:
        environment_name, instance_id, steps = base.load_trajectory(
            self.trajectory_path
        )
        terminal_step_position = next(
            (
                position
                for position, step in enumerate(steps, start=1)
                if step.action.lower() in base.SUBMIT_ACTIONS
            ),
            None,
        )
        executable_steps = (
            steps[: terminal_step_position - 1]
            if terminal_step_position is not None
            else steps
        )
        self.output_dir.mkdir(parents=True, exist_ok=True)
        replay_started_at = base.absolute_timestamp()
        replay_started = time.perf_counter()
        step_results: list[dict[str, Any]] = []
        stopped_early = False
        result: dict[str, Any] | None = None
        instance_patch_result: dict[str, Any] | None = None
        initial_create_info: dict[str, Any] | None = None

        try:
            self._event(
                "info",
                f"Same-sandbox replay started trajectory={self.trajectory_path}",
            )
            self._running_lease = await self.running_slot_scheduler.acquire(
                self.run_slot_task_id
            )
            try:
                initial_create_info = await self._create_sandbox()
                await self._probe_sandbox_ready()
                await self._validate_workdir()
                instance_patch_result = await self._initialize_instance_state(
                    instance_id
                )
            except BaseException:
                if self.sandbox is not None:
                    await self._force_stop_for_running_release(
                        reason="same_sandbox_initialization_failed"
                    )
                elif self._create_state_uncertain:
                    await self.running_slot_scheduler.close()
                if self._running_lease is not None and self.sandbox is None:
                    await self._running_lease.release()
                    self._running_lease = None
                raise

            for step in executable_steps:
                step_result = await self._run_same_sandbox_step(
                    step, len(executable_steps), len(steps)
                )
                step_results.append(step_result)
                if step_result["infrastructure_error"]:
                    details = {
                        "stage": "command",
                        "step_index": step.index,
                        "action": step.action,
                        "error": step_result["stderr"],
                    }
                    raise base.SandboxInfrastructureError(
                        "Sandbox infrastructure failed while dispatching command: "
                        + step_result["stderr"],
                        details,
                    )
                if self.options.stop_on_error and not step_result["succeeded"]:
                    stopped_early = True
                    break

            submitted = terminal_step_position is not None and not stopped_early
            succeeded_count = sum(1 for item in step_results if item["succeeded"])
            failed_count = len(step_results) - succeeded_count
            replay_completed = not stopped_early
            result = {
                "version": "0.4.0-same-sandbox-snapshot",
                "lifecycle_mode": "same_sandbox_snapshot",
                "trajectory_path": str(self.trajectory_path),
                "instance_id": instance_id,
                "template": self.options.template,
                "write_mode": self.options.write_mode,
                "sandbox_id": self.sandbox_id,
                "current_snapshot_id": self.current_snapshot_id,
                "snapshot_run_id": self.snapshot_run_id,
                "environment": environment_name,
                "started_at": replay_started_at,
                "completed_at": base.absolute_timestamp(),
                "trajectory_actions": len(steps),
                "executable_actions": len(executable_steps),
                "terminal_action_position": terminal_step_position,
                "submitted": submitted,
                "stopped_early": stopped_early,
                "replay_completed": replay_completed,
                "succeeded": replay_completed,
                "commands": len(step_results),
                "succeeded_commands": succeeded_count,
                "failed_commands": failed_count,
                "elapsed_sec": time.perf_counter() - replay_started,
                "requested_delay_sec": sum(
                    item["requested_delay_sec"] for item in step_results
                ),
                "actual_inactive_sec": sum(
                    item["inactive_sec"] for item in step_results
                ),
                "reload_total_sec": 0.0,
                **self._snapshot_aggregates(),
                "initial_create": initial_create_info,
                "instance_patch": instance_patch_result,
                "snapshots": self.created_snapshots,
                "steps": step_results,
            }
            base.write_json(self.output_dir / "replay-result.json", result)
            self._job_event(
                "info",
                "Same-sandbox replay completed "
                f"succeeded={result['succeeded']} commands={result['commands']} "
                f"snapshots={result['snapshot_count']} "
                f"elapsed={result['elapsed_sec']:.3f}s",
                timestamp=result["completed_at"],
            )
            return result
        except BaseException as exc:
            error_detail = f"{type(exc).__name__}: {exc!r}"
            self._event("error", f"Same-sandbox replay failed: {error_detail}")
            error_result = {
                "version": "0.4.0-same-sandbox-snapshot",
                "lifecycle_mode": "same_sandbox_snapshot",
                "trajectory_path": str(self.trajectory_path),
                "instance_id": instance_id,
                "template": self.options.template,
                "sandbox_id": self.sandbox_id,
                "current_snapshot_id": self.current_snapshot_id,
                "snapshot_run_id": self.snapshot_run_id,
                "replay_completed": False,
                "succeeded": False,
                "started_at": replay_started_at,
                "completed_at": base.absolute_timestamp(),
                "error": error_detail,
                "elapsed_sec": time.perf_counter() - replay_started,
                "reload_total_sec": 0.0,
                **self._snapshot_aggregates(),
                "instance_patch": instance_patch_result,
                "snapshots": self.created_snapshots,
                "steps": step_results,
            }
            if isinstance(exc, base.SandboxInfrastructureError):
                error_result["infrastructure_error"] = exc.details
            base.write_json(self.output_dir / "replay-result.json", error_result)
            raise
        finally:
            should_keep = (
                self.options.keep_sandbox_on_failure
                and (result is None or not result.get("succeeded", False))
                and self.sandbox is not None
            )
            if should_keep:
                self._event(
                    "warning", f"Keeping failed sandbox {self.sandbox_id} for debugging"
                )
            else:
                await self._cleanup()


base.ReplayRunner = SameSandboxReplayRunner
_BASE_BUILD_PARSER = base.build_parser


def build_parser():
    parser = _BASE_BUILD_PARSER()
    parser.prog = "replay-aenv-snapshot-same-sandbox"
    parser.description = (
        "Replay trajectories by repeatedly snapshotting one live AgentENV sandbox."
    )
    for action in parser._actions:
        if isinstance(action, base.argparse._SubParsersAction):
            for name, subparser in action.choices.items():
                subparser.prog = f"{parser.prog} {name}"
    return parser


def main() -> int:
    original_build_parser = base.build_parser
    base.build_parser = build_parser
    try:
        return base.main()
    finally:
        base.build_parser = original_build_parser


if __name__ == "__main__":
    raise SystemExit(main())
