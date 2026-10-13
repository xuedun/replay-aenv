from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import IntEnum
import time
from typing import Any


def is_transient_sandbox_error(exc: BaseException) -> bool:
    """Return whether a failed infrastructure request is safe to retry."""

    text = f"{type(exc).__name__}: {exc}".lower()
    return any(
        marker in text
        for marker in (
            "502:",
            "http 502",
            "bad gateway",
            "503:",
            "http 503",
            "service unavailable",
            "504:",
            "http 504",
            "gateway timeout",
            "429:",
            "http 429",
            "too many requests",
            "rate limit",
            "timed out",
            "timeout",
            "connection reset",
            "connection refused",
            "connection aborted",
            "temporarily unavailable",
        )
    )


class OperationType(IntEnum):
    RESUME = 0
    CLEANUP = 10
    PAUSE = 20
    CREATE = 30
    COMMAND = 40


@dataclass(slots=True, frozen=True)
class ControlPlaneConfig:
    qps: float = 20.0
    inflight_cap: int = 1024

    @classmethod
    def single_task(cls) -> "ControlPlaneConfig":
        # A single replay cannot create a request burst, so avoid adding a
        # visible delay while still using the same scheduler implementation.
        return cls(qps=1_000.0, inflight_cap=1)


@dataclass(slots=True)
class _RateWaiter:
    operation: OperationType
    future: asyncio.Future[None]
    enqueued_at: float
    reserved: bool = False
    granted: bool = False


class SmoothRateLimiter:
    """Emit one smooth FIFO request stream with bounded in-flight work.

    Every new request and every retry enters the same queue. Operation type is
    recorded only for metrics: create, pause, resume, cleanup and command all
    have identical admission priority and are dispatched in arrival order.
    """

    def __init__(self, *, qps: float, inflight_cap: int) -> None:
        if qps <= 0:
            raise ValueError("control-plane-qps must be positive")
        if inflight_cap <= 0:
            raise ValueError("control-plane in-flight cap must be positive")

        self.qps = qps
        self.inflight_cap = inflight_cap
        self._interval_sec = 1.0 / qps
        self._condition = asyncio.Condition()
        self._queue: deque[_RateWaiter] = deque()
        self._next_dispatch_at = 0.0
        self._in_flight = 0
        self._dispatched = 0
        self._dispatcher: asyncio.Task[None] | None = None
        self._wait_total_sec = 0.0
        self._wait_max_sec = 0.0
        self._dispatched_by_operation: dict[OperationType, int] = {
            operation: 0 for operation in OperationType
        }

    def _waiting(self) -> int:
        return len(self._queue)

    def snapshot(self) -> dict[str, Any]:
        return {
            "mode": "rate",
            "qps": self.qps,
            "inflight_cap": self.inflight_cap,
            "in_flight": self._in_flight,
            "waiting": self._waiting(),
            "dispatched": self._dispatched,
            "average_wait_sec": (
                self._wait_total_sec / self._dispatched if self._dispatched else 0.0
            ),
            "max_wait_sec": self._wait_max_sec,
            "dispatched_by_operation": {
                operation.name.lower(): count
                for operation, count in self._dispatched_by_operation.items()
            },
            "waiting_by_operation": {
                operation.name.lower(): sum(
                    1 for waiter in self._queue if waiter.operation == operation
                )
                for operation in OperationType
            },
        }

    def _select_waiter(self) -> _RateWaiter | None:
        while self._queue and self._queue[0].future.cancelled():
            self._queue.popleft()
        return self._queue.popleft() if self._queue else None

    def _ensure_dispatcher_locked(self) -> None:
        if self._dispatcher is None or self._dispatcher.done():
            self._dispatcher = asyncio.create_task(self._dispatch_loop())

    async def _dispatch_loop(self) -> None:
        while True:
            async with self._condition:
                await self._condition.wait_for(
                    lambda: self._waiting() == 0
                    or self._in_flight < self.inflight_cap
                )
                if self._waiting() == 0:
                    self._dispatcher = None
                    return

                waiter = self._select_waiter()
                if waiter is None:
                    continue
                waiter.reserved = True
                self._in_flight += 1
                now = time.monotonic()
                dispatch_at = max(now, self._next_dispatch_at)
                wait_sec = max(0.0, dispatch_at - now)

            if wait_sec:
                await asyncio.sleep(wait_sec)

            # Schedule from the actual dispatch time rather than the ideal
            # deadline. If the event loop wakes late, do not "catch up" by
            # emitting following requests in a burst.
            async with self._condition:
                self._next_dispatch_at = time.monotonic() + self._interval_sec

            if waiter.future.cancelled():
                async with self._condition:
                    self._in_flight -= 1
                    self._condition.notify_all()
                continue

            queue_wait_sec = time.monotonic() - waiter.enqueued_at
            self._dispatched += 1
            self._wait_total_sec += queue_wait_sec
            self._wait_max_sec = max(self._wait_max_sec, queue_wait_sec)
            self._dispatched_by_operation[waiter.operation] += 1
            waiter.granted = True
            waiter.future.set_result(None)

    async def _acquire(self, operation: OperationType) -> None:
        loop = asyncio.get_running_loop()
        waiter = _RateWaiter(
            operation=operation,
            future=loop.create_future(),
            enqueued_at=time.monotonic(),
        )
        async with self._condition:
            self._queue.append(waiter)
            self._ensure_dispatcher_locked()
            self._condition.notify_all()

        try:
            await waiter.future
        except BaseException:
            async with self._condition:
                if not waiter.reserved:
                    try:
                        self._queue.remove(waiter)
                    except ValueError:
                        pass
                elif waiter.granted:
                    # The caller was cancelled after admission but before its
                    # context body started, so it will never call _release().
                    self._in_flight -= 1
                # A reserved but not-yet-granted waiter is released by the
                # dispatcher after it observes the cancelled future.
                self._condition.notify_all()
            raise

    async def _release(self) -> None:
        async with self._condition:
            self._in_flight -= 1
            if self._in_flight < 0:
                raise RuntimeError("global rate limiter released without acquire")
            self._condition.notify_all()

    @asynccontextmanager
    async def slot(self, *, operation: OperationType) -> AsyncIterator[None]:
        acquired = False
        try:
            await self._acquire(operation)
            acquired = True
            yield
        finally:
            if acquired:
                await self._release()


class ControlPlaneScheduler:
    """Route every infrastructure request through one global QPS queue."""

    def __init__(self, config: ControlPlaneConfig) -> None:
        self.config = config
        self.rate_limiter = SmoothRateLimiter(
            qps=config.qps,
            inflight_cap=config.inflight_cap,
        )

    @classmethod
    def single_task(cls) -> "ControlPlaneScheduler":
        return cls(ControlPlaneConfig.single_task())

    @property
    def uses_rate_limit(self) -> bool:
        return True

    @asynccontextmanager
    async def create_slot(self) -> AsyncIterator[None]:
        async with self.rate_limiter.slot(operation=OperationType.CREATE):
            yield

    @asynccontextmanager
    async def pause_slot(self) -> AsyncIterator[None]:
        async with self.rate_limiter.slot(operation=OperationType.PAUSE):
            yield

    @asynccontextmanager
    async def resume_slot(self) -> AsyncIterator[None]:
        async with self.rate_limiter.slot(operation=OperationType.RESUME):
            yield

    @asynccontextmanager
    async def cleanup_slot(self) -> AsyncIterator[None]:
        async with self.rate_limiter.slot(operation=OperationType.CLEANUP):
            yield

    @asynccontextmanager
    async def command_start_slot(self) -> AsyncIterator[None]:
        # Only stream establishment is rate-limited. Command execution after a
        # handle is returned remains concurrent.
        async with self.rate_limiter.slot(operation=OperationType.COMMAND):
            yield

    def snapshot(self) -> dict[str, Any]:
        return self.rate_limiter.snapshot()
