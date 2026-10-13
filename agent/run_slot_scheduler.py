from __future__ import annotations

import asyncio
import heapq
import itertools
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any


@dataclass(order=True, slots=True)
class _RunRequest:
    ready_at: float
    sequence: int
    task_id: str = field(compare=False)
    queued_at: float = field(compare=False)
    future: asyncio.Future["RunningLease"] = field(compare=False)


class RunningLease:
    """One reserved RUNNING sandbox slot.

    The reservation is made before a sandbox is created or resumed and must stay
    held until the sandbox is confirmed paused, stopped, or deleted.
    """

    __slots__ = (
        "_scheduler",
        "lease_id",
        "task_id",
        "acquired_at",
        "queue_wait_sec",
        "_released",
    )

    def __init__(
        self,
        scheduler: "RunningSlotScheduler",
        *,
        lease_id: int,
        task_id: str,
        acquired_at: float,
        queue_wait_sec: float,
    ) -> None:
        self._scheduler = scheduler
        self.lease_id = lease_id
        self.task_id = task_id
        self.acquired_at = acquired_at
        self.queue_wait_sec = queue_wait_sec
        self._released = False

    @property
    def released(self) -> bool:
        return self._released

    async def release(self) -> None:
        if self._released:
            raise RuntimeError(f"Running lease {self.lease_id} released twice")
        self._released = True
        await self._scheduler._release(self)


class RunningSlotScheduler:
    """FIFO admission for complete ``resume -> command -> pause`` slices.

    Requests remain in a monotonic-time delay heap until ``ready_at``.  Eligible
    requests are admitted in ready-time/arrival order.  ``active`` includes a
    reservation before resume starts, so client-side admission can never exceed
    ``maximum`` even while lifecycle calls are in flight.
    """

    def __init__(self, maximum: int) -> None:
        if maximum < 1:
            raise ValueError("running slot maximum must be positive")
        self.maximum = maximum
        self._condition = asyncio.Condition()
        self._sequence = itertools.count(1)
        self._lease_ids = itertools.count(1)
        self._delayed: list[_RunRequest] = []
        self._ready: deque[_RunRequest] = deque()
        self._active: dict[int, RunningLease] = {}
        self._peak_active = 0
        self._granted = 0
        self._total_queue_wait_sec = 0.0
        self._dispatcher: asyncio.Task[None] | None = None
        self._closed = False

    @classmethod
    def single_task(cls) -> "RunningSlotScheduler":
        return cls(1)

    def _ensure_dispatcher(self) -> None:
        if self._dispatcher is None:
            self._dispatcher = asyncio.create_task(self._dispatch_loop())

    async def acquire(
        self,
        task_id: str,
        *,
        ready_at: float | None = None,
    ) -> RunningLease:
        if not task_id:
            raise ValueError("task_id must not be empty")
        if self._closed:
            raise RuntimeError("running slot scheduler is closed")

        loop = asyncio.get_running_loop()
        now = time.monotonic()
        request = _RunRequest(
            ready_at=now if ready_at is None else max(now, ready_at),
            sequence=next(self._sequence),
            task_id=task_id,
            queued_at=now,
            future=loop.create_future(),
        )
        self._ensure_dispatcher()
        async with self._condition:
            if self._closed:
                raise RuntimeError("running slot scheduler is closed")
            heapq.heappush(self._delayed, request)
            self._condition.notify_all()

        try:
            return await request.future
        except BaseException:
            granted_lease: RunningLease | None = None
            if request.future.done() and not request.future.cancelled():
                granted_lease = request.future.result()
            else:
                request.future.cancel()
            async with self._condition:
                self._condition.notify_all()
            if granted_lease is not None and not granted_lease.released:
                await granted_lease.release()
            raise

    async def _dispatch_loop(self) -> None:
        while True:
            async with self._condition:
                now = time.monotonic()
                while self._delayed and self._delayed[0].ready_at <= now:
                    request = heapq.heappop(self._delayed)
                    if not request.future.cancelled():
                        self._ready.append(request)

                while self._ready and len(self._active) < self.maximum:
                    request = self._ready.popleft()
                    if request.future.cancelled():
                        continue
                    acquired_at = time.monotonic()
                    lease = RunningLease(
                        self,
                        lease_id=next(self._lease_ids),
                        task_id=request.task_id,
                        acquired_at=acquired_at,
                        queue_wait_sec=acquired_at - request.queued_at,
                    )
                    self._active[lease.lease_id] = lease
                    self._peak_active = max(self._peak_active, len(self._active))
                    self._granted += 1
                    self._total_queue_wait_sec += lease.queue_wait_sec
                    request.future.set_result(lease)

                if self._closed:
                    pending = list(self._ready)
                    pending.extend(self._delayed)
                    self._ready.clear()
                    self._delayed.clear()
                    for request in pending:
                        if not request.future.done():
                            request.future.set_exception(
                                RuntimeError("running slot scheduler closed")
                            )
                    return

                timeout: float | None = None
                if self._delayed:
                    timeout = max(0.0, self._delayed[0].ready_at - time.monotonic())

                try:
                    if timeout is None:
                        await self._condition.wait()
                    else:
                        await asyncio.wait_for(self._condition.wait(), timeout=timeout)
                except TimeoutError:
                    pass

    async def _release(self, lease: RunningLease) -> None:
        async with self._condition:
            active = self._active.pop(lease.lease_id, None)
            if active is None:
                raise RuntimeError(f"Unknown running lease {lease.lease_id}")
            self._condition.notify_all()

    def snapshot(self) -> dict[str, Any]:
        waiting = len(self._ready) + sum(
            1 for request in self._delayed if not request.future.cancelled()
        )
        return {
            "maximum": self.maximum,
            "active": len(self._active),
            "peak_active": self._peak_active,
            "waiting": waiting,
            "granted": self._granted,
            "average_queue_wait_sec": (
                self._total_queue_wait_sec / self._granted if self._granted else 0.0
            ),
        }

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        async with self._condition:
            self._condition.notify_all()
        if self._dispatcher is not None:
            await self._dispatcher
