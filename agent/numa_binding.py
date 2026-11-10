"""Best-effort NUMA binding for local Firecracker sandboxes.

Divides the planned VM set into contiguous groups (one group per NUMA node,
e.g. four equal parts on a four-node host) and pins each sandbox's
firecracker process to its group's node CPUs, so guest memory faults and
vCPU threads stay node-local.

Sandbox -> firecracker PID mapping reuses the MMDS trick from the direct-envd
route resolver: firecracker exposes `instanceID` (= AgentENV sandbox id) on
its local API socket.
"""

from __future__ import annotations

import asyncio
import http.client
import json
import re
import socket
import subprocess
from pathlib import Path


def numa_topology() -> list[tuple[int, str]]:
    """Return [(node_id, cpulist)] read from /sys/devices/system/node."""

    base = Path("/sys/devices/system/node")
    if not base.is_dir():
        return []
    nodes: list[tuple[int, str]] = []
    for entry in base.iterdir():
        match = re.fullmatch(r"node(\d+)", entry.name)
        if not match:
            continue
        try:
            cpulist = (entry / "cpulist").read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if cpulist:
            nodes.append((int(match.group(1)), cpulist))
    nodes.sort()
    return nodes


def node_for_vm(index: int, total: int, num_nodes: int) -> int:
    """Map VM ordinal to a NUMA node using contiguous equal-sized groups.

    With total=9 and num_nodes=4 the VMs land on nodes
    0,0,0,1,1,2,2,3,3 (3+2+2+2), i.e. "divide VMs into four parts".
    """

    if num_nodes <= 0:
        return 0
    return (index * num_nodes) // max(total, 1)


def _mmds_instance_id(socket_path: Path, timeout_sec: float = 0.5) -> str | None:
    """Read the non-secret instanceID field from a firecracker MMDS socket."""

    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(timeout_sec)
            client.connect(str(socket_path))
            client.sendall(
                b"GET /mmds HTTP/1.1\r\n"
                b"Host: localhost\r\n"
                b"Connection: close\r\n\r\n"
            )
            response = http.client.HTTPResponse(client)
            response.begin()
            if response.status != 200:
                return None
            payload = json.loads(response.read())
    except (
        OSError,
        TimeoutError,
        http.client.HTTPException,
        json.JSONDecodeError,
    ):
        return None
    if not isinstance(payload, dict):
        return None
    instance_id = payload.get("instanceID")
    return instance_id if isinstance(instance_id, str) and instance_id else None


def _live_firecrackers() -> list[tuple[int, Path]]:
    """Return [(pid, api_socket_path)] for every live firecracker process."""

    processes: list[tuple[int, Path]] = []
    for proc_dir in Path("/proc").iterdir():
        if not proc_dir.name.isdigit():
            continue
        try:
            arguments = [
                part.decode("utf-8", errors="replace")
                for part in (proc_dir / "cmdline").read_bytes().split(b"\0")
                if part
            ]
            if not arguments or Path(arguments[0]).name != "firecracker":
                continue
            if "--api-sock" not in arguments:
                continue
            socket_path = Path(arguments[arguments.index("--api-sock") + 1])
        except (OSError, ValueError, IndexError):
            continue
        if socket_path.is_socket():
            processes.append((int(proc_dir.name), socket_path))
    return processes


def firecracker_pid_for_sandbox(sandbox_id: str) -> int | None:
    """Find the firecracker PID whose MMDS instanceID equals sandbox_id."""

    for pid, socket_path in _live_firecrackers():
        if _mmds_instance_id(socket_path) == sandbox_id:
            return pid
    return None


def _taskset(pid: int, cpulist: str) -> str | None:
    """Run `taskset -pc <cpulist> <pid>`; return its stdout on success."""

    try:
        proc = subprocess.run(
            ["taskset", "-pc", cpulist, str(pid)],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if proc.returncode != 0:
        return None
    return proc.stdout.strip()


async def bind_sandbox_to_numa(
    sandbox_id: str,
    *,
    index: int,
    total: int,
    attempts: int = 6,
    retry_delay_sec: float = 0.5,
) -> dict[str, object]:
    """Pin the sandbox's firecracker process to its NUMA group's CPUs.

    Best-effort and non-raising: always returns a report dict so replay is
    never affected by binding failures. Memory pages faulted before binding
    stay where they are; binding right after create covers the lazily
    faulted guest memory during replay.
    """

    nodes = numa_topology()
    num_nodes = len(nodes)
    report: dict[str, object] = {
        "applied": False,
        "sandbox_id": sandbox_id,
        "index": index,
        "total": total,
    }
    if num_nodes == 0:
        report["reason"] = "no NUMA topology under /sys/devices/system/node"
        return report
    node_id, cpulist = nodes[node_for_vm(index, total, num_nodes)]
    report["node"] = node_id
    report["cpulist"] = cpulist

    pid = None
    for attempt in range(1, attempts + 1):
        pid = await asyncio.to_thread(firecracker_pid_for_sandbox, sandbox_id)
        if pid is not None:
            break
        if attempt < attempts:
            await asyncio.sleep(retry_delay_sec)
    if pid is None:
        report["reason"] = "firecracker process not found via MMDS scan"
        return report
    report["pid"] = pid

    output = await asyncio.to_thread(_taskset, pid, cpulist)
    if output is None:
        report["reason"] = f"taskset -pc {cpulist} {pid} failed"
        return report
    report["applied"] = True
    report["taskset"] = output.splitlines()[-1] if output else ""
    return report
