"""CCL (L3-domain) CPU affinity binding for local Firecracker sandboxes.

Pins each sandbox's firecracker process -- *every* thread -- to one
silicon L3 domain so concurrent VMs stop contending for the same L3 and
stay on node-local memory.  Architecture independent: AMD CCD, Kunpeng CL
and Intel CCX all expose ``cache/index3/shared_cpu_list``, so detection is
pure sysfs with no per-vendor branching.

Why not ``taskset -p <pid>``: that syscall form targets the thread group
leader only.  Firecracker is 1 VMM thread + N vCPU threads, so a TGID-only
bind leaves every vCPU thread free to roam the whole machine -- the exact
threads that need pinning.  This module calls ``os.sched_setaffinity`` per
tid instead: main thread first (``clone()`` copies the creating thread's
mask, so later threads inherit it), then a ``/proc/<pid>/task`` sweep
repeated until no new tid appears.

AENV keeps a warm firecracker pool, so each firecracker exists *before*
create with only a random ``--api-sock`` path (no sandbox id in its
cmdline).  Sandbox -> pid therefore goes through MMDS: scan /proc for
firecracker processes, connect each one's MMDS unix socket and read
``instanceID`` (= AgentENV sandbox id).

Best-effort by contract: every failure (non-Linux host, no root, pid
unresolved, setaffinity error, cpuset restriction) degrades to a report
field and never raises, so a binding problem can never unwind an
already-successful create or resume.

Ported from agent_vm_bench ``src/env_provider/aenv/_ccl.py`` and adapted to
this repo's asyncio agents (blocking syscalls run under ``asyncio.to_thread``).
"""

from __future__ import annotations

import asyncio
import http.client
import json
import os
import socket
import threading
import time
from pathlib import Path

# Resolve retries / per-scan timeout. The firecracker may take a moment to be
# registered in the warm pool after create() returns; a few short scans cover
# that race without stalling the create path.
PID_RESOLVE_ATTEMPTS = 8
PID_RESOLVE_TIMEOUT_SEC = 1.0

# Sysfs root for topology detection. Module attribute (not a default argument)
# so tests can monkeypatch it; production is always /sys.
_SYSFS_ROOT = Path("/sys")

GRANULARITIES = ("auto", "l3", "numa", "socket")


# ---------------------------------------------------------------------------
# cpulist parsing / formatting
# ---------------------------------------------------------------------------
def parse_cpu_list(cpu_list: str) -> set[int]:
    """Parse a kernel cpulist string (``'0-7,192-199'``) into a set of ids."""

    cpus: set[int] = set()
    for part in (cpu_list or "").split(","):
        part = part.strip()
        if not part:
            continue
        bounds = part.split("-", 1)
        try:
            lo = int(bounds[0])
            hi = int(bounds[1]) if len(bounds) > 1 else lo
        except ValueError:
            continue
        cpus.update(range(lo, hi + 1))
    return cpus


def format_cpu_list(cpus: set[int]) -> str:
    """Format CPU ids as a kernel cpulist (``{0,1,2,192}`` -> ``'0-2,192'``)."""

    parts: list[str] = []
    start = prev = -1
    for cpu in sorted(cpus) + [-2]:  # sentinel flushes the last range
        if start < 0:
            start = prev = cpu
        elif cpu == prev + 1:
            prev = cpu
        else:
            parts.append(str(start) if start == prev else f"{start}-{prev}")
            start = prev = cpu
    return ",".join(parts)


def _read_cpulist_file(path: Path) -> set[int] | None:
    try:
        text = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return parse_cpu_list(text) or None


# ---------------------------------------------------------------------------
# Topology detection (pure sysfs; AMD CCD / Kunpeng CL / Intel CCX alike)
# ---------------------------------------------------------------------------
def detect_l3_domains(sysfs_root: Path) -> list[set[int]]:
    """Detect L3 cache domains from ``cache/index3/shared_cpu_list``.

    Every member of a domain reports the same list, so dedupe by ``min(cpus)``.
    Sorted by first CPU id so the index -> domain mapping is deterministic
    across runs.
    """

    domains: dict[int, set[int]] = {}
    try:
        entries = list((sysfs_root / "devices/system/cpu").iterdir())
    except OSError:
        return []
    for entry in entries:
        if not entry.name.startswith("cpu") or not entry.name[3:].isdigit():
            continue
        cpus = _read_cpulist_file(entry / "cache" / "index3" / "shared_cpu_list")
        if cpus:
            domains.setdefault(min(cpus), cpus)
    return [domains[k] for k in sorted(domains)]


def detect_numa_domains(sysfs_root: Path) -> list[set[int]]:
    """Detect NUMA node CPU sets, sorted by node id."""

    domains: dict[int, set[int]] = {}
    try:
        entries = list((sysfs_root / "devices/system/node").iterdir())
    except OSError:
        return []
    for entry in entries:
        if not entry.name.startswith("node") or not entry.name[4:].isdigit():
            continue
        cpus = _read_cpulist_file(entry / "cpulist")
        if cpus:
            domains[int(entry.name[4:])] = cpus
    return [domains[k] for k in sorted(domains)]


def detect_socket_domains(sysfs_root: Path) -> list[set[int]]:
    """Detect socket CPU sets via ``topology/physical_package_id`` (sysfs has
    no socket cpulist file), sorted by package id."""

    packages: dict[int, set[int]] = {}
    try:
        entries = list((sysfs_root / "devices/system/cpu").iterdir())
    except OSError:
        return []
    for entry in entries:
        if not entry.name.startswith("cpu") or not entry.name[3:].isdigit():
            continue
        try:
            pkg = int(
                (entry / "topology" / "physical_package_id").read_text(encoding="utf-8").strip()
            )
        except (OSError, ValueError):
            continue
        packages.setdefault(pkg, set()).add(int(entry.name[3:]))
    return [packages[k] for k in sorted(packages)]


_DETECTORS = {
    "l3": detect_l3_domains,
    "numa": detect_numa_domains,
    "socket": detect_socket_domains,
}

# Process-level cache: one agent process = one host. Concurrent binds may
# detect twice -- idempotent, so no lock.
_TOPOLOGY_CACHE: tuple[list[set[int]], str] | None = None


def select_domains(
    granularity: str = "auto",
    sysfs_root: Path | None = None,
) -> tuple[list[set[int]], str]:
    """Pick binding domains per ``granularity``. Cached per process.

    - ``l3`` / ``numa`` / ``socket``: force that sysfs level.
    - ``auto`` (default): L3 domains whenever sysfs exposes them -- AMD CCD and
      Kunpeng CL both bind at silicon granularity; NUMA only when the host
      exposes no L3 domains; else socket.

    Returns ``(domains sorted by first cpu, source label for logging)``.
    """

    global _TOPOLOGY_CACHE
    if _TOPOLOGY_CACHE is not None:
        return _TOPOLOGY_CACHE
    root = _SYSFS_ROOT if sysfs_root is None else sysfs_root
    granularity = str(granularity or "auto").lower()
    if granularity not in GRANULARITIES:
        granularity = "auto"

    domains: list[set[int]] = []
    source = "none"
    if granularity in _DETECTORS:
        domains = _DETECTORS[granularity](root)
        source = f"sysfs-{granularity}"
    else:  # auto
        domains = detect_l3_domains(root)
        source = "sysfs-l3(auto)"
        if not domains:
            domains = detect_numa_domains(root)
            source = "sysfs-numa(auto: no l3 domains)"
        if not domains:
            domains = detect_socket_domains(root)
            source = "sysfs-socket(auto: no l3/numa)"

    _TOPOLOGY_CACHE = (domains, source)
    return _TOPOLOGY_CACHE


def reset_ccl_cache() -> None:
    """Clear the pid and topology caches (tests, or a host topology change)."""

    global _TOPOLOGY_CACHE
    RESOLVER.invalidate()
    _TOPOLOGY_CACHE = None


# ---------------------------------------------------------------------------
# Thread-level affinity binding
# ---------------------------------------------------------------------------
def _task_tids(pid: int) -> list[int]:
    """Every thread id of a process (``/proc/<pid>/task/*``, includes pid)."""

    try:
        entries = (Path("/proc") / str(pid) / "task").iterdir()
        return sorted(int(e.name) for e in entries if e.name.isdigit())
    except OSError:
        return [pid]


def _task_allowed_cpus(pid: int) -> set[int] | None:
    """CPUs the task may actually run on (cgroup cpuset restriction plus
    online CPUs), or ``None`` when it cannot be read."""

    get_affinity = getattr(os, "sched_getaffinity", None)
    if get_affinity is None:
        return None
    try:
        return get_affinity(pid)
    except OSError:
        return None


def bind_process_all_threads(
    pid: int,
    cpus: set[int],
    max_passes: int = 3,
) -> tuple[bool, str]:
    """One-shot pin of EVERY thread of ``pid`` to ``cpus`` (no polling).

    Order matters: the main thread is bound first, so any thread spawned
    afterwards inherits the mask (Linux ``clone()`` copies the creating
    thread's affinity).  Then every ``/proc/<pid>/task`` tid is swept,
    repeating until a re-list shows no unbound tid (normally one pass --
    firecracker's thread set is fixed after boot: 1 VMM + N vCPU threads).

    The request is first intersected with the mask the task is actually
    permitted to run on.  A deployment that confines the sandbox process -- an
    AENV server or an agent running inside a cpuset-limited container --
    otherwise turns a domain outside that set into a bare ``EINVAL`` which
    names nothing.  The intersection binds what is permitted; no permitted CPU
    at all is a reported skip carrying the real cause.

    Verification compares every live thread against the kernel-honoured
    effective mask of the main thread (the kernel may narrow the request to
    online/cgroup-allowed CPUs; a narrowing is reported, not failed).
    Returns ``(verified, detail)``.
    """

    requested = set(cpus)
    allowed = _task_allowed_cpus(pid)
    if allowed is not None:
        cpus = requested & allowed
        if not cpus:
            return False, (
                f"skip: requested={format_cpu_list(requested)} has no CPU the task may run on "
                f"(task_allowed={format_cpu_list(allowed)}; cgroup cpuset restriction or offline CPUs)"
            )
    try:
        os.sched_setaffinity(pid, cpus)
    except OSError as err:
        return False, f"setaffinity(main tid={pid}) failed: {err!r}"
    try:
        effective = os.sched_getaffinity(pid)
    except OSError:
        effective = set(cpus)

    bound = {pid}
    for _ in range(max_passes):
        for tid in _task_tids(pid):
            if tid in bound:
                continue
            try:
                os.sched_setaffinity(tid, cpus)
                bound.add(tid)
            except OSError:
                pass  # thread exited between listing and binding
        if set(_task_tids(pid)) <= bound:
            break

    mismatched = []
    checked = 0
    for tid in _task_tids(pid):
        try:
            if os.sched_getaffinity(tid) != effective:
                mismatched.append(tid)
            checked += 1
        except OSError:
            pass  # thread exited; nothing left to verify

    detail = f"threads={len(bound)}"
    if not checked:
        detail += " no_live_thread_to_verify"
    if effective != requested:
        detail += (
            f" narrowed_to={format_cpu_list(effective)}"
            f" (requested={format_cpu_list(requested)})"
        )
    if mismatched:
        detail += f" mismatched_tids={mismatched}"
    # checked > 0: a process that died between bind and verify has no live
    # thread to compare -- that is NOT a verified bind.
    return not mismatched and checked > 0, detail


# ---------------------------------------------------------------------------
# sandbox_id -> firecracker pid (MMDS instanceID scan)
# ---------------------------------------------------------------------------
class FirecrackerPidResolver:
    """Map sandbox_id -> firecracker pid by reading MMDS instanceID locally.

    A cache hit short-circuits without a rescan.  Pause/resume invalidates the
    entry for that sandbox first, because resume spawns a *fresh* firecracker
    from the warm pool under a new pid.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._pid_map: dict[str, int] = {}

    def invalidate(self, sandbox_id: str | None = None) -> None:
        """Drop one cached mapping (or all of them when sandbox_id is None)."""

        with self._lock:
            if sandbox_id is None:
                self._pid_map.clear()
            else:
                self._pid_map.pop(sandbox_id, None)

    @staticmethod
    def _live_firecrackers() -> list[tuple[int, Path]]:
        """List (pid, api-sock-path) for every live firecracker on this host."""

        processes: list[tuple[int, Path]] = []
        try:
            proc_dirs = list(Path("/proc").iterdir())
        except OSError:
            return processes
        for proc_dir in proc_dirs:
            if not proc_dir.name.isdigit():
                continue
            try:
                arguments = [
                    part.decode("utf-8", errors="replace")
                    for part in (proc_dir / "cmdline").read_bytes().split(b"\0")
                    if part
                ]
                socket_index = arguments.index("--api-sock") + 1
                socket_path = Path(arguments[socket_index])
            except (OSError, ValueError, IndexError):
                continue
            if not arguments or Path(arguments[0]).name != "firecracker":
                continue
            if socket_path.is_socket():
                processes.append((int(proc_dir.name), socket_path))
        processes.sort(reverse=True)
        return processes

    @staticmethod
    def _mmds_instance_id(socket_path: Path, timeout_sec: float) -> str | None:
        """Read MMDS locally and retain only the non-secret instanceID field."""

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

    def _scan_pids(self, timeout_sec: float) -> tuple[int, int]:
        """Populate the cache from a /proc + MMDS sweep. Returns (live, identified)."""

        processes = self._live_firecrackers()
        identified = 0
        for pid, socket_path in processes:
            instance_id = self._mmds_instance_id(socket_path, timeout_sec)
            if instance_id is None:
                continue
            identified += 1
            self._pid_map.setdefault(instance_id, pid)
        return len(processes), identified

    def resolve(
        self,
        sandbox_id: str,
        *,
        attempts: int = PID_RESOLVE_ATTEMPTS,
        timeout_sec: float = PID_RESOLVE_TIMEOUT_SEC,
    ) -> tuple[int | None, str]:
        """Resolve a sandbox_id to its firecracker pid, with retry + cache.

        Returns ``(pid_or_None, last_scan_description)``.  Never raises.
        """

        last_scan = "scan did not run"
        for attempt in range(1, attempts + 1):
            cached = self._pid_map.get(sandbox_id)
            if cached is not None:
                return cached, "cache=hit"
            if attempt == 1:
                # Let a just-created sandbox register in the warm pool first.
                time.sleep(0.05)
            with self._lock:
                cached = self._pid_map.get(sandbox_id)
                if cached is not None:
                    return cached, "cache=hit"
                live, identified = self._scan_pids(min(max(timeout_sec, 0.2), 3.0))
                last_scan = f"live_firecrackers={live} identified={identified}"
                cached = self._pid_map.get(sandbox_id)
                if cached is not None:
                    return cached, f"cache=hit-after-scan {last_scan}"
            if attempt < attempts:
                time.sleep(min(0.2 * attempt, 1.0))
        return None, f"unresolved after {attempts} attempts ({last_scan})"


# Module-level singleton: one agent process = one run; the cache persists
# across sandboxes so the first bind primes it for the rest.
RESOLVER = FirecrackerPidResolver()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------
def _bind_sync(
    sandbox_id: str,
    *,
    index: int,
    granularity: str = "auto",
    invalidate_cache: bool = False,
) -> dict[str, object]:
    """Synchronous bind core. Returns a report dict; never raises."""

    report: dict[str, object] = {
        "applied": False,
        "verified": False,
        "sandbox_id": sandbox_id,
        "index": index,
        "granularity": granularity,
    }

    if not hasattr(os, "sched_setaffinity"):
        report["reason"] = "os.sched_setaffinity unavailable (need a Linux host)"
        return report
    if not sandbox_id:
        report["reason"] = "sandbox_id not set"
        return report

    if invalidate_cache:
        # Resume spawns a fresh firecracker under a new pid; the create-time
        # mapping is stale by definition.
        RESOLVER.invalidate(sandbox_id)

    try:
        pid, scan_note = RESOLVER.resolve(sandbox_id)
    except Exception as exc:  # noqa: BLE001 -- best-effort, never raise
        report["reason"] = f"pid resolve raised {type(exc).__name__}: {exc!r}"
        return report
    if pid is None:
        report["reason"] = f"firecracker pid not found via MMDS scan ({scan_note}); need root"
        return report
    report["pid"] = pid
    report["pid_scan"] = scan_note

    try:
        domains, source = select_domains(granularity)
    except Exception as exc:  # noqa: BLE001 -- best-effort, never raise
        report["reason"] = f"domain detection raised {type(exc).__name__}: {exc!r}"
        return report
    if not domains:
        report["reason"] = f"no CPU domains detected (source={source})"
        return report
    report["source"] = source

    # Assign only from domains this firecracker may actually use: a cgroup
    # cpuset (or offline CPUs) can rule a whole domain out, and skipping the
    # bind would leave that sandbox unpinned while the remaining domains still
    # give it a private slice of what it is allowed to run on.  No restriction
    # -> the full list, so the mapping is unchanged on an unrestricted host.
    allowed = _task_allowed_cpus(pid)
    usable = [d for d in domains if d & allowed] if allowed is not None else domains
    if not usable:
        usable = domains
    domain_idx = index % len(usable)
    domain = usable[domain_idx]
    report["domain_index"] = domain_idx
    report["domain_count"] = len(usable)
    report["cpulist"] = format_cpu_list(domain)

    verified, detail = bind_process_all_threads(pid, domain)
    report["applied"] = True
    report["verified"] = verified
    report["detail"] = detail
    if not verified:
        report["reason"] = detail
    return report


async def bind_sandbox_to_ccl(
    sandbox_id: str,
    *,
    index: int,
    granularity: str = "auto",
    invalidate_cache: bool = False,
) -> dict[str, object]:
    """Pin one sandbox's firecracker -- every thread -- to its L3/NUMA domain.

    Blocking syscalls (/proc scan, MMDS socket, sched_setaffinity) run under
    ``asyncio.to_thread`` so the replay event loop is never stalled.
    Best-effort: always returns a report dict, never raises.
    """

    return await asyncio.to_thread(
        _bind_sync,
        sandbox_id,
        index=index,
        granularity=granularity,
        invalidate_cache=invalidate_cache,
    )


def format_report(report: dict[str, object]) -> str:
    """Render a bind report as one log line (agent ``_event`` friendly)."""

    if report.get("applied"):
        return (
            "CCL bind sandbox={sid} vm_index={idx} domain={di}/{dc} source={src} "
            "cpulist={cpus} pid={pid} verified={ver} detail={detail}".format(
                sid=report.get("sandbox_id"),
                idx=report.get("index"),
                di=report.get("domain_index"),
                dc=report.get("domain_count"),
                src=report.get("source"),
                cpus=report.get("cpulist"),
                pid=report.get("pid", ""),
                ver=report.get("verified"),
                detail=report.get("detail", ""),
            )
        )
    return (
        "CCL bind skipped sandbox={sid} vm_index={idx} reason={reason}".format(
            sid=report.get("sandbox_id") or "(unset)",
            idx=report.get("index"),
            reason=report.get("reason", "unknown"),
        )
    )


# Back-compat shim: the earlier NUMA-node-only helper kept this name.  It now
# routes through the full CCL path with the granularity forced to ``numa``.
async def bind_sandbox_to_numa(
    sandbox_id: str,
    *,
    index: int,
    total: int = 1,
    invalidate_cache: bool = False,
) -> dict[str, object]:
    """Deprecated: use :func:`bind_sandbox_to_ccl`. Binds at NUMA granularity."""

    report = await bind_sandbox_to_ccl(
        sandbox_id,
        index=index,
        granularity="numa",
        invalidate_cache=invalidate_cache,
    )
    report["total"] = total
    report["node"] = report.get("domain_index")
    return report


def topology_summary() -> str:
    """One-line description of the detected topology (for job.log headers)."""

    try:
        domains, source = select_domains("auto")
    except Exception as exc:  # noqa: BLE001
        return f"ccl_topology=detect-failed({type(exc).__name__})"
    if not domains:
        return f"ccl_topology=none(source={source})"
    sizes = sorted(len(d) for d in domains)
    return (
        f"ccl_topology=domains:{len(domains)} source={source} "
        f"cpus_per_domain(min/max)={sizes[0]}/{sizes[-1]} "
        f"sample={format_cpu_list(domains[0])}"
    )
