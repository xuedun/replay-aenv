"""1Hz observability monitor for AgentEnv benchmark runs (v3).

Samples every second (JSONL, one line per second):
  ts, host cpu%, per-daemon CPU cores (firecracker / server / uvm-ublk-daemon),
  mem used/free/cached/buffers/dirty/writeback,
  firecracker count / total RSS GB / per-NUMA-node MB,
  per-NUMA-node system MemUsed GB,
  ublk device count, sandbox running/paused counts,
  per-disk read/write MB/s, io util%, inflight ios

And logs sandbox lifecycle events (JSONL):
  ts, event (created|paused|resumed|killed), sandbox_id, task_id, state,
  template, api_started_at
  paused events additionally carry per-pause snapshot sizes of the CURRENT
  generation: snapshot_bytes (logical, inherited layers included) /
  snapshot_disk_bytes (physical blocks UNIQUE to this generation = real
  marginal disk cost of this pause) / snapshot_inherited_bytes (logical
  bytes hardlinked from older generations) / snapshot_files / snapshot_dir
  (uuid name) — plus snapshot_tree_disk_bytes (inode-deduped whole tree,
  == du) and snapshot_generations. Overlaybd snapshots are incremental:
  resume leaves generation dirs on disk and each new generation hardlinks
  the previous commits as inherited-layers/, so generations accumulate and
  naive size sums double-count; only kill (or server-restart orphan cleanup)
  removes the tree. pause_seq is a 1-based count of pauses for that sandbox
  this run, so per-pause size growth can be plotted directly.

Usage: python3 spotbox_monitor.py <out_dir> [sample_interval_s]
  sample_interval_s: default 1.0. Use e.g. 0.1 for single-task runs where
  pause/resume windows are sub-second. Sandbox list polling stays at >=1s
  cadence regardless (it is the expensive call); only host /proc + disk
  metrics are sampled at the high frequency.
Env: needs E2B_API_KEY / E2B_API_URL (see repo README §4).
Stop with SIGINT/SIGTERM.
"""

import asyncio
import glob
import json
import os
import re
import signal
import sys
from datetime import datetime

from e2b import AsyncSandbox

CLK_TCK = os.sysconf("SC_CLK_TCK")
PAGE_KB = os.sysconf("SC_PAGE_SIZE") // 1024
DISKS = ["sda", "sdb", "sdc"]
SNAP_BASE = "/var/lib/aenv/persisted-sandboxes/artifacts"
UBLK_DAEMON_COMM = "uvm-ublk-daemon"
NUMA_NODES = sorted(
    int(p.rsplit("node", 1)[1])
    for p in glob.glob("/sys/devices/system/node/node[0-9]*")
)
NUMA_RE = re.compile(r"N(\d+)=(\d+)")


def now_ts():
    return datetime.now().isoformat(timespec="milliseconds")


def find_pid_by_comm(comm, cmdline_match=None):
    """First pid whose /proc/<pid>/comm equals comm (optionally cmdline-filtered)."""
    for path in glob.glob("/proc/[0-9]*/comm"):
        try:
            with open(path) as f:
                if f.read().strip() != comm:
                    continue
            pid = int(path.split("/")[2])
            if cmdline_match:
                with open(f"/proc/{pid}/cmdline") as f:
                    if cmdline_match not in f.read():
                        continue
            return pid
        except (OSError, ValueError):
            pass
    return None


def snapshot_dir_stats(sandbox_id):
    """Snapshot sizes for one paused sandbox under persisted artifacts.

    Overlaybd snapshots are INCREMENTAL across a sandbox's pause/resume
    cycles: resume deletes only the RocksDB record and leaves generation
    dirs on disk (kill or server-restart orphan cleanup removes the whole
    <sid> tree), and each new generation hardlinks the previous memory/rootfs
    commits as inherited-layers/ (verified empirically: same st_dev/st_ino).
    So naive st_size/st_blocks sums double-count inherited layers.

    Generations are walked in uuid-v7 name order (= creation order) and each
    inode is attributed to the FIRST generation containing it; inheritance
    only ever points backwards, so a generation's unique blocks are exactly
    the physical disk cost of that pause.

    Returns (gen_apparent, gen_unique_disk, gen_files, gen_name,
             tree_unique_disk, n_gens, gen_inherited_apparent) or None:
      gen_apparent           logical bytes of the current (newest) generation,
                             inherited layers included
      gen_unique_disk        physical blocks unique to the current generation
                             (real marginal disk cost of this pause)
      tree_unique_disk       inode-deduped whole-tree blocks (== du)
      gen_inherited_apparent logical bytes the current gen inherits from
                             older generations
    """
    base = os.path.join(SNAP_BASE, sandbox_id)
    try:
        gens = sorted(
            e.name for e in os.scandir(base) if e.is_dir(follow_symlinks=False)
        )
    except OSError:
        return None
    if not gens:
        return None

    seen = set()
    per_gen = []  # (name, apparent, unique_disk, n_files, inherited_apparent)
    for g in gens:
        apparent = unique_disk = n_files = inherited = 0
        for root, _dirs, files in os.walk(os.path.join(base, g)):
            for name in files:
                try:
                    st = os.lstat(os.path.join(root, name))
                except OSError:
                    continue
                apparent += st.st_size
                n_files += 1
                key = (st.st_dev, st.st_ino)
                if key in seen:
                    inherited += st.st_size
                    continue
                seen.add(key)
                unique_disk += st.st_blocks * 512
        per_gen.append((g, apparent, unique_disk, n_files, inherited))

    name, apparent, unique_disk, n_files, inherited = per_gen[-1]
    tree_disk = sum(p[2] for p in per_gen)
    return apparent, unique_disk, n_files, name, tree_disk, len(gens), inherited


def parse_cpu_range(spec):
    """Parse CPU range string like '0-127' or '0,1,5,10-20' into a set of int."""
    result = set()
    for part in spec.split(","):
        part = part.strip()
        if "-" in part:
            lo, hi = part.split("-", 1)
            result.update(range(int(lo), int(hi) + 1))
        elif part:
            result.add(int(part))
    return result


def read_host_cpu(cpu_range=None):
    """Read /proc/stat. If cpu_range (set of int) is given, aggregate per-CPU
    lines for those cores only; otherwise read the aggregate 'cpu' line.

    Returns (total, idle, user, nice, system, guest, guest_nice) all in jiffies.
    """
    with open("/proc/stat") as f:
        lines = f.readlines()

    user = nice = system = idle = guest = guest_nice = 0
    total = 0

    if cpu_range is None:
        # Aggregate "cpu" line
        for line in lines:
            parts = line.split()
            if parts and parts[0] == "cpu":
                vals = list(map(int, parts[1:]))
                break
        else:
            return 0, 0, 0, 0, 0, 0, 0
        user = vals[0]
        nice = vals[1]
        system = vals[2]
        idle = vals[3] + vals[4]
        guest = vals[8] if len(vals) > 8 else 0
        guest_nice = vals[9] if len(vals) > 9 else 0
        total = sum(vals[:8])
    else:
        # Per-CPU lines for specified range
        for line in lines:
            parts = line.split()
            if not parts or not parts[0].startswith("cpu"):
                continue
            if parts[0] == "cpu":
                continue  # skip aggregate
            try:
                cpu_id = int(parts[0][3:])
            except ValueError:
                continue
            if cpu_id not in cpu_range:
                continue
            vals = list(map(int, parts[1:]))
            user += vals[0]
            nice += vals[1]
            system += vals[2]
            idle += vals[3] + vals[4]
            guest += vals[8] if len(vals) > 8 else 0
            guest_nice += vals[9] if len(vals) > 9 else 0
            total += sum(vals[:8])

    return total, idle, user, nice, system, guest, guest_nice


def read_meminfo():
    info = {}
    with open("/proc/meminfo") as f:
        for line in f:
            k, v = line.split(":")
            info[k] = int(v.split()[0])  # kB
    return info


def read_node_mem_used_gb():
    out = []
    for n in NUMA_NODES:
        used = None
        try:
            info = {}
            with open(f"/sys/devices/system/node/node{n}/meminfo") as f:
                for line in f:
                    parts = line.replace(":", "").split()
                    info[parts[2]] = int(parts[3])  # "Node 0 MemTotal:  x kB"
            used = (info["MemTotal"] - info["MemFree"]) / 2**20
        except (OSError, KeyError, IndexError, ValueError):
            pass
        out.append(round(used, 2) if used is not None else None)
    return out


def read_fc_pids():
    pids = []
    for path in glob.glob("/proc/[0-9]*/comm"):
        try:
            with open(path) as f:
                if f.read().strip() == "firecracker":
                    pids.append(int(path.split("/")[2]))
        except (OSError, ValueError):
            pass
    return pids


def find_fc_parent_pid():
    """Pid of the AgentEnv server (one-time full /proc scan at startup).

    Firecracker VMs are spawned as children of server threads, so afterwards
    the per-sample pid list comes from /proc/<server>/task/*/children (~1ms)
    instead of rescanning every /proc comm (~140ms with thousands of procs).
    """
    for path in glob.glob("/proc/[0-9]*/comm"):
        try:
            with open(path) as f:
                if f.read().strip() != "server":
                    continue
            pid = int(path.split("/")[2])
            with open(f"/proc/{pid}/cmdline") as f:
                if "release/server" in f.read():
                    return pid
        except (OSError, ValueError):
            pass
    return None


def read_fc_pids_fast(parent_pid):
    out = []
    try:
        for path in glob.glob(f"/proc/{parent_pid}/task/*/children"):
            try:
                with open(path) as f:
                    for tok in f.read().split():
                        try:
                            with open(f"/proc/{tok}/comm") as cf:
                                if cf.read().strip() == "firecracker":
                                    out.append(int(tok))
                        except OSError:
                            pass
            except OSError:
                pass
    except OSError:
        pass
    return out


def read_fc_rss_kb(pid):
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS"):
                    return int(line.split()[1])
    except OSError:
        pass
    return 0


def read_fc_jiffies(pid):
    try:
        with open(f"/proc/{pid}/stat") as f:
            rest = f.read().rsplit(")", 1)[1].split()
        return int(rest[11]) + int(rest[12])  # utime + stime
    except (OSError, IndexError, ValueError):
        return None


def read_fc_numa_mb(pid):
    """Per-NUMA-node mapped pages for one process (what numastat -p shows)."""
    per_node = {}
    try:
        with open(f"/proc/{pid}/numa_maps") as f:
            for line in f:
                for m in NUMA_RE.finditer(line):
                    node, pages = int(m.group(1)), int(m.group(2))
                    per_node[node] = per_node.get(node, 0) + pages
    except OSError:
        pass
    return {n: per_node.get(n, 0) * PAGE_KB / 1024 for n in NUMA_NODES}


def resolve_cgroup_paths(cgroup_name, cgroup_version="auto"):
    """Resolve cgroup paths for CPU and memory controllers.

    Supports cgroup v1 and v2. When version is "auto", detects by checking
    which files exist.
    Returns (cpu_path, mem_path, version) or (None, None, None) if not found.
    """
    if cgroup_version in ("auto", "v2"):
        # cgroup v2: unified hierarchy
        base = f"/sys/fs/cgroup/{cgroup_name}"
        cpu_ok = os.path.isfile(f"{base}/cpu.stat")
        mem_ok = os.path.isfile(f"{base}/memory.current")
        if cpu_ok and mem_ok:
            return base, base, "v2"
        # Also try without subdirectory (cgroup at root)
        base = "/sys/fs/cgroup"
        if cgroup_name:
            base = f"/sys/fs/cgroup/{cgroup_name}"
        if os.path.isfile(f"{base}/cpu.stat") and os.path.isfile(f"{base}/memory.current"):
            return base, base, "v2"

    if cgroup_version in ("auto", "v1"):
        # cgroup v1: separate hierarchies
        cpu_base = "/sys/fs/cgroup/cpu,cpuacct"
        mem_base = "/sys/fs/cgroup/memory"
        candidates = [
            (f"{cpu_base}/{cgroup_name}", f"{mem_base}/{cgroup_name}"),
            (f"{cpu_base}/{cgroup_name}", mem_base),
            (cpu_base, f"{mem_base}/{cgroup_name}"),
        ]
        for cpu_path, mem_path in candidates:
            if os.path.isfile(f"{cpu_path}/cpuacct.usage") and os.path.isfile(f"{mem_path}/memory.usage_in_bytes"):
                return cpu_path, mem_path, "v1"

    return None, None, None


def read_cgroup_cpu(cpu_path, version="v1"):
    """Read cgroup CPU accounting: (usage_ns, user_jiffies, system_jiffies).

    v1: cpuacct.usage (ns) + cpuacct.stat (jiffies)
    v2: cpu.stat (usage_usec, user_usec, system_usec → convert to ns/jiffies)
    """
    usage_ns = 0
    user_jiffies = 0
    system_jiffies = 0

    if version == "v2":
        try:
            with open(f"{cpu_path}/cpu.stat") as f:
                for line in f:
                    parts = line.split()
                    if len(parts) == 2:
                        key, val = parts[0], int(parts[1])
                        if key == "usage_usec":
                            usage_ns = val * 1000
                        elif key == "user_usec":
                            user_jiffies = val * 1000 // CLK_TCK
                        elif key == "system_usec":
                            system_jiffies = val * 1000 // CLK_TCK
        except (OSError, ValueError, IndexError):
            pass
    else:
        try:
            with open(f"{cpu_path}/cpuacct.usage") as f:
                usage_ns = int(f.read().strip())
        except (OSError, ValueError):
            pass
        try:
            with open(f"{cpu_path}/cpuacct.stat") as f:
                for line in f:
                    parts = line.split()
                    if parts[0] == "user":
                        user_jiffies = int(parts[1])
                    elif parts[0] == "system":
                        system_jiffies = int(parts[1])
        except (OSError, ValueError, IndexError):
            pass

    return usage_ns, user_jiffies, system_jiffies


def read_cgroup_mem(mem_path, version="v1"):
    """Read cgroup memory stats: returns dict with usage/rss/cache/shmem/dirty.

    v1: memory.usage_in_bytes + memory.stat (rss/cache/shmem/dirty)
    v2: memory.current + memory.stat (anon/file/shmem/file_dirty)
    """
    out = {
        "usage_bytes": 0,
        "rss_bytes": 0,
        "cache_bytes": 0,
        "shmem_bytes": 0,
        "dirty_bytes": 0,
        "writeback_bytes": 0,
    }

    if version == "v2":
        try:
            with open(f"{mem_path}/memory.current") as f:
                out["usage_bytes"] = int(f.read().strip())
        except (OSError, ValueError):
            pass
        # v2 stat key mapping (anon=RSS, file=cache, file_dirty=dirty)
        stat_keys = {
            "anon": "rss_bytes",
            "file": "cache_bytes",
            "shmem": "shmem_bytes",
            "file_dirty": "dirty_bytes",
            "file_writeback": "writeback_bytes",
        }
        try:
            with open(f"{mem_path}/memory.stat") as f:
                for line in f:
                    parts = line.split()
                    if len(parts) == 2 and parts[0] in stat_keys:
                        out[stat_keys[parts[0]]] = int(parts[1])
        except (OSError, ValueError):
            pass
    else:
        try:
            with open(f"{mem_path}/memory.usage_in_bytes") as f:
                out["usage_bytes"] = int(f.read().strip())
        except (OSError, ValueError):
            pass
        stat_keys = {
            "total_rss": "rss_bytes",
            "total_cache": "cache_bytes",
            "total_shmem": "shmem_bytes",
            "total_dirty": "dirty_bytes",
            "total_writeback": "writeback_bytes",
        }
        try:
            with open(f"{mem_path}/memory.stat") as f:
                for line in f:
                    parts = line.split()
                    if len(parts) == 2 and parts[0] in stat_keys:
                        out[stat_keys[parts[0]]] = int(parts[1])
        except (OSError, ValueError):
            pass

    return out


def read_diskstats():
    # per-device /sys/block/<d>/stat (~2us each) instead of /proc/diskstats,
    # which costs ~200ms on hosts with thousands of loop/nbd/ublk devices
    stats = {}
    for d in DISKS:
        try:
            with open(f"/sys/block/{d}/stat") as f:
                v = list(map(int, f.read().split()))
        except OSError:
            continue
        stats[d] = {
            "sectors_read": v[2],
            "sectors_written": v[6],
            "inflight": v[8],
            "ms_io": v[9],
        }
    return stats


def count_ublk():
    return len(glob.glob("/dev/ublkb*"))


async def list_sandboxes():
    items = []
    paginator = AsyncSandbox.list()
    while True:
        page = await paginator.next_items()
        items.extend(page)
        if not paginator.has_next:
            break
    return items


async def main(out_dir, sample_interval=1.0, cgroup_name=None, cgroup_version="auto", cpu_range_spec=None):
    os.makedirs(out_dir, exist_ok=True)
    metrics_path = os.path.join(out_dir, "metrics_1s.jsonl")
    events_path = os.path.join(out_dir, "sandbox_events.jsonl")
    stop = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    # Parse CPU range for per-core aggregation (e.g. "0-127" → {0,1,...,127})
    # In cgroup mode, cpu_range scopes /proc/stat reads to the cgroup's bound CPUs.
    cpu_range = parse_cpu_range(cpu_range_spec) if cpu_range_spec else None
    if cpu_range is not None:
        print(f"cpu_range: {cpu_range_spec} ({len(cpu_range)} cores)")

    # Determine whether to use cpu_range for host CPU reads.
    # In cgroup mode with cpu_range: read only specified cores (scoped accuracy, O(1) file read).
    # In non-cgroup mode or without cpu_range: read aggregate /proc/stat line (system-wide).
    use_cpu_range = cpu_range if (cgroup_name and cpu_range is not None) else None

    # Resolve cgroup paths (v1 or v2; user pre-creates the cgroup)
    cgroup_cpu_path = None
    cgroup_mem_path = None
    cgroup_ver = None
    prev_cgroup_usage_ns = None
    prev_cgroup_user_jiffies = None
    prev_cgroup_sys_jiffies = None
    if cgroup_name:
        cgroup_cpu_path, cgroup_mem_path, cgroup_ver = resolve_cgroup_paths(cgroup_name, cgroup_version)
        if cgroup_cpu_path and cgroup_mem_path:
            print(f"cgroup: v{cgroup_ver} cpu={cgroup_cpu_path} memory={cgroup_mem_path}")
            prev_cgroup_usage_ns, prev_cgroup_user_jiffies, prev_cgroup_sys_jiffies = (
                read_cgroup_cpu(cgroup_cpu_path, cgroup_ver)
            )
        else:
            print(f"cgroup: WARNING paths not found for '{cgroup_name}' (version={cgroup_version}), skipping cgroup metrics")

    prev_cpu = read_host_cpu(use_cpu_range)
    prev_host_guest = prev_cpu[5]
    prev_host_user = prev_cpu[2]
    prev_host_nice = prev_cpu[3]
    prev_host_sys = prev_cpu[4]
    prev_host_guest_nice = prev_cpu[6]
    prev_cpu = (prev_cpu[0], prev_cpu[1])
    prev_disk = read_diskstats()
    prev_daemon_jiffies = {}
    prev_fc_jiffies = {}
    pause_counts = {}
    prev_t = loop.time()
    known = {}
    n_running = n_paused = 0
    last_poll = -1e9  # force sandbox list on first iteration
    poll_cadence = max(1.0, sample_interval)
    fc_parent = find_fc_parent_pid()
    if fc_parent:
        print(f"fc pid source: /proc/{fc_parent}/task/*/children")
    daemon_pids = {"server": fc_parent, "ublk": find_pid_by_comm(UBLK_DAEMON_COMM)}
    print(f"daemon pids: {daemon_pids}")

    with open(metrics_path, "w", buffering=1) as mf, open(
        events_path, "w", buffering=1
    ) as ef:
        ef.write(json.dumps({"ts": now_ts(), "event": "monitor_start"}) + "\n")
        while not stop.is_set():
            t0 = loop.time()
            interval = max(0.01, t0 - prev_t)
            prev_t = t0
            ts = now_ts()

            # Host-level CPU (always collect, cheap: single /proc/stat read)
            # In cgroup mode with cpu_range: aggregate only bound cores.
            # Otherwise: system-wide aggregate.
            total, idle, host_user, host_nice, host_sys, host_guest, host_guest_nice = read_host_cpu(use_cpu_range)
            d_total = total - prev_cpu[0]
            d_idle = idle - prev_cpu[1]
            cpu_pct = 100.0 * (1 - d_idle / d_total) if d_total else 0.0
            # Per-category deltas (all in jiffies)
            d_user = max(0, host_user - prev_host_user)
            d_nice = max(0, host_nice - prev_host_nice)
            d_sys = max(0, host_sys - prev_host_sys)
            d_guest = max(0, host_guest - prev_host_guest)
            d_guest_nice = max(0, host_guest_nice - prev_host_guest_nice)
            # Convert to cores
            host_user_cores = d_user / CLK_TCK / interval if CLK_TCK else 0.0
            host_sys_cores = d_sys / CLK_TCK / interval if CLK_TCK else 0.0
            host_guest_cores = d_guest / CLK_TCK / interval if CLK_TCK else 0.0
            host_guest_nice_cores = d_guest_nice / CLK_TCK / interval if CLK_TCK else 0.0
            prev_cpu = (total, idle)
            prev_host_user = host_user
            prev_host_nice = host_nice
            prev_host_sys = host_sys
            prev_host_guest = host_guest
            prev_host_guest_nice = host_guest_nice

            mi = read_meminfo()

            # In cgroup mode, skip expensive per-FC-pid reads (numa_maps, status,
            # stat jiffies). The cgroup already aggregates CPU and memory for all
            # FC + server + ublk processes. This keeps each iteration O(1)
            # regardless of VM count, giving consistent ~1s sampling intervals.
            if cgroup_cpu_path and cgroup_mem_path:
                pids = []
                fc_rss_gb = 0.0
                fc_numa_mb = [0.0] * len(NUMA_NODES)
                fc_cores = 0.0
                daemon_cores = {"server": 0.0, "ublk": 0.0}
            else:
                pids = read_fc_pids_fast(fc_parent) if fc_parent else read_fc_pids()
                fc_rss_gb = sum(read_fc_rss_kb(p) for p in pids) / 2**20
                fc_numa_mb = [0.0] * len(NUMA_NODES)
                for p in pids:
                    per = read_fc_numa_mb(p)
                    for i, n in enumerate(NUMA_NODES):
                        fc_numa_mb[i] += per.get(n, 0.0)
                fc_numa_mb = [round(x, 1) for x in fc_numa_mb]

                cur_jiffies = {}
                fc_cpu_secs = 0.0
                for p in pids:
                    j = read_fc_jiffies(p)
                    if j is None:
                        continue
                    cur_jiffies[p] = j
                    if p in prev_fc_jiffies:
                        fc_cpu_secs += max(0, j - prev_fc_jiffies[p]) / CLK_TCK
                prev_fc_jiffies = cur_jiffies
                fc_cores = fc_cpu_secs / interval

                daemon_cores = {}
                for name, pid in daemon_pids.items():
                    j = read_fc_jiffies(pid) if pid else None
                    if j is None:
                        pid = find_pid_by_comm(
                            "server", "release/server"
                        ) if name == "server" else find_pid_by_comm(UBLK_DAEMON_COMM)
                        daemon_pids[name] = pid
                        j = read_fc_jiffies(pid) if pid else None
                        if j is None:
                            daemon_cores[name] = 0.0
                            continue
                    prev_j = prev_daemon_jiffies.get(name)
                    prev_daemon_jiffies[name] = j
                    daemon_cores[name] = (
                        round(max(0, j - prev_j) / CLK_TCK / interval, 3)
                        if prev_j is not None
                        else 0.0
                    )

            disk_out = {}
            cur_disk = read_diskstats()
            for d in DISKS:
                cur_d = cur_disk.get(d)
                prev_d = prev_disk.get(d, {})
                if not cur_d:
                    continue
                r_mb = (cur_d["sectors_read"] - prev_d.get("sectors_read", 0)) * 512 / 2**20 / interval
                w_mb = (cur_d["sectors_written"] - prev_d.get("sectors_written", 0)) * 512 / 2**20 / interval
                util = (cur_d["ms_io"] - prev_d.get("ms_io", 0)) / 10.0 / interval
                disk_out[d] = {
                    "r_mb_s": round(r_mb, 1),
                    "w_mb_s": round(w_mb, 1),
                    "util_pct": round(util, 1),
                    "inflight": cur_d["inflight"],
                }
            prev_disk = cur_disk

            # Cgroup CPU and memory sampling (optional, when --cgroup-name is set)
            cgroup_metrics = {}
            if cgroup_cpu_path and cgroup_mem_path:
                cur_usage_ns, cur_user_j, cur_sys_j = read_cgroup_cpu(cgroup_cpu_path, cgroup_ver)
                if prev_cgroup_usage_ns is not None:
                    d_usage = max(0, cur_usage_ns - prev_cgroup_usage_ns)
                    d_user_j = max(0, cur_user_j - prev_cgroup_user_jiffies)
                    d_sys_j = max(0, cur_sys_j - prev_cgroup_sys_jiffies)
                    cgroup_metrics["cgroup_cpu_cores"] = round(d_usage / 1e9 / interval, 3)
                    cgroup_metrics["cgroup_cpu_user_cores"] = round(d_user_j / CLK_TCK / interval, 3)
                    cgroup_metrics["cgroup_cpu_sys_cores"] = round(d_sys_j / CLK_TCK / interval, 3)
                prev_cgroup_usage_ns = cur_usage_ns
                prev_cgroup_user_jiffies = cur_user_j
                prev_cgroup_sys_jiffies = cur_sys_j

                cmem = read_cgroup_mem(cgroup_mem_path, cgroup_ver)
                cgroup_metrics["cgroup_mem_usage_gb"] = round(cmem["usage_bytes"] / 2**30, 3)
                cgroup_metrics["cgroup_mem_rss_gb"] = round(cmem["rss_bytes"] / 2**30, 3)
                cgroup_metrics["cgroup_mem_cache_gb"] = round(cmem["cache_bytes"] / 2**30, 3)
                cgroup_metrics["cgroup_mem_shmem_gb"] = round(cmem["shmem_bytes"] / 2**30, 3)
                cgroup_metrics["cgroup_mem_dirty_mb"] = round(cmem["dirty_bytes"] / 2**20, 1)
                cgroup_metrics["cgroup_mem_writeback_mb"] = round(cmem["writeback_bytes"] / 2**20, 1)

            if t0 - last_poll >= poll_cadence:
                last_poll = t0
                sandboxes = []
                try:
                    sandboxes = await list_sandboxes()
                except Exception as e:
                    ef.write(
                        json.dumps({"ts": ts, "event": "poll_error", "error": str(e)[:200]})
                        + "\n"
                    )

                cur = {}
                n_running = n_paused = 0
                for sb in sandboxes:
                    state = str(sb.state).split(".")[-1].lower()
                    task_id = (sb.metadata or {}).get("session_id", "")
                    cur[sb.sandbox_id] = {
                        "state": state,
                        "task_id": task_id,
                        "template": sb.name or sb.template_id,
                        "api_started_at": str(sb.started_at or ""),
                    }
                    if state == "running":
                        n_running += 1
                    elif state == "paused":
                        n_paused += 1

                for sid, info in cur.items():
                    if sid not in known:
                        ef.write(
                            json.dumps(
                                {
                                    "ts": ts,
                                    "event": "created",
                                    "sandbox_id": sid,
                                    "task_id": info["task_id"],
                                    "state": info["state"],
                                    "template": info["template"],
                                    "api_started_at": info["api_started_at"],
                                }
                            )
                            + "\n"
                        )
                    elif known[sid]["state"] != info["state"]:
                        ev = {"running": "resumed", "paused": "paused"}.get(
                            info["state"], f"state:{info['state']}"
                        )
                        rec = {
                            "ts": ts,
                            "event": ev,
                            "sandbox_id": sid,
                            "task_id": info["task_id"],
                            "state": info["state"],
                        }
                        if ev == "paused":
                            pause_counts[sid] = pause_counts.get(sid, 0) + 1
                            rec["pause_seq"] = pause_counts[sid]
                            snap = snapshot_dir_stats(sid)
                            if snap:
                                rec["snapshot_bytes"] = snap[0]
                                rec["snapshot_disk_bytes"] = snap[1]
                                rec["snapshot_files"] = snap[2]
                                rec["snapshot_dir"] = snap[3]
                                rec["snapshot_tree_disk_bytes"] = snap[4]
                                rec["snapshot_generations"] = snap[5]
                                rec["snapshot_inherited_bytes"] = snap[6]
                        ef.write(json.dumps(rec) + "\n")
                for sid, info in known.items():
                    if sid not in cur:
                        ef.write(
                            json.dumps(
                                {
                                    "ts": ts,
                                    "event": "killed",
                                    "sandbox_id": sid,
                                    "task_id": info["task_id"],
                                    "state": "gone",
                                }
                            )
                            + "\n"
                        )
                known = cur

            # Build per-category CPU metrics (only in cgroup mode with cpu_range)
            cpu_breakdown = {}
            if use_cpu_range is not None:
                cpu_breakdown = {
                    "host_user_cores": round(host_user_cores, 3),
                    "host_sys_cores": round(host_sys_cores, 3),
                    "host_guest_cores": round(host_guest_cores, 3),
                    "host_guest_nice_cores": round(host_guest_nice_cores, 3),
                }

            mf.write(
                json.dumps(
                    {
                        "ts": ts,
                        "interval_s": round(interval, 2),
                        "cpu_pct": round(cpu_pct, 1),
                        **cpu_breakdown,
                        "mem_used_gb": round((mi["MemTotal"] - mi["MemAvailable"]) / 2**20, 2),
                        "mem_free_gb": round(mi["MemFree"] / 2**20, 2),
                        "mem_cached_gb": round(
                            (mi.get("Cached", 0) + mi.get("SReclaimable", 0)) / 2**20, 2
                        ),
                        "mem_buffers_mb": round(mi.get("Buffers", 0) / 1024, 1),
                        "mem_dirty_mb": round(mi.get("Dirty", 0) / 1024, 1),
                        "mem_writeback_mb": round(mi.get("Writeback", 0) / 1024, 1),
                        "node_mem_used_gb": read_node_mem_used_gb(),
                        "fc_procs": len(pids),
                        "fc_rss_gb": round(fc_rss_gb, 2),
                        "fc_cores": round(fc_cores, 2),
                        "server_cores": daemon_cores.get("server", 0.0),
                        "ublk_cores": daemon_cores.get("ublk", 0.0),
                        "fc_numa_mb": fc_numa_mb,
                        "ublk_devices": count_ublk(),
                        "sandboxes_running": n_running,
                        "sandboxes_paused": n_paused,
                        "disk": disk_out,
                        **cgroup_metrics,
                    }
                )
                + "\n"
            )

            elapsed = loop.time() - t0
            try:
                await asyncio.wait_for(stop.wait(), timeout=max(0.01, sample_interval - elapsed))
            except asyncio.TimeoutError:
                pass

        ef.write(json.dumps({"ts": now_ts(), "event": "monitor_stop"}) + "\n")

    print(f"wrote {metrics_path} and {events_path}")


if __name__ == "__main__":
    _cgroup_name = sys.argv[3] if len(sys.argv) > 3 else None
    _cgroup_version = sys.argv[4] if len(sys.argv) > 4 else "auto"
    _cpu_range_spec = sys.argv[5] if len(sys.argv) > 5 else None
    asyncio.run(main(sys.argv[1], float(sys.argv[2]) if len(sys.argv) > 2 else 1.0,
                     cgroup_name=_cgroup_name, cgroup_version=_cgroup_version,
                     cpu_range_spec=_cpu_range_spec))
