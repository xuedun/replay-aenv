#!/usr/bin/env python3
"""Standalone host + cgroup monitor (no third-party dependencies).

Samples at fixed interval, writes JSONL metrics + CSV summary.

Collects:
  - Host CPU: /proc/stat aggregate or per-core range → cpu_pct, user/sys/guest cores
  - Host memory: /proc/meminfo → used/cached/buffers/dirty/writeback GB+MB
  - Cgroup CPU: cpuacct.usage (v1) or cpu.stat usage_usec (v2) → cgroup_cpu_cores
  - Cgroup memory: memory.usage_in_bytes (v1) or memory.current (v2) → usage/rss/cache GB
  - Per-process: server / ublk-daemon / firecracker count + total RSS
  - Disk IO: per-device read/write MB/s + util%

Usage:
  python3 host_cgroup_monitor.py <output_dir> [interval_s] [cgroup_name] [cgroup_version] [cpu_range]

  output_dir       Directory to write metrics.jsonl + summary.csv
  interval_s       Sampling interval in seconds (default: 1.0)
  cgroup_name      Cgroup name under /sys/fs/cgroup/ (e.g. "aenv")
  cgroup_version   "auto" (default), "v1", or "v2"
  cpu_range        CPU range for /proc/stat aggregation (e.g. "0-159")

Stop with SIGINT/SIGTERM.
"""
import csv
import glob
import json
import os
import re
import signal
import sys
import time
from datetime import datetime
from pathlib import Path

CLK_TCK = os.sysconf("SC_CLK_TCK")
PAGE_KB = os.sysconf("SC_PAGE_SIZE") // 1024
NUMA_NODES = sorted(
    int(p.rsplit("node", 1)[1])
    for p in glob.glob("/sys/devices/system/node/node[0-9]*")
)
NUMA_RE = re.compile(r"N(\d+)=(\d+)")
DISKS = ["sda", "sdb", "sdc"]

# ── Helpers ────────────────────────────────────────────────


def now_ts():
    return datetime.now().isoformat(timespec="milliseconds")


def parse_cpu_range(spec):
    if not spec:
        return None
    result = set()
    for part in spec.split(","):
        part = part.strip()
        if "-" in part:
            lo, hi = part.split("-", 1)
            result.update(range(int(lo), int(hi) + 1))
        elif part:
            result.add(int(part))
    return result if result else None


def find_pid_by_comm(comm, cmdline_match=None):
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


def find_fc_pids(parent_pid=None):
    if parent_pid:
        out = []
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
        return out
    out = []
    for path in glob.glob("/proc/[0-9]*/comm"):
        try:
            with open(path) as f:
                if f.read().strip() == "firecracker":
                    out.append(int(path.split("/")[2]))
        except (OSError, ValueError):
            pass
    return out


def read_proc_rss_kb(pid):
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS"):
                    return int(line.split()[1])
    except OSError:
        pass
    return 0


def read_proc_jiffies(pid):
    try:
        with open(f"/proc/{pid}/stat") as f:
            rest = f.read().rsplit(")", 1)[1].split()
        return int(rest[11]) + int(rest[12])
    except (OSError, IndexError, ValueError):
        return None


# ── Host CPU ───────────────────────────────────────────────


def read_host_cpu(cpu_range=None):
    with open("/proc/stat") as f:
        lines = f.readlines()

    user = nice = system = idle = guest = guest_nice = 0
    total = 0

    if cpu_range is None:
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
        for line in lines:
            parts = line.split()
            if not parts or not parts[0].startswith("cpu") or parts[0] == "cpu":
                continue
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


# ── Host memory ────────────────────────────────────────────


def read_meminfo():
    info = {}
    with open("/proc/meminfo") as f:
        for line in f:
            k, v = line.split(":")
            info[k] = int(v.split()[0])
    return info


def read_node_mem_used_gb():
    out = []
    for n in NUMA_NODES:
        try:
            info = {}
            with open(f"/sys/devices/system/node/node{n}/meminfo") as f:
                for line in f:
                    parts = line.replace(":", "").split()
                    info[parts[2]] = int(parts[3])
            used = (info["MemTotal"] - info["MemFree"]) / 2**20
            out.append(round(used, 2))
        except (OSError, KeyError, IndexError, ValueError):
            out.append(None)
    return out


# ── Cgroup ─────────────────────────────────────────────────


def resolve_cgroup_paths(cgroup_name, cgroup_version="auto"):
    if not cgroup_name:
        return None, None, None

    if cgroup_version in ("auto", "v2"):
        base = f"/sys/fs/cgroup/{cgroup_name}"
        if os.path.isfile(f"{base}/cpu.stat") and os.path.isfile(f"{base}/memory.current"):
            return base, base, "v2"

    if cgroup_version in ("auto", "v1"):
        cpu_base = "/sys/fs/cgroup/cpu,cpuacct"
        mem_base = "/sys/fs/cgroup/memory"
        candidates = [
            (f"{cpu_base}/{cgroup_name}", f"{mem_base}/{cgroup_name}"),
            (f"{cpu_base}/{cgroup_name}", mem_base),
            (cpu_base, f"{mem_base}/{cgroup_name}"),
        ]
        for cpu_path, mem_path in candidates:
            if os.path.isfile(f"{cpu_path}/cpuacct.usage") and os.path.isfile(
                f"{mem_path}/memory.usage_in_bytes"
            ):
                return cpu_path, mem_path, "v1"

    return None, None, None


def read_cgroup_cpu(cpu_path, version="v1"):
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
            "rss": "rss_bytes",
            "cache": "cache_bytes",
            "shmem": "shmem_bytes",
            "dirty": "dirty_bytes",
            "writeback": "writeback_bytes",
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


# ── Disk ───────────────────────────────────────────────────


def read_diskstats():
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


# ── Main ───────────────────────────────────────────────────


def main(out_dir, interval=1.0, cgroup_name=None, cgroup_version="auto", cpu_range_spec=None):
    os.makedirs(out_dir, exist_ok=True)
    metrics_path = os.path.join(out_dir, "metrics.jsonl")
    summary_path = os.path.join(out_dir, "summary.csv")
    stop = False

    def _signal(signum, frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _signal)
    signal.signal(signal.SIGTERM, _signal)

    cpu_range = parse_cpu_range(cpu_range_spec)
    if cpu_range is not None:
        print(f"cpu_range: {cpu_range_spec} ({len(cpu_range)} cores)")

    use_cpu_range = cpu_range if (cgroup_name and cpu_range is not None) else None

    cgroup_cpu_path = cgroup_mem_path = cgroup_ver = None
    prev_cg_usage_ns = prev_cg_user_j = prev_cg_sys_j = None
    if cgroup_name:
        cgroup_cpu_path, cgroup_mem_path, cgroup_ver = resolve_cgroup_paths(
            cgroup_name, cgroup_version
        )
        if cgroup_cpu_path and cgroup_mem_path:
            print(f"cgroup: v{cgroup_ver} cpu={cgroup_cpu_path} mem={cgroup_mem_path}")
            prev_cg_usage_ns, prev_cg_user_j, prev_cg_sys_j = read_cgroup_cpu(
                cgroup_cpu_path, cgroup_ver
            )
        else:
            print(f"cgroup: WARNING paths not found for '{cgroup_name}', skipping cgroup metrics")

    fc_parent = find_pid_by_comm("server", "release/server")
    if fc_parent:
        print(f"server pid: {fc_parent}")
    daemon_pids = {"server": fc_parent, "ublk": find_pid_by_comm("uvm-ublk-daemon")}

    prev_cpu = read_host_cpu(use_cpu_range)
    prev_host_user = prev_cpu[2]
    prev_host_nice = prev_cpu[3]
    prev_host_sys = prev_cpu[4]
    prev_host_guest = prev_cpu[5]
    prev_host_guest_nice = prev_cpu[6]
    prev_cpu = (prev_cpu[0], prev_cpu[1])
    prev_disk = read_diskstats()
    prev_daemon_jiffies = {}
    prev_fc_jiffies = {}
    prev_t = time.monotonic()

    csv_fields = [
        "ts", "interval_s", "cpu_pct", "host_user_cores", "host_sys_cores",
        "host_guest_cores", "mem_used_gb", "mem_free_gb", "mem_cached_gb",
        "mem_buffers_mb", "mem_dirty_mb", "mem_writeback_mb",
        "cgroup_cpu_cores", "cgroup_cpu_user_cores", "cgroup_cpu_sys_cores",
        "cgroup_mem_usage_gb", "cgroup_mem_rss_gb", "cgroup_mem_cache_gb",
        "cgroup_mem_shmem_gb", "cgroup_mem_dirty_mb", "cgroup_mem_writeback_mb",
        "fc_procs", "fc_rss_gb", "fc_cores",
        "server_cores", "ublk_cores",
        "ublk_devices",
    ] + [f"disk_{d}_{k}" for d in DISKS for k in ("r_mbs", "w_mbs", "util_pct", "inflight")]

    with open(metrics_path, "w", buffering=1) as mf, open(
        summary_path, "w", newline="", buffering=1
    ) as sf:
        sf_writer = csv.DictWriter(sf, fieldnames=csv_fields, extrasaction="ignore")
        sf_writer.writeheader()

        print(f"monitoring started: interval={interval}s output={out_dir}")
        print(f"  metrics: {metrics_path}")
        print(f"  summary: {summary_path}")

        while not stop:
            t0 = time.monotonic()
            dt = max(0.01, t0 - prev_t)
            prev_t = t0
            ts = now_ts()

            total, idle, host_user, host_nice, host_sys, host_guest, host_guest_nice = (
                read_host_cpu(use_cpu_range)
            )
            d_total = total - prev_cpu[0]
            d_idle = idle - prev_cpu[1]
            cpu_pct = 100.0 * (1 - d_idle / d_total) if d_total else 0.0
            d_user = max(0, host_user - prev_host_user)
            d_sys = max(0, host_sys - prev_host_sys)
            d_guest = max(0, host_guest - prev_host_guest)
            d_guest_nice = max(0, host_guest_nice - prev_host_guest_nice)
            host_user_cores = d_user / CLK_TCK / dt if CLK_TCK else 0.0
            host_sys_cores = d_sys / CLK_TCK / dt if CLK_TCK else 0.0
            host_guest_cores = d_guest / CLK_TCK / dt if CLK_TCK else 0.0
            prev_cpu = (total, idle)
            prev_host_user = host_user
            prev_host_nice = host_nice
            prev_host_sys = host_sys
            prev_host_guest = host_guest
            prev_host_guest_nice = host_guest_nice

            mi = read_meminfo()

            cgroup_metrics = {}
            if cgroup_cpu_path and cgroup_mem_path:
                cur_ns, cur_user_j, cur_sys_j = read_cgroup_cpu(
                    cgroup_cpu_path, cgroup_ver
                )
                if prev_cg_usage_ns is not None:
                    d_usage = max(0, cur_ns - prev_cg_usage_ns)
                    d_user_j = max(0, cur_user_j - prev_cg_user_j)
                    d_sys_j = max(0, cur_sys_j - prev_cg_sys_j)
                    cgroup_metrics["cgroup_cpu_cores"] = round(
                        d_usage / 1e9 / dt, 3
                    )
                    cgroup_metrics["cgroup_cpu_user_cores"] = round(
                        d_user_j / CLK_TCK / dt, 3
                    )
                    cgroup_metrics["cgroup_cpu_sys_cores"] = round(
                        d_sys_j / CLK_TCK / dt, 3
                    )
                prev_cg_usage_ns = cur_ns
                prev_cg_user_j = cur_user_j
                prev_cg_sys_j = cur_sys_j

                cmem = read_cgroup_mem(cgroup_mem_path, cgroup_ver)
                cgroup_metrics["cgroup_mem_usage_gb"] = round(
                    cmem["usage_bytes"] / 2**30, 3
                )
                cgroup_metrics["cgroup_mem_rss_gb"] = round(
                    cmem["rss_bytes"] / 2**30, 3
                )
                cgroup_metrics["cgroup_mem_cache_gb"] = round(
                    cmem["cache_bytes"] / 2**30, 3
                )
                cgroup_metrics["cgroup_mem_shmem_gb"] = round(
                    cmem["shmem_bytes"] / 2**30, 3
                )
                cgroup_metrics["cgroup_mem_dirty_mb"] = round(
                    cmem["dirty_bytes"] / 2**20, 1
                )
                cgroup_metrics["cgroup_mem_writeback_mb"] = round(
                    cmem["writeback_bytes"] / 2**20, 1
                )

            pids = find_fc_pids(fc_parent)
            fc_rss_gb = sum(read_proc_rss_kb(p) for p in pids) / 2**20
            cur_jiffies = {}
            fc_cpu_secs = 0.0
            for p in pids:
                j = read_proc_jiffies(p)
                if j is None:
                    continue
                cur_jiffies[p] = j
                if p in prev_fc_jiffies:
                    fc_cpu_secs += max(0, j - prev_fc_jiffies[p]) / CLK_TCK
            prev_fc_jiffies = cur_jiffies
            fc_cores = fc_cpu_secs / dt

            daemon_cores = {}
            for name, pid in daemon_pids.items():
                j = read_proc_jiffies(pid) if pid else None
                if j is None:
                    pid = find_pid_by_comm(
                        "server", "release/server"
                    ) if name == "server" else find_pid_by_comm("uvm-ublk-daemon")
                    daemon_pids[name] = pid
                    j = read_proc_jiffies(pid) if pid else None
                    if j is None:
                        daemon_cores[name] = 0.0
                        continue
                prev_j = prev_daemon_jiffies.get(name)
                prev_daemon_jiffies[name] = j
                daemon_cores[name] = (
                    round(max(0, j - prev_j) / CLK_TCK / dt, 3)
                    if prev_j is not None
                    else 0.0
                )

            cur_disk = read_diskstats()
            disk_out = {}
            for d in DISKS:
                cur_d = cur_disk.get(d)
                prev_d = prev_disk.get(d, {})
                if not cur_d:
                    continue
                r_mb = (cur_d["sectors_read"] - prev_d.get("sectors_read", 0)) * 512 / 2**20 / dt
                w_mb = (cur_d["sectors_written"] - prev_d.get("sectors_written", 0)) * 512 / 2**20 / dt
                util = (cur_d["ms_io"] - prev_d.get("ms_io", 0)) / 10.0 / dt
                disk_out[d] = {
                    "r_mbs": round(r_mb, 1),
                    "w_mbs": round(w_mb, 1),
                    "util_pct": round(util, 1),
                    "inflight": cur_d["inflight"],
                }
            prev_disk = cur_disk

            cpu_breakdown = {}
            if use_cpu_range is not None:
                cpu_breakdown = {
                    "host_user_cores": round(host_user_cores, 3),
                    "host_sys_cores": round(host_sys_cores, 3),
                    "host_guest_cores": round(host_guest_cores, 3),
                }

            record = {
                "ts": ts,
                "interval_s": round(dt, 2),
                "cpu_pct": round(cpu_pct, 1),
                **cpu_breakdown,
                "mem_used_gb": round(
                    (mi["MemTotal"] - mi["MemAvailable"]) / 2**20, 2
                ),
                "mem_free_gb": round(mi["MemFree"] / 2**20, 2),
                "mem_cached_gb": round(
                    (mi.get("Cached", 0) + mi.get("SReclaimable", 0)) / 2**20, 2
                ),
                "mem_buffers_mb": round(mi.get("Buffers", 0) / 1024, 1),
                "mem_dirty_mb": round(mi.get("Dirty", 0) / 1024, 1),
                "mem_writeback_mb": round(mi.get("Writeback", 0) / 1024, 1),
                "fc_procs": len(pids),
                "fc_rss_gb": round(fc_rss_gb, 2),
                "fc_cores": round(fc_cores, 2),
                "server_cores": daemon_cores.get("server", 0.0),
                "ublk_cores": daemon_cores.get("ublk", 0.0),
                "ublk_devices": count_ublk(),
                "disk": disk_out,
                **cgroup_metrics,
            }

            mf.write(json.dumps(record) + "\n")

            flat = {}
            for k, v in record.items():
                if isinstance(v, dict):
                    for dk, dv in v.items():
                        flat[f"disk_{k}_{dk}" if k == "disk" else f"{k}_{dk}"] = dv
                else:
                    flat[k] = v
            sf_writer.writerow(flattened := flat)

            elapsed = time.monotonic() - t0
            sleep_time = max(0.01, interval - elapsed)
            time.sleep(sleep_time)

    print(f"\nwrote {metrics_path}")
    print(f"wrote {summary_path}")


if __name__ == "__main__":
    _out_dir = sys.argv[1] if len(sys.argv) > 1 else "./monitor-output"
    _interval = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0
    _cgroup_name = sys.argv[3] if len(sys.argv) > 3 else None
    _cgroup_version = sys.argv[4] if len(sys.argv) > 4 else "auto"
    _cpu_range = sys.argv[5] if len(sys.argv) > 5 else None
    main(_out_dir, _interval, _cgroup_name, _cgroup_version, _cpu_range)
