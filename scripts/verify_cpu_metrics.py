#!/usr/bin/env python3
"""验证 /proc/stat per-CPU 范围采集 vs cgroup 采集的一致性。

不启动 firecracker，仅采集空闲机器上的 host CPU 范围和 cgroup 数据，
对比二者的 user/sys 是否一致（guest 应该都为 0）。

用法:
    python3 verify_cpu_metrics.py <cgroup_name> <cgroup_version> <cpu_range> [interval] [duration]
    python3 verify_cpu_metrics.py aenv v1 0-127 1.0 10
    python3 verify_cpu_metrics.py aenv v2 0-127 2.0 30
"""
import json
import os
import sys
import time
from pathlib import Path

CLK_TCK = os.sysconf("SC_CLK_TCK")


def parse_cpu_range(spec):
    result = set()
    for part in spec.split(","):
        part = part.strip()
        if "-" in part:
            lo, hi = part.split("-", 1)
            result.update(range(int(lo), int(hi) + 1))
        elif part:
            result.add(int(part))
    return result


def read_proc_stat_cpu_range(cpu_range):
    """Read /proc/stat, aggregate only CPUs in range. Returns (total, idle, user, nice, system, guest, guest_nice)."""
    with open("/proc/stat") as f:
        lines = f.readlines()
    user = nice = system = idle = guest = guest_nice = total = 0
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


def resolve_cgroup_paths(cgroup_name, cgroup_version="auto"):
    if cgroup_version in ("auto", "v2"):
        base = f"/sys/fs/cgroup/{cgroup_name}"
        if os.path.isfile(f"{base}/cpu.stat") and os.path.isfile(f"{base}/memory.current"):
            return base, base, "v2"
    if cgroup_version in ("auto", "v1"):
        cpu_base = "/sys/fs/cgroup/cpu,cpuacct"
        mem_base = "/sys/fs/cgroup/memory"
        for cpu_path, mem_path in [
            (f"{cpu_base}/{cgroup_name}", f"{mem_base}/{cgroup_name}"),
            (cpu_base, mem_base),
        ]:
            if os.path.isfile(f"{cpu_path}/cpuacct.usage") and os.path.isfile(f"{mem_path}/memory.usage_in_bytes"):
                return cpu_path, mem_path, "v1"
    return None, None, None


def read_cgroup_cpu_v1(cpu_path):
    usage_ns = 0
    user_j = 0
    sys_j = 0
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
                    user_j = int(parts[1])
                elif parts[0] == "system":
                    sys_j = int(parts[1])
    except (OSError, ValueError, IndexError):
        pass
    return usage_ns, user_j, sys_j


def read_cgroup_cpu_v2(cpu_path):
    usage_ns = 0
    user_j = 0
    sys_j = 0
    try:
        with open(f"{cpu_path}/cpu.stat") as f:
            for line in f:
                parts = line.split()
                if len(parts) == 2:
                    key, val = parts[0], int(parts[1])
                    if key == "usage_usec":
                        usage_ns = val * 1000
                    elif key == "user_usec":
                        user_j = val * 1000 // CLK_TCK
                    elif key == "system_usec":
                        sys_j = val * 1000 // CLK_TCK
    except (OSError, ValueError, IndexError):
        pass
    return usage_ns, user_j, sys_j


def read_cgroup_cpu(cpu_path, version):
    if version == "v2":
        return read_cgroup_cpu_v2(cpu_path)
    else:
        return read_cgroup_cpu_v1(cpu_path)


def main():
    if len(sys.argv) < 4:
        print(__doc__)
        return 1

    cgroup_name = sys.argv[1]
    cgroup_version = sys.argv[2]
    cpu_range_spec = sys.argv[3]
    interval = float(sys.argv[4]) if len(sys.argv) > 4 else 1.0
    duration = float(sys.argv[5]) if len(sys.argv) > 5 else 10.0

    cpu_range = parse_cpu_range(cpu_range_spec)
    print(f"cpu_range: {cpu_range_spec} ({len(cpu_range)} cores)")
    print(f"interval: {interval}s, duration: {duration}s")
    print(f"CLK_TCK: {CLK_TCK}")

    # Resolve cgroup
    cg_cpu_path, cg_mem_path, cg_ver = resolve_cgroup_paths(cgroup_name, cgroup_version)
    if not cg_cpu_path:
        print(f"cgroup not found: {cgroup_name} (version={cgroup_version})")
        return 1
    print(f"cgroup: v{cg_ver} cpu={cg_cpu_path}")

    # Verify cgroup.procs has processes
    try:
        procs_path = f"{cg_cpu_path}/cgroup.procs"
        with open(procs_path) as f:
            pids = [l.strip() for l in f if l.strip()]
        print(f"cgroup.procs: {len(pids)} process(es): {pids[:5]}")
    except OSError:
        print("cgroup.procs: not readable")

    # Initial reads
    h_total, h_idle, h_user, h_nice, h_sys, h_guest, h_gnice = read_proc_stat_cpu_range(cpu_range)
    cg_usage, cg_user, cg_sys = read_cgroup_cpu(cg_cpu_path, cg_ver)

    print(f"\n{'time':>6s}  {'host_user':>10s} {'host_sys':>10s} {'host_guest':>10s}  "
          f"{'cg_user':>10s} {'cg_sys':>10s}  "
          f"{'Δuser':>10s} {'Δsys':>10s}")
    print("-" * 90)

    samples = []
    t_start = time.monotonic()
    prev_h = (h_total, h_idle, h_user, h_nice, h_sys, h_guest, h_gnice)
    prev_cg = (cg_usage, cg_user, cg_sys)

    while time.monotonic() - t_start < duration:
        time.sleep(interval)
        now = time.monotonic()
        dt = now - (t_start + len(samples) * interval)
        dt = max(0.01, dt)

        # Read host per-CPU range
        h_total, h_idle, h_user, h_nice, h_sys, h_guest, h_gnice = read_proc_stat_cpu_range(cpu_range)
        d_h_user = max(0, h_user - prev_h[2]) / CLK_TCK / dt
        d_h_sys = max(0, h_sys - prev_h[4]) / CLK_TCK / dt
        d_h_guest = max(0, h_guest - prev_h[5]) / CLK_TCK / dt
        d_h_gnice = max(0, h_gnice - prev_h[6]) / CLK_TCK / dt
        prev_h = (h_total, h_idle, h_user, h_nice, h_sys, h_guest, h_gnice)

        # Read cgroup
        cg_usage, cg_user, cg_sys = read_cgroup_cpu(cg_cpu_path, cg_ver)
        d_cg_user = max(0, cg_user - prev_cg[1]) / CLK_TCK / dt
        d_cg_sys = max(0, cg_sys - prev_cg[2]) / CLK_TCK / dt
        prev_cg = (cg_usage, cg_user, cg_sys)

        delta_user = d_h_user - d_cg_user
        delta_sys = d_h_sys - d_cg_sys

        t_elapsed = now - t_start
        print(f"{t_elapsed:6.1f}  {d_h_user:10.3f} {d_h_sys:10.3f} {d_h_guest:10.3f}  "
              f"{d_cg_user:10.3f} {d_cg_sys:10.3f}  "
              f"{delta_user:10.3f} {delta_sys:10.3f}")

        samples.append({
            "t": round(t_elapsed, 1),
            "host_user": round(d_h_user, 3),
            "host_sys": round(d_h_sys, 3),
            "host_guest": round(d_h_guest, 3),
            "host_guest_nice": round(d_h_gnice, 3),
            "cg_user": round(d_cg_user, 3),
            "cg_sys": round(d_cg_sys, 3),
            "delta_user": round(delta_user, 3),
            "delta_sys": round(delta_sys, 3),
        })

    # Summary
    print("-" * 90)
    if samples:
        avg_h_user = sum(s["host_user"] for s in samples) / len(samples)
        avg_h_sys = sum(s["host_sys"] for s in samples) / len(samples)
        avg_cg_user = sum(s["cg_user"] for s in samples) / len(samples)
        avg_cg_sys = sum(s["cg_sys"] for s in samples) / len(samples)
        avg_d_user = sum(s["delta_user"] for s in samples) / len(samples)
        avg_d_sys = sum(s["delta_sys"] for s in samples) / len(samples)
        print(f"avg       {avg_h_user:10.3f} {avg_h_sys:10.3f} {'':10s}  "
              f"{avg_cg_user:10.3f} {avg_cg_sys:10.3f}  "
              f"{avg_d_user:10.3f} {avg_d_sys:10.3f}")
        print()
        print(f"Δuser (host - cgroup) = {avg_d_user:.3f} cores  ← 系统进程用户态开销")
        print(f"Δsys  (host - cgroup) = {avg_d_sys:.3f} cores  ← 系统进程内核态开销")
        print(f"如果 Δ 接近 0: 两者一致，cgroup 可替代 host 范围采集")
        print(f"如果 Δ > 0:   host 含 cgroup 外的系统进程，cgroup 更精确")

    # Save raw data
    out_file = Path(f"/tmp/cpu_verify_{cgroup_name}_{int(time.time())}.json")
    with open(out_file, "w") as f:
        json.dump({
            "cgroup_name": cgroup_name,
            "cgroup_version": cg_ver,
            "cpu_range": cpu_range_spec,
            "clk_tck": CLK_TCK,
            "samples": samples,
        }, f, indent=2)
    print(f"\nraw data: {out_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
