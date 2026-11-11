#!/bin/bash
# AgentENV startup with cgroup containment + cpuset CPU binding.
# The script shell joins /sys/fs/cgroup/aenv FIRST; every later child
# (make -> aenv-server -> uvm-ublk-daemon -> firecracker VMs) inherits it,
# so spotbox_monitor.py --cgroup-name aenv aggregates the whole fleet.
#
# CPU binding: the whole fleet (server + daemon + all VMs) is restricted to
# AENV_CPUS / AENV_MEMS via cpuset. Defaults bind ALL online CPUs (no
# functional change); override to emulate a smaller host, e.g.:
#   AENV_CPUS=0-191 ./start_aenv_cgroup.sh        # half the cores
# ccl_bind domains are intersected with the task's allowed CPUs, so VM
# placements automatically stay inside the bound range.

echo 0 > /sys/module/kvm/parameters/vm_uevent_notify

# ── cgroup: create + bind + join (v2 and v1 both handled) ───────────────
AENV_CGROUP_NAME=aenv
# Fleet CPU/NUMA binding. Empty = auto-detect full host (all online CPUs /
# all online NUMA nodes). Export AENV_CPUS/AENV_MEMS or edit here to narrow.
AENV_CPUS="${AENV_CPUS:-}"
AENV_MEMS="${AENV_MEMS:-}"
[ -z "$AENV_CPUS" ] && AENV_CPUS="$(cat /sys/devices/system/cpu/online 2>/dev/null | tr -d ' ')"
[ -z "$AENV_MEMS" ] && AENV_MEMS="$(cat /sys/devices/system/node/online 2>/dev/null | tr -d ' ')"
[ -z "$AENV_CPUS" ] && AENV_CPUS="0-383"
[ -z "$AENV_MEMS" ] && AENV_MEMS="0"

if [ -f /sys/fs/cgroup/cgroup.controllers ]; then
    # cgroup v2 unified hierarchy (Debian 12 default)
    AENV_CGROUP=/sys/fs/cgroup/$AENV_CGROUP_NAME
    mkdir -p "$AENV_CGROUP"
    if [ ! -e "$AENV_CGROUP/cpu.stat" ] || [ ! -e "$AENV_CGROUP/memory.current" ] \
       || [ ! -e "$AENV_CGROUP/cpuset.cpus" ]; then
        # controllers not enabled in root's subtree_control yet; best-effort
        echo "+cpu +memory +cpuset" > /sys/fs/cgroup/cgroup.subtree_control 2>/dev/null || true
    fi
    if [ -e "$AENV_CGROUP/cpu.stat" ] && [ -e "$AENV_CGROUP/memory.current" ]; then
        # Bind BEFORE joining so every child is born inside the mask.
        if [ -e "$AENV_CGROUP/cpuset.cpus" ]; then
            if echo "$AENV_CPUS" > "$AENV_CGROUP/cpuset.cpus" 2>/dev/null \
               && echo "$AENV_MEMS" > "$AENV_CGROUP/cpuset.mems" 2>/dev/null; then
                echo "cpuset bound: cpus=$AENV_CPUS mems=$AENV_MEMS " \
                     "(effective: $(cat "$AENV_CGROUP/cpuset.cpus.effective" 2>/dev/null))"
            else
                echo "WARNING: cpuset.cpus/mems write failed on $AENV_CGROUP; fleet not CPU-bound" >&2
            fi
        else
            echo "WARNING: cpuset controller unavailable; fleet not CPU-bound" >&2
        fi
        echo $$ > "$AENV_CGROUP/cgroup.procs"
        echo "joined cgroup v2: $AENV_CGROUP (pid $$)"
    else
        echo "WARNING: $AENV_CGROUP lacks cpu.stat/memory.current; " \
             "cgroup metrics will be unavailable" >&2
    fi
else
    # cgroup v1: separate cpu / memory / cpuset hierarchies
    AENV_CPU_V1=/sys/fs/cgroup/cpu,cpuacct/$AENV_CGROUP_NAME
    AENV_MEM_V1=/sys/fs/cgroup/memory/$AENV_CGROUP_NAME
    AENV_CPSET_V1=/sys/fs/cgroup/cpuset/$AENV_CGROUP_NAME
    mkdir -p "$AENV_CPU_V1" "$AENV_MEM_V1" "$AENV_CPSET_V1" 2>/dev/null || true
    if [ -e "$AENV_CPU_V1/cpuacct.usage" ]; then
        # v1 cpuset: cpus/mems MUST be set before tasks can be attached
        if [ -e "$AENV_CPSET_V1/cpuset.cpus" ]; then
            if echo "$AENV_CPUS" > "$AENV_CPSET_V1/cpuset.cpus" 2>/dev/null \
               && echo "$AENV_MEMS" > "$AENV_CPSET_V1/cpuset.mems" 2>/dev/null \
               && echo $$ > "$AENV_CPSET_V1/tasks" 2>/dev/null; then
                echo "cpuset bound (v1): cpus=$AENV_CPUS mems=$AENV_MEMS"
            else
                echo "WARNING: v1 cpuset bind failed; fleet not CPU-bound" >&2
            fi
        fi
        echo $$ > "$AENV_CPU_V1/tasks"
        echo $$ > "$AENV_MEM_V1/tasks"
        echo "joined cgroup v1: cpu,cpuacct/$AENV_CGROUP_NAME + memory/$AENV_CGROUP_NAME (pid $$)"
    else
        echo "WARNING: v1 cgroup hierarchies not found; cgroup metrics unavailable" >&2
    fi
fi
# Optional further limits (cpuset binding above is active by default):
# echo "383000000 400000" > $AENV_CGROUP/cpu.max        # CFS quota ~0.96x of 384 cores
# echo $((384*1024*1024*1024)) > $AENV_CGROUP/memory.max  # 384G

sysctl -w net.ipv4.neigh.default.gc_thresh3=16384
sysctl -w net.ipv4.neigh.default.gc_thresh2=8192
sysctl -w net.ipv4.neigh.default.gc_thresh1=4096
sysctl -w net.netfilter.nf_conntrack_max=1048576
sysctl -w fs.inotify.max_user_instances=8192
sysctl -w net.ipv6.conf.default.disable_ipv6=1

ulimit -n 1048576 2>/dev/null || true

rm /data/aenv/logs/ublk-daemon.log

rm -rf /data/aenv/image-cache/remote-blocks/premerged-index
mkdir -p /data/aenv/image-cache/remote-blocks/premerged-index
ls /data/aenv/image-cache/remote-blocks/premerged-index

rmmod ublk_drv
modprobe ublk_drv ublks_max=10240

AENV_HOME_PATH=/data/aenv API_ADDR=0.0.0.0:8000 AENV_RUN_USER=root make start-server-release
