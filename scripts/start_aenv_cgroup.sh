#!/bin/bash
# AgentENV startup with cgroup containment (for replay cgroup metrics).
# The script shell joins /sys/fs/cgroup/aenv FIRST; every later child
# (make -> aenv-server -> uvm-ublk-daemon -> firecracker VMs) inherits it,
# so spotbox_monitor.py --cgroup-name aenv aggregates the whole fleet.

echo 0 > /sys/module/kvm/parameters/vm_uevent_notify

# ── cgroup: create + join (v2 and v1 both handled) ─────────────────────
AENV_CGROUP_NAME=aenv
if [ -f /sys/fs/cgroup/cgroup.controllers ]; then
    # cgroup v2 unified hierarchy (Debian 12 default)
    AENV_CGROUP=/sys/fs/cgroup/$AENV_CGROUP_NAME
    mkdir -p "$AENV_CGROUP"
    if [ ! -e "$AENV_CGROUP/cpu.stat" ] || [ ! -e "$AENV_CGROUP/memory.current" ]; then
        # controllers not enabled in root's subtree_control yet; best-effort
        # (+cpuset enables the optional cpuset.cpus limit block below)
        echo "+cpu +memory +cpuset" > /sys/fs/cgroup/cgroup.subtree_control 2>/dev/null || true
    fi
    if [ -e "$AENV_CGROUP/cpu.stat" ] && [ -e "$AENV_CGROUP/memory.current" ]; then
        echo $$ > "$AENV_CGROUP/cgroup.procs"
        echo "joined cgroup v2: $AENV_CGROUP (pid $$)"
    else
        echo "WARNING: $AENV_CGROUP lacks cpu.stat/memory.current; " \
             "cgroup metrics will be unavailable" >&2
    fi
else
    # cgroup v1: separate cpu and memory hierarchies
    mkdir -p /sys/fs/cgroup/cpu,cpuacct/$AENV_CGROUP_NAME \
             /sys/fs/cgroup/memory/$AENV_CGROUP_NAME 2>/dev/null || true
    if [ -e /sys/fs/cgroup/cpu,cpuacct/$AENV_CGROUP_NAME/cpuacct.usage ]; then
        echo $$ > /sys/fs/cgroup/cpu,cpuacct/$AENV_CGROUP_NAME/tasks
        echo $$ > /sys/fs/cgroup/memory/$AENV_CGROUP_NAME/tasks
        echo "joined cgroup v1: cpu,cpuacct/$AENV_CGROUP_NAME + memory/$AENV_CGROUP_NAME (pid $$)"
    else
        echo "WARNING: v1 cgroup hierarchies not found; cgroup metrics unavailable" >&2
    fi
fi
# Optional limits (collection-only by default — uncomment to enforce):
# echo "383000000 400000" > $AENV_CGROUP/cpu.max        # ~0.96x of 384 cores
# echo $((384*1024*1024*1024)) > $AENV_CGROUP/memory.max  # 384G
# echo 0-383 > $AENV_CGROUP/cpuset.cpus  # v2 needs +cpuset in root subtree_control

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
