#!/bin/sh
set -eu

BACKUP_DIR=/tmp/freq-clamp-backup
MARKER=$BACKUP_DIR/CLAMPED
DEFAULT_FREQ=2600000

policies() {
    ls -d /sys/devices/system/cpu/cpufreq/policy* 2>/dev/null
}

die() {
    echo "ERROR: $*" >&2
    exit 1
}

read_field() {
    sed -n "${2}p" "$1" 2>/dev/null || true
}

cmd_status() {
    driver=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_driver 2>/dev/null || echo "n/a")
    echo "driver: $driver"
    if [ -f "$MARKER" ]; then
        echo "state : CLAMPED to $(cat "$MARKER") kHz (backup: $BACKUP_DIR)"
    else
        echo "state : unclamped"
    fi
    printf '%-8s %-12s %-12s %-12s %-6s %-6s %s\n' policy governor cur max boost cpb epp
    for p in $(policies); do
        n=${p##*/}
        cur=$(cat "$p/scaling_cur_freq" 2>/dev/null || echo "-")
        gov=$(cat "$p/scaling_governor" 2>/dev/null || echo "-")
        mx=$(cat "$p/scaling_max_freq" 2>/dev/null || echo "-")
        bo=$(cat "$p/boost" 2>/dev/null || echo "-")
        cb=$(cat "$p/cpb" 2>/dev/null || echo "-")
        ep=$(cat "$p/energy_performance_preference" 2>/dev/null || echo "-")
        printf '%-8s %-12s %-12s %-12s %-6s %-6s %s\n' "$n" "$gov" "$cur" "$mx" "$bo" "$cb" "$ep"
    done
}

cmd_backup() {
    rm -rf "$BACKUP_DIR"
    mkdir -p "$BACKUP_DIR"
    for p in $(policies); do
        n=${p##*/}
        {
            cat "$p/scaling_governor"
            cat "$p/scaling_min_freq"
            cat "$p/scaling_max_freq"
            if [ -f "$p/boost" ]; then cat "$p/boost"; fi
            if [ -f "$p/cpb" ]; then cat "$p/cpb"; fi
            if [ -f "$p/energy_performance_preference" ]; then
                cat "$p/energy_performance_preference"
            fi
        } > "$BACKUP_DIR/$n"
    done
}

cmd_limit() {
    freq=${1:-$DEFAULT_FREQ}
    case $freq in
        ''|*[!0-9]*) die "freq must be kHz, e.g. 2600000" ;;
    esac
    if [ -f "$MARKER" ]; then
        die "already clamped to $(cat "$MARKER"); run '$0 restore' first"
    fi
    n_policies=$(policies | wc -l)
    if [ "$n_policies" -eq 0 ]; then
        die "no cpufreq policies found (cpufreq disabled?)"
    fi
    cmd_backup
    for p in $(policies); do
        min=$(cat "$p/scaling_min_freq")
        if [ "$min" -gt "$freq" ]; then
            echo "$freq" > "$p/scaling_min_freq"
        fi
        echo "$freq" > "$p/scaling_max_freq"
        if [ -f "$p/boost" ]; then echo 0 > "$p/boost"; fi
        if [ -f "$p/cpb" ]; then echo 0 > "$p/cpb"; fi
    done
    echo "$freq" > "$MARKER"
    echo "clamped $n_policies policies to ${freq} kHz (governor untouched; backup: $BACKUP_DIR)"
}

cmd_restore() {
    if [ ! -f "$MARKER" ]; then
        die "not clamped"
    fi
    for p in $(policies); do
        n=${p##*/}
        f="$BACKUP_DIR/$n"
        if [ ! -f "$f" ]; then
            echo "WARN: no backup for policy $n, skipping" >&2
            continue
        fi
        gov=$(read_field "$f" 1)
        min=$(read_field "$f" 2)
        max=$(read_field "$f" 3)
        boost=$(read_field "$f" 4)
        cpb=$(read_field "$f" 5)
        epp=$(read_field "$f" 6)
        if [ -n "$max" ]; then echo "$max" > "$p/scaling_max_freq"; fi
        if [ -n "$min" ]; then echo "$min" > "$p/scaling_min_freq"; fi
        if [ -n "$gov" ]; then echo "$gov" > "$p/scaling_governor"; fi
        if [ -n "$boost" ] && [ -f "$p/boost" ]; then echo "$boost" > "$p/boost"; fi
        if [ -n "$cpb" ] && [ -f "$p/cpb" ]; then echo "$cpb" > "$p/cpb"; fi
        if [ -n "$epp" ] && [ -f "$p/energy_performance_preference" ]; then
            echo "$epp" > "$p/energy_performance_preference" 2>/dev/null || true
        fi
    done
    rm -f "$MARKER"
    echo "restored all policies from $BACKUP_DIR"
}

[ "$(id -u)" -eq 0 ] || die "run as root (sudo $0 ...)"

case ${1:-} in
    limit)   shift; cmd_limit "$@" ;;
    restore) cmd_restore ;;
    status)  cmd_status ;;
    *) echo "usage: sudo $0 {limit [freq_kHz] | restore | status}"
       echo "  limit   clamp all policies (default ${DEFAULT_FREQ} kHz), backup originals"
       echo "  restore restore governor/min/max/boost/cpb/epp from backup"
       echo "  status  show per-policy state"
       exit 1 ;;
esac
