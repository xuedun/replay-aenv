#!/usr/bin/env python3
"""Fix cgroup v2 user/sys CPU columns in metrics_1s.jsonl (post-hoc).

spotbox_monitor's read_cgroup_cpu converted cpu.stat user_usec/system_usec
to jiffies as `usec * 1000 // CLK_TCK` instead of `usec * CLK_TCK // 1e6`,
inflating cgroup_cpu_user_cores / cgroup_cpu_sys_cores by CLK_TCK^2/1e9
(100000x with CLK_TCK=100) on cgroup v2 hosts. cgroup_cpu_cores (the
usage_usec path) was always correct.

The stored values are a constant multiple of the true values, so this
script multiplies them back by CLK_TCK^2/1e9. Guarded: a file whose
user/sys medians already look sane (< 10x its total) is skipped, which
also makes the fix idempotent -- running it twice is safe, and v1 data
(never affected) is never touched.

Usage:
  python3 fix_cgroup_user_sys.py <metrics_1s.jsonl | run_dir> [--dry-run]
                                    # a directory is rglob'd for metrics_1s.jsonl
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from pathlib import Path

CLK_TCK = os.sysconf("SC_CLK_TCK")
FACTOR = (CLK_TCK * CLK_TCK) / 1e9  # 1e-5 on CLK_TCK=100 hosts


def looks_broken(rows: list[dict]) -> bool:
    """True when user/sys magnitudes are impossible (clearly the 1e5x bug).

    Compares medians of user+sys against cgroup_cpu_cores; inflated data
    exceeds the total by orders of magnitude."""
    user = sorted(
        r["cgroup_cpu_user_cores"]
        for r in rows
        if isinstance(r.get("cgroup_cpu_user_cores"), (int, float))
    )
    total = sorted(
        r["cgroup_cpu_cores"]
        for r in rows
        if isinstance(r.get("cgroup_cpu_cores"), (int, float))
    )
    if not user or not total:
        return False
    med_user = user[len(user) // 2]
    med_total = total[len(total) // 2]
    return med_user > 10 * max(med_total, 1.0)


def fix_file(path: Path, dry_run: bool) -> bool:
    rows: list[dict] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                rows.append({})
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                rows.append({})

    if not looks_broken(rows):
        print(f"  [skip] {path} (sane magnitudes: v1 data or already fixed)")
        return False

    before_user = [
        r.get("cgroup_cpu_user_cores") for r in rows
        if isinstance(r.get("cgroup_cpu_user_cores"), (int, float))
    ]
    for r in rows:
        for key in ("cgroup_cpu_user_cores", "cgroup_cpu_sys_cores"):
            v = r.get(key)
            if isinstance(v, (int, float)):
                r[key] = round(v * FACTOR, 3)

    # Sanity: corrected user+sys should now track the (always-correct) total.
    pairs = [
        (r.get("cgroup_cpu_user_cores", 0) + r.get("cgroup_cpu_sys_cores", 0),
         r.get("cgroup_cpu_cores"))
        for r in rows
        if isinstance(r.get("cgroup_cpu_cores"), (int, float))
    ]
    after_user = [
        r.get("cgroup_cpu_user_cores") for r in rows
        if isinstance(r.get("cgroup_cpu_user_cores"), (int, float))
    ]
    med_pair = sorted(pairs)[len(pairs) // 2] if pairs else (0, 0)
    print(
        f"  [fix ] {path}: user median {before_user[len(before_user)//2]}"
        f" -> {after_user[len(after_user)//2]}; "
        f"sanity user+sys={med_pair[0]:.3f} vs total={med_pair[1]:.3f}"
    )

    if dry_run:
        return True
    backup = path.with_suffix(path.suffix + ".bak")
    if not backup.exists():
        shutil.copy2(path, backup)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            if r:
                f.write(json.dumps(r) + "\n")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("path", help="metrics_1s.jsonl file or a run directory")
    parser.add_argument("--dry-run", action="store_true",
                        help="report what would change without writing")
    args = parser.parse_args()

    target = Path(args.path)
    if target.is_dir():
        files = sorted(target.rglob("metrics_1s.jsonl"))
    elif target.is_file():
        files = [target]
    else:
        print(f"ERROR: no such file or directory: {target}", file=sys.stderr)
        return 1

    print(f"CLK_TCK={CLK_TCK} correction factor={FACTOR:g}")
    fixed = sum(1 for f in files if fix_file(f, args.dry_run))
    print(f"{fixed}/{len(files)} files fixed"
          + (" (dry-run)" if args.dry_run else "")
          + "; original kept as .bak alongside each fixed file")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
