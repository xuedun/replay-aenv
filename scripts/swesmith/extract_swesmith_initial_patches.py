#!/usr/bin/env python3
"""Extract per-instance SWE-smith mutation patches from pinned Parquet shards."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path

import pyarrow.parquet as pq


BASE_COMMIT_RE = re.compile(r"\.([0-9a-f]{7,40})\.[^.]+__[^.]+$")


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def changed_files(patch: str) -> list[str]:
    files: list[str] = []
    for line in patch.splitlines():
        if not line.startswith("diff --git a/"):
            continue
        parts = line.split(" b/", 1)
        if len(parts) == 2:
            files.append(parts[1])
    return files


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--trajectory-dir",
        type=Path,
        default=Path("delay_time_trajectories"),
        help="Replay JSON directory (default: delay_time_trajectories)",
    )
    parser.add_argument(
        "--parquet-dir",
        type=Path,
        default=Path("SWE-smith-instances/data"),
        help="Downloaded SWE-smith Parquet directory",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("instance_overlays"),
        help="Output directory for per-instance patches",
    )
    parser.add_argument(
        "--dataset-revision",
        default="ea6d7173829c7ec8fa16c22055699ff2e9188091",
        help="Pinned SWE-bench/SWE-smith dataset revision",
    )
    args = parser.parse_args()

    instance_ids = {
        path.name.removesuffix(".replay.json")
        for path in args.trajectory_dir.glob("*.replay.json")
    }
    if not instance_ids:
        raise SystemExit(f"No replay JSON files found under {args.trajectory_dir}")

    found: dict[str, dict[str, str]] = {}
    shards = sorted(args.parquet_dir.glob("train-*.parquet"))
    if not shards:
        raise SystemExit(f"No train Parquet shards found under {args.parquet_dir}")

    for shard in shards:
        parquet = pq.ParquetFile(shard)
        for batch in parquet.iter_batches(
            batch_size=1024,
            columns=["instance_id", "patch", "repo", "image_name"],
        ):
            for row in batch.to_pylist():
                instance_id = row["instance_id"]
                if instance_id not in instance_ids:
                    continue
                if instance_id in found:
                    raise SystemExit(f"Duplicate dataset row for {instance_id}")
                found[instance_id] = row
        if len(found) == len(instance_ids):
            break

    missing = sorted(instance_ids - found.keys())
    if missing:
        raise SystemExit("Missing dataset rows:\n" + "\n".join(missing))

    args.output_dir.mkdir(parents=True, exist_ok=True)
    summary: list[dict[str, object]] = []
    for instance_id in sorted(instance_ids):
        row = found[instance_id]
        patch_text = row["patch"]
        if not isinstance(patch_text, str) or not patch_text.strip():
            raise SystemExit(f"Empty patch for {instance_id}")
        patch_bytes = (patch_text.rstrip("\n") + "\n").encode()
        match = BASE_COMMIT_RE.search(instance_id)
        if not match:
            raise SystemExit(f"Cannot parse base commit from {instance_id}")
        base_commit = match.group(1)
        files = changed_files(patch_text)

        instance_dir = args.output_dir / instance_id
        instance_dir.mkdir(parents=True, exist_ok=True)
        patch_path = instance_dir / "initial.patch"
        patch_path.write_bytes(patch_bytes)
        manifest = {
            "instance_id": instance_id,
            "base_commit": base_commit,
            "repository": row.get("repo"),
            "image_name": row.get("image_name"),
            "source_dataset": "SWE-bench/SWE-smith",
            "source_revision": args.dataset_revision,
            "source_field": "patch",
            "apply_direction": "forward",
            "patch_file": "initial.patch",
            "patch_sha256": sha256(patch_bytes),
            "patch_bytes": len(patch_bytes),
            "changed_files": files,
            "validation": {"status": "pending"},
        }
        (instance_dir / "manifest.json").write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        summary.append(manifest)

    root_manifest = {
        "source_dataset": "SWE-bench/SWE-smith",
        "source_revision": args.dataset_revision,
        "instance_count": len(summary),
        "apply_direction": "forward",
        "instances": summary,
    }
    (args.output_dir / "manifest.json").write_text(
        json.dumps(root_manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(f"Extracted {len(summary)} initial-state patches to {args.output_dir}")


if __name__ == "__main__":
    main()
