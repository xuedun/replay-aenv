#!/usr/bin/env python3
"""Validate initial-state patches against a clean disposable repository."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path


def run(*args: str, cwd: Path, capture: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=cwd,
        check=True,
        text=True,
        capture_output=capture,
    )


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--overlay-dir", type=Path, required=True)
    args = parser.parse_args()

    root_manifest_path = args.overlay_dir / "manifest.json"
    root_manifest = json.loads(root_manifest_path.read_text(encoding="utf-8"))
    failures: list[dict[str, str]] = []
    validated: list[dict[str, object]] = []

    for instance in root_manifest["instances"]:
        instance_id = instance["instance_id"]
        instance_dir = args.overlay_dir / instance_id
        manifest_path = instance_dir / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        patch_path = instance_dir / manifest["patch_file"]
        base_commit = manifest["base_commit"]
        try:
            resolved_base = run(
                "git", "rev-parse", f"{base_commit}^{{commit}}", cwd=args.repo
            ).stdout.strip()
            run("git", "reset", "--hard", resolved_base, cwd=args.repo)
            run("git", "clean", "-fdx", cwd=args.repo)
            run("git", "apply", "--check", str(patch_path), cwd=args.repo)
            run("git", "apply", str(patch_path), cwd=args.repo)
            diff_check = subprocess.run(
                ["git", "diff", "--check"],
                cwd=args.repo,
                check=False,
                text=True,
                capture_output=True,
            )
            changed = run("git", "diff", "--name-only", cwd=args.repo).stdout.splitlines()
            expected_changed = manifest["changed_files"]
            if sorted(changed) != sorted(expected_changed):
                raise RuntimeError(
                    f"changed files mismatch: expected={expected_changed!r}, actual={changed!r}"
                )
            file_hashes = {
                name: sha256((args.repo / name).read_bytes()) for name in changed
            }
            diff_bytes = subprocess.run(
                ["git", "diff", "--binary"],
                cwd=args.repo,
                check=True,
                capture_output=True,
            ).stdout
            validation = {
                "status": "passed",
                "validated_at_utc": datetime.now(timezone.utc).isoformat(),
                "base_commit_resolved": resolved_base,
                "git_apply_check": "passed",
                "git_diff_check": (
                    "passed" if diff_check.returncode == 0 else "warning"
                ),
                "git_diff_check_output": (
                    diff_check.stdout + diff_check.stderr
                ).strip(),
                "changed_files": changed,
                "changed_file_sha256": file_hashes,
                "working_tree_diff_sha256": sha256(diff_bytes),
            }
            manifest["validation"] = validation
            manifest_path.write_text(
                json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            validated.append(manifest)
            print(f"[passed] {instance_id}")
        except Exception as exc:  # keep validating to produce a complete report
            message = str(exc)
            manifest["validation"] = {
                "status": "failed",
                "validated_at_utc": datetime.now(timezone.utc).isoformat(),
                "error": message,
            }
            manifest_path.write_text(
                json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            failures.append({"instance_id": instance_id, "error": message})
            print(f"[failed] {instance_id}: {message}")

    run("git", "reset", "--hard", "HEAD", cwd=args.repo)
    run("git", "clean", "-fdx", cwd=args.repo)
    root_manifest["instances"] = validated + [
        json.loads(
            (args.overlay_dir / item["instance_id"] / "manifest.json").read_text(
                encoding="utf-8"
            )
        )
        for item in failures
    ]
    root_manifest["instances"].sort(key=lambda item: item["instance_id"])
    root_manifest["validation"] = {
        "validated_at_utc": datetime.now(timezone.utc).isoformat(),
        "image_repository": "django-money:poc_v2",
        "passed": len(validated),
        "failed": len(failures),
        "failures": failures,
    }
    root_manifest_path.write_text(
        json.dumps(root_manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    if failures:
        raise SystemExit(f"{len(failures)} patch(es) failed validation")
    print(f"Validated {len(validated)} patches successfully")


if __name__ == "__main__":
    main()
