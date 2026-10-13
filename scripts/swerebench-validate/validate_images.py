#!/usr/bin/env python3
"""swerebench ARM 镜像 replay 可用性验证流水线。

输入一个镜像列表(每行一个 SWR 引用,如
  swr.cn-north-4.myhuaweicloud.com/kunpeng-ai/swerebench-arm64-actions-stale:334-ec96ff6
# 开头的行与空行忽略),对每条依次执行:

  1. podman pull(本地已有则跳过)
  2. 架构检查(必须 arm64/linux)
  3. push 到本地 registry 127.0.0.1:5000(已在仓则跳过)
  4. aenv pull 建模板 swb-<slug>-<num>(已 ready 则跳过)
  5. 从任务数据集(SWE-rebench-V2 parquet)按 tag 里的 commit 反查 instance_id
  6. 从 Open-SWE-Traces(minisweagent/swe-rebench-v2 分片 + index.json)取官方轨迹,
     转成 replay-aenv 格式(纯 bash action;首步建 /testbed 软链到镜像实际 workdir)
  7. replay_agent.py run 回放,--workdir 取镜像的 Dockerfile WORKDIR(默认 /stale)
  8. 解析 replay-result.json 出判定:PASS(status=success 且 submitted)/ FAIL / SKIP

结果追加写到 <runs-dir>/report.jsonl,并打印汇总表。
数据文件(parquet 分片、任务表、索引)默认放 --data-dir(建议持久目录,
不随 /tmp 清理)。首次使用先跑一次 --build-index 建轨迹索引。

用法:
  python3 validate_images.py --build-index \\
      --data-dir /home/zkwang/replay-aenv/data/swerebench
  python3 validate_images.py --list images.txt --data-dir ... --runs-dir runs/validate-xxx

依赖:pyarrow(replay-aenv/.venv)、podman、aenv、replay-aenv/agent/replay_agent.py。
连接信息从 /root/.config/harbor-agentenv/env 读取(E2B_API_URL/E2B_API_KEY)。
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

REPLAY_ROOT = Path(__file__).resolve().parents[2]
REPLAY_AGENT = REPLAY_ROOT / "agent" / "replay_agent.py"
REPLAY_PY = REPLAY_ROOT / ".venv" / "bin" / "python"
ENV_FILE = Path("/root/.config/harbor-agentenv/env")
LOCAL_REGISTRY = "127.0.0.1:5000"
SUBMIT_MARKER = "COMPLETE_TASK_AND_SUBMIT_FINAL_OUTPUT"


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def run(cmd: list[str], timeout: int = 3600, **kw) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, **kw)


def load_env() -> dict[str, str]:
    env = dict(os.environ)
    for line in ENV_FILE.read_text().splitlines():
        m = re.match(r'^([A-Z0-9_]+)="?([^"\n]*)"?$', line.strip())
        if m:
            env.setdefault(m.group(1), m.group(2))
    env.setdefault("E2B_SANDBOX_URL", env.get("E2B_API_URL", ""))
    env.setdefault("E2B_ACCESS_TOKEN", "dummy")
    return env


# ---------- 镜像引用解析 ----------

class ImageRef:
    def __init__(self, raw: str):
        self.raw = raw.strip()
        repo_tag = self.raw.rsplit("/", 1)[-1]  # swerebench-arm64-actions-stale:334-ec96ff6
        self.slug, _, self.tag = repo_tag.partition(":")
        if not self.slug or not self.tag:
            raise ValueError(f"无法解析镜像引用: {raw}")
        # slug: swerebench-arm64-actions-stale -> actions-stale
        self.task_slug = re.sub(r"^swerebench-arm64-", "", self.slug)
        self.num, _, self.commit = self.tag.partition("-")
        self.local = f"{LOCAL_REGISTRY}/swerebench/{self.task_slug}:{self.tag}"
        self.template = f"swb-{self.task_slug}-{self.num}"


# ---------- 阶段:pull / arch / push / template ----------

def ensure_pulled(ref: ImageRef) -> str | None:
    if run(["podman", "image", "exists", ref.raw]).returncode == 0:
        return None
    r = run(["podman", "pull", ref.raw], timeout=3600)
    return None if r.returncode == 0 else f"pull 失败: {r.stderr[-300:]}"


def check_arch(ref: ImageRef) -> str | None:
    r = run(["podman", "inspect", "--format", "{{.Architecture}} {{.Os}}", ref.raw])
    val = r.stdout.strip()
    return None if val == "arm64 linux" else f"架构不符: {val!r}"


def image_workdir(ref: ImageRef) -> str:
    r = run(["podman", "inspect", "--format", "{{.Config.WorkingDir}}", ref.raw])
    return r.stdout.strip() or "/stale"


def ensure_pushed(ref: ImageRef) -> str | None:
    if run(["podman", "image", "exists", ref.local]).returncode == 0:
        return None
    if run(["podman", "tag", ref.raw, ref.local]).returncode != 0:
        return "tag 失败"
    r = run(["podman", "push", "--tls-verify=false", ref.local], timeout=3600)
    return None if r.returncode == 0 else f"push 失败: {r.stderr[-300:]}"


def ensure_template(ref: ImageRef) -> str | None:
    r = run(["aenv", "template", "list"], timeout=120)
    try:
        templates = json.loads(r.stdout)
        for t in templates:
            if ref.template in (t.get("names") or []) and t.get("buildStatus") == "ready":
                return None
    except json.JSONDecodeError:
        pass
    r = run(["aenv", "pull", ref.local, "--name", ref.template, "--timeout", "1800"],
            timeout=2000)
    if r.returncode != 0:
        return f"aenv pull 失败: {(r.stdout + r.stderr)[-300:]}"
    return None


# ---------- 阶段:instance / trace ----------

def find_instance_id(tasks_parquet: Path, ref: ImageRef) -> str | None:
    import pyarrow.parquet as pq
    import pyarrow.compute as pc
    t = pq.read_table(tasks_parquet, columns=["instance_id", "repo", "base_commit"])
    matches = []
    for iid, repo, base in zip(
        t.column("instance_id").to_pylist(),
        t.column("repo").to_pylist(),
        t.column("base_commit").to_pylist(),
    ):
        if base and base.startswith(ref.commit):
            matches.append(iid)
    if len(matches) == 1:
        return matches[0]
    if not matches:  # 回退:命名规则推导
        guess = f"{ref.task_slug.replace('-', '__', 1)}-{ref.num}"
        return guess
    return matches[0]


def build_index(data_dir: Path) -> None:
    import pyarrow.parquet as pq
    index: dict[str, list] = {}
    for shard in sorted(data_dir.glob("mini-v2-shard*.parquet")):
        t = pq.read_table(shard, columns=["instance_id"])
        for row, iid in enumerate(t.column("instance_id").to_pylist()):
            index.setdefault(iid, [shard.name, row])
        log(f"索引 {shard.name}: 累计 {len(index)} 实例")
    (data_dir / "index.json").write_text(json.dumps(index))
    log(f"索引完成: {len(index)} 实例 -> {data_dir/'index.json'}")


def extract_trace(data_dir: Path, instance_id: str) -> dict | None:
    idx_path = data_dir / "index.json"
    if not idx_path.exists():
        raise SystemExit(f"索引不存在,先跑 --build-index: {idx_path}")
    index = json.loads(idx_path.read_text())
    hit = index.get(instance_id)
    if not hit:
        return None
    import pyarrow.parquet as pq
    shard, row = hit
    t = pq.read_table(str(data_dir / shard))
    return t.to_pylist()[row]


def convert_trace(row: dict, dst: Path, repo_dir: str, delay: float = 0.2) -> int:
    steps = []
    for msg in row["trajectory"]:
        if msg.get("role") != "assistant":
            continue
        calls = msg.get("tool_calls") or []
        if not calls:
            continue
        if len(calls) != 1 or calls[0]["function"]["name"] != "bash":
            raise ValueError("非 bash tool_call,该 trace 不适用本转换器")
        cmd = json.loads(calls[0]["function"]["arguments"])["command"]
        if SUBMIT_MARKER in cmd:
            cmd = "submit"
        steps.append({
            "action": cmd,
            "thought": (msg.get("content") or "").strip(),
            "response": (msg.get("content") or "").strip(),
            "delay_time": delay,
        })
    if repo_dir != "/testbed":  # 原版 trace 里大量 /testbed 绝对路径
        steps.insert(0, {
            "action": f"ln -sfn {repo_dir} /testbed && ls /testbed | head -3",
            "thought": "软链对齐原版 trace 的 /testbed 路径",
            "response": "",
            "delay_time": 0.0,
        })
    out = {
        "environment": "main",
        "trajectory": steps,
        "history": [],
        "info": {
            "instance_id": row.get("instance_id"),
            "source": "nvidia/Open-SWE-Traces minisweagent/swe-rebench-v2",
            "trajectory_id": row.get("trajectory_id"),
            "resolved": row.get("resolved"),
        },
        "replay_config": {},
    }
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_text(json.dumps(out, ensure_ascii=False, indent=1))
    return len(steps)


# ---------- 阶段:replay ----------

def run_replay(traj: Path, template: str, workdir: str, out_dir: Path,
               timeout_sec: int, env: dict[str, str]) -> tuple[dict | None, str | None]:
    out_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        str(REPLAY_PY), str(REPLAY_AGENT), "run",
        "--trajectory", str(traj),
        "--template", template,
        "--workdir", workdir,
        "--skip-instance-patch",
        "--timeout-sec", str(timeout_sec),
        "--output-dir", str(out_dir),
    ]
    log_file = out_dir / "replay-driver.log"
    with log_file.open("w") as lf:
        r = subprocess.run(cmd, cwd=REPLAY_ROOT, env=env, stdout=lf,
                           stderr=subprocess.STDOUT, timeout=timeout_sec + 900)
    result_path = out_dir / "replay-result.json"
    if not result_path.exists():
        tail = log_file.read_text()[-300:] if log_file.exists() else ""
        return None, f"无 replay-result.json(rc={r.returncode}): {tail}"
    return json.loads(result_path.read_text()), None


def verdict_of(result: dict) -> str:
    if result.get("submitted") and result.get("succeeded"):
        return "PASS"
    return "FAIL"


# ---------- 主流程 ----------

def validate_one(raw: str, args, env, report_fp) -> dict:
    rec: dict = {"image": raw, "verdict": "FAIL", "stages": {}}
    t0 = time.time()
    try:
        ref = ImageRef(raw)
    except ValueError as e:
        rec.update(verdict="SKIP", error=str(e))
        return rec
    rec.update(instance=ref.template, template=ref.template)

    for stage, fn in [
        ("pull", lambda: ensure_pulled(ref)),
        ("arch", lambda: check_arch(ref)),
        ("push", lambda: ensure_pushed(ref)),
        ("template", lambda: ensure_template(ref)),
    ]:
        err = fn()
        rec["stages"][stage] = "ok" if err is None else f"error: {err}"
        if err:
            rec.update(verdict="FAIL", error=f"{stage}: {err}",
                       elapsed_sec=round(time.time() - t0, 1))
            return rec

    workdir = image_workdir(ref)
    rec["workdir"] = workdir

    instance_id = find_instance_id(Path(args.tasks_parquet), ref)
    rec["instance_id"] = instance_id
    row = extract_trace(Path(args.data_dir), instance_id) if instance_id else None
    if row is None:
        rec.update(verdict="SKIP_NO_TRACE", elapsed_sec=round(time.time() - t0, 1))
        return rec

    traj_path = Path(args.runs_dir) / ref.template / "trajectory.replay.json"
    n_steps = convert_trace(row, traj_path, repo_dir=workdir, delay=args.delay)
    rec["trace_steps"] = n_steps

    run_dir = Path(args.runs_dir) / ref.template / "run"
    result, err = run_replay(traj_path, ref.template, workdir, run_dir,
                             args.timeout_sec, env)
    if err:
        rec.update(verdict="FAIL", error=err, elapsed_sec=round(time.time() - t0, 1))
        return rec
    rec.update(
        verdict=verdict_of(result),
        submitted=bool(result.get("submitted")),
        commands=result.get("commands"),
        succeeded_commands=result.get("succeeded_commands"),
        failed_commands=result.get("failed_commands"),
        replay_elapsed_sec=result.get("elapsed_sec"),
        run_dir=str(run_dir),
        elapsed_sec=round(time.time() - t0, 1),
    )
    return rec


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--list", help="镜像列表文件(每行一个 SWR 引用)")
    ap.add_argument("--data-dir", default="/home/zkwang/replay-aenv/data/swerebench",
                    help="trace 分片 + 索引 + 任务表所在目录")
    ap.add_argument("--tasks-parquet", default=None,
                    help="SWE-rebench-V2 任务表 parquet(默认 <data-dir>/swerebench-v2-tasks.parquet)")
    ap.add_argument("--runs-dir", default=None,
                    help="回放输出目录(默认 runs/validate-<时间戳>)")
    ap.add_argument("--build-index", action="store_true", help="只建轨迹索引后退出")
    ap.add_argument("--timeout-sec", type=int, default=2400)
    ap.add_argument("--delay", type=float, default=0.2, help="每步 delay_time 秒")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    if args.tasks_parquet is None:
        args.tasks_parquet = str(data_dir / "swerebench-v2-tasks.parquet")

    if args.build_index:
        build_index(data_dir)
        return

    if not args.list:
        ap.error("--list 或 --build-index 必须给一个")
    images = [l.strip() for l in Path(args.list).read_text().splitlines()
              if l.strip() and not l.startswith("#")]
    if args.limit:
        images = images[: args.limit]
    if not images:
        raise SystemExit("列表为空")

    runs_dir = Path(args.runs_dir or
                    REPLAY_ROOT / "runs" / f"validate-{time.strftime('%Y%m%d-%H%M%S')}")
    runs_dir.mkdir(parents=True, exist_ok=True)
    report_path = runs_dir / "report.jsonl"
    env = load_env()

    log(f"共 {len(images)} 个镜像,报告: {report_path}")
    counts: dict[str, int] = {}
    with report_path.open("a") as fp:
        for i, raw in enumerate(images, 1):
            log(f"[{i}/{len(images)}] {raw}")
            try:
                rec = validate_one(raw, args, env, fp)
            except Exception as e:  # 单个镜像失败不阻断整批
                rec = {"image": raw, "verdict": "FAIL", "error": f"未捕获异常: {e}"}
            fp.write(json.dumps(rec, ensure_ascii=False) + "\n")
            fp.flush()
            counts[rec["verdict"]] = counts.get(rec["verdict"], 0) + 1
            log(f"  -> {rec['verdict']} "
                f"(cmds={rec.get('succeeded_commands')}/{rec.get('commands')}, "
                f"elapsed={rec.get('elapsed_sec')}s"
                f"{', ' + rec['error'][:120] if rec.get('error') else ''})")

    log("汇总: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    log(f"报告: {report_path}")


if __name__ == "__main__":
    main()
