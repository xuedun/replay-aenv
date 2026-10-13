# ReplayAgent-AgentENV 快速开始

`agent/replay_agent.py` 是一个不依赖 Harbor 的独立轨迹回放程序。它通过 AgentENV
提供的 E2B 兼容 API 创建沙箱，按照轨迹记录的顺序执行 shell action。

每条轨迹初始化时，程序创建沙箱、执行就绪探测、校验工作目录并应用实例补丁，
随后暂停沙箱。每条 action 的执行流程为：

```text
沙箱保持 paused → 等待轨迹中的 delay_time → 获取 RUNNING 名额 → resume
→ 就绪探测 → 执行 action → pause → 释放 RUNNING 名额
```

## 1. 使用前提

- AgentENV 已部署并提供 E2B 兼容 API；
- AgentENV 中已有状态为 `ready` 的任务 template；
- template 中已经准备好任务代码、依赖和工作目录，例如 `/testbed`；
- 已获得本仓库和待回放的 `.json` 轨迹文件；
- 运行主机可以访问 AgentENV API。

## 2. 已验证版本

| 组件 | 已验证版本 |
|---|---|
| Python | 3.12.13 |
| E2B Python SDK | 2.37.0 |
| ReplayAgent | 0.1.0 |
| AgentENV API | E2B 兼容 API |
| 测试 template | `ubuntu`、`django-money-task-v2` |

## 3. 安装运行环境

进入仓库根目录，推荐使用 `uv` 创建独立 Python 环境并安装项目：

```bash
uv venv --python 3.12
uv pip install --python .venv/bin/python -e .

.venv/bin/python --version
.venv/bin/python -c 'import importlib.metadata; print(importlib.metadata.version("e2b"))'
```

如果不使用 `uv`，也可以使用已有的 Python 3.12 环境安装：

```bash
python3.12 -m pip install -e .
```

## 4. 设置 AgentENV 连接信息

填写 AgentENV 部署方提供的 API 地址和密钥：

```bash
export E2B_API_URL='<AgentENV API URL>'
export E2B_SANDBOX_URL="${E2B_SANDBOX_URL:-$E2B_API_URL}"
export E2B_API_KEY='<AgentENV API Key>'
export E2B_ACCESS_TOKEN='<AgentENV Access Token>'
```

单机、无鉴权的 AgentENV 示例：

```bash
export E2B_API_URL='http://127.0.0.1:8000'
export E2B_SANDBOX_URL="$E2B_API_URL"
export E2B_API_KEY='e2b_0000000000000000000000000000000000000000'
export E2B_ACCESS_TOKEN='dummy'
```

## 5. 检查 AgentENV 和 template

先确认 AgentENV 服务健康：

```bash
curl --fail --silent --output /dev/null \
  --write-out 'HTTP %{http_code}\n' \
  "$E2B_API_URL/health"
```

如果运行主机安装了 `aenv` CLI，可以先列出 template：

```bash
aenv template list
```

无论是否安装 `aenv` CLI，都可以通过本脚本检查目标 template：

```bash
export TEMPLATE_NAME='django-money-task-v2'

.venv/bin/python agent/replay_agent.py check \
  --template "$TEMPLATE_NAME"
```


## 6. 运行单任务

```bash
export TRAJECTORY_FILE='/path/to/example.replay.json'
export TEMPLATE_NAME='django-money-task-v2'
export SANDBOX_WORKDIR='/testbed'

.venv/bin/python agent/replay_agent.py run \
  --trajectory "$TRAJECTORY_FILE" \
  --template "$TEMPLATE_NAME" \
  --workdir "$SANDBOX_WORKDIR" \
  --timeout-sec 300
```

默认结果保存在当前目录的 `runs/` 下，也可以显式指定：

```bash
.venv/bin/python agent/replay_agent.py run \
  --trajectory "$TRAJECTORY_FILE" \
  --template "$TEMPLATE_NAME" \
  --workdir "$SANDBOX_WORKDIR" \
  --output-dir '/data/replay-runs/example-001'
```

每条轨迹使用一个独立沙箱。正常完成、action 失败或程序异常时，程序都会尝试在
`finally` 阶段删除沙箱。指定 `--keep-sandbox-on-failure` 后，只有确认已经暂停的
失败沙箱才会保留；无法确认安全暂停时仍会执行清理。

## 7. 批量运行

### 7.1 同一 template 回放多条轨迹

`batch` 读取指定目录第一层中的 `.json` 和 `.traj` 文件，并为每条轨迹创建独立
sandbox。下面的命令使用同一个 AgentENV template 回放多条轨迹：

```bash
.venv/bin/python agent/replay_agent.py batch \
  --trajectory-dir "./delay_time_trajectories" \
  --template "django-money-task-v2" \
  --workdir "/testbed" \
  --target-count 180 \
  --concurrency 180 \
  --running-concurrency 30 \
  --launch-interval-sec 0.3 \
  --control-plane-qps 500 \
  --timeout-sec 600
```

批量与混合回放的常用命令行参数如下：

| 参数 | 功能 | 默认值 |
|---|---|---|
| `--trajectory-dir` | `batch` 模式的轨迹目录，只读取目录第一层的 `.json` 和 `.traj` 文件 | 无，必填 |
| `--config` | `mix` 模式的多 template 配置文件 | 无，必填 |
| `--template` | `batch` 模式使用的 AgentENV template | `django-money-replay-v2` |
| `--workdir` | sandbox 内执行 action 的工作目录 | `/testbed` |
| `--target-count` | `batch` 的总回放次数；为 `0` 时每个轨迹执行一次，超过文件数时循环复用 | `0` |
| `--limit` | 最多选择的源轨迹文件数；为 `0` 时不限制 | `0` |
| `--concurrency` | 同时处于回放生命周期中的轨迹任务数 | `batch` 为 `4`；`mix` 使用 config 值 |
| `--running-concurrency` | 同时处于创建或 `resume → command → pause` 阶段的 RUNNING sandbox 上限 | `0`，即使用 `--concurrency` |
| `--launch-interval-sec` | 相邻轨迹任务实际启动的最小间隔，避免集中启动 | `0.0` 秒 |
| `--control-plane-qps` | pause、resume、cleanup 和命令连接等控制面请求的全局启动速率 | `20.0` |
| `--control-plane-max-inflight` | 等待控制面响应的最大请求数，实际不超过 `--concurrency` | `1024` |
| `--timeout-sec` | 单条 action 的执行超时 | `300` 秒 |
| `--output-dir` | 结果输出目录 | 未指定时写入当前目录的 `runs/` |
| `--dry-run` | 仅校验 `mix` 配置并打印调度预览，不创建 sandbox | 关闭 |

### 7.2 多 template（多镜像）混合回放

`mix` 支持在一个批次中使用多个 AgentENV template。每个 template 可以配置独立的
轨迹目录和执行数量，不同负载共享全局并发、RUNNING 并发及控制面 QPS 限制。

例如，新建 `mix-config.json`：

```json
{
  "concurrency": 60,
  "workloads": [
    {
      "name": "django-money-v2",
      "template": "django-money-task-v2",
      "trajectory_dir": "/data/replay/trajectories/django-money-v2",
      "vm_count": 40
    },
    {
      "name": "django-money-v3",
      "template": "django-money-task-v3",
      "trajectory_dir": "/data/replay/trajectories/django-money-v3",
      "vm_count": 20
    }
  ]
}
```

`workloads[]` 表示 `workloads` 数组中的每个 JSON 对象，不是命令行参数。

| Config 字段 | 功能 | 默认值 |
|---|---|---|
| `concurrency` | `mix` 的全局轨迹并发，可被命令行 `--concurrency` 覆盖 | 无；未传命令行参数时必填 |
| `workloads[].name` | workload 名称，用于结果目录和汇总 | 对应的 `template` 名称 |
| `workloads[].template` | workload 使用的 AgentENV template，可对应不同镜像 | 无，必填 |
| `workloads[].trajectory_dir` | workload 的轨迹目录；相对路径以 config 文件所在目录为基准 | 无，必填 |
| `workloads[].vm_count` | workload 的总回放次数，不是并发数；超过轨迹数量时循环复用 | 无，必填 |

建议先使用 `--dry-run` 校验配置并查看调度预览，此过程不会创建 sandbox：

```bash
.venv/bin/python agent/replay_agent.py mix \
  --config ./mix-config.json \
  --dry-run
```

确认无误后执行混合回放：

```bash
.venv/bin/python agent/replay_agent.py mix \
  --config ./mix-config.json \
  --workdir /testbed \
  --running-concurrency 30 \
  --launch-interval-sec 0.3 \
  --control-plane-qps 500 \
  --timeout-sec 600
```

仓库中的 `agent/mix-config.example.json` 也可以作为配置模板。

### 7.3 运行监控

```bash
python3 agentenv_monitor.py \
  --path <aenv目录> \
  --interval 2 \
  --output-dir <目录>
```


如果安装了 `aenv` CLI，运行期间可在另一个终端观察沙箱：

```bash
watch -n 0.2 'date; aenv list --output table'
```

或者只统计状态：

```bash
watch -n 2 "aenv list --output table | awk '/running/ {r++} /paused/ {p++} END {printf \"Running: %d\nPaused: %d\n\", r+0, p+0}'"
```

## 8. 返回码

| 返回码 | 含义 |
|---:|---|
| `0` | 检查成功，或全部回放命令成功 |
| `1` | 回放完成，但存在失败命令；或检查未通过 |
| `2` | 未被单任务隔离逻辑捕获的全局异常，例如顶层参数、配置、批处理初始化或调度器异常 |
| `130` | 用户中断 |

批量模式会把单条轨迹的 action 失败及多数 AgentENV/基础设施异常记录为失败任务，
不会阻止其他轨迹继续执行；只要批次中存在失败，批量命令最终返回 1。

## 9. 常见问题

### `Invalid API key format`

新版 E2B SDK 会在请求发出前校验 key 格式。设置部署方提供的 key，或在本地无
鉴权环境中使用第 4 节的格式化占位值。

### `template_exists: false`

检查 template 名称，并确认其状态为 `ready`：

```bash
aenv template list
```

### All connection attempts failed

客户端无法建立 TCP 连接，通常表示 AgentENV 服务未启动、监听地址不正确或服务端
发生异常。

![All connection attempts failed](image/All%20connection%20attempts%20failed.png)

### `ERROR Replay failed: ReadTimeout`

准备执行下一条命令前，请求长时间没有返回，回放被终止。

![Replay ReadTimeout](image/ERROR%20Replay%20failed:%20ReadTimeout.png)

先检查是否存在残留 sandbox、Firecracker 进程或 ublk 设备：

```bash
echo "sandbox=$(aenv list --output json | jq length)"
echo "firecracker=$(ps -C firecracker -o stat= | grep -vc '^Z' || true)"
echo "zombie=$(ps -C firecracker -o stat= | grep -c '^Z' || true)"
echo "ublk=$(find /sys/class/ublk-char -mindepth 1 -maxdepth 1 | wc -l)"
curl -sS -o /dev/null -w 'health=%{http_code}\n' http://127.0.0.1:8000/health
```

> **危险操作：** 以下清理命令会删除 AgentENV 中登记的全部 sandbox、停止服务并
> 强制结束 Firecracker/ublk 进程。只可在专用、隔离的测试主机上执行。执行前必须
> 确认该 AgentENV 实例没有其他用户、任务或生产工作负载，并逐项核对进程匹配条件。

```bash
# 1. 删除 AgentENV 登记的所有 sandbox
aenv list --output json \
  | jq -r '.[].sandboxID' \
  | xargs -r -n1 -P20 aenv delete

# 2. 停止 AgentENV server
server_pid=$(pgrep -f '^./target/release/server --config config/arm-config.toml$')
[ -n "$server_pid" ] && kill -TERM "$server_pid"
sleep 20
[ -n "$server_pid" ] && kill -0 "$server_pid" 2>/dev/null && kill -KILL "$server_pid"

# 3. 停止 ublk daemon【正常会自动清理，不用执行】
pkill -TERM -f '[u]vm-ublk-daemon' || true
sleep 5
pkill -KILL -f '[u]vm-ublk-daemon' || true

# 4. 清理残留 Firecracker【正常会自动清理，不用执行】
pkill -KILL -f '^/var/lib/aenv/deps/firecracker/.*/firecracker --api-sock /var/lib/aenv/firecracker-work/' || true

# 5. 删除失效的 daemon socket【正常会自动清理，不用执行】
rm -f /run/aenv/ublk-daemon.sock
```

## 10. 过载基准测试

两个基准测试驱动脚本位于 `scripts/` 目录，用于验证 AgentENV 在不同负载模式下的
性能、CPU 利用率和内存占用。

### 10.1 COW（无生命周期）场景

沙箱创建后全程不卸载，脏页通过内核 COW 保留在匿名页。

```bash
# 单后端
sudo DUAL_BACKEND=false bash /path/to/start-server.sh

# 运行 COW 基准测试
python3 scripts/run_no_lifecycle_oversubscription_benchmark.py \
  --running-concurrency 8 \
  --ratios 1,2,4,8 \
  --repeats 2 \
  --trajectory-dir ./delay_time_trajectories \
  --api-url http://127.0.0.1:8000 \
  --command-transport aenv-proxy \
  --cgroup-name aenv \
  --cpu-range 0-159 \
  --observability \
  --observability-interval-sec 1.0 \
  --output-root ./runs/cow-$(date +%Y%m%d-%H%M%S)
```

### 10.2 Pause-Resume 场景

沙箱每个 action 后 pause → delete → sleep → resume，睡眠期间 VM 完全卸载。

```bash
# 单后端
sudo DUAL_BACKEND=false bash /path/to/start-server.sh

# 双后端
sudo DUAL_BACKEND=true bash /path/to/start-server.sh

# 运行 pause-resume 基准测试
python3 scripts/run_pause_resume_oversubscription_benchmark.py \
  --running-concurrency 8 \
  --ratios 1,2,4,8 \
  --repeats 2 \
  --trajectory-dir ./delay_time_trajectories \
  --api-url http://127.0.0.1:8000 \
  --command-transport aenv-proxy \
  --cgroup-name aenv \
  --cpu-range 0-159 \
  --pause-duration-sec 0 \
  --observability \
  --observability-interval-sec 1.0 \
  --output-root ./runs/pause-resume-$(date +%Y%m%d-%H%M%S)
```

### 10.3 Noise-Fill 恒定压力模式

默认情况下，轨迹长短不一时短轨迹完成后并发数会下降。`--noise-fill` 启用后，
先完成的 worker 立即启动 noise VM 保持恒定并发压力，直到该轮所有真实任务完成。

- **COW 场景**: `replay_agent_no_lifecycle_noise.py`，noise VM 走 create→command→delete
- **Pause-Resume 场景**: `replay_agent_noise.py`，noise VM 走完整生命周期 create→init→pause→resume→command→pause→delete

`--rollout N` 控制每个 worker 执行几轮真实轨迹，轮间用 noise VM 填充。

```bash
# COW + noise-fill
python3 scripts/run_no_lifecycle_oversubscription_benchmark.py \
  --running-concurrency 8 --ratios 1,2,4,8 \
  --cgroup-name aenv --cpu-range 0-159 \
  --noise-fill --rollout 1 \
  --output-root ./runs/cow-noise-$(date +%Y%m%d-%H%M%S)

# Pause-Resume + noise-fill (单后端)
sudo DUAL_BACKEND=false bash /path/to/start-server.sh
python3 scripts/run_pause_resume_oversubscription_benchmark.py \
  --running-concurrency 8 --ratios 1,2,4,8 \
  --cgroup-name aenv --cpu-range 0-159 \
  --noise-fill --rollout 1 \
  --output-root ./runs/pr-noise-$(date +%Y%m%d-%H%M%S)

# Pause-Resume + noise-fill (双后端)
sudo DUAL_BACKEND=true bash /path/to/start-server.sh
python3 scripts/run_pause_resume_oversubscription_benchmark.py \
  --running-concurrency 8 --ratios 1,2,4,8 \
  --cgroup-name aenv --cpu-range 0-159 \
  --noise-fill --rollout 1 \
  --output-root ./runs/dual-pr-noise-$(date +%Y%m%d-%H%M%S)
```

### 10.4 三组对比实验

```bash
# Group A: 单后端 + COW
sudo DUAL_BACKEND=false bash /path/to/start-server.sh
python3 scripts/run_no_lifecycle_oversubscription_benchmark.py \
  --running-concurrency 8 --ratios 1,2,4,8 --cgroup-name aenv \
  --output-root ./runs/groupA-cow

# Group B: 单后端 + pause-resume
# server 已是单后端, 无需重启
python3 scripts/run_pause_resume_oversubscription_benchmark.py \
  --running-concurrency 8 --ratios 1,2,4,8 --cgroup-name aenv \
  --output-root ./runs/groupB-pause-resume

# Group C: 双后端 + pause-resume
sudo DUAL_BACKEND=true bash /path/to/start-server.sh
python3 scripts/run_pause_resume_oversubscription_benchmark.py \
  --running-concurrency 8 --ratios 1,2,4,8 --cgroup-name aenv \
  --output-root ./runs/groupC-dual-pause-resume
```

### 10.5 参数说明

| 参数 | 默认 | 说明 |
|------|------|------|
| `--running-concurrency` | (必填) | 基准并发数 N, 也可用 `N_RUNNING` 环境变量 |
| `--ratios` | `1,2,...,12` | 过载倍数 k, 每组运行 k×N 个任务 |
| `--repeats` | 2 | 每组重复次数 |
| `--api-url` | `$E2B_API_URL` 或 `http://127.0.0.1:8000` | AgentENV API 地址 |
| `--api-key-file` | `/var/lib/aenv/secrets/api-key` | API Key 文件路径 |
| `--command-transport` | `aenv-proxy` | 命令传输: `aenv-proxy` 或 `direct-envd` |
| `--direct-envd-port` | 49983 | direct-envd 模式端口 |
| `--cgroup-name` | (空) | cgroup 名称, 传入后 spotbox_monitor 采集 cgroup CPU/内存 |
| `--cgroup-version` | `auto` | cgroup 版本: `auto`/`v1`/`v2` |
| `--cpu-range` | (空) | CPU 核心范围, 如 `0-159`, 配合 cpuset 使用 |
| `--noise-fill` | 关 | 完成真实任务后启动 noise VM 保持恒定并发压力 |
| `--rollout` | 1 | 真实任务轮数，仅 `--noise-fill` 时生效 |
| `--pause-duration-sec` | 0 | pause-resume 专用: pause 后额外等待秒数 |
| `--extra-delay-sec` | 0 | 每个 action 前额外等待秒数 |
| `--delay` | (空) | 覆盖轨迹中的 delay_time 为固定值 |
| `--stop-on-error` | 关 | 命令返回非零时停止该轨迹 |
| `--cooldown-sec` | 30 (PR) / 180 (COW) | 试验间冷却秒数 |
| `--observability` | 开 | 是否运行 spotbox_monitor |
| `--observability-interval-sec` | 1.0 | 监控采样间隔秒 |
| `--output-root` | 自动 | 输出目录 |

### 10.6 外部监控脚本

`scripts/observability/spotbox_monitor.py` 是独立的 1Hz 监控脚本, 采集:

- **整机 CPU**: `/proc/stat` 聚合, 输出 `cpu_pct` + per-category cores
- **整机内存**: `/proc/meminfo`, 输出 `mem_used_gb` / `mem_cached_gb` / `mem_dirty_mb`
- **cgroup CPU**: `cpuacct.usage` (v1) 或 `cpu.stat` (v2), 输出 `cgroup_cpu_cores`
- **cgroup 内存**: `memory.usage_in_bytes` (v1) 或 `memory.current` (v2), 输出 `cgroup_mem_usage_gb` / `cgroup_mem_rss_gb` / `cgroup_mem_cache_gb`
- **Firecracker 进程**: PID 数 + 总 RSS + per-NUMA 分布 + CPU cores
- **磁盘 IO**: per-device 读/写 MB/s + util%
- **沙箱事件**: created/paused/resumed/killed 事件流 + 快照大小

```bash
# 独立运行 (无需 e2b SDK 时不采集沙箱事件)
python3 scripts/observability/spotbox_monitor.py \
  ./monitor-output \
  1.0 \
  aenv \
  auto \
  0-159

# 参数: <output_dir> [interval_s] [cgroup_name] [cgroup_version] [cpu_range]
```

输出文件:
- `metrics_1s.jsonl`: 每秒一行 JSON, 包含所有指标
- `sandbox_events.jsonl`: 沙箱生命周期事件 (需 e2b SDK)

## 11. Docker 环境构建与任务初始化

以下命令均从 `replay-aenv` 仓库根目录执行。

### 10.1 构建基础镜像

```bash
docker build \
  --progress=plain \
  --build-arg PIP_INDEX_URL=https://mirrors.aliyun.com/pypi/simple \
  --build-arg PIP_DEFAULT_TIMEOUT=300 \
  -f dockerfiles/base-image/Dockerfile.base \
  -t harbor-swe-tools:base \
  dockerfiles/base-image
```

### 10.2 构建任务镜像

```bash
docker build \
  --progress=plain \
  --build-arg BASE_IMAGE=harbor-swe-tools:base \
  --build-arg TARGET_REPO=https://gh-proxy.com/https://github.com/django-money/django-money.git \
  --build-arg TARGET_COMMIT=835c1ab8 \
  --build-arg PIP_INDEX_URL=https://mirrors.aliyun.com/pypi/simple \
  --build-arg PIP_DEFAULT_TIMEOUT=300 \
  -f dockerfiles/django-money_task/environment/Dockerfile \
  -t django-money:poc_v2 \
  dockerfiles/django-money_task
```

### 10.3 验证任务镜像

```bash
docker run --rm django-money:poc_v2 \
  bash -lc 'source /opt/sweagent/activate-tools.sh; cd /testbed; git rev-parse HEAD; command -v str_replace_editor; python --version; pytest --version'
```

### 10.4 推送镜像并创建 AgentEnv 模板

```bash
docker tag django-money:poc_v2 127.0.0.1:5000/django-money:poc_v2
docker push 127.0.0.1:5000/django-money:poc_v2

aenv pull \
  --name django-money-task-v2 \
  --cpu 1 \
  --memory 2048 \
  --timeout 1800 \
  127.0.0.1:5000/django-money:poc_v2

aenv template list
```

重复创建模板时应使用新的模板名称，避免继续引用旧镜像。

### 10.5 删除容器或镜像

`docker rm` 删除容器，参数必须是容器 ID 或容器名：

```bash
docker ps -a
docker rm <container_id_or_name>
```

删除镜像使用 `docker rmi`：

```bash
docker images
docker rmi django-money:poc_v2
```

删除前应确认没有模板或容器仍在引用该镜像。

### 10.6 准备任务初始状态补丁

每个 SWE-smith 任务都有自己的缺陷代码状态。镜像只有干净代码时，轨迹中的
`str_replace_editor` 可能找不到 `old_str`。

任务补丁来自 [SWE-bench/SWE-smith](https://huggingface.co/datasets/SWE-bench/SWE-smith/viewer/default/train)。
[SWE-smith-trajectories](https://huggingface.co/datasets/SWE-bench/SWE-smith-trajectories/viewer/default/tool)
用于提供轨迹及对应的 `instance_id`。脚本根据 `delay_time_trajectories` 中实际存在的
任务动态提取补丁，不限制任务数量。

补丁保存为：

```text
instance_overlays/<instance_id>/initial.patch
```

#### 10.6.1 下载、提取和验证

```bash
# 下载固定版本的官方任务数据
scripts/swesmith/download_swe_smith_instances.sh

# 提取 delay_time_trajectories 中全部任务的补丁
# Python 环境需要安装 pyarrow
python scripts/swesmith/extract_swesmith_initial_patches.py

# 在任务镜像中验证补丁
docker run --rm --entrypoint /bin/bash \
  -v "$PWD/instance_overlays:/instance_overlays" \
  -v "$PWD/scripts/swesmith/validate_swesmith_initial_patches.py:/validate.py:ro" \
  django-money:poc_v2 \
  -lc 'python /validate.py --repo /testbed --overlay-dir /instance_overlays'
```

三个脚本的职责分别是：

| 脚本 | 功能 |
|---|---|
| `download_swe_smith_instances.sh` | 下载固定版本的 SWE-smith 任务数据 |
| `extract_swesmith_initial_patches.py` | 按轨迹中的 `instance_id` 提取初始缺陷补丁 |
| `validate_swesmith_initial_patches.py` | 在干净基础提交上检查并应用每份补丁 |

#### 10.6.2 回放时自动应用补丁

无需手动执行。`ReplayRunner._initialize_instance_state()` 会在创建 sandbox 后、
执行第一条 action 前自动完成：

1. 从轨迹取得 `instance_id`；
2. 读取对应的 `manifest.json` 和 `initial.patch`；
3. 校验补丁 SHA-256 和基础提交；
4. 上传、检查并应用补丁；
5. 核对变更文件后开始回放。

默认补丁目录为 `instance_overlays`。使用其他目录时指定：

```bash
python agent/replay_agent.py batch \
  --trajectory-dir delay_time_trajectories \
  --instance-overlay-dir <补丁目录>
```

补丁缺失或校验失败时，任务会在执行轨迹前失败。只有明确不需要初始补丁时才使用
`--skip-instance-patch`。
