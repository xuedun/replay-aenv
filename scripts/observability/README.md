# AgentEnv 性能观测工具

回放压测的采集 + 报告工具链。整个流程分四步:**启动采集 → 跑回放 → 停采集 → 出报告**。

## 0. 准备

```bash
# 连接信息(见仓库根 README 第 4 节)
export E2B_API_URL=http://<agentenv-ip>:8000
export E2B_API_KEY=...
export E2B_SANDBOX_URL=$E2B_API_URL E2B_ACCESS_TOKEN=$E2B_API_KEY

# 依赖:采集要 e2b SDK(仓库 .venv 已有);xlsx 报告要 openpyxl;画图要 matplotlib
uv pip install -e .
uv pip install openpyxl matplotlib
```

## 0.1 创建 cgroup 并绑定 AgentENV server

压测前在 AgentENV 机器上创建 cgroup，限制 CPU 和内存，并将 server 进程绑定到
cgroup。server fork 出的 firecracker / ublk-daemon 子进程会自动继承 cgroup。

以 128 核 / 512GB 为例：

### cgroup v1

```bash
# 创建 cgroup
sudo mkdir -p /sys/fs/cgroup/cpu,cpuacct/aenv
sudo mkdir -p /sys/fs/cgroup/memory/aenv

# CPU 限制: 128 核
#   cfs_period_us = 100000 (100ms, 调度周期)
#   cfs_quota_us  = 12800000 (128 * 100000, 每 100ms 可用 12.8s CPU)
sudo sh -c 'echo 100000  > /sys/fs/cgroup/cpu,cpuacct/aenv/cpu.cfs_period_us'
sudo sh -c 'echo 12800000 > /sys/fs/cgroup/cpu,cpuacct/aenv/cpu.cfs_quota_us'

# 内存限制: 512GB = 549755813888 bytes
sudo sh -c 'echo 549755813888 > /sys/fs/cgroup/memory/aenv/memory.limit_in_bytes'

# 启动 server 并绑定
sudo /path/to/server --config config/default.toml &
SERVER_PID=$!
sudo sh -c "echo $SERVER_PID > /sys/fs/cgroup/cpu,cpuacct/aenv/cgroup.procs"
sudo sh -c "echo $SERVER_PID > /sys/fs/cgroup/memory/aenv/cgroup.procs"

# 验证
cat /sys/fs/cgroup/cpu,cpuacct/aenv/cpu.cfs_quota_us   # 12800000
cat /sys/fs/cgroup/memory/aenv/memory.limit_in_bytes   # 549755813888
cat /sys/fs/cgroup/cpu,cpuacct/aenv/cgroup.procs       # server PID

# 子进程 (firecracker, ublk-daemon) 自动继承，无需单独绑定
```

### cgroup v2

```bash
# 创建 cgroup (统一层级)
sudo mkdir -p /sys/fs/cgroup/aenv

# 启用 controller (如果未启用)
sudo sh -c 'echo "+cpu +memory" > /sys/fs/cgroup/cgroup.subtree_control' 2>/dev/null || true

# CPU 限制: 128 核
#   格式: "quota period"  →  "12800000 100000"
sudo sh -c 'echo "12800000 100000" > /sys/fs/cgroup/aenv/cpu.max'

# 内存限制: 512GB
sudo sh -c 'echo 549755813888 > /sys/fs/cgroup/aenv/memory.max'

# 启动 server 并绑定
sudo /path/to/server --config config/default.toml &
SERVER_PID=$!
sudo sh -c "echo $SERVER_PID > /sys/fs/cgroup/aenv/cgroup.procs"

# 验证
cat /sys/fs/cgroup/aenv/cpu.max        # 12800000 100000
cat /sys/fs/cgroup/aenv/memory.max     # 549755813888
cat /sys/fs/cgroup/aenv/cgroup.procs   # server PID
```

### 启动时直接绑定（无需先启动再写 PID）

#### 方式 A：systemd-run（推荐）

```bash
sudo systemd-run --slice=aenv --unit=aenv-server \
  --property="CPUQuota=12800%" \
  --property="MemoryMax=549755813888" \
  --property="AllowedCPUs=0-127" \
  /path/to/server --config config/default.toml
```

systemd 自动创建 cgroup，进程启动即绑定。后续管理：
```bash
systemctl status aenv-server    # 查看状态
systemctl stop aenv-server      # 停止
journalctl -u aenv-server       # 查看日志
```

#### 方式 B：shell exec（无 systemd）

```bash
# 先创建 cgroup 并设置限制
sudo mkdir -p /sys/fs/cgroup/aenv
sudo sh -c 'echo "+cpu +memory +cpuset" > /sys/fs/cgroup/cgroup.subtree_control'
sudo sh -c 'echo "12800000 100000" > /sys/fs/cgroup/aenv/cpu.max'
sudo sh -c 'echo 549755813888 > /sys/fs/cgroup/aenv/memory.max'
sudo sh -c 'echo "0-127" > /sys/fs/cgroup/aenv/cpuset.cpus'

# 启动即绑定: shell 写自己的 PID 到 cgroup, 然后 exec 替换为 server
sudo sh -c 'echo $$ > /sys/fs/cgroup/aenv/cgroup.procs && exec /path/to/server --config config/default.toml'
```

原理：`sh` 先把 PID 写入 `cgroup.procs`，然后 `exec` 用 server 替换自己。
`exec` 不改变 cgroup 归属，server 自动继承。子进程（firecracker、ublk-daemon）同样继承。

### 计算公式

| 资源 | v1 | v2 | 计算 |
|------|-----|-----|------|
| CPU N 核 | `cpu.cfs_quota_us = N × 100000` | `cpu.max = "N*100000 100000"` | quota = N × period |
| 内存 M GB | `memory.limit_in_bytes = M × 1073741824` | `memory.max = M × 1073741824` | M × 1024³ |

### 绑定指定 CPU 核 (cpuset)

除了限制 CPU 配额，还可以用 `cpuset` 把进程绑定到指定物理核，独占这些 CPU：

#### v1

```bash
# 创建 cpuset cgroup
sudo mkdir -p /sys/fs/cgroup/cpuset/aenv

# 绑定 CPU 0-127
sudo sh -c 'echo "0-127" > /sys/fs/cgroup/cpuset/aenv/cpuset.cpus'

# 绑定 NUMA 内存节点 (用 numactl --hardware 查看节点数)
sudo sh -c 'echo 0 > /sys/fs/cgroup/cpuset/aenv/cpuset.mems'

# 绑定 server 进程
sudo sh -c "echo $SERVER_PID > /sys/fs/cgroup/cpuset/aenv/cgroup.procs"
```

#### v2

```bash
# 确认内核支持 cpuset (>= 5.13)
grep cpuset /sys/fs/cgroup/cgroup.controllers

# 启用 cpuset controller
sudo sh -c 'echo "+cpuset" > /sys/fs/cgroup/cgroup.subtree_control'

# 绑定 CPU 0-127
sudo sh -c 'echo "0-127" > /sys/fs/cgroup/aenv/cpuset.cpus'

# 绑定 NUMA 内存节点
sudo sh -c 'echo 0 > /sys/fs/cgroup/aenv/cpuset.mems'

# 绑定 server 进程
sudo sh -c "echo $SERVER_PID > /sys/fs/cgroup/aenv/cgroup.procs"
```

#### cpuset vs cfs_quota

| 方式 | 效果 | 适用场景 |
|------|------|---------|
| `cpuset.cpus` | 进程**只能**在指定 CPU 上运行 | 独占 CPU，避免与其他进程争抢 |
| `cpu.max` / `cfs_quota` | 进程可用 CPU **时间配额**限制 | 限制总量但不绑核 |

两者可组合：绑核 0-127 + 配额 128 核 = 这 128 个核被 AgentENV 独占。

### 检测当前系统使用哪个版本

```bash
# v2: 输出 cgroup2fs
# v1: 输出 tmpfs
stat -fc %T /sys/fs/cgroup/

# v2 有统一文件:
ls /sys/fs/cgroup/cgroup.controllers 2>/dev/null && echo "v2"

# v1 有分层挂载:
ls /sys/fs/cgroup/cpu,cpuacct/ 2>/dev/null && echo "v1"
```

### benchmark 命令加 cgroup 采集

```bash
# v1
--cgroup-name aenv --cgroup-version v1

# v2
--cgroup-name aenv --cgroup-version v2

# 自动检测 (默认)
--cgroup-name aenv
```

## 1. 启动采集

```bash
OBS=obs-$(date +%Y%m%d__%H%M%S); python3 scripts/observability/spotbox_monitor.py $OBS &
MON_PID=$!
```

一条命令:目录自动创建,$OBS 留给第 4 步用。

- 默认 1Hz;想看亚秒级 pause 窗口在末尾加采样间隔,如 `... $OBS 0.1 &`
- 采集内容:主机 CPU/内存/脏页/磁盘 IO、firecracker/server/ublk CPU、
  沙箱生命周期事件、每次 pause 的快照大小(增量口径)
- 产物:`$OBS/metrics_1s.jsonl` + `$OBS/sandbox_events.jsonl`

## 2. 跑回放

按仓库根 README 跑就行(本仓库 replay agent 或 harbor 都可以),把输出目录记住:

- 本仓库 agent → 输出目录里有 `replay-result.json` / `run.log` / `job.log`
- harbor → job 目录(含 `config.json`)或 `aenv-replay-batch-*` 批量目录

## 3. 停采集

```bash
kill -INT $MON_PID; wait $MON_PID
```

必须 SIGINT/SIGTERM 优雅停止,否则最后一段数据丢失。

## 4. 出报告

```bash
# xlsx 表格报告(8 个 sheet 含折线图)——最常用
python3 scripts/observability/make_obs_xlsx.py $OBS <回放输出目录或父目录>
# 产物: $OBS/report.xlsx

# markdown 报告,参数一样
python3 scripts/observability/make_obs_report.py $OBS <回放输出目录或父目录>

# pause/resume 分阶段资源分析(直接吃日志,不需要目录结构)
python3 scripts/observability/analyze_pause_resume.py $OBS --runlog <日志文件|目录> --plot
```

`<回放输出目录>` 三种给法都可以:

| 给法 | 效果 |
|---|---|
| 单个输出目录(含 `replay-result.json`) | 单任务报告 |
| 父目录(子目录各含 `replay-result.json`)/ harbor 批量目录 | 跨任务汇总报告 |
| harbor job 目录(含 `config.json`) | 单任务报告(harbor 结构) |

## 各工具一句话说明

| 文件 | 作用 |
|---|---|
| `spotbox_monitor.py` | 采集器(第 1 步) |
| `make_obs_xlsx.py` | xlsx 报告:概览/每步耗时/任务汇总/主机资源/磁盘IO/沙箱事件/pause快照/甘特图/并发状态 |
| `make_obs_report.py` | 同上的 markdown 版 |
| `analyze_pause_resume.py` | pause/paused/resume/exec 分阶段主机资源分析 + 甘特图;日志格式自动识别(当前 `Agent interaction timing` 行、旧 jobs.txt、harbor run.log 三种) |
| `probe_snapshot_generations.py` | 单沙箱逐代快照文件清单,诊断 overlaybd 层 compaction |

## 注意事项

- 无 harbor 的输出目录没有 verifier,报告里 reward 显示 None,属正常
- 基准测试前如需干净基线,清空 `/var/lib/aenv/persisted-sandboxes` 的
  artifacts + records.db 并重启 AgentEnv server
- monitor 的 `ublk_devices` 只涨不降(空闲池不回收),反映历史峰值不是当前占用
- 采集与回放时间段不重叠时,分析报告降级为纯日志时长记录(不报错)
- **running 窗口小于轮询间隔时相邻 pause 会被合并漏捕**:exec 仅 ~0.3s 的任务,
  1Hz 轮询常看不到 running 窗口,两次 pause 合成一个事件(实测 44 次捕 18 次),
  `pause_seq` ≠ replay step index。逐步对齐必须用 `replay-result.json` 的时间戳
- **新代写入中间态会造成假 snapshot_files 骤降**:代目录创建后文件仍在写入/hardlink
  时被抓到,会呈现 files=2 / inherited=0,看起来像 compaction。判真 drop 的条件:
  inherited 同时坍塌**且**后续代从低点重新爬坡;逐代权威清单用
  `probe_snapshot_generations.py` 在 pause 返回后读,不要依赖 monitor 瞬时值
- snapshot compaction 的 pause 时耗实测与机制详见
  [snapshot-compaction-notes.md](snapshot-compaction-notes.md)
