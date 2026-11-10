# replay-aenv 代码仓完整梳理

> 分析对象：`/home/cxd/replay-aenv-main`（git remote：`git@github.com:xuedun/replay-aenv.git`）
> 分析日期：2026-11-06。基于对全部 17k 行 Python 代码、21 个本地提交、未跟踪文件与 `runs/` 产物的完整阅读。

---

## 1. 仓库定位

replay-aenv 是 **AgentENV（`/home/cxd/AgentENV` Rust 工作区）的客户端压测/回放工具**：

- 通过 AgentENV 暴露的 **E2B 兼容 API**（`AsyncSandbox.create / connect / beta_pause / commands.run / files.write`）驱动沙箱；
- 读取 SWE-smith / Open-SWE-Traces 格式的**轨迹 JSON**（`trajectory[].{action, delay_time, response, thought}`），在沙箱内逐条重放 shell action；
- 用**过载基准（oversubscription benchmark）**衡量 AgentENV 在 pause/resume、COW、快照、代理读等不同生命周期策略下的吞吐、时延与资源占用；
- 自带 1Hz 主机/cgroup 观测（spotbox_monitor）与报表（xlsx/markdown/甘特图）链路。

一句话：**"轨迹 = 负载模型，agent 变体 = 生命周期策略，scripts = 实验矩阵，observability = 测量仪"**。

---

## 2. 目录结构总览

```text
replay-aenv-main/
├── agent/                          # 核心：回放 agent 与变体（约 15k 行）
│   ├── replay_agent.py             # ① 主变体：pause-resume 生命周期（2828 行）
│   ├── replay_agent_no_lifecycle.py       # ② COW：全程 RUNNING，不 pause（2838 行）
│   ├── replay_agent_noise.py              # ②' ① + noise-fill 恒定压力（2848 行）
│   ├── replay_agent_no_lifecycle_noise.py # ②'' ② + noise-fill（2932 行，原始自带）
│   ├── replay_agent_snapshot.py           # ③ 快照链：exec→snapshot→kill→从快照重建（3166 行）
│   ├── replay_agent_snapshot_same_sandbox.py # ③' 单沙箱反复快照（326 行）
│   ├── replay_agent_proxy.py              # ④ ① + 只读步骤走 proxy-read（516 行，用户新增）
│   ├── replay_step_snapshots.py           # ⑤ 每步落一个命名快照（654 行，未跟踪）
│   ├── control_plane_scheduler.py         # 全局 QPS 限速器（pause/resume 口径）
│   ├── control_plane_scheduler_snapshot.py# 同上（snapshot/reload 口径）
│   ├── run_slot_scheduler.py              # RUNNING 槽位 FIFO 准入调度器
│   └── mix-config.example.json            # 多模板混跑配置样例
├── scripts/
│   ├── run_pause_resume_oversubscription_benchmark.py   # PR 过载基准驱动（1765 行）
│   ├── run_no_lifecycle_oversubscription_benchmark.py   # COW 过载基准驱动（1828 行）
│   ├── run_proxy_read_oversubscription_benchmark.py     # proxy-read 基准（196 行，猴子补丁复用 PR）
│   ├── run_k_task_degradation_benchmark.py             # k 并发退化曲线（1072 行）
│   ├── process_results.py / summarize_vm_runs.py / summarize_timing_percentiles.py
│   ├── summarize_snapshot_metrics.py / plot_trajectory_completion_time.py
│   ├── extract_ready_vcpu_timeseries.py / verify_cpu_metrics.py
│   ├── observability/               # spotbox_monitor / host_cgroup_monitor / 报表 3 件套 / 快照探针
│   ├── swesmith/                    # SWE-smith 任务数据下载、初始补丁提取与校验
│   └── swerebench-validate/         # ARM 镜像端到端可用性验证流水线
├── dockerfiles/                     # base-image（SWE-agent 工具集）+ django-money 任务镜像
├── other_replay/                    # browser / pdf 两套非 SWE 轨迹样例
├── runs/                            # 基准运行产物（4 个 N=1 的本地验证运行）
├── image-map.json                   # 未跟踪：轨迹→模板映射（给 ⑤ 用）
├── pyproject.toml / uv.lock         # Python 3.12，唯一依赖 e2b SDK
└── README.md                        # 使用手册（742 行，含 10.x 基准章节）
```

---

## 3. 核心入口

### 3.1 安装型 CLI（`pyproject.toml [project.scripts]`）

| 命令 | 模块 | 生命周期语义 |
|---|---|---|
| `replay-aenv` | `agent.replay_agent:main` | 每条 action：paused → 等待 delay_time → 抢 RUNNING 槽 → resume → 执行 → pause → 释放槽 |
| `replay-aenv-no-lifecycle` | `agent.replay_agent_no_lifecycle:main` | create → init → 连续执行全部命令 → delete，全程 RUNNING（COW） |
| `replay-aenv-snapshot` | `agent.replay_agent_snapshot:main` | 每步：从上一快照重建沙箱 → 执行 → 快照+删除（checkpoint/reload 链） |
| `replay-aenv-snapshot-same-sandbox` | `agent.replay_agent_snapshot_same_sandbox:main` | 单沙箱每步后原地快照 |

### 3.2 `agent/replay_agent.py` 的子命令（所有变体共用同一套 argparse 结构）

- `run`：单轨迹回放（`run_single` → `ReplayRunner.run()`）
- `batch`：目录级批量（`--trajectory-dir --target-count --concurrency --running-concurrency`）
- `mix`：多 template/多镜像混合（`--config mix-config.json`，加权轮转交错调度，支持 `--dry-run`）
- `check`：探测 `/health` + `AsyncTemplate.alias_exists`

非安装型变体直接 `python agent/replay_agent_proxy.py ...` 等运行（pyproject 未注册）。

### 3.3 基准驱动（`scripts/`，均为独立 python 脚本）

| 脚本 | 驱动的 agent | 实验设计 |
|---|---|---|
| `run_pause_resume_oversubscription_benchmark.py` | `replay_agent.py`（`--noise-fill` 时 `replay_agent_noise.py`） | 每 ratio k：k×N 个任务跑在 N 个 RUNNING 槽上（1:k 过载） |
| `run_no_lifecycle_oversubscription_benchmark.py` | `replay_agent_no_lifecycle.py`（noise 时 `_noise`） | k×N 任务 × k×N 槽（纯并发放大，M=`--total-multiplier`） |
| `run_proxy_read_oversubscription_benchmark.py` | `replay_agent_proxy.py` | 与 PR 完全同构，只读步骤免 resume |
| `run_k_task_degradation_benchmark.py` | `.venv/bin/replay-aenv`（安装入口） | k 个任务挤 1 个槽，k=1 基线退化曲线 + 手写 SVG 图 |

驱动均通过 **subprocess 组装命令行**拉起 agent（`command_for_trial`），以 `mix-config.json` 下发负载，随后解析 `batch-summary.json` / `replay-result.json` 汇成 4 层 CSV。

### 3.4 观测链路入口

- `scripts/observability/spotbox_monitor.py <out_dir> [interval] [cgroup] [cg_ver] [cpu_range]` → `metrics_1s.jsonl` + `sandbox_events.jsonl`
- `make_obs_xlsx.py` / `make_obs_report.py` / `analyze_pause_resume.py`（`--runlog job.log --plot`）→ `report.xlsx` / `report.md` / `gantt.png` / pause-resume 相图
- 基准驱动会**自动**在每个 trial 里启停 monitor 并生成全部报表（`start_observability` / `generate_observability_reports`）。

---

## 4. 核心代码逻辑

### 4.1 `replay_agent.py`（主变体）执行模型

**轨迹解析**（`load_trajectory`）：读 JSON，容错提取 `instance_id`（字段缺失时从文件名剥后缀恢复），每步解析 `action + delay_time`。终态 action（submit/finish/done）之前的步骤才执行。

**ReplayRunner 生命周期**（`run()`，agent/replay_agent.py:1732）：

1. **初始化阶段**（先抢 RUNNING 槽，因 create 返回 RUNNING 沙箱）：
   `_create_sandbox → _probe_sandbox_ready(true 命令探测) → _validate_workdir → _initialize_instance_state(SWE-smith 缺陷补丁) → _pause_sandbox_strict`；
2. **逐步执行** `_run_slice`（agent/replay_agent.py:1410）——本仓最核心的时序抽象：
   ```
   paused → sleep(delay_time + pause_duration + extra_delay)     # 模拟 Agent 推理时延
         → RunningSlotScheduler.acquire(ready_at)               # FIFO 准入，超发排队=capacity_wait
         → resume（beta connect，经全局 QPS 队列）→ ready probe
         → commands.run（background=True 建流，等待异步完成）
         → pause → release RUNNING 槽
   ```
   每步产出一条 `Agent interaction timing step=i/n pause=.. delay=.. capacity_wait=.. resume=.. command=.. total=..s` 日志行（job.log + run.log + 控制台），是**所有下游分析脚本的统一数据契约**；
3. **失败语义**：命令非零只是"观测记录"，不影响任务成功（除非 `--stop-on-error`）；基础设施错误（`SandboxInfrastructureError`）、命令超时（`CommandTimeoutError` 可选 continue）才终止该任务；
4. **清理**（`_cleanup`）：paused 沙箱直接 kill；kill 失败且未 paused 时按"先 resume 再删"或 `_force_stop_for_running_release` 兜底，任何情况下不破坏 RUNNING 槽硬上限。

**关键安全设计（全部围绕"create 不幂等 + 槽位不超发"）**：

- 每个 runner 生成 `replay_create_key`（uuid）写入沙箱 metadata；create 超时是**二义性**的 → `_reconcile_ambiguous_create` 需连续两次成功查询无匹配才允许重试，宁可任务失败也不重复 create；
- `_create_state_uncertain` 期间 RUNNING 槽**不归还**，极端情况直接 `running_slot_scheduler.close()` 关闭准入；
- WaitReady 失败（双后端 resume 时 guest 崩溃）识别失败沙箱 ID → kill → **`os.kill(os.getpid(), SIGINT)` 自杀式停机**，触发全量 `cleanup_all_sandboxes`；
- pause/resume 为**无限重试 + 指数退避**（`min(2**attempt, 60)`），仅瞬时错误（502/503/504/429/timeout/reset 等 `is_transient_sandbox_error`）可重试。

**两个调度器**（batch 模式全局共享注入，单任务模式容量 1）：

- `ControlPlaneScheduler`（control_plane_scheduler.py）：**全局平滑 QPS 限速 + inflight 上限**。pause/resume/cleanup/create/command-start 五类操作同优先级 FIFO，"晚醒不补发"防止突发；只串行化命令**建流**，命令本体并发；
- `RunningSlotScheduler`（run_slot_scheduler.py）：**单调时间堆 + FIFO 的 RUNNING 槽准入**。`acquire(task_id, ready_at)` 支持延迟就绪（轨迹 delay 期间不占槽），租约（`RunningLease`）必须持到确认 paused/删除为止。

**批处理编排**（`run_batch`）：`--concurrency` 个 worker 从 asyncio.Queue 取任务（而非每任务一个协程，内存稳定）；`--launch-interval-sec` 防惊群启动；mix 模式用平滑加权轮转（`build_mixed_schedule`）按 `vm_count` 交错多 workload；WaitReady 异常触发 `fatal_event` 全局停机。

### 4.2 变体矩阵

| 变体 | 相对基线的差异 | 独有机制 |
|---|---|---|
| `no_lifecycle` | 删除 pause/resume；一条 RUNNING 槽持满全程 | **`LocalFirecrackerRouteResolver`**：扫描 `/proc/*/cmdline` 找 firecracker → 连其 `--api-sock` 读 MMDS `instanceID` → `nsenter -n ip addr show vpeer` + 主机路由表反解 envd 可达 IP，**本地构造 e2b 数据面客户端**（`_build_direct_data_sandbox`）绕过 AgentENV 代理（`--command-transport direct-envd`）；二义 create 一律**删除**不 reconnect |
| `noise`（PR） | 复制主变体 + worker 亲和 + rollout 循环 | 真实任务完成后立即起 noise VM（完整 PR 生命周期）填充并发，直至本轮全部真实任务结束；`--rollout N` 轮次，`rollout_end_times` 记入 batch-summary |
| `no_lifecycle_noise` | 同上（COW 口径，原始自带） | noise VM 走 create→command→delete |
| `snapshot` | 以 `AsyncSnapshot.create` / 以快照 ID 作 template 重建沙箱 | `_checkpoint_and_stop`（快照+删除）/ `_reload_snapshot_strict`（`stage=reload` 也过 QPS 队列）；HMAC-SHA256 直读 OSS catalog/工件做 `probe`（容量、层索引、别名解析）；`control_plane_scheduler_snapshot` 的操作类型换成 SNAPSHOT/RELOAD |
| `snapshot_same_sandbox` | 继承 snapshot，326 行薄层 | 单沙箱每步 `_create_snapshot_strict`，聚合 `snapshot_*` 与 S3 写入量指标 |
| `proxy`（用户新增） | 继承主变体，仅覆写 `_run_slice`（516 行） | 见 §5-C |
| `replay_step_snapshots`（未跟踪） | 独立小工具，不走 ReplayRunner 框架 | 见 §5-H |

### 4.3 基准驱动通用骨架（以 PR 驱动为例）

`main`（scripts/run_pause_resume_oversubscription_benchmark.py:1620）：

1. `validate_args` → N 必填（`N_RUNNING` 环境变量或 `--running-concurrency`）；
2. `selection_records`：按自然序取前 N 条轨迹，`--image-map`（JSON/CSV）或轨迹内嵌 `image/template` 字段解析每条轨迹的模板；
3. `prepare_selection`：把轨迹 symlink 到 `selected-trajectories/slot-NNNN/`，落 `trajectory-selection.{json,csv}`；
4. trial 循环（ratio × repeat）：`write_mix-config.json`（concurrency=k×N）→ `start_observability`（spotbox_monitor 子进程，等两个 jsonl 非空）→ Popen agent 子进程（stdout tee 进 `benchmark-driver.log`）→ `stop_observability`（SIGINT→30s→kill）→ `generate_observability_reports`；
5. 每个 trial 后即写 4 层输出（断点续跑安全）：`trial-summary.csv`（每 trial 一行）/ `ratio-summary.csv`（含对 k=1 的退化与吞吐增益百分比）/ `trajectory-detail.csv`（每任务）/ `step-detail.csv`（每步全时序字段）+ `benchmark-report.json`；
6. 有效性判定（`trial_from_summary`）：`total==k*N && succeeded==k*N && running_slots.maximum==N && peak_active<=N`。

**proxy-read 驱动的复用方式**（scripts/run_proxy_read_oversubscription_benchmark.py:58-148）：`importlib` 按文件路径加载 PR 驱动模块，仅猴子补丁 5 处——`RUN_NAME_PREFIX`、STEP/TASK 列追加 proxy 字段、`command_for_trial`（把命令行里的 agent 路径换成 `replay_agent_proxy.py` 并追加 `--proxy-read-*` 三个参数）、`collect_task_and_step_records`（按 `transport` 分桶算 `proxy_read_ratio`）、`build_parser`（新增参数组）。零代码复制。

### 4.4 观测链路

- **spotbox_monitor**（831 行，asyncio）：每秒聚合 /proc/stat（可限定 CPU 范围）/ /proc/meminfo / NUMA、firecracker 进程数+RSS（server 父进程 children 快路径）、磁盘 per-device IO、cgroup v1/v2 CPU+内存；1Hz 轮询沙箱列表差分出 created/paused/resumed/killed 事件；**`paused` 事件携带增量快照大小**——按 inode 首次归属代数去重，区分 unique-disk 与 hardlink 继承层；
- **host_cgroup_monitor**（用户新增，610 行）：零依赖同步版，无 e2b/无事件，额外输出扁平 `summary.csv`；
- **analyze_pause_resume**（982 行）：三代日志格式考古（v3 = `Agent interaction timing` 行自包含时间戳；v2 = jobs.txt 调度器行；legacy = lifecycle 行），把 monitor 采样按 pause/paused/resume/exec/gap 五相归桶，输出 markdown/xlsx/相图；
- **make_obs_report / make_obs_xlsx**：用 replay-result.json 的毫秒级步骤时间戳**修正** 1Hz monitor 漏采（约 40% 亚秒 pause 窗口），生成 8-9 个 sheet 的 xlsx + 甘特图 + 并发状态统计。

### 4.5 数据准备（dockerfiles + swesmith + swerebench）

- `dockerfiles/base-image/Dockerfile.base`：Ubuntu22.04 + SWE-agent 全套 shell 工具（str_replace_editor/filemap/search/registry 等）装进 `/opt/sweagent`，生成 `activate-tools.sh`；`django-money_task/` 在其上克隆仓库并内置 `writeTxt`（精确写 N 个 1MiB 高熵文件，用于快照增长测量）；
- `scripts/swesmith/`：下载固定版本 SWE-smith parquet → 按轨迹 instance_id 提取**缺陷初始状态补丁**到 `instance_overlays/<id>/`（manifest 含 base_commit/sha256/changed_files）→ 回放时 `_initialize_instance_state` 自动校验并应用（SHA-256 + 基础提交 + 变更文件三重校验）；
- `scripts/swerebench-validate/`：ARM 镜像可用性流水线（podman pull→arch 检查→推本地 registry→`aenv pull` 建模板→按 commit 反查任务与官方轨迹→转换→`replay_agent.py run` 验证 PASS/FAIL）。

---

## 5. 你的关键改动（21 个本地提交 + 2 个未跟踪文件）

**基线说明**：git 历史 21 个提交全部为本人（c00918769）于 2026-10-13 ~ 10-28 完成；root 提交 `62ed491` 是"原始拉取的代码 + 第一批功能"的一次性导入，其提交信息明确列出了相对原始快照的新增内容。据此可判定：**原始仓库** = replay_agent.py（基础 PR 版）、no_lifecycle(+noise)、snapshot(+same_sandbox) 四个生命周期变体、两个调度器、PR/COW/k-task 三个基准驱动、observability 三件套、swesmith/swerebench、dockerfiles、other_replay；**其余全部为你的增量**。

### A. 根提交 `62ed491`：noise 压力模式 + 命令通道/时延参数

- 新增 `agent/replay_agent_noise.py`（PR 口径 noise-fill：worker 亲和 + rollout 轮次 + 常驻并发压力）；
- `replay_agent.py` 新增 `--command-transport`（aenv-proxy/direct-envd）、`--direct-envd-port`、`--extra-delay-sec`、`--delay`（覆盖轨迹 delay_time）；
- PR 基准驱动同步 COW 版特性（`--cgroup-name --cpu-range --api-url --noise-fill --rollout` 等）；
- 新增零依赖 `scripts/observability/host_cgroup_monitor.py`；README 补基准/noise/监控文档。

### B. 生命周期健壮性系列（10-13，7 个提交）

| 提交 | 内容 |
|---|---|
| `264c7b2` | Ctrl-C（KeyboardInterrupt）时全量 `cleanup_all_sandboxes`，防止残留沙箱拖垮下一轮 |
| `bd367e0` | `_pause_sandbox_strict` 补上与 resume 对称的重试逻辑（此前 pause 一次失败即抛） |
| `cf1a56d` | pause/resume 改为**无限重试 + 指数退避**（瞬时错误才重试，日志带 retry_mode=rate_limit） |
| `dacdcbb`→`249e307`→`1bd5811` | WaitReady 失败（双后端 resume 崩溃）三步收敛：识别失败 VM 并 kill → 全体 worker 立即停机 → **向自身发 SIGINT** 让整个进程（含驱动层 Ctrl-C 分支）走统一清理路径 |
| `cb83d5d` / `ac89afa` | PR 驱动透传 `--timeout-sec`；PR 基准 `running_concurrency = k×N`（与 COW 口径一致，N 仅作 1:k 基数） |

### C. Proxy-Read 场景（10-27，4 个提交，最大的一块功能）

- `55fa436`：新增 `agent/replay_agent_proxy.py`（只读步骤在 **paused 状态**直接执行）与 `scripts/run_proxy_read_oversubscription_benchmark.py`（猴子补丁复用 PR 驱动）：
  - **命令分类器** `proxy_read_eligible`：白名单（ls/find/cat/grep/stat/diff 等 30+ 只读命令）+ 保守排除（`$` 环境变量、反引号/`$(` 命令替换、任何 `>`/`<` 重定向、find 的 `-delete/-exec` 等突变原语），按 `&&/||/|/;/换行` 分段逐段校验首词；
  - 调 `POST /sandboxes/{id}/proxy-read`（server 侧 `chroot <目标rootfs> /bin/sh -c`，exit 127 回退）；503 `rebind_in_progress` 在重试窗口内退避重试；
  - **不占 RUNNING 槽、不 resume、不 pause**；首步强制走 resume（等 BIND 完成）；失败透明回退（`transport=resume_fallback`），未开 `[proxy]` 的 server 自动退化为基线 PR；
  - step/task 结果新增 `transport / proxy_eligible / proxy_skip_reason / proxy_duration_ms / binding_generation / running_slot_saved_steps / proxy_read_ratio` 等字段；
- `43aa6e4`：驱动抽出 `RUN_NAME_PREFIX` 补丁点，运行目录命名为 `proxy-read-oversub-N*`；`1a86119`：proxy 步骤同样输出 `Agent interaction timing` 行（resume/capacity_wait 恒 0，附 `server_exec`/`binding_generation`）；`2213617`：README 10.5 对齐本地单 VM 验证环境。

### D. Debug 与可观测性修正（10-20/21）

- `8a7f6de`：`--debug-breakpoint` **仅错误断点**（pause/resume 失败、WaitReady、命令超时/基础设施错误时挂起沙箱供检查，tty 回车 / 非 tty 删文件继续）；
- `f2236df`：三处修正——cgroup v1 `total_swap`→`total_shmem`（shmem 指标错读成 swap）、基准默认 API 端口 8000→**8002**（对齐 aenv server 部署）、cgroup_version 必须先于 cpu_range 传给 monitor；另加 `--keep-sandbox-on-failure` 对已消失沙箱的防护；
- `1b11882`：cgroup v1 `memory.stat` 键改用 `total_rss/total_cache`（两 monitor 同步修）。

### E. 新增分析工具（4 个脚本）

| 提交 | 脚本 | 用途 |
|---|---|---|
| `fd5bcff`+`c7d6f8d` | `process_results.py` | 离线双件套：每 VM 时序 CSV（自动识别 pause-resume/COW 场景）+ cgroup CPU/内存叠图 |
| `898b56d` | `extract_ready_vcpu_timeseries.py`（650 行） | 从 replay-result 重建 **ready vCPU 时间序列**（PR: resume→pause 窗口；COW: create→delete 减 delay 睡眠窗），扫线合并区间，输出事件/分桶 CSV + `cpus-per-core` 过载曲线图 |
| `9f1f05e`+`a0894de` | `summarize_vm_runs.py` | 逐 VM 解析 job.log timing 行 → 单 CSV；后按 `transport` 拆分 exec/pause 均值并加 `server_exec_ms`（proxy server 侧耗时） |

### F. README 重写与扩充

根提交 623 行 → 现 742 行：新增 §10 全部基准章节（10.1 COW / 10.2 PR / 10.3 noise-fill / 10.4 三组对比 / 10.5 proxy-read 原理与判定规则 / 10.6 参数表 / 10.7 监控脚本），以及本地 aenv 服务起停、cgroup 绑核、单机验证命令实录。

### G. 依赖原始 README 章节的迁移

原 README 前 9 节（快速开始/安装/环境变量/单任务/批量/混跑/返回码/FAQ/Docker 构建）来自原始仓库，你补充了 10.x 章节及本地化命令。

### H. 未跟踪文件（最近的工作，尚未提交）

- **`agent/replay_step_snapshots.py`**（654 行）：独立的"每步一快照"回放工具。两种模式：`chain`（默认，create→exec→snapshot→kill→**从快照重建**→exec…验证快照链可恢复性）与 `--checkpoint-mode`（单沙箱 exec→snapshot 原地循环）。自动在多个搜索目录定位轨迹、合并多份 image-map、从 `/home/cxd/data/manifest.csv` 复刻 `swb-<slug>-<tag>` 模板名解析、跳过 e2b key 格式校验、API key 自动从 `/var/lib/aenv/secrets/api-key` 读取；输出 `runs/step-snapshots/<slug>-<runid>/` 的 run.log + replay-result.json（含每步 exec/snapshot/create/delete 秒数与快照清单）；
- **`image-map.json`**：单条 `joke2k__faker-2007.replay.json → 模板 UUID` 映射，服务于上者。

---

## 6. 场景语义速查

| 场景 | 沙箱状态机 | 测什么 |
|---|---|---|
| **COW**（no_lifecycle） | create→…→delete 全程 RUNNING | 内存超发下脏页 COW 的 CPU/内存代价 |
| **PR**（主变体） | 每步 resume→exec→pause | pause/resume 生命周期开销 + 过载排队 |
| **noise-fill** | 真实任务 + noise VM 填充 | 恒定并发压力下的尾延迟（消除任务完成度衰减） |
| **proxy-read** | 只读步骤不离开 paused | 代理读对 RUNNING 槽/吞吐的节省（对比 PR 基线） |
| **snapshot** | exec→snapshot→kill→从快照重建 | 快照/重建链路的时延与 OSS 写放大 |
| **k-degradation** | k 任务挤 1 槽 | 单槽串行下的完成时间退化曲线 |

---

## 7. 运行产物结构（`runs/`）

```text
runs/<prefix>-N{N}-{ts}/            # prefix: pause-resume-oversub | proxy-read-oversub | no-lifecycle-oversub
├── benchmark-report.json           # 配置回显 + 全部结果
├── trial-summary.csv / ratio-summary.csv / trajectory-detail.csv / step-detail.csv   # 4 层 CSV
├── trajectory-selection.{json,csv} + selected-trajectories/slot-NNNN/               # 轨迹 staging（symlink）
└── ratio-XX/repeat-YY/
    ├── mix-config.json             # 该 trial 的负载定义（slot-NNNN × vm_count）
    ├── benchmark-driver.log        # 驱动拉起 agent 的完整 stdout
    ├── batch-summary.json + job.log（聚合）
    ├── slot-NNNN/NNN-<task>/       # 每任务 job.log / run.log / replay-result.json
    └── observability/              # metrics_1s.jsonl / sandbox_events.jsonl / report.{xlsx,md} / gantt.png / ...
```

当前 `runs/` 里有 4 个 N=1 单任务本地验证运行（PR × 2、proxy-read × 2，2026-10-28），即 proxy-read 功能落地时的冒烟验证。

---

## 8. 快速命令参考

```bash
# 单任务回放
.venv/bin/python agent/replay_agent.py run --trajectory x.replay.json \
  --template django-money-task-v2 --workdir /testbed --timeout-sec 300

# 检查服务与模板
.venv/bin/python agent/replay_agent.py check --template <name>

# PR 过载基准（N=1 冒烟）
N_RUNNING=1 .venv/bin/python scripts/run_pause_resume_oversubscription_benchmark.py \
  --trajectory-dir /home/cxd/data/replay --image-map /home/cxd/data/trajectory-templates.json \
  --workdir / --ratios 1 --repeats 1 --cgroup-name aenv.slice --cpu-range 0-95 \
  --command-timeout-sec 1200 --command-timeout-continue

# proxy-read 基准（server 需开 [proxy]）
N_RUNNING=1 .venv/bin/python scripts/run_proxy_read_oversubscription_benchmark.py \
  --trajectory-dir ... --image-map ... --api-url http://127.0.0.1:8002 --ratios 1 --repeats 1

# 每步快照工具（未提交）
.venv/bin/python agent/replay_step_snapshots.py <instance_id> [--checkpoint-mode]

# 事后分析
python3 scripts/summarize_vm_runs.py runs/<run-dir>            # 逐 VM CSV
python3 scripts/extract_ready_vcpu_timeseries.py runs/<run-dir> # ready-vCPU 曲线
python3 scripts/process_results.py <results_dir> <output_dir>   # 时序 + cgroup 图
```

---

## 9. 工程特点小结

1. **数据契约统一**：所有变体都向 `job.log` 输出同构 `Agent interaction timing` 行、向 `replay-result.json` 写同名字段，使 4 层 CSV / 报表 / 分析脚本对全部场景通用；
2. **正确性优先的并发控制**：QPS 限速（防突发）、RUNNING 槽硬上限（防超发）、create 幂等键对账（防重复）、槽位"持到确认暂停"（防泄漏），四重防护贯穿所有变体；
3. **复用而非复制**：proxy 变体子类化只覆写 `_run_slice`，proxy 驱动猴子补丁只改 5 处，快照调度器与基础调度器仅差操作枚举；
4. **测量链路自洽**：1Hz monitor 的漏采用 replay-result 毫秒时间戳修正，快照大小用 inode 归属去重，ready-vCPU 用事件区间重建——每个指标都有明确的误差来源与补偿手段。
