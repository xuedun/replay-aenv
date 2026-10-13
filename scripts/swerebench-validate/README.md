# swerebench v2 ARM 镜像 replay 验证流水线

给一份华为云 SWR 镜像列表,自动验证每个镜像能否用于轨迹回放:**全程不调 LLM、零
token**。验证信号来自 NVIDIA Open-SWE-Traces 官方轨迹(任务本身来自
nebius/SWE-rebench-V2,与本仓镜像同源)。

2026-08-24 首跑通过:`swerebench-arm64-actions-stale:334-ec96ff6` 全链路 PASS
(72 步官方轨迹,70/71 命令成功,submitted,110s)。

## 用法

```bash
cd /home/zkwang/replay-aenv

# 首次(或分片更新后)建索引
.venv/bin/python scripts/swerebench-validate/validate_images.py --build-index

# 跑验证(列表文件每行一个镜像引用,# 开头为注释)
.venv/bin/python scripts/swerebench-validate/validate_images.py --list <images.txt>

# 常用参数:--limit N 先跑前 N 个试水;--runs-dir 指定输出目录;
#           --timeout-sec 单任务回放超时(默认 2400)
```

产物:`runs/validate-<时间戳>/report.jsonl`(每镜像一条)+ 每镜像独立
`run/replay-result.json` + `replay-driver.log`。

## 每个镜像经历的 8 个阶段

| # | 阶段 | 幂等/跳过逻辑 |
|---|---|---|
| 1 | `podman pull` SWR 镜像 | 本地已有则跳过 |
| 2 | 架构检查(必须 arm64/linux) | — |
| 3 | tag + push 到 `127.0.0.1:5000/swerebench/<task>:<tag>` | 本地 tag 已存在则跳过 |
| 4 | `aenv pull --name swb-<slug>-<num>` 建模板 | 同名 ready 模板存在则跳过 |
| 5 | 按 tag 中 commit 从任务表反查 `instance_id` | 查不到回退命名推导 |
| 6 | 按 instance_id 从索引取官方轨迹,转 replay 格式 | — |
| 7 | `agent/replay_agent.py run` 回放(workdir 取镜像 Dockerfile WORKDIR) | — |
| 8 | 解析 replay-result.json 出判定 | — |

单个镜像失败不阻断整批;每个阶段的结果都记录在 report.jsonl 的 `stages` 字段。

## 判定口径

| verdict | 含义 |
|---|---|
| `PASS` | 回放完成,`status=success` 且 `submitted=true`(允许个别命令非零,轨迹原样如此) |
| `FAIL` | 任一阶段失败,`error` 字段带具体原因 |
| `SKIP_NO_TRACE` | 该实例未被 minisweagent 配置收录(见下文覆盖率) |

注意 PASS 只代表「镜像可回放」,不代表任务被解对;`succeeded_commands/commands`
反映轨迹内命令的命令级成功率。

## 数据源(均已落盘 data/swerebench/,被 gitignore)

| 文件 | 来源 | 说明 |
|---|---|---|
| `mini-v2-shard*.parquet` ×27 (2.6GB) | `nvidia/Open-SWE-Traces`,config `minisweagent/qwen36_27b`,split `swe-rebench-v2` | 经 hf-mirror.com 下载(huggingface.co 本机不通) |
| `swerebench-v2-tasks.parquet` (429MB) | `nebius/SWE-rebench-V2` | 任务表,32079 行,含 base_commit/patch/FAIL_TO_PASS |
| `index.json` | `--build-index` 生成 | instance_id → (分片, 行号),22548 实例 |

**为什么选 minisweagent 配置**:它的轨迹每步就是一个 bash tool_call,没有
str_replace_editor 之类的专用工具依赖,转成 replay-aenv 格式是 1:1 的;轨迹末尾的
`echo COMPLETE_TASK_AND_SUBMIT_FINAL_OUTPUT` 转成 `submit`。sweagent/openhands
配置的轨迹含专用工具调用,需要额外转换器,暂未实现(对应实例会 SKIP)。

**覆盖率**:minisweagent 配置收录 22548 / 32079 实例(70%)。镜像列表里落在
缺口里的实例会判 SKIP_NO_TRACE,必要时再补 sweagent 配置的下载与转换。

## 镜像 ↔ 实例 ↔ 轨迹 的对应关系

- SWR 镜像名:`swr.cn-north-4.myhuaweicloud.com/kunpeng-ai/swerebench-arm64-<repo-slug>:<num>-<commit短哈希>`
- 任务表 instance_id:`<owner>__<repo>-<num>`(如 `actions__stale-334`),
  权威反查方式是用 tag 里的 commit 匹配任务表 `base_commit` 前缀(流水线就是这么做的)
- 已验证镜像内仓库在 `/stale`(非 SWE-bench 惯例的 /testbed);官方 trace 里全是
  /testbed 绝对路径 → 转换时在轨迹第 0 步插 `ln -sfn <workdir> /testbed`

## 已知坑(踩过的)

1. **workdir 不在 /testbed**:回放必须显式 `--workdir /stale`(或镜像实际 WORKDIR)。
   harbor 路线还会从任务 Dockerfile 的 WORKDIR 解析 env exec cwd,用 django-money_task
   跑非 /testbed 镜像会直接炸 `cwd '/testbed' does not exist` —— 所以本流水线只走
   replay-aenv 的 `agent/replay_agent.py`,不走 harbor。
2. **轨迹无 delay_time**:Open-SWE-Traces 不带逐步耗时,转换统一给 0.2s
   (`--delay` 可调)。要压测 pause/resume 密集度的场景可以调大。
3. **同名模板**:不同 tag 的同 repo 镜像会生成不同模板名(`swb-actions-stale-334`
   vs `-335`),不会冲突;但重复跑同一镜像复用已有模板,不重复构建。
4. **PASS 里的失败命令**:官方轨迹本身可能含失败探针步(如 actions-stale-334 的
   verify_fix.js 引用了不存在的 dist 模块),属轨迹原貌,不影响「可回放」判定。
   要更严的口径可以自行加 `--要求 failed_commands=0` 的过滤。

## 环境依赖

- replay-aenv/.venv(2026-08-24 建,`uv venv --python 3.12 && uv pip install -e .`,
  另装 pyarrow)
- AgentEnv server 本机 :8000,连接信息从 `/root/.config/harbor-agentenv/env` 读
- podman + 本地 registry(127.0.0.1:5000)+ aenv CLI
- SWR 登录态:`podman login swr.cn-north-4.myhuaweicloud.com`(凭据找镜像提供方)

## 待办/扩展

- [ ] 并发验证(当前串行,单镜像 ~2min;瓶颈在单沙箱回放,可按 RUNNING 名额并发)
- [ ] sweagent / openhands 配置的轨迹转换器(补 30% 覆盖率缺口)
- [ ] 可选严格模式:failed_commands=0 才 PASS
- [ ] 对接 verifier:任务表有 FAIL_TO_PASS/PASS_TO_PASS,回放后可加真实 reward 判定
