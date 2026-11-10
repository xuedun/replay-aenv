#!/bin/bash
# ==============================================================================
# memory_comparison.sh — 对比"50 个不同 case" vs "单 case × 50 VM"的峰值内存
#
# 用法:
#   bash scripts/memory_comparison.sh [--traj-dir DIR] [--image-map FILE]
#        [--count N] [--api-url URL] [--output-root DIR] [--workdir W]
#
# 默认值:
#   --traj-dir    /home/cxd/data/replay-50
#   --image-map   /home/cxd/data/trajectory-templates.json
#   --count       50
#   --api-url     http://localhost:8002
#   --output-root runs/mem-cmp-<时间戳>
#   --workdir     /
#
# 流程:
#   1. 场景 A（mix）: 用整个轨迹目录跑 N 个 VM（N 个不同 case）
#   2. 场景 B（single）: 逐个 case 建 tmp 目录、跑 N 个 VM（同 case × N）
#   3. 汇总每个场景的 cgroup 峰值内存
#   4. 生成对比表
# ==============================================================================
set -euo pipefail

# ── 默认参数 ──────────────────────────────────────────────────────────────────
TRAJ_DIR="/home/cxd/data/replay-50"
IMAGE_MAP="/home/cxd/data/trajectory-templates.json"
COUNT=50
API_URL="http://localhost:8002"
OUTPUT_ROOT=""
WORKDIR="/"
DELAY=0
SINGLE_CASE_LIMIT=3
BENCH_SCRIPT="scripts/run_no_lifecycle_oversubscription_benchmark.py"
VENV_PY=".venv/bin/python"

# ── 解析参数 ──────────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --traj-dir)    TRAJ_DIR="$2";    shift 2 ;;
        --image-map)   IMAGE_MAP="$2";   shift 2 ;;
        --count)       COUNT="$2";       shift 2 ;;
        --api-url)     API_URL="$2";     shift 2 ;;
        --output-root) OUTPUT_ROOT="$2"; shift 2 ;;
        --workdir)     WORKDIR="$2";     shift 2 ;;
        --delay)       DELAY="$2";       shift 2 ;;
        --single-cases) SINGLE_CASE_LIMIT="$2"; shift 2 ;;
        *) echo "未知参数: $1"; exit 1 ;;
    esac
done

if [ -z "$OUTPUT_ROOT" ]; then
    OUTPUT_ROOT="runs/mem-cmp-$(date +%Y%m%d-%H%M%S)"
fi
mkdir -p "$OUTPUT_ROOT"

STAMP=$(date '+%H:%M:%S')
echo "=================================================================="
echo "  内存对比测试: ${COUNT} 个不同 case vs 单 case × ${COUNT} VM"
echo "  开始时间: ${STAMP}"
echo "  输出目录: ${OUTPUT_ROOT}"
echo "  delay: ${DELAY}s (0=无延迟满速)"
echo "  单case测试数: ${SINGLE_CASE_LIMIT}"
echo "=================================================================="
echo ""

# ── 前置检查 ──────────────────────────────────────────────────────────────────
if [ ! -f "$VENV_PY" ]; then
    echo "ERROR: 找不到 $VENV_PY，请在 replay-aenv-main 目录下运行"
    exit 1
fi
if [ ! -d "$TRAJ_DIR" ]; then
    echo "ERROR: 轨迹目录不存在: $TRAJ_DIR"
    exit 1
fi
if [ ! -f "$IMAGE_MAP" ]; then
    echo "ERROR: image-map 不存在: $IMAGE_MAP"
    exit 1
fi

# 检查 server 可达
HEALTH=$(curl -sS -m 5 -o /dev/null -w "%{http_code}" "$API_URL/health" 2>/dev/null || echo "000")
if [ "$HEALTH" != "200" ]; then
    echo "ERROR: AgentENV server 不可达 ($API_URL/health → HTTP $HEALTH)"
    exit 1
fi
echo "[前置检查] server OK, 模板数量: $(aenv template list 2>/dev/null | python3 -c "import json,sys; print(len(json.load(sys.stdin)))" 2>/dev/null || echo '?')"
echo ""

# ── 工具函数 ──────────────────────────────────────────────────────────────────
drop_page_cache() {
    # 清空 page cache / dentries / inodes，保证冷启动
    sync
    echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || \
        sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' 2>/dev/null || \
        echo "  [warn] drop_caches 需要 root，跳过"
    # 等待回收完成
    sleep 2
    # 打印当前可用内存
    local mem_available
    mem_available=$(awk '/MemAvailable/ {printf "%.1f", $2/1024/1024}' /proc/meminfo)
    echo "  [cache] 已清理, 当前可用内存: ${mem_available}GB"
}
extract_peak() {
    # 从 metrics_1s.jsonl 中提取 cgroup_mem_usage_gb 峰值
    local obs_dir="$1"
    local jsonl="$obs_dir/metrics_1s.jsonl"
    if [ ! -f "$jsonl" ]; then
        echo "N/A"
        return
    fi
    python3 -c "
import json
rows = [json.loads(l) for l in open('$jsonl') if l.strip()]
cg = [r.get('cgroup_mem_usage_gb', 0) for r in rows]
host = [r.get('mem_used_gb', 0) for r in rows]
cg_rss = [r.get('cgroup_mem_rss_gb', 0) for r in rows]
if not cg or max(cg) == 0:
    print(f'N/A|{max(host):.1f}|0')
else:
    print(f'{max(cg):.1f}|{max(host):.1f}|{max(cg_rss):.1f}')
" 2>/dev/null || echo "N/A|N/A|N/A"
}

run_bench() {
    # 跑一次基准并返回输出根目录
    local traj_dir="$1"
    local tag="$2"
    local out_dir="$OUTPUT_ROOT/$tag"

    echo "  [$tag] 启动: N_RUNNING=${COUNT}, traj_dir=$traj_dir"

    N_RUNNING=$COUNT $VENV_PY $BENCH_SCRIPT \
        --trajectory-dir "$traj_dir" \
        --image-map "$IMAGE_MAP" \
        --workdir "$WORKDIR" \
        --ratios 1 \
        --repeats 1 \
        --delay "$DELAY" \
        --rollout 1 \
        --launch-interval-sec 0.5 \
        --cgroup-name aenv.slice \
        --cpu-range 0-383 \
        --command-timeout-sec 1200 \
        --command-timeout-continue \
        --api-url "$API_URL" \
        --observability \
        --observability-interval-sec 1.0 \
        --output-root "$out_dir" \
        2>&1 | tail -5

    echo "  [$tag] 完成，输出: $out_dir"
    echo ""
}

# ── 场景 A: 混合 N 个不同 case ───────────────────────────────────────────────
echo "─── 场景 A: ${COUNT} 个不同 case（混合） ───"
echo ""

echo "[cache] 清理 page cache ..."
drop_page_cache
echo ""

run_bench "$TRAJ_DIR" "A-mix-${COUNT}cases"

MIX_PEAK=$(extract_peak "$OUTPUT_ROOT/A-mix-${COUNT}cases/ratio-01/repeat-01/observability")
MIX_CG=$(echo "$MIX_PEAK" | cut -d'|' -f1)
MIX_HOST=$(echo "$MIX_PEAK" | cut -d'|' -f2)
MIX_RSS=$(echo "$MIX_PEAK" | cut -d'|' -f3)

echo "  [A] cgroup峰值: ${MIX_CG}GB | host峰值: ${MIX_HOST}GB | cgroup RSS: ${MIX_RSS}GB"
echo ""

# ── 场景 B: 单 case × N 个 VM（逐个 case 测试）────────────────────────────────
echo "─── 场景 B: 单 case × ${COUNT} VM（循环每个 case） ───"
echo ""

echo "[cache] 清理 page cache ..."
drop_page_cache
echo ""

# 取轨迹目录中的前几个 case（默认取 3 个做对比，可改）
SINGLE_CASES=$(ls "$TRAJ_DIR"/*.json 2>/dev/null | head -$SINGLE_CASE_LIMIT)
if [ -z "$SINGLE_CASES" ]; then
    echo "ERROR: 轨迹目录中没有 .json 文件"
    exit 1
fi

declare -A SINGLE_RESULTS

for traj_file in $SINGLE_CASES; do
    case_name=$(basename "$traj_file" .replay.json)
    # 截断过长名称
    short_name="${case_name:0:30}"

    echo "  ── [B] case: $short_name ──"
    drop_page_cache

    # 建单 case 临时目录
    tmp_dir="/tmp/mem-cmp-single-$RANDOM"
    mkdir -p "$tmp_dir"
    cp "$traj_file" "$tmp_dir/"

    # 跑基准
    tag="B-single-${short_name}"
    run_bench "$tmp_dir" "$tag"

    # 提取峰值
    peak=$(extract_peak "$OUTPUT_ROOT/$tag/ratio-01/repeat-01/observability")
    cg=$(echo "$peak" | cut -d'|' -f1)
    host=$(echo "$peak" | cut -d'|' -f2)
    rss=$(echo "$peak" | cut -d'|' -f3)

    SINGLE_RESULTS[$short_name]="$cg|$host|$rss"
    echo "  [B:$short_name] cgroup峰值: ${cg}GB | host峰值: ${host}GB | RSS: ${rss}GB"
    echo ""

    # 清理临时目录
    rm -rf "$tmp_dir"
done

# ── 汇总对比 ──────────────────────────────────────────────────────────────────
echo "=================================================================="
echo "  峰值内存对比结果"
echo "=================================================================="
echo ""

RESULT_FILE="$OUTPUT_ROOT/comparison.txt"
{
    echo "AgentENV 峰值内存对比测试"
    echo "时间: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "VM 数: $COUNT"
    echo ""
    echo "┌────────────────────────────────┬──────────┬──────────┬──────────┐"
    echo "│ 场景                            │ cgroup GB │ host GB  │ RSS GB   │"
    echo "├────────────────────────────────┼──────────┼──────────┼──────────┤"
    printf "│ A: %d 个不同 case（混合）        │ %8s │ %8s │ %8s │\n" "$COUNT" "$MIX_CG" "$MIX_HOST" "$MIX_RSS"
    for name in "${!SINGLE_RESULTS[@]}"; do
        IFS='|' read -r cg host rss <<< "${SINGLE_RESULTS[$name]}"
        printf "│ B: %-28s │ %8s │ %8s │ %8s │\n" "$name" "$cg" "$host" "$rss"
    done
    echo "└────────────────────────────────┴──────────┴──────────┴──────────┘"
    echo ""
    echo "详细数据:"
    echo "  A: $OUTPUT_ROOT/A-mix-${COUNT}cases/"
    for name in "${!SINGLE_RESULTS[@]}"; do
        echo "  B: $OUTPUT_ROOT/B-single-${name}/"
    done
} | tee "$RESULT_FILE"

echo ""
echo "结果已保存: $RESULT_FILE"
