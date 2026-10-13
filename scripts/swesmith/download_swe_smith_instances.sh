#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_root="$(cd "${script_dir}/../.." && pwd)"
target_dir="${project_root}/SWE-smith-instances"
revision="ea6d7173829c7ec8fa16c22055699ff2e9188091"
repo_url="https://hf-mirror.com/datasets/SWE-bench/SWE-smith/resolve/${revision}"

mkdir -p "${target_dir}/data"

for index in $(seq 0 10); do
  shard="$(printf '%05d' "${index}")"
  relative_path="data/train-${shard}-of-00011.parquet"
  destination="${target_dir}/${relative_path}"
  partial="${destination}.part"

  if [[ -s "${destination}" ]]; then
    echo "[skip] ${relative_path}"
    continue
  fi

  echo "[download] ${relative_path}"
  curl -fL \
    --retry 20 \
    --retry-delay 5 \
    --retry-all-errors \
    --connect-timeout 20 \
    --speed-time 120 \
    --speed-limit 1024 \
    -C - \
    -o "${partial}" \
    "${repo_url}/${relative_path}?download=true"
  mv "${partial}" "${destination}"
done

cd "${target_dir}"
sha256sum data/train-*-of-00011.parquet > SHA256SUMS
cat > DOWNLOAD_INFO.txt <<EOF
dataset=SWE-bench/SWE-smith
revision=${revision}
source_endpoint=https://huggingface.co/datasets/SWE-bench/SWE-smith
download_endpoint=https://hf-mirror.com/datasets/SWE-bench/SWE-smith
downloaded_at_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
EOF

echo "[complete] SWE-smith instance dataset"
du -ch data/train-*-of-00011.parquet | tail -n 1
