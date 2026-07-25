#!/usr/bin/env bash
# 绕开被墙的 docker hub CDN：从 quay.io 拉 MinIO（官方双发布），再 retag 成 compose 用的规范名。
set -u
pairs=(
  "quay.io/minio/minio:latest|minio/minio:latest"
  "quay.io/minio/mc:latest|minio/mc:latest"
)
for pair in "${pairs[@]}"; do
  src="${pair%%|*}"; dst="${pair##*|}"
  ok=0
  for attempt in $(seq 1 10); do
    echo ">>> [$attempt] pull $src"
    if docker pull "$src"; then
      docker tag "$src" "$dst" && echo ">>> OK retag $src -> $dst"
      ok=1; break
    fi
    echo ">>> retry in 4s..."; sleep 4
  done
  [ "$ok" -eq 1 ] || { echo "!!! FAILED: $src"; exit 1; }
done
echo "MINIO_IMAGES_READY"
