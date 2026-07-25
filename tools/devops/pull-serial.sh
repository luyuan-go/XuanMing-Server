#!/usr/bin/env bash
# 串行逐个拉基础镜像 + 每个带重试。避开 compose 并行拉把 CDN 连接拉爆导致的 EOF 断流。
# 已拉好的层会续传，重试成本低。
set -u
images=(
  "registry:2"
  "minio/minio:latest"
  "minio/mc:latest"
  "joxit/docker-registry-ui:main"
  "jenkins/jenkins:lts-jdk21"
)
for img in "${images[@]}"; do
  ok=0
  for attempt in 1 2 3 4 5 6 7 8; do
    echo ">>> pulling $img (attempt $attempt)"
    if docker pull "$img"; then ok=1; echo ">>> OK: $img"; break; fi
    echo ">>> retry $img in 3s..."; sleep 3
  done
  if [ "$ok" -ne 1 ]; then echo "!!! FAILED after retries: $img"; exit 1; fi
done
echo "ALL_IMAGES_PULLED"
