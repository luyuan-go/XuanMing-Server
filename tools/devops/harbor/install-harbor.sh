#!/usr/bin/env bash
# Harbor 私有镜像库安装（发布线③制品库层的"正式版"，替代 registry:2）。
#
# ⚠️ 平台限制：Harbor 官方只支持 **Linux 宿主机**（install.sh 依赖 bash/systemd 环境）。
#    Windows 上没有受支持的装法 —— 在 Windows 开发机上请继续用 tools/devops 的 registry:2。
#    本脚本给的是**构建机 / 服务器（Linux）**上的标准装法。
#
# ⚠️ 与 registry:2 是【二选一替代】，不是叠加：
#    Harbor 自带 registry 组件并占用 80/443。规模小用 registry:2 足够；
#    需要镜像扫描（Trivy）、RBAC、复制、保留策略、审计时再上 Harbor。
#
# 💡 墙内要点：**必须用 offline installer**（默认）。
#    offline 包自带全部组件镜像的 tar，安装时 docker load 本地导入，**完全不碰 Docker Hub**，
#    正好绕开 production.cloudfront.docker.com 被限速的问题。
#    online installer 会去 Docker Hub 拉镜像 —— 墙内必失败，别用。
#
# 用法：
#   ./install-harbor.sh                                  # 默认 v2.15.2，http，端口 80
#   HARBOR_VERSION=v2.15.2 HARBOR_HOSTNAME=harbor.local HARBOR_PORT=8090 ./install-harbor.sh
#   HARBOR_ADMIN_PASSWORD='<强密码>' ./install-harbor.sh
set -euo pipefail

HARBOR_VERSION="${HARBOR_VERSION:-v2.15.2}"
HARBOR_HOSTNAME="${HARBOR_HOSTNAME:-$(hostname -I 2>/dev/null | awk '{print $1}')}"
HARBOR_PORT="${HARBOR_PORT:-80}"
HARBOR_ADMIN_PASSWORD="${HARBOR_ADMIN_PASSWORD:-}"
INSTALL_DIR="${INSTALL_DIR:-/opt}"

echo "==> 前置检查"
[ "$(id -u)" -eq 0 ] || { echo "需要 root（Harbor install.sh 要写 /data 并起容器）：请用 sudo 运行"; exit 1; }
command -v docker >/dev/null || { echo "缺 docker"; exit 1; }
docker compose version >/dev/null 2>&1 || { echo "缺 docker compose v2"; exit 1; }
[ -n "$HARBOR_HOSTNAME" ] || { echo "解析不到本机 IP，请显式设置 HARBOR_HOSTNAME"; exit 1; }

if [ -z "$HARBOR_ADMIN_PASSWORD" ]; then
  echo "!! 未设置 HARBOR_ADMIN_PASSWORD，将使用 Harbor 默认口令 Harbor12345"
  echo "!! 生产环境务必设置：HARBOR_ADMIN_PASSWORD='<强密码>' $0"
  HARBOR_ADMIN_PASSWORD="Harbor12345"
fi

TGZ="harbor-offline-installer-${HARBOR_VERSION}.tgz"
URL="https://github.com/goharbor/harbor/releases/download/${HARBOR_VERSION}/${TGZ}"

echo "==> 下载 offline installer（约 700MB~1GB，自带镜像，不走 Docker Hub）"
cd "$INSTALL_DIR"
if [ -f "$TGZ" ]; then
  echo "    已存在 $TGZ，跳过下载（要重下先删掉它）"
else
  # -C 断点续传：墙内大文件容易断
  curl -fL --retry 10 --retry-delay 5 -C - -o "$TGZ" "$URL"
fi

echo "==> 解包"
tar xzf "$TGZ"
cd harbor

echo "==> 生成 harbor.yml"
[ -f harbor.yml ] || cp harbor.yml.tmpl harbor.yml
# 官方模板默认启用 https 并要求证书；本脚本按"内网 http"最小可用配置改写。
# 需要 https 时请自行填 certificate/private_key 两行并保留 https 段。
sed -i "s|^hostname: .*|hostname: ${HARBOR_HOSTNAME}|" harbor.yml
sed -i "s|^  port: 80$|  port: ${HARBOR_PORT}|" harbor.yml
sed -i "s|^harbor_admin_password: .*|harbor_admin_password: ${HARBOR_ADMIN_PASSWORD}|" harbor.yml
# 注释掉整个 https 段（内网 http 起步；生产请配证书后还原）
sed -i '/^https:/,/^  private_key:/ s|^|#|' harbor.yml

echo "==> 安装（含 docker load 本地镜像，耗时数分钟）"
./install.sh

cat <<EOF

========== Harbor 安装完成 ==========
  地址：http://${HARBOR_HOSTNAME}:${HARBOR_PORT}
  用户：admin
  密码：${HARBOR_ADMIN_PASSWORD}

后续接线：
  1) 客户端 docker 需信任该 http registry（非 localhost 的 http 必须显式声明）：
     /etc/docker/daemon.json 加 {"insecure-registries":["${HARBOR_HOSTNAME}:${HARBOR_PORT}"]} 后重启 docker
  2) 推镜像：
     docker login ${HARBOR_HOSTNAME}:${HARBOR_PORT}
     pwsh deploy/ds/build-image-minikube.ps1 -BuildOnHost -PushRegistry -RegistryHost ${HARBOR_HOSTNAME}:${HARBOR_PORT}
  3) 在 Harbor UI 建项目 pandora，并配保留策略（Projects → pandora → Policy → Tag Retention）
=====================================
EOF
