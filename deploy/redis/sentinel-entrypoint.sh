#!/bin/sh
# Pandora Redis Sentinel 启动脚本。
#
# 为什么用脚本而非静态 sentinel.conf:Sentinel 启动时会把发现的主从信息**回写**到
# 配置文件,只读挂载(:ro)会让 Sentinel 启动失败。这里在容器内生成可写副本再启动。
#
# 入参(环境变量,见 docker-compose.redis-sentinel.yml):
#   REDIS_PASSWORD  主从与 Sentinel 鉴权密码
#   SENTINEL_QUORUM 判定客观下线所需票数(三哨兵建议 2)
#   SENTINEL_ANNOUNCE_HOSTNAMES 是否对外通告主机名(默认 yes=compose 行为:容器名可解析;
#     K8s Deployment 的 Pod 主机名不可 DNS 解析,置 no 走 Pod IP 通告,集群内全路由可达)
set -e

CONF=/tmp/sentinel.conf
MASTER_NAME=pandora-master
MASTER_HOST=redis-master
MASTER_PORT=6379

cat > "$CONF" <<EOF
port 26379
sentinel resolve-hostnames yes
sentinel announce-hostnames ${SENTINEL_ANNOUNCE_HOSTNAMES:-yes}
sentinel monitor ${MASTER_NAME} ${MASTER_HOST} ${MASTER_PORT} ${SENTINEL_QUORUM:-2}
sentinel auth-pass ${MASTER_NAME} ${REDIS_PASSWORD}
sentinel down-after-milliseconds ${MASTER_NAME} 5000
sentinel failover-timeout ${MASTER_NAME} 15000
sentinel parallel-syncs ${MASTER_NAME} 1
EOF

exec redis-server "$CONF" --sentinel
