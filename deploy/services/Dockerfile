# Pandora Go 业务服务通用镜像(15 个服务共用一份 Dockerfile)。
#
# 本仓库是 go.work 多模块工作区(根目录没有 go.mod),所以构建上下文必须是仓库根,
# 把整个 workspace 拷进来,再按 SERVICE_DIR / CMD_NAME 构建指定服务的 cmd。
#
# 构建(由 tools/scripts/start.ps1 / docker-compose.services.yml 自动带 build-arg):
#   docker build -f deploy/services/Dockerfile \
#     --build-arg SERVICE_DIR=services/account/login \
#     --build-arg CMD_NAME=login \
#     -t pandora/login:dev .
#
# 运行期配置:镜像内已烘焙各服务 etc/(含 killswitch.yaml 兜底),但 127.0.0.1 地址
# 在容器里不可用 —— compose 用 bind-mount、k8s 用 ConfigMap 覆盖主配置为「集群版」
# (mysql/redis/kafka/etcd 改容器服务名,见 tools/scripts/gen_cluster_config.ps1)。

ARG GO_VERSION=1.26.5
ARG GO_IMAGE_VARIANT=bookworm
# 基础镜像仓库前缀:默认官方 docker.io;国内机器由 start.ps1 传 docker.m.daocloud.io 等加速镜像。
ARG BASE_REGISTRY=docker.io

# ===== 构建阶段 =====
FROM ${BASE_REGISTRY}/library/golang:${GO_VERSION}-${GO_IMAGE_VARIANT} AS builder

ARG GO_VERSION

WORKDIR /src

# 拷整个 workspace(go.work + pkg + proto + services)。.dockerignore 已排除产物/缓存。
COPY . .

# 纯 Go 依赖(mysql/redis/sarama 均无 CGO),静态编译产出单文件二进制。
# 不使用 Dockerfile 外部 frontend / RUN --mount,避免内网机器启动时额外拉
# docker/dockerfile:* 导致一键构建卡在 Docker Hub。
# GOPROXY:容器内拉 go 模块;国内机器默认走 goproxy.cn,可由 start.ps1 / 环境变量覆盖。
# **分隔符必须是 `|` 而不是 `,`**:逗号只在代理返回 404/410 时才退到下一个源,网络层错误
# (unexpected EOF / 超时 / 连接重置)直接失败,`,direct` 那个 direct 根本轮不到;竖线才是
# 「任何错误都回退」。2026-08-12 事故链:goproxy.cn 抖动 → 不回退 → 22 个服务里必有构建
# 失败 → start.ps1 导入离线镜像包兜底 → 旧 commit 二进制配上新生成的配置 →
# inventory 缺 default_identify_rule 启动即 CrashLoop → rollout 180s 超时。
ARG GOPROXY=https://goproxy.cn|https://proxy.golang.org|direct
ENV CGO_ENABLED=0 GOOS=linux GOFLAGS=-trimpath GOPROXY=${GOPROXY}
RUN actual="$(go env GOVERSION)" \
    && test "$actual" = "go${GO_VERSION}" \
       || (echo "ERROR: required go${GO_VERSION}, actual=${actual}" && exit 1)

# 模块下载独立成层:22 个服务共用同一份 workspace,依赖图完全相同。
# 本层的 cache key 只取决于 workspace 源码,与服务名 / 版本烙印无关 —— 一次 start.ps1
# 里只有第一个服务真的下载,其余 21 个直接命中本层缓存。
# 必须放在 per-service ARG **之前**:BUILD_TIME 每次运行都是新值,若与 go build 同层,
# 22 个服务每次都要各自重拉整张依赖图(2026-08-12 事故日志里 125 条代理报错即由此而来)。
RUN go mod download

# per-service 构建参数到这里才声明,避免参与上面各层的 cache key。
ARG SERVICE_DIR
ARG CMD_NAME

# 版本烙印:由 start.ps1 / compose 传入 git 版本信息,编译期 -ldflags -X 注入二进制。
# 不传时用占位默认值(本地随手 build 也能跑),线上发布务必传真实 git tag/commit。
ARG VERSION=dev
ARG GIT_COMMIT=unknown
ARG BUILD_TIME=unknown

RUN test -n "$SERVICE_DIR" || (echo "ERROR: build-arg SERVICE_DIR 必填" && exit 1) \
    && test -n "$CMD_NAME" || (echo "ERROR: build-arg CMD_NAME 必填" && exit 1) \
    && cd "/src/${SERVICE_DIR}" \
    && go build \
        -ldflags="-s -w \
            -X github.com/luyuancpp/pandora/pkg/version.Version=${VERSION} \
            -X github.com/luyuancpp/pandora/pkg/version.Commit=${GIT_COMMIT} \
            -X github.com/luyuancpp/pandora/pkg/version.BuildTime=${BUILD_TIME}" \
        -o /out/app "./cmd/${CMD_NAME}"

# ===== 运行阶段 =====
# 用 scratch 免任何运行基础镜像 / apk 联网:CGO_ENABLED=0 已产出静态二进制,
# 只需从 builder 拷 TLS 根证书与时区库即可运行。
# 好处:① 离线可构建(不依赖 alpine:3.20、apk CDN),内网/断网机器也能 build;
#       ② 镜像极小,docker save/load 跨机传输快;③ 攻击面最小(无 shell/包管理器)。
FROM scratch

ARG SERVICE_DIR

# golang 官方镜像自带 ca 证书与时区库,直接拷贝,无需联网 apk。
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

WORKDIR /app

COPY --from=builder /out/app /app/app
# 烘焙该服务的 etc/(killswitch.yaml 等兜底文件);主配置运行期会被集群版覆盖。
COPY ${SERVICE_DIR}/etc/ /app/etc/

# 非 root 运行:scratch 无 /etc/passwd,用纯数字 uid:gid(与旧 alpine 版 pandora 用户一致 = 10001)。
USER 10001:10001

# 约定:集群版主配置统一挂成 /app/etc/cluster.yaml(compose bind-mount / k8s ConfigMap)。
# compose、k8s 也可显式覆盖 -conf;独立 docker run 时默认读 cluster.yaml。
ENTRYPOINT ["/app/app"]
CMD ["-conf", "etc/cluster.yaml"]
