# Pandora 版本化 MySQL 迁移器。构建上下文必须是仓库根：
#   docker build -f deploy/migrate/Dockerfile -t pandora/migrate:<immutable-tag> .
ARG GO_VERSION=1.26.5
ARG GO_IMAGE_VARIANT=bookworm
ARG BASE_REGISTRY=docker.io

FROM ${BASE_REGISTRY}/library/golang:${GO_VERSION}-${GO_IMAGE_VARIANT} AS builder

ARG GO_VERSION
# 分隔符必须是 `|`:逗号只在 404/410 回退,网络层错误直接失败(详见 deploy/services/Dockerfile)。
ARG GOPROXY=https://goproxy.cn|https://proxy.golang.org|direct
ENV CGO_ENABLED=0 GOOS=linux GOWORK=off GOFLAGS=-mod=readonly GOPROXY=${GOPROXY}

WORKDIR /src
COPY tools/migrate/go.mod tools/migrate/go.sum ./
RUN actual="$(go env GOVERSION)" \
    && test "$actual" = "go${GO_VERSION}" \
       || (echo "ERROR: required go${GO_VERSION}, actual=${actual}" && exit 1)
RUN go mod download

COPY tools/migrate/*.go ./
COPY tools/migrate/migrations/ ./migrations/
RUN go test ./... \
    && go build -trimpath -ldflags="-s -w" -o /out/pandora-migrate .

FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /out/pandora-migrate /app/pandora-migrate

USER 10001:10001
ENTRYPOINT ["/app/pandora-migrate"]
