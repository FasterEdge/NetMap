# syntax=docker/dockerfile:1.7
# 多阶段构建：golang 编译 → alpine 精简运行镜像（约 10MB）
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/app ./cmd/netmap

FROM alpine:3.20
RUN addgroup -S app && adduser -S -G app app && \
    mkdir -p /data && chown app:app /data
COPY --from=build /out/app /usr/local/bin/app
USER app
# 工作目录必须可写：默认 StateFile 为相对路径 netmap-state.json，
# 否则非 root 用户写入失败（shutdown 保存快照时 permission denied）
WORKDIR /data
EXPOSE 8080
ENTRYPOINT ["app"]
