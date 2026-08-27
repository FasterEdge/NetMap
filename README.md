<div align="center">
  <img src="Logo.png" alt="NetMap" width="120"/>  
  <h2>NetMap</h2>
  <h3>FasterEdge 网络拓扑管理器</h3>
</div>

独立运行的、带 Web 前端的网络拓扑管理器。

接收来自 FasterEdge 节点(`NetMapAbility`)的拓扑快照,在浏览器中实时渲染网络拓扑图。

### 一、设计

- **后端**:Go 标准库 + `net/http`,使用 FasterEdge 的 `NetMapData` / `NetMapAbility` 数据模型
- **前端**:纯 vanilla JS + SVG 自绘拓扑(无外部 CDN,可完全离线运行),`go:embed` 嵌入二进制
- **传输**:REST API,周期性轮询上游节点拉取拓扑(JSON over HTTP)
- **存储**:内存拓扑表,支持并发读写

### 二、用法

```bash
# 启动一个 hub,周期性轮询 3 个 FasterEdge 节点
netmap -addr=:8080 -name=hub-01 -allow-private-nodes \
       -node=edge-a=http://10.0.0.1:8080 \
       -node=edge-b=http://10.0.0.2:8080 \
       -node=cloud-c=https://ctrl.example.com

# 浏览器打开 http://localhost:8080/ 即可看到拓扑图。
```

### 三、HTTP API

| 路径 | 方法 | 说明 |
|---|---|---|
| `/` | GET | 内嵌的 Web 前端(SVG 拓扑图) |
| `/api/topology` | GET | 完整拓扑(`{self, peers[]}`) |
| `/api/peers` | GET | 对等节点列表 |
| `/api/self` | GET | 本节点信息 |
| `/api/healthz` | GET | 健康检查 |
| `/api/v1/topology` | POST | 使用 Bearer Token 上报拓扑；仅配置 `-ingest-token` 后启用 |
| `/api/v1/sources` | GET | 数据源状态列表 |
| `/api/...` | OPTIONS | CORS 预检(返回 204) |

### 四、开发

```bash
go test ./...                 # 单元测试
go build -o netmap ./cmd/netmap
./netmap -name=dev
```

### 五、端口与 CORS

- `-addr`:HTTP 监听地址,默认 `:8080`
- `-origin`:CORS 允许的 origin,默认空(关闭 CORS),`*` 启用
- `-name`:本节点显示名
- `-allow-private-nodes`:允许轮询回环、RFC1918、链路本地及组播地址；仅建议开发环境使用
- `-state-file`:状态快照文件，默认 `netmap-state.json`，设为空可禁用持久化
- `-ingest-token`:启用拓扑上报接口所需的 Bearer Token

### 六、架构

```
            ┌────────────────────┐
            │   FasterEdge 节点  │
            │ (跑 NetMapAbility) │
            └──────────┬─────────┘
                       │ HTTP /api/topology (轮询)
                       ▼
            ┌────────────────────┐
            │      NetMap        │
            │  ┌──────────────┐  │
            │  │  Store       │  │ ◀──── 内存拓扑
            │  └──────────────┘  │
            │  ┌──────────────┐  │
            │  │  HTTP Server │  │ ────▶ 浏览器 (内嵌 SVG)
            │  └──────────────┘  │
            └────────────────────┘
```

### 七、跨平台

无 cgo、无 unsafe 依赖,跨 6 平台编译通过(linux/amd64, linux/arm64, windows/amd64, darwin/amd64, darwin/arm64, freebsd/amd64)。

### 八、与 FasterEdge 的关系

NetMap 是 FasterEdge 的**独立生态项目**——它不替代 `NetMapAbility`,而是在多节点部署场景中扮演"中心化拓扑面板"角色:

- `NetMapAbility` 负责**单节点**的本地拓扑
- `NetMap` 负责**多节点**的全局视图

二者通过 JSON over HTTP 互联,FastEdge 节点自身只需开放 `/api/topology` 即可被 NetMap 拉取。
