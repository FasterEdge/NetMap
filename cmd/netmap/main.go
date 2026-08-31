// FasterEdge 开源项目 · https://github.com/FasterEdge · https://gitee.com/FasterEdge
// netmap 是一个独立运行的、带 Web 前端的网络拓扑管理器。
// 它接收 FasterEdge 节点的 NetMapAbility 上报,并在浏览器中渲染网络拓扑。
//
// 该文件只承担"启动器"角色 —— 真正可测试的装配逻辑在 run.go。
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// 顶层 main 只负责把 os.Args 喂给 run(),并在 run 返回时把退出
	// 码透传给操作系统。main 本身不再持有任何业务状态。
	logger := log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[0], os.Args[1:], logger); err != nil {
		// 我们用 log.Fatal 而不是 panic,这样 deferred 资源能被清理掉;
		// 但退出码必须明确 —— 0 表示主动终止,非 0 表示错误退出。
		logger.Printf("fatal: %v", err)
		os.Exit(1)
	}
}
