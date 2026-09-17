// ─────────────────────────────────────────────────────────────
// FasterEdge 开源项目
// Github: https://github.com/FasterEdge
// Gitee:  https://gitee.com/FasterEdge
// ─────────────────────────────────────────────────────────────
package source

import (
	"context"
	"fmt"
	"net"
	"strings"
)

// CheckDialHost 对拨号目标 host(IP literal 或 hostname)做 IP-class 检查,
// 供 HTTP Transport 的 DialContext 在每次拨号前调用。Validate 只覆盖初始
// URL, 而 redirect 目标可能是内网 IP literal(不经 Validate), 且 hostname 的
// DNS 解析发生在拨号时刻(DNS 重绑定), 故二者都必须在此拦截。
//
// 白名单命中直接放行; AllowPrivate 由 CheckResolvedIP 内部处理(返回空放行)。
func (p ValidationPolicy) CheckDialHost(ctx context.Context, host string) error {
	if p.AllowedHosts != nil {
		if _, ok := p.AllowedHosts[strings.ToLower(host)]; ok {
			return nil
		}
	}
	if ip := net.ParseIP(host); ip != nil {
		if reason := p.CheckResolvedIP(ip); reason != "" {
			return &DisallowError{Reason: reason, Msg: fmt.Sprintf("address %s denied (%s)", ip, reason)}
		}
		return nil
	}
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return err
	}
	for _, ia := range ips {
		if reason := p.CheckResolvedIP(ia.IP); reason != "" {
			return &DisallowError{Reason: reason, Msg: fmt.Sprintf("resolved address %s denied (%s)", ia.IP, reason)}
		}
	}
	return nil
}
