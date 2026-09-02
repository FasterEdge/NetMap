// FasterEdge 开源项目 - Github: https://github.com/FasterEdge - Gitee: https://gitee.com/FasterEdge
//go:build !windows

package snapshot

// 非 Windows 平台(linux/darwin 等)的原子 rename 不阻塞并发 open,
// 因此所有打开错误都是真实失败,不重试。
func isRetryableOpenError(err error) bool {
	return false
}
