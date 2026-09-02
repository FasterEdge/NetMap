// FasterEdge 开源项目 - Github: https://github.com/FasterEdge - Gitee: https://gitee.com/FasterEdge
//go:build windows

package snapshot

import (
	"errors"
	"syscall"
)

// Windows 上 writeAtomic 的原子 rename 会短暂占用目标文件,导致并发的
// os.Open 返回 ERROR_SHARING_VIOLATION(32) 或 ERROR_ACCESS_DENIED(5)。
// 这两个都是瞬时竞态,Load 应重试而不是直接失败。
func isRetryableOpenError(err error) bool {
	if err == nil {
		return false
	}
	const (
		errorSharingViolation = syscall.Errno(32)
		errorAccessDenied     = syscall.Errno(5)
	)
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno == errorSharingViolation || errno == errorAccessDenied
	}
	return false
}
