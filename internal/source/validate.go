// ─────────────────────────────────────────────────────────────
// FasterEdge 开源项目
// Github: https://github.com/FasterEdge
// Gitee:  https://gitee.com/FasterEdge
// ─────────────────────────────────────────────────────────────
// Package source holds per-source validation and provenance tracking for
// the FasterEdge nodes NetMap polls.
package source

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
)

// DisallowReason explains why a particular base URL was rejected. The
// values are stable strings so callers can render them in logs / metrics.
type DisallowReason string

const (
	ReasonEmpty          DisallowReason = "empty"
	ReasonBadScheme      DisallowReason = "bad_scheme"
	ReasonBadURL         DisallowReason = "bad_url"
	ReasonLoopback       DisallowReason = "loopback"
	ReasonPrivate        DisallowReason = "private"
	ReasonLinkLocal      DisallowReason = "link_local"
	ReasonMulticast      DisallowReason = "multicast"
	ReasonUnspecified    DisallowReason = "unspecified"
	ReasonInvalidLiteral DisallowReason = "invalid_literal"
)

// ValidationPolicy controls whether private / loopback / link-local /
// multicast hosts are accepted. By construction it is deny-by-default; the
// CLI flag -allow-private-nodes opts in to a permissive mode for
// development & lab use.
type ValidationPolicy struct {
	// AllowPrivate, when true, permits loopback, RFC1918 private, link
	// local and multicast addresses. It is intended for dev/test only.
	AllowPrivate bool
	// AllowedHosts is an explicit allow-list of hostnames. When non-empty,
	// a host must match one of these entries (case-insensitive) to be
	// accepted regardless of IP class. IP literals are validated through
	// the IP-class rules instead.
	AllowedHosts map[string]struct{}
}

// DefaultPolicy is the deny-by-default policy used when callers do not
// explicitly request permissive mode.
func DefaultPolicy() ValidationPolicy {
	return ValidationPolicy{}
}

// PermissivePolicy is the policy used when -allow-private-nodes is set.
func PermissivePolicy() ValidationPolicy {
	return ValidationPolicy{AllowPrivate: true}
}

// Validate performs structural validation on a raw base URL string and
// returns the normalised URL when it passes the policy. When the URL is
// rejected, the returned error is wrapped with a DisallowReason for
// programmatic inspection.
func (p ValidationPolicy) Validate(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", &DisallowError{Reason: ReasonEmpty, Msg: "baseURL is empty"}
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", &DisallowError{Reason: ReasonBadURL, Msg: err.Error()}
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", &DisallowError{Reason: ReasonBadScheme, Msg: fmt.Sprintf("scheme %q not allowed", u.Scheme)}
	}
	if u.Host == "" {
		return "", &DisallowError{Reason: ReasonBadURL, Msg: "empty host"}
	}
	host := u.Hostname()
	port := u.Port()
	// Allow-list overrides IP class rules.
	if p.AllowedHosts != nil {
		if _, ok := p.AllowedHosts[strings.ToLower(host)]; ok {
			return normalise(u, port), nil
		}
	}
	if ip := net.ParseIP(host); ip != nil {
		if reason := p.classifyIP(ip); reason != "" {
			return "", &DisallowError{Reason: reason, Msg: fmt.Sprintf("address %s denied (%s)", ip, reason)}
		}
		return normalise(u, port), nil
	}
	// Hostname path: defer IP-class checks to DNS time. We do not resolve
	// here on purpose — resolution is the HTTP client's job. We just
	// require syntactically sane characters.
	if !isValidHostname(host) {
		return "", &DisallowError{Reason: ReasonInvalidLiteral, Msg: fmt.Sprintf("hostname %q is not valid", host)}
	}
	return normalise(u, port), nil
}

// classifyIP returns the deny reason for an IP literal that violates the
// policy, or the empty string when the address is acceptable.
func (p ValidationPolicy) classifyIP(ip net.IP) DisallowReason {
	if ip.IsLoopback() {
		if p.AllowPrivate {
			return ""
		}
		return ReasonLoopback
	}
	if ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		if p.AllowPrivate {
			return ""
		}
		return ReasonLinkLocal
	}
	if ip.IsMulticast() {
		if p.AllowPrivate {
			return ""
		}
		return ReasonMulticast
	}
	if ip.IsUnspecified() {
		return ReasonUnspecified
	}
	if ip.IsPrivate() {
		if p.AllowPrivate {
			return ""
		}
		return ReasonPrivate
	}
	return ""
}
func normalise(u *url.URL, port string) string {
	host := u.Host
	// Trim any default port so callers always see the canonical form.
	if (u.Scheme == "http" && port == "80") || (u.Scheme == "https" && port == "443") {
		host = u.Hostname()
	}
	// Strip trailing slash on the path so we don't end up with "//api" joins.
	path := strings.TrimRight(u.Path, "/")
	if path == "" {
		return fmt.Sprintf("%s://%s", u.Scheme, host)
	}
	return fmt.Sprintf("%s://%s%s", u.Scheme, host, path)
}
func isValidHostname(h string) bool {
	if h == "" || len(h) > 253 {
		return false
	}
	for _, label := range strings.Split(h, ".") {
		if label == "" || len(label) > 63 {
			return false
		}
		for _, r := range label {
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' {
				continue
			}
			return false
		}
		if label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
	}
	return true
}

// DisallowError is returned when a base URL is rejected by the policy. It
// carries the reason so log lines / metric labels can be structured.
type DisallowError struct {
	Reason DisallowReason
	Msg    string
}

func (e *DisallowError) Error() string { return fmt.Sprintf("%s: %s", e.Reason, e.Msg) }
func (e *DisallowError) Is(target error) bool {
	var t *DisallowError
	if errors.As(target, &t) {
		return e.Reason == t.Reason
	}
	return false
}
