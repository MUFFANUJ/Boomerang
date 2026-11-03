// Package ssrf keeps webhook deliveries from reaching private or internal networks. URLs are
// checked when endpoints are registered, and again at connect time (after DNS resolution),
// which defeats DNS rebinding. Hosts on the allowlist (the demo's mock receivers) are exempt.
package ssrf

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strings"
	"time"
)

// ErrBlocked is returned for disallowed destinations.
var ErrBlocked = errors.New("destination blocked by SSRF policy")

var blockedPrefixes = func() []netip.Prefix {
	var out []netip.Prefix
	for _, s := range []string{
		"0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16", "172.16.0.0/12",
		"192.0.0.0/24", "192.0.2.0/24", "192.88.99.0/24", "192.168.0.0/16", "198.18.0.0/15", "198.51.100.0/24",
		"203.0.113.0/24", "224.0.0.0/4", "240.0.0.0/4", "255.255.255.255/32",
		"::/128", "::1/128", "::ffff:0:0/96", "64:ff9b::/96", "100::/64", "2001:db8::/32", "fc00::/7", "fe80::/10", "ff00::/8",
	} {
		out = append(out, netip.MustParsePrefix(s))
	}
	return out
}()

// Policy validates destinations.
type Policy struct {
	allow    map[string]bool
	resolver *net.Resolver
}

// New creates a policy with an exact-hostname allowlist.
func New(allowHosts []string) *Policy {
	m := map[string]bool{}
	for _, h := range allowHosts {
		m[strings.ToLower(h)] = true
	}
	return &Policy{allow: m, resolver: net.DefaultResolver}
}

// Allowed reports whether the hostname is on the allowlist.
func (p *Policy) Allowed(host string) bool { return p.allow[strings.ToLower(host)] }

// IsPublic reports whether an address may be contacted.
func IsPublic(addr netip.Addr) bool {
	addr = addr.Unmap()
	for _, pf := range blockedPrefixes {
		if pf.Contains(addr) {
			return false
		}
	}
	return addr.IsValid()
}

// ValidateURL checks scheme, host and resolved addresses.
func (p *Policy) ValidateURL(ctx context.Context, raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid url: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("%w: scheme must be http or https", ErrBlocked)
	}
	if u.User != nil {
		return fmt.Errorf("%w: credentials in URL are not allowed", ErrBlocked)
	}
	host := u.Hostname()
	if host == "" {
		return errors.New("invalid url: missing host")
	}
	if p.Allowed(host) {
		return nil
	}
	if strings.EqualFold(host, "localhost") || strings.HasSuffix(strings.ToLower(host), ".internal") || strings.HasSuffix(strings.ToLower(host), ".local") {
		return fmt.Errorf("%w: %s is an internal hostname", ErrBlocked, host)
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		if !IsPublic(ip) {
			return fmt.Errorf("%w: %s is a private or reserved address", ErrBlocked, ip)
		}
		return nil
	}
	rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	ips, err := p.resolver.LookupNetIP(rctx, "ip", host)
	if err != nil {
		return fmt.Errorf("could not resolve %s: %w", host, err)
	}
	for _, ip := range ips {
		if !IsPublic(ip) {
			return fmt.Errorf("%w: %s resolves to private address %s", ErrBlocked, host, ip)
		}
	}
	return nil
}

// DialContext dials only public addresses (or allowlisted hosts). Use it as an
// http.Transport DialContext.
func (p *Policy) DialContext(dialer *net.Dialer) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		if p.Allowed(host) {
			return dialer.DialContext(ctx, network, addr)
		}
		ips, err := p.resolver.LookupNetIP(ctx, "ip", host)
		if err != nil {
			return nil, err
		}
		var lastErr error = ErrBlocked
		for _, ip := range ips {
			if !IsPublic(ip) {
				lastErr = fmt.Errorf("%w: %s resolves to %s", ErrBlocked, host, ip)
				continue
			}
			c, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
			if err == nil {
				return c, nil
			}
			lastErr = err
		}
		return nil, lastErr
	}
}
