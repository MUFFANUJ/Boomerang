package ssrf

import (
	"context"
	"errors"
	"testing"
)

func TestValidateURL(t *testing.T) {
	p := New([]string{"receivers"})
	ctx := context.Background()
	blocked := []string{
		"http://127.0.0.1/hook",
		"http://10.1.2.3/hook",
		"http://169.254.169.254/latest/meta-data",
		"http://[::1]:8080/",
		"http://192.168.1.10/",
		"http://localhost:9000/",
		"http://metadata.google.internal/",
		"ftp://example.com/",
		"http://user:pass@example.com/",
	}
	for _, u := range blocked {
		if err := p.ValidateURL(ctx, u); err == nil || !errors.Is(err, ErrBlocked) {
			t.Errorf("%s: expected ErrBlocked, got %v", u, err)
		}
	}
	allowed := []string{
		"http://receivers:9000/email", // allowlisted demo receiver
		"https://8.8.8.8/webhooks",
	}
	for _, u := range allowed {
		if err := p.ValidateURL(ctx, u); err != nil {
			t.Errorf("%s: expected allowed, got %v", u, err)
		}
	}
}
