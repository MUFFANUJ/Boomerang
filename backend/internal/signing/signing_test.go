package signing

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

const secret = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"

func TestSignVerifyRoundTrip(t *testing.T) {
	now := time.Now()
	body := []byte(`{"id":"evt_1","type":"order.created"}`)
	ts := now.Unix()
	h := SignatureHeader([]string{secret}, "dlv_1", ts, body)
	if !strings.HasPrefix(h, "v1,") {
		t.Fatalf("unexpected header %q", h)
	}
	if err := Verify([]string{secret}, "dlv_1", strconv.FormatInt(ts, 10), h, body, now, DefaultTolerance); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

func TestVerifyRejectsTampering(t *testing.T) {
	now := time.Now()
	ts := now.Unix()
	h := SignatureHeader([]string{secret}, "dlv_1", ts, []byte(`{"a":1}`))
	tsS := strconv.FormatInt(ts, 10)
	cases := map[string]error{
		"body":   Verify([]string{secret}, "dlv_1", tsS, h, []byte(`{"a":2}`), now, DefaultTolerance),
		"msg id": Verify([]string{secret}, "dlv_2", tsS, h, []byte(`{"a":1}`), now, DefaultTolerance),
		"secret": Verify([]string{"whsec_b3RoZXI="}, "dlv_1", tsS, h, []byte(`{"a":1}`), now, DefaultTolerance),
	}
	for name, err := range cases {
		if err != ErrNoMatch {
			t.Errorf("%s: want ErrNoMatch, got %v", name, err)
		}
	}
}

func TestVerifyRejectsReplay(t *testing.T) {
	now := time.Now()
	old := now.Add(-10 * time.Minute).Unix()
	body := []byte(`{}`)
	h := SignatureHeader([]string{secret}, "dlv_1", old, body)
	if err := Verify([]string{secret}, "dlv_1", strconv.FormatInt(old, 10), h, body, now, DefaultTolerance); err != ErrTimestampExpired {
		t.Fatalf("want ErrTimestampExpired, got %v", err)
	}
}

func TestRotationSignsWithBothSecrets(t *testing.T) {
	now := time.Now()
	ts := now.Unix()
	body := []byte(`{}`)
	newSecret := "whsec_bmV3LXNlY3JldC1ieXRlcy0xMjM0NTY="
	h := SignatureHeader([]string{newSecret, secret}, "dlv_1", ts, body)
	if n := len(strings.Fields(h)); n != 2 {
		t.Fatalf("want 2 signatures, got %d", n)
	}
	// A receiver that has not updated yet still verifies with the old secret.
	if err := Verify([]string{secret}, "dlv_1", strconv.FormatInt(ts, 10), h, body, now, DefaultTolerance); err != nil {
		t.Fatalf("old secret should verify during rotation: %v", err)
	}
	if err := Verify([]string{newSecret}, "dlv_1", strconv.FormatInt(ts, 10), h, body, now, DefaultTolerance); err != nil {
		t.Fatalf("new secret should verify: %v", err)
	}
}
