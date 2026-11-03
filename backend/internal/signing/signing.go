// Package signing implements Boomerang's webhook signatures: HMAC-SHA256 over
// "<message id>.<unix timestamp>.<body>", with timestamps to stop replays and multiple
// signatures during secret rotation.
package signing

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"time"
)

// Header names.
const (
	HeaderID        = "Boomerang-Id"
	HeaderEventID   = "Boomerang-Event-Id"
	HeaderEventType = "Boomerang-Event-Type"
	HeaderAttempt   = "Boomerang-Attempt"
	HeaderTimestamp = "Boomerang-Timestamp"
	HeaderSignature = "Boomerang-Signature"
)

// DefaultTolerance is how far a timestamp may drift before a receiver rejects it.
const DefaultTolerance = 5 * time.Minute

var (
	ErrNoSignature      = errors.New("missing signature")
	ErrBadTimestamp     = errors.New("invalid timestamp")
	ErrTimestampExpired = errors.New("timestamp outside tolerance (possible replay)")
	ErrNoMatch          = errors.New("no signature matched")
)

func secretBytes(secret string) []byte {
	if s, ok := strings.CutPrefix(secret, "whsec_"); ok {
		if b, err := base64.StdEncoding.DecodeString(s); err == nil {
			return b
		}
	}
	return []byte(secret)
}

// Sign returns a single "v1,<base64>" signature.
func Sign(secret, msgID string, ts int64, body []byte) string {
	mac := hmac.New(sha256.New, secretBytes(secret))
	mac.Write([]byte(msgID))
	mac.Write([]byte{'.'})
	mac.Write([]byte(strconv.FormatInt(ts, 10)))
	mac.Write([]byte{'.'})
	mac.Write(body)
	return "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// SignatureHeader signs with every active secret (newest first), space separated.
func SignatureHeader(secrets []string, msgID string, ts int64, body []byte) string {
	parts := make([]string, 0, len(secrets))
	for _, s := range secrets {
		if s != "" {
			parts = append(parts, Sign(s, msgID, ts, body))
		}
	}
	return strings.Join(parts, " ")
}

// Verify checks a request against the receiver's secrets.
func Verify(secrets []string, msgID, tsHeader, sigHeader string, body []byte, now time.Time, tolerance time.Duration) error {
	if sigHeader == "" {
		return ErrNoSignature
	}
	ts, err := strconv.ParseInt(tsHeader, 10, 64)
	if err != nil {
		return ErrBadTimestamp
	}
	d := now.Sub(time.Unix(ts, 0))
	if d < -tolerance || d > tolerance {
		return ErrTimestampExpired
	}
	for _, s := range secrets {
		want := Sign(s, msgID, ts, body)
		for _, got := range strings.Fields(sigHeader) {
			if hmac.Equal([]byte(got), []byte(want)) {
				return nil
			}
		}
	}
	return ErrNoMatch
}
