// Package httpx holds small HTTP helpers shared by the services.
package httpx

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// JSON writes v as JSON with the given status.
func JSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// Error writes a JSON error.
func Error(w http.ResponseWriter, status int, code, msg string) {
	JSON(w, status, map[string]string{"error": msg, "code": code})
}

// ErrTooLarge is returned by Decode when the body exceeds the limit.
var ErrTooLarge = errors.New("request body too large")

// Decode reads a JSON body of at most limit bytes into dst.
func Decode(r *http.Request, limit int64, dst any) error {
	b, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		return err
	}
	if int64(len(b)) > limit {
		return ErrTooLarge
	}
	if len(b) == 0 {
		return nil
	}
	return json.Unmarshal(b, dst)
}

// statusRecorder captures the response status for logging.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Hijack lets WebSocket upgrades through the wrapper.
func (s *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := s.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("hijacking not supported")
	}
	s.status = http.StatusSwitchingProtocols
	return h.Hijack()
}

// Flush supports streaming responses.
func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap exposes the underlying writer to http.ResponseController.
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// Logging logs non-2xx responses and slow requests.
func Logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: 200}
		next.ServeHTTP(rec, r)
		d := time.Since(start)
		if rec.status == http.StatusSwitchingProtocols {
			return // long-lived WebSocket; not a slow request
		}
		if rec.status >= 500 || d > time.Second {
			slog.Warn("request", "method", r.Method, "path", r.URL.Path, "status", rec.status, "duration_ms", d.Milliseconds())
		} else if rec.status >= 400 && rec.status != 429 {
			slog.Info("request", "method", r.Method, "path", r.URL.Path, "status", rec.status, "duration_ms", d.Milliseconds())
		}
	})
}

// Recover turns panics into 500s.
func Recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				slog.Error("panic", "panic", v, "path", r.URL.Path)
				Error(w, http.StatusInternalServerError, "internal", "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
