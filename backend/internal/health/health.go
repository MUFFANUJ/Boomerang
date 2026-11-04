// Package health classifies endpoint health for the dashboard.
package health

import "math"

// Status values.
const (
	Healthy     = "healthy"
	Degraded    = "degraded"
	Failing     = "failing"
	Paused      = "paused"
	CircuitOpen = "circuit_open"
)

// Classify turns breaker state and recent success rate into a status and a 0..100 score.
func Classify(paused bool, circuitState string, successRate *float64) (string, int) {
	score := 100
	if successRate != nil {
		score = int(math.Round(*successRate * 100))
	}
	switch {
	case paused:
		return Paused, score
	case circuitState == "open" || circuitState == "half_open":
		return CircuitOpen, min(score, 10)
	case successRate == nil || *successRate >= 0.95:
		return Healthy, score
	case *successRate >= 0.7:
		return Degraded, score
	default:
		return Failing, score
	}
}
