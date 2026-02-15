// Package retry provides exponential backoff retry logic for Fitz client backpressure scenarios.
package retry

import (
	"context"
	"math"
	"math/rand"
	"time"
)

// BackoffConfig defines exponential backoff parameters
type BackoffConfig struct {
	InitialDelay time.Duration // Starting delay (e.g., 10ms)
	MaxDelay     time.Duration // Maximum delay (e.g., 1s)
	Multiplier   float64       // Growth factor per retry (e.g., 2.0)
	JitterFactor float64       // Randomness factor [0, 1]
}

// DefaultBackoff provides sensible defaults: 10ms initial, 1s max, 2x multiplier, 0.1 jitter
var DefaultBackoff = BackoffConfig{
	InitialDelay: 10 * time.Millisecond,
	MaxDelay:     1 * time.Second,
	Multiplier:   2.0,
	JitterFactor: 0.1,
}

// Do executes fn with exponential backoff retry on isRetryable errors.
// isRetryable should return true for errors that warrant retry (e.g., backpressure).
// maxRetries limits the number of attempts (0 = no retries, 1 = only one attempt, etc).
func Do(ctx context.Context, cfg BackoffConfig, maxRetries int, fn func() error, isRetryable func(error) bool) error {
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if err := fn(); err == nil {
			return nil
		} else if !isRetryable(err) {
			return err
		} else {
			lastErr = err
			if attempt < maxRetries {
				delay := calculateDelay(cfg, attempt)
				select {
				case <-time.After(delay):
					// Continue to next attempt
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	}
	return lastErr
}

// calculateDelay computes exponential backoff with jitter
func calculateDelay(cfg BackoffConfig, attempt int) time.Duration {
	// Base delay = initial * (multiplier ^ attempt)
	baseDelay := float64(cfg.InitialDelay) * math.Pow(cfg.Multiplier, float64(attempt))

	// Cap at maximum
	if baseDelay > float64(cfg.MaxDelay) {
		baseDelay = float64(cfg.MaxDelay)
	}

	// Add jitter: ±jitterFactor * baseDelay
	jitterRange := baseDelay * cfg.JitterFactor
	jitter := (rand.Float64()*2 - 1) * jitterRange // [-jitterRange, +jitterRange]

	finalDelay := baseDelay + jitter
	if finalDelay < 0 {
		finalDelay = 0
	}

	return time.Duration(finalDelay)
}
