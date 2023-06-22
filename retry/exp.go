package retry

import (
	"time"
)

// ExpConfig is used to configure exponential backoff
type ExpConfig struct {
	Min     time.Duration
	Max     time.Duration
	Scale   float64
	Instant bool // If false, Delays() method will return 0 when first time called and backoff value otherwise.
}

// Delays implements interface Config
func (ec ExpConfig) Delays() DelayFn {
	b, zero := NewExpBackoff(ec), !ec.Instant
	return func() (time.Duration, bool) {
		if zero {
			zero = false
			return 0, true
		}
		return b.Backoff(), true
	}
}

// Exponential contains the current state of the backoff logic
type Exponential struct {
	config  ExpConfig
	current time.Duration
}

// DefaultExpBackoffConfig is a suggested configuration
var DefaultExpBackoffConfig = ExpConfig{
	Min:   10 * time.Millisecond,
	Max:   1 * time.Minute,
	Scale: 2.0,
}

// NewExpBackoff creates new expBackoff
func NewExpBackoff(config ExpConfig) *Exponential {
	return &Exponential{
		config:  config,
		current: config.Min,
	}
}

// Backoff returns the duration to wait and updates the inner state
func (b *Exponential) Backoff() time.Duration {
	beforeScale := b.current
	b.current = time.Duration(float64(b.current) * b.config.Scale)
	if b.current > b.config.Max {
		b.current = b.config.Max
	}
	return beforeScale
}

// Reset resets the backoff state
func (b *Exponential) Reset() {
	b.current = b.config.Min
}
