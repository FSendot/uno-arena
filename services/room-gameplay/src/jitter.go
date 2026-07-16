package main

import (
	"math/rand/v2"
	"time"
)

const cadenceJitterFraction = 0.20

// jitterDuration spreads replica work across a bounded +/-20 percent window.
// The sample form is pure so tests can assert scheduling without sleeping.
func jitterDuration(base time.Duration, sample float64) time.Duration {
	if base <= 0 {
		return base
	}
	if sample < 0 {
		sample = 0
	}
	if sample > 1 {
		sample = 1
	}
	factor := (1 - cadenceJitterFraction) + (2 * cadenceJitterFraction * sample)
	return time.Duration(float64(base) * factor)
}

func randomJitterDuration(base time.Duration) time.Duration {
	return jitterDuration(base, rand.Float64())
}
