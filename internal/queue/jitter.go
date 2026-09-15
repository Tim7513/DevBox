package queue

import (
	"math/rand/v2"
)

// jitter returns a uniform float in [0,1) for the full-jitter backoff.
//
// Isolated behind a function so tests can reason about the schedule, and so the
// randomness source is a single documented choice: math/rand/v2's global
// generator is goroutine-safe and seeded per process, which is what we want —
// two schedulers must not produce correlated retry delays, and nothing here is
// security-sensitive.
func jitter() float64 { return rand.Float64() }
