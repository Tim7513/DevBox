package state

import (
	"math"
	"testing"
	"time"
)

func TestBackoffWindowGrowsThenCaps(t *testing.T) {
	b := Backoff{Base: time.Second, Cap: 10 * time.Second, MaxShift: 16}
	want := []time.Duration{
		1 * time.Second,  // attempt 1
		2 * time.Second,  // attempt 2
		4 * time.Second,  // attempt 3
		8 * time.Second,  // attempt 4
		10 * time.Second, // attempt 5 — capped (would be 16s)
		10 * time.Second, // attempt 6 — stays capped
	}
	for i, w := range want {
		if got := b.Window(i + 1); got != w {
			t.Errorf("Window(%d) = %v, want %v", i+1, got, w)
		}
	}
}

// The exponent must not be able to overflow the duration, no matter what
// attempt count arrives from the database.
func TestBackoffDoesNotOverflow(t *testing.T) {
	b := DefaultBackoff
	for _, attempt := range []int{0, -5, 1, 62, 63, 64, 1 << 20, math.MaxInt32} {
		got := b.Window(attempt)
		if got <= 0 {
			t.Errorf("Window(%d) = %v; must stay positive", attempt, got)
		}
		if got > b.Cap {
			t.Errorf("Window(%d) = %v, exceeds cap %v", attempt, got, b.Cap)
		}
	}
}

func TestBackoffZeroValueFallsBackToDefaults(t *testing.T) {
	var zero Backoff
	if got := zero.Window(1); got != DefaultBackoff.Base {
		t.Errorf("zero-value Window(1) = %v, want %v", got, DefaultBackoff.Base)
	}
	if got := zero.Window(100); got != DefaultBackoff.Cap {
		t.Errorf("zero-value Window(100) = %v, want %v", got, DefaultBackoff.Cap)
	}
}

// Full jitter: the delay must span essentially the whole window, because
// spreading a correlated retry cohort is the entire reason for the design.
func TestDelayIsFullJitter(t *testing.T) {
	b := Backoff{Base: time.Second, Cap: time.Minute, MaxShift: 16}
	const attempt = 4 // window = 8s

	if got := b.Delay(attempt, 0); got != 0 {
		t.Errorf("Delay at r=0 = %v, want 0 (full jitter reaches the bottom)", got)
	}
	near := b.Delay(attempt, 0.999999)
	if near < 7*time.Second || near > 8*time.Second {
		t.Errorf("Delay at r≈1 = %v, want just under the 8s window", near)
	}
	mid := b.Delay(attempt, 0.5)
	if mid < 3900*time.Millisecond || mid > 4100*time.Millisecond {
		t.Errorf("Delay at r=0.5 = %v, want ≈4s", mid)
	}
}

func TestDelayClampsOutOfRangeRandomness(t *testing.T) {
	b := Backoff{Base: time.Second, Cap: time.Minute, MaxShift: 16}
	for _, r := range []float64{-1, -0.0001, 1, 1.5, 42} {
		got := b.Delay(3, r)
		if got < 0 || got > b.Window(3) {
			t.Errorf("Delay(3, %v) = %v, outside [0, %v]", r, got, b.Window(3))
		}
	}
}

// The schedule must fit inside a sane operational envelope: a task that
// exhausts its budget should do so in minutes, not hours, or the user is left
// staring at a spinner with no signal.
func TestDefaultScheduleTotalIsBounded(t *testing.T) {
	var worst time.Duration
	for attempt := 1; attempt <= TaskProvision.MaxAttempts(); attempt++ {
		worst += DefaultBackoff.Window(attempt)
	}
	if worst > 10*time.Minute {
		t.Errorf("worst-case provision retry schedule = %v, want ≤ 10m", worst)
	}
	if worst < 30*time.Second {
		t.Errorf("worst-case provision retry schedule = %v, suspiciously short", worst)
	}
}
