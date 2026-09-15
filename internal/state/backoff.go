package state

import "time"

// Backoff computes retry delays: exponential growth with full jitter, capped.
//
// Full jitter (delay uniform in [0, exp)) rather than equal jitter or plain
// exponential, because DevBox's dominant retry scenario is correlated: a worker
// dies holding N leases, the reaper requeues all N in the same pass, and they
// all become retryable at the same instant. Plain exponential would send that
// whole cohort at the fleet simultaneously on every attempt, and keep them
// synchronised forever. Full jitter spreads the cohort across the entire window
// on the first retry and destroys the correlation permanently.
//
// The cost is that an individual retry may fire almost immediately. That is
// acceptable here: the attempt budget, not the delay, is what bounds total work.
type Backoff struct {
	// Base is the delay scale for the first retry.
	Base time.Duration
	// Cap bounds the exponential window regardless of attempt count.
	Cap time.Duration
	// MaxShift bounds the exponent so 1<<attempt cannot overflow on a task
	// whose attempts column was corrupted or whose budget is very large.
	MaxShift uint
}

// DefaultBackoff is the schedule used by the retry scheduler.
//
// Base 2s: shorter than a heartbeat interval is pointless, since most retryable
// failures are a worker or dependency that needs at least a moment. Cap 2m:
// beyond that an environment is functionally down and the user should be
// looking at the error rather than waiting on an invisible timer; the attempt
// budget expires within a few multiples of the cap for every task type.
var DefaultBackoff = Backoff{
	Base:     2 * time.Second,
	Cap:      2 * time.Minute,
	MaxShift: 16,
}

// Window returns the upper bound of the jitter window for a given attempt
// number (1-based: the delay before retry #1 is Window(1)). It is deterministic
// and exported separately from Delay so tests can assert the growth curve
// without reasoning about randomness.
func (b Backoff) Window(attempt int) time.Duration {
	base := b.Base
	if base <= 0 {
		base = DefaultBackoff.Base
	}
	capped := b.Cap
	if capped <= 0 {
		capped = DefaultBackoff.Cap
	}
	maxShift := b.MaxShift
	if maxShift == 0 {
		maxShift = DefaultBackoff.MaxShift
	}

	if attempt < 1 {
		attempt = 1
	}
	shift := uint(attempt - 1)
	if shift > maxShift {
		shift = maxShift
	}

	// Compute in the integer domain and check the cap before it can overflow.
	window := base
	for i := uint(0); i < shift; i++ {
		window *= 2
		if window >= capped {
			return capped
		}
	}
	if window > capped {
		return capped
	}
	return window
}

// Delay returns the actual backoff for an attempt, drawing full jitter from r.
//
// r is a float in [0,1) supplied by the caller rather than drawn here, so the
// function stays pure and the retry schedule is exactly reproducible in tests
// and benchmarks.
func (b Backoff) Delay(attempt int, r float64) time.Duration {
	if r < 0 {
		r = 0
	}
	if r >= 1 {
		// Guard against a caller handing us 1.0; the window is a half-open
		// bound and a delay exactly equal to the cap is not harmful, but
		// clamping keeps the contract honest.
		r = 0.999999
	}
	return time.Duration(float64(b.Window(attempt)) * r)
}
