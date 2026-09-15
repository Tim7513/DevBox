package state

import (
	"errors"
	"testing"
)

var allEnvEvents = []EnvEventKind{
	EnvEvProvisionStarted, EnvEvRunning, EnvEvStopStarted,
	EnvEvStopped, EnvEvFailed, EnvEvOrphaned, EnvEvDestroyed,
}

// TestEnvTransitionMatrix asserts the complete legal/illegal table for observed
// state, including the idempotent self-transitions that at-least-once delivery
// forces us to accept.
func TestEnvTransitionMatrix(t *testing.T) {
	legal := map[EnvState]map[EnvEventKind]EnvState{
		EnvPending: {
			EnvEvProvisionStarted: EnvProvisioning,
			EnvEvStopped:          EnvStopped,
			EnvEvFailed:           EnvFailed,
			EnvEvDestroyed:        EnvDestroyed,
		},
		EnvProvisioning: {
			EnvEvRunning:     EnvRunning,
			EnvEvStopStarted: EnvStopping,
			EnvEvFailed:      EnvFailed,
			EnvEvOrphaned:    EnvFailed,
			EnvEvDestroyed:   EnvDestroyed,
		},
		EnvRunning: {
			EnvEvRunning:     EnvRunning, // idempotent re-report
			EnvEvStopStarted: EnvStopping,
			EnvEvFailed:      EnvFailed,
			EnvEvOrphaned:    EnvFailed,
			EnvEvDestroyed:   EnvDestroyed,
		},
		EnvStopping: {
			EnvEvStopped:   EnvStopped,
			EnvEvFailed:    EnvFailed,
			EnvEvOrphaned:  EnvFailed,
			EnvEvDestroyed: EnvDestroyed,
		},
		EnvStopped: {
			EnvEvProvisionStarted: EnvProvisioning,
			EnvEvStopped:          EnvStopped, // idempotent
			EnvEvDestroyed:        EnvDestroyed,
		},
		EnvFailed: {
			EnvEvProvisionStarted: EnvProvisioning, // recovery
			EnvEvDestroyed:        EnvDestroyed,
		},
		EnvDestroyed: {
			EnvEvDestroyed: EnvDestroyed, // absorbing + idempotent
		},
	}

	for _, from := range allEnvStates {
		for _, ev := range allEnvEvents {
			from, ev := from, ev
			t.Run(string(from)+"/"+string(ev), func(t *testing.T) {
				cur := Env{Observed: from, Desired: DesireRunning, WorkerID: "w-old"}
				res, err := ApplyEnv(cur, EnvEvent{Kind: ev, WorkerID: "w-new", Err: "e"}, t0)

				to, want := legal[from][ev]
				if !want {
					if err == nil {
						t.Fatalf("expected rejection, got transition to %s", res.Next.Observed)
					}
					if !errors.Is(err, ErrIllegalTransition) && !errors.Is(err, ErrInvalidEvent) {
						t.Fatalf("rejection should carry a sentinel, got %v", err)
					}
					return
				}
				if err != nil {
					t.Fatalf("expected legal transition, got %v", err)
				}
				if res.Next.Observed != to {
					t.Errorf("landed in %s, want %s", res.Next.Observed, to)
				}
				if res.Audit.From != string(from) || res.Audit.To != string(to) {
					t.Errorf("audit %s→%s, want %s→%s", res.Audit.From, res.Audit.To, from, to)
				}
				if res.Audit.Reason == "" {
					t.Error("every transition must record a reason")
				}
			})
		}
	}
}

// The §17 invariant: the snapshot pointer only ever advances on an event that
// carries a verified id. A stop that failed to upload must leave the previous
// snapshot in place rather than nulling it out.
func TestSnapshotPointerOnlyAdvancesOnVerifiedUpload(t *testing.T) {
	t.Run("verified upload advances", func(t *testing.T) {
		env := Env{Observed: EnvStopping, Desired: DesireStopped, SnapshotID: "snap-old"}
		res, err := ApplyEnv(env, EnvEvent{Kind: EnvEvStopped, SnapshotID: "snap-new"}, t0)
		if err != nil {
			t.Fatal(err)
		}
		if res.Next.SnapshotID != "snap-new" {
			t.Errorf("SnapshotID = %q, want snap-new", res.Next.SnapshotID)
		}
	})

	t.Run("missing id preserves the previous snapshot", func(t *testing.T) {
		env := Env{Observed: EnvStopping, Desired: DesireStopped, SnapshotID: "snap-old"}
		res, err := ApplyEnv(env, EnvEvent{Kind: EnvEvStopped}, t0)
		if err != nil {
			t.Fatal(err)
		}
		if res.Next.SnapshotID != "snap-old" {
			t.Errorf("SnapshotID = %q; a failed upload must never clobber the last good snapshot", res.Next.SnapshotID)
		}
	})

	t.Run("failure preserves the previous snapshot", func(t *testing.T) {
		env := Env{Observed: EnvRunning, Desired: DesireRunning, SnapshotID: "snap-old", WorkerID: "w1"}
		res, err := ApplyEnv(env, EnvEvent{Kind: EnvEvFailed, Err: "container OOMKilled"}, t0)
		if err != nil {
			t.Fatal(err)
		}
		if res.Next.SnapshotID != "snap-old" {
			t.Error("a crash must not lose the snapshot pointer; that is the user's data")
		}
	})
}

// Worker binding must be cleared on every exit from compute, so placement is
// free to choose a different node next time (§17: recreate anywhere).
func TestWorkerBindingClearedOnEveryComputeExit(t *testing.T) {
	for _, tc := range []struct {
		name string
		from EnvState
		ev   EnvEvent
	}{
		{"stopped", EnvStopping, EnvEvent{Kind: EnvEvStopped, SnapshotID: "s"}},
		{"failed", EnvRunning, EnvEvent{Kind: EnvEvFailed, Err: "crash"}},
		{"orphaned", EnvRunning, EnvEvent{Kind: EnvEvOrphaned, WorkerID: "w-dead"}},
		{"destroyed", EnvRunning, EnvEvent{Kind: EnvEvDestroyed}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := Env{Observed: tc.from, Desired: DesireRunning, WorkerID: "w-old"}
			res, err := ApplyEnv(env, tc.ev, t0)
			if err != nil {
				t.Fatal(err)
			}
			if res.Next.WorkerID != "" {
				t.Errorf("WorkerID = %q, want cleared", res.Next.WorkerID)
			}
			var cleared bool
			for _, e := range res.Effects {
				if _, ok := e.(ClearWorkerBinding); ok {
					cleared = true
				}
			}
			if !cleared {
				t.Error("missing ClearWorkerBinding effect")
			}
		})
	}
}

// Orphaning is recorded distinctly from an ordinary failure, because the
// operator reading the audit log needs to tell "your build broke" apart from
// "we lost the node you were on".
func TestOrphanedIsAttributedToTheReaper(t *testing.T) {
	env := Env{Observed: EnvRunning, Desired: DesireRunning, WorkerID: "w-dead"}
	res, err := ApplyEnv(env, EnvEvent{Kind: EnvEvOrphaned, WorkerID: "w-dead"}, t0)
	if err != nil {
		t.Fatal(err)
	}
	if res.Audit.Actor != "scheduler:reaper" {
		t.Errorf("actor = %q, want scheduler:reaper", res.Audit.Actor)
	}
	if res.Next.Observed != EnvFailed {
		t.Errorf("observed = %s, want failed", res.Next.Observed)
	}
}

func TestRecoveryAttemptIsCountedOnlyFromFailed(t *testing.T) {
	t.Run("from failed counts", func(t *testing.T) {
		env := Env{Observed: EnvFailed, Desired: DesireRunning, RecoverAttempts: 1}
		res, err := ApplyEnv(env, EnvEvent{Kind: EnvEvProvisionStarted, WorkerID: "w2"}, t0)
		if err != nil {
			t.Fatal(err)
		}
		if res.Next.RecoverAttempts != 2 {
			t.Errorf("RecoverAttempts = %d, want 2", res.Next.RecoverAttempts)
		}
		if res.Audit.Actor != "scheduler:reaper" {
			t.Errorf("actor = %q, want scheduler:reaper", res.Audit.Actor)
		}
	})

	t.Run("user-initiated start does not count", func(t *testing.T) {
		env := Env{Observed: EnvStopped, Desired: DesireRunning, RecoverAttempts: 1}
		res, err := ApplyEnv(env, EnvEvent{Kind: EnvEvProvisionStarted, WorkerID: "w2"}, t0)
		if err != nil {
			t.Fatal(err)
		}
		if res.Next.RecoverAttempts != 1 {
			t.Errorf("RecoverAttempts = %d, want 1 unchanged", res.Next.RecoverAttempts)
		}
	})
}

func TestDestroyedIsAbsorbing(t *testing.T) {
	env := Env{Observed: EnvDestroyed, Desired: DesireRunning}
	for _, ev := range allEnvEvents {
		if ev == EnvEvDestroyed {
			continue // idempotent re-report is allowed
		}
		if _, err := ApplyEnv(env, EnvEvent{Kind: ev, WorkerID: "w"}, t0); !errors.Is(err, ErrIllegalTransition) {
			t.Errorf("destroyed accepted %s; must be absorbing", ev)
		}
	}
}

func TestEnvUnknownEventRejected(t *testing.T) {
	env := Env{Observed: EnvRunning, Desired: DesireRunning}
	if _, err := ApplyEnv(env, EnvEvent{Kind: "bogus"}, t0); !errors.Is(err, ErrInvalidEvent) {
		t.Error("unknown event must be rejected with ErrInvalidEvent")
	}
	if _, err := ApplyEnv(Env{Observed: "bogus"}, EnvEvent{Kind: EnvEvRunning}, t0); err == nil {
		t.Error("unknown observed state must be rejected")
	}
}

func TestEnvStateClassifiers(t *testing.T) {
	transient := map[EnvState]bool{EnvProvisioning: true, EnvStopping: true}
	holdsCompute := map[EnvState]bool{EnvProvisioning: true, EnvRunning: true, EnvStopping: true}
	for _, s := range allEnvStates {
		if s.Transient() != transient[s] {
			t.Errorf("%s.Transient() = %v, want %v", s, s.Transient(), transient[s])
		}
		if s.HoldsCompute() != holdsCompute[s] {
			t.Errorf("%s.HoldsCompute() = %v, want %v", s, s.HoldsCompute(), holdsCompute[s])
		}
	}
}
