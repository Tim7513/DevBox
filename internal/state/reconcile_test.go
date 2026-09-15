package state

import (
	"testing"
	"time"
)

var allEnvStates = []EnvState{EnvPending, EnvProvisioning, EnvRunning, EnvStopping, EnvStopped, EnvFailed, EnvDestroyed}
var allDesired = []DesiredState{DesireRunning, DesireStopped, DesireDestroyed}

func TestReconcileDecisionTable(t *testing.T) {
	type key struct {
		observed EnvState
		desired  DesiredState
	}
	want := map[key]Decision{
		// desired=running
		{EnvPending, DesireRunning}:      {Action: ActionEnqueue, Task: TaskProvision},
		{EnvProvisioning, DesireRunning}: {Action: ActionNone},
		{EnvRunning, DesireRunning}:      {Action: ActionNone},
		{EnvStopping, DesireRunning}:     {Action: ActionNone},
		{EnvStopped, DesireRunning}:      {Action: ActionEnqueue, Task: TaskStart},
		{EnvFailed, DesireRunning}:       {Action: ActionEnqueue, Task: TaskRecover},
		{EnvDestroyed, DesireRunning}:    {Action: ActionNone},

		// desired=stopped
		{EnvPending, DesireStopped}:      {Action: ActionSettle, Settle: EnvStopped},
		{EnvProvisioning, DesireStopped}: {Action: ActionNone},
		{EnvRunning, DesireStopped}:      {Action: ActionEnqueue, Task: TaskStop},
		{EnvStopping, DesireStopped}:     {Action: ActionNone},
		{EnvStopped, DesireStopped}:      {Action: ActionNone},
		{EnvFailed, DesireStopped}:       {Action: ActionNone},
		{EnvDestroyed, DesireStopped}:    {Action: ActionNone},

		// desired=destroyed wins from every live state
		{EnvPending, DesireDestroyed}:      {Action: ActionEnqueue, Task: TaskDestroy},
		{EnvProvisioning, DesireDestroyed}: {Action: ActionEnqueue, Task: TaskDestroy},
		{EnvRunning, DesireDestroyed}:      {Action: ActionEnqueue, Task: TaskDestroy},
		{EnvStopping, DesireDestroyed}:     {Action: ActionEnqueue, Task: TaskDestroy},
		{EnvStopped, DesireDestroyed}:      {Action: ActionEnqueue, Task: TaskDestroy},
		{EnvFailed, DesireDestroyed}:       {Action: ActionEnqueue, Task: TaskDestroy},
		{EnvDestroyed, DesireDestroyed}:    {Action: ActionNone},
	}

	for _, obs := range allEnvStates {
		for _, des := range allDesired {
			obs, des := obs, des
			t.Run(string(obs)+"→"+string(des), func(t *testing.T) {
				exp, ok := want[key{obs, des}]
				if !ok {
					t.Fatalf("test table is missing the (%s,%s) case", obs, des)
				}
				got := Reconcile(Env{Observed: obs, Desired: des}, false, t0)
				if got.Action != exp.Action {
					t.Fatalf("action = %s (%s), want %s", got.Action, got.Reason, exp.Action)
				}
				if got.Task != exp.Task {
					t.Errorf("task = %q, want %q", got.Task, exp.Task)
				}
				if got.Settle != exp.Settle {
					t.Errorf("settle = %q, want %q", got.Settle, exp.Settle)
				}
				if got.Reason == "" {
					t.Error("every decision must carry a reason for the audit log")
				}
			})
		}
	}
}

// An in-flight task suppresses all new work, from every combination. This is
// the reconciler's half of the per-environment serialization guarantee.
func TestActiveTaskSuppressesAllWork(t *testing.T) {
	for _, obs := range allEnvStates {
		for _, des := range allDesired {
			d := Reconcile(Env{Observed: obs, Desired: des}, true, t0)
			if d.Action != ActionNone {
				t.Errorf("(%s,%s) with active task: action = %s, want none", obs, des, d.Action)
			}
		}
	}
}

// TestReconcileConverges is the core anti-oscillation property (§33.5). From
// every starting combination, driving the loop with successful task execution
// must reach a fixed point in a bounded number of steps.
func TestReconcileConverges(t *testing.T) {
	const maxSteps = 12

	for _, obs := range allEnvStates {
		for _, des := range allDesired {
			obs, des := obs, des
			t.Run(string(obs)+"→"+string(des), func(t *testing.T) {
				// Start transient states already past StuckThreshold. A
				// transient environment with no task in flight is stranded by
				// definition, and the interesting question is whether the loop
				// digs itself out — not whether it waits.
				env := Env{Observed: obs, Desired: des,
					ObservedSince: t0.Add(-StuckThreshold - time.Minute)}
				seen := map[Env]int{}

				for step := 0; step < maxSteps; step++ {
					// A repeated snapshot with work still to do is a cycle.
					if prev, dup := seen[env]; dup {
						t.Fatalf("oscillation: snapshot %+v revisited (step %d and %d)", env, prev, step)
					}
					seen[env] = step

					d := Reconcile(env, false, t0)
					switch d.Action {
					case ActionNone, ActionPark:
						// Fixed point reached. Assert it is a *correct* fixed
						// point: either converged, or parked for a stated
						// reason, never silently stuck.
						assertLegitimateFixedPoint(t, env, d)
						return
					case ActionSettle:
						env.Observed = d.Settle
					case ActionEnqueue:
						env = simulateTaskSuccess(t, env, d.Task)
					}
				}
				t.Fatalf("did not converge within %d steps; ended at %+v", maxSteps, env)
			})
		}
	}
}

// TestRecoveryLoopIsBounded pins down the one cycle in the graph: an
// environment that fails on every worker must stop being rescheduled rather
// than bouncing around the fleet forever.
func TestRecoveryLoopIsBounded(t *testing.T) {
	env := Env{Observed: EnvFailed, Desired: DesireRunning}
	var recoveries int

	for step := 0; step < 50; step++ {
		d := Reconcile(env, false, t0)
		if d.Action == ActionPark {
			if recoveries != MaxRecoverAttempts {
				t.Fatalf("parked after %d recoveries, want %d", recoveries, MaxRecoverAttempts)
			}
			if d.Reason == "" {
				t.Error("parking must explain itself")
			}
			return
		}
		if d.Action != ActionEnqueue || d.Task != TaskRecover {
			t.Fatalf("step %d: got %s/%s, want enqueue/recover", step, d.Action, d.Task)
		}
		recoveries++
		// Simulate the recovery attempt failing again on a fresh worker.
		env = applyOrFail(t, env, EnvEvent{Kind: EnvEvProvisionStarted, WorkerID: "worker-retry"})
		env = applyOrFail(t, env, EnvEvent{Kind: EnvEvFailed, Err: "build OOMed again"})
	}
	t.Fatalf("recovery never parked after %d attempts", recoveries)
}

// A recovery that actually succeeds must reset the budget, so a long-lived
// environment that survives one worker death this month and another next month
// is not permanently penalised.
func TestSuccessfulRecoveryResetsBudget(t *testing.T) {
	env := Env{Observed: EnvFailed, Desired: DesireRunning, RecoverAttempts: 2}

	env = applyOrFail(t, env, EnvEvent{Kind: EnvEvProvisionStarted, WorkerID: "worker-b"})
	if env.RecoverAttempts != 3 {
		t.Fatalf("RecoverAttempts = %d, want 3", env.RecoverAttempts)
	}
	env = applyOrFail(t, env, EnvEvent{Kind: EnvEvRunning, WorkerID: "worker-b"})
	if env.RecoverAttempts != 0 {
		t.Errorf("RecoverAttempts = %d, want 0 after reaching running", env.RecoverAttempts)
	}
	if d := Reconcile(env, false, t0); d.Action != ActionNone {
		t.Errorf("recovered environment should be converged, got %s", d.Action)
	}
}

// A user asking to stop a failed environment must not be fought by the
// recovery logic. This is the desired-vs-observed precedence rule.
func TestStopWinsOverRecovery(t *testing.T) {
	env := Env{Observed: EnvFailed, Desired: DesireStopped, RecoverAttempts: 0}
	d := Reconcile(env, false, t0)
	if d.Action != ActionNone {
		t.Fatalf("action = %s (%s), want none: the user asked for it to be off", d.Action, d.Reason)
	}
}

// Flipping desired mid-flight must not cancel in-flight work; the loop waits
// for the transient state to settle and then acts on real facts.
func TestDesiredFlipMidFlightDoesNotThrash(t *testing.T) {
	env := Env{Observed: EnvProvisioning, Desired: DesireRunning, WorkerID: "w1"}

	// User changes their mind while the provision is still running.
	env.Desired = DesireStopped
	if d := Reconcile(env, false, t0); d.Action != ActionNone {
		t.Fatalf("mid-provision flip produced %s/%s; must wait for settle", d.Action, d.Task)
	}

	// Provision lands. Now the loop acts.
	env = applyOrFail(t, env, EnvEvent{Kind: EnvEvRunning, WorkerID: "w1"})
	d := Reconcile(env, false, t0)
	if d.Action != ActionEnqueue || d.Task != TaskStop {
		t.Fatalf("after settle: got %s/%s, want enqueue/stop", d.Action, d.Task)
	}
}

// A transient environment whose task is genuinely in flight must be left
// alone, but one that has been transient with nothing in flight for longer than
// StuckThreshold is stranded and must be dug out. Getting this boundary wrong
// in either direction is a real outage: too eager and we interrupt healthy
// provisions, too lazy and environments hang in "provisioning" forever.
func TestStrandedTransientEnvironmentIsRecovered(t *testing.T) {
	for _, obs := range []EnvState{EnvProvisioning, EnvStopping} {
		t.Run(string(obs), func(t *testing.T) {
			t.Run("in flight is left alone", func(t *testing.T) {
				env := Env{Observed: obs, Desired: DesireRunning, ObservedSince: t0.Add(-time.Hour)}
				if d := Reconcile(env, true, t0); d.Action != ActionNone {
					t.Errorf("action = %s, want none while a task is in flight", d.Action)
				}
			})

			t.Run("recently transient waits", func(t *testing.T) {
				env := Env{Observed: obs, Desired: DesireRunning,
					ObservedSince: t0.Add(-StuckThreshold + time.Second)}
				if d := Reconcile(env, false, t0); d.Action != ActionNone {
					t.Errorf("action = %s (%s), want none just inside the threshold", d.Action, d.Reason)
				}
			})

			t.Run("stranded past threshold settles to failed", func(t *testing.T) {
				env := Env{Observed: obs, Desired: DesireRunning,
					ObservedSince: t0.Add(-StuckThreshold - time.Second)}
				d := Reconcile(env, false, t0)
				if d.Action != ActionSettle || d.Settle != EnvFailed {
					t.Fatalf("action = %s/%s, want settle/failed", d.Action, d.Settle)
				}
				// And from there the ordinary recovery path takes over.
				env.Observed = d.Settle
				if next := Reconcile(env, false, t0); next.Action != ActionEnqueue || next.Task != TaskRecover {
					t.Errorf("after settling: got %s/%s, want enqueue/recover", next.Action, next.Task)
				}
			})

			t.Run("unknown ObservedSince waits rather than guessing", func(t *testing.T) {
				env := Env{Observed: obs, Desired: DesireRunning}
				if d := Reconcile(env, false, t0); d.Action != ActionNone {
					t.Errorf("action = %s, want none when ObservedSince is unset", d.Action)
				}
			})
		})
	}
}

// ObservedSince must track real transitions only. An idempotent re-report is
// not progress, and letting it reset the clock would defeat stuck-detection for
// exactly the environment most likely to be stuck.
func TestObservedSinceTracksRealTransitionsOnly(t *testing.T) {
	old := t0.Add(-time.Hour)

	t.Run("real transition resets", func(t *testing.T) {
		env := Env{Observed: EnvProvisioning, Desired: DesireRunning, ObservedSince: old}
		res, err := ApplyEnv(env, EnvEvent{Kind: EnvEvRunning, WorkerID: "w1"}, t0)
		if err != nil {
			t.Fatal(err)
		}
		if !res.Next.ObservedSince.Equal(t0) {
			t.Errorf("ObservedSince = %v, want %v", res.Next.ObservedSince, t0)
		}
	})

	t.Run("self-transition does not reset", func(t *testing.T) {
		env := Env{Observed: EnvRunning, Desired: DesireRunning, ObservedSince: old}
		res, err := ApplyEnv(env, EnvEvent{Kind: EnvEvRunning, WorkerID: "w1"}, t0)
		if err != nil {
			t.Fatal(err)
		}
		if !res.Next.ObservedSince.Equal(old) {
			t.Errorf("ObservedSince = %v, want it unchanged at %v", res.Next.ObservedSince, old)
		}
	})
}

func TestConvergedHelper(t *testing.T) {
	if !Converged(Env{Observed: EnvRunning, Desired: DesireRunning}, false, t0) {
		t.Error("running/running should be converged")
	}
	if Converged(Env{Observed: EnvStopped, Desired: DesireRunning}, false, t0) {
		t.Error("stopped/running should not be converged")
	}
}

func TestReconcileRefusesUnknownStates(t *testing.T) {
	if d := Reconcile(Env{Observed: "bogus", Desired: DesireRunning}, false, t0); d.Action != ActionNone {
		t.Error("unknown observed state must produce no action")
	}
	if d := Reconcile(Env{Observed: EnvRunning, Desired: "bogus"}, false, t0); d.Action != ActionNone {
		t.Error("unknown desired state must produce no action")
	}
}

// --- helpers ---

// simulateTaskSuccess applies the observed-state events a worker would report
// while executing the given task successfully.
func simulateTaskSuccess(t *testing.T, env Env, tt TaskType) Env {
	t.Helper()
	const worker = "worker-sim"
	switch tt {
	case TaskProvision, TaskStart, TaskRecover:
		env = applyOrFail(t, env, EnvEvent{Kind: EnvEvProvisionStarted, WorkerID: worker})
		return applyOrFail(t, env, EnvEvent{Kind: EnvEvRunning, WorkerID: worker})
	case TaskStop:
		env = applyOrFail(t, env, EnvEvent{Kind: EnvEvStopStarted, WorkerID: worker})
		return applyOrFail(t, env, EnvEvent{Kind: EnvEvStopped, WorkerID: worker, SnapshotID: "snap-1"})
	case TaskDestroy:
		return applyOrFail(t, env, EnvEvent{Kind: EnvEvDestroyed, WorkerID: worker})
	case TaskSnapshot:
		return env
	}
	t.Fatalf("unhandled task type %q in simulator", tt)
	return env
}

func applyOrFail(t *testing.T, env Env, ev EnvEvent) Env {
	t.Helper()
	res, err := ApplyEnv(env, ev, t0)
	if err != nil {
		t.Fatalf("ApplyEnv(%s, %s): %v", env.Observed, ev.Kind, err)
	}
	return res.Next
}

func assertLegitimateFixedPoint(t *testing.T, env Env, d Decision) {
	t.Helper()
	if d.Action == ActionPark {
		return // parking is a deliberate, explained stop
	}
	switch env.Desired {
	case DesireRunning:
		if env.Observed != EnvRunning && env.Observed != EnvDestroyed {
			t.Fatalf("settled at observed=%s with desired=running (%s)", env.Observed, d.Reason)
		}
	case DesireStopped:
		if env.Observed != EnvStopped && env.Observed != EnvFailed && env.Observed != EnvDestroyed {
			t.Fatalf("settled at observed=%s with desired=stopped (%s)", env.Observed, d.Reason)
		}
	case DesireDestroyed:
		if env.Observed != EnvDestroyed {
			t.Fatalf("settled at observed=%s with desired=destroyed (%s)", env.Observed, d.Reason)
		}
	}
}
