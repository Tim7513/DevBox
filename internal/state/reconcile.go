package state

import (
	"fmt"
	"time"
)

// StuckThreshold is how long an environment may sit in a transient observed
// state with no task in flight before the reconciler declares it stranded.
//
// It is deliberately far longer than any legitimate gap. The lease TTL is 15s
// and the reaper runs well inside that, so the primary mechanisms have many
// chances to act first; this is a backstop, and a backstop that fires early
// would fight the machinery it is backing up. Five minutes is also comfortably
// longer than a slow provision's individual state transitions, so it cannot
// trip on a merely sluggish worker.
const StuckThreshold = 5 * time.Minute

// ActionKind is what the reconciler decided to do about one environment.
type ActionKind string

const (
	// ActionNone: the environment is converged, converging, or parked. Do
	// nothing. The overwhelming majority of reconciler passes end here, which
	// is the property that keeps the loop cheap.
	ActionNone ActionKind = "none"
	// ActionEnqueue: create a task to move observed one step toward desired.
	ActionEnqueue ActionKind = "enqueue"
	// ActionSettle: observed can be moved to its target with no compute at all
	// (e.g. a never-provisioned environment asked to stop). The caller applies
	// the observed-state change directly rather than burning a worker on a
	// guaranteed no-op.
	ActionSettle ActionKind = "settle"
	// ActionPark: desired is unreachable and further attempts would loop. The
	// caller records a terminal failure with Reason and stops trying. This is
	// the escape hatch that makes the loop well-founded.
	ActionPark ActionKind = "park"
)

// Decision is the reconciler's output for a single environment.
type Decision struct {
	Action ActionKind
	// Task is set when Action is ActionEnqueue.
	Task TaskType
	// Settle is set when Action is ActionSettle.
	Settle EnvState
	// Reason is always set and is written to the audit log, so an operator can
	// always answer "why did the system do that".
	Reason string
}

// Reconcile decides the single next step for one environment.
//
// This is the Kubernetes-style level-triggered loop: it looks only at current
// facts, never at history or at what a previous pass decided, so a scheduler
// that crashes mid-pass and a fresh leader that takes over reach the same
// conclusion from the same database rows. That property is what makes leader
// failover safe (§33.4) — there is no in-memory reconciler state to lose.
//
// Convergence argument. The loop terminates because every ActionEnqueue moves
// observed strictly closer to desired along a finite, acyclic path, and because
// the only cycle in the state graph (failed → provisioning → failed) is cut by
// the RecoverAttempts bound, which is monotonic and never reset except on a
// confirmed arrival at running. Every other outcome is ActionNone, which by
// definition cannot oscillate.
//
// hasActiveTask must reflect the same predicate as TaskState.Active and as the
// partial unique index on tasks. When it is true the answer is always
// ActionNone: something is already in flight for this environment, and the
// index would reject a second task anyway.
//
// now is used only for stuck-detection (see StuckThreshold); the decision is
// otherwise a pure function of the two states.
func Reconcile(env Env, hasActiveTask bool, now time.Time) Decision {
	if !env.Observed.Valid() {
		return Decision{Action: ActionNone, Reason: "unknown observed state; refusing to act"}
	}
	if !env.Desired.Valid() {
		return Decision{Action: ActionNone, Reason: "unknown desired state; refusing to act"}
	}

	// Rule 1: never issue work against an environment that already has work in
	// flight. Per-environment serialization is enforced by the database, but
	// bailing here saves a guaranteed constraint violation every pass.
	if hasActiveTask {
		return Decision{Action: ActionNone, Reason: "task already in flight"}
	}

	// Rule 2: destroyed is absorbing. Nothing resurrects an environment.
	if env.Observed == EnvDestroyed {
		return Decision{Action: ActionNone, Reason: "environment is destroyed"}
	}

	// Rule 3: destroy wins over everything else, from any state. A user asking
	// to destroy should not have to wait for a provision to finish first.
	if env.Desired == DesireDestroyed {
		return Decision{Action: ActionEnqueue, Task: TaskDestroy, Reason: "desired=destroyed"}
	}

	// Rule 4: a transient environment is normally converging under a task that
	// has not yet reported. Interrupting it is exactly the flap we are trying
	// to avoid — wait for it to settle and reconsider on the next pass with
	// real facts.
	//
	// But we have already established there is no active task, and a transient
	// observed state with nothing in flight is *stranded*: no worker will ever
	// report, so nothing will ever move it. Returning ActionNone here would
	// park the environment in provisioning forever, which is a liveness hole,
	// not convergence.
	//
	// This should not happen. Worker task completion and the observed-state
	// write commit in one transaction, and the reaper orphans environments
	// whose worker dies, so the window is nominally zero. The grace period
	// below exists because "nominally zero" is exactly the kind of assumption
	// that turns into a stuck environment at 3am when some new path forgets the
	// atomicity. Treating it as failed hands the environment to the ordinary
	// recovery path (bounded by RecoverAttempts), which is strictly better than
	// waiting on a report that is never coming.
	if env.Observed.Transient() {
		if env.ObservedSince.IsZero() || now.Sub(env.ObservedSince) < StuckThreshold {
			return Decision{Action: ActionNone,
				Reason: fmt.Sprintf("observed=%s is transient; awaiting settle", env.Observed)}
		}
		return Decision{Action: ActionSettle, Settle: EnvFailed, Reason: fmt.Sprintf(
			"stranded in %s for %s with no task in flight; treating as failed so recovery can proceed",
			env.Observed, now.Sub(env.ObservedSince).Truncate(time.Second))}
	}

	switch env.Desired {
	case DesireRunning:
		return reconcileToRunning(env)
	case DesireStopped:
		return reconcileToStopped(env)
	}
	return Decision{Action: ActionNone, Reason: "no rule matched"}
}

func reconcileToRunning(env Env) Decision {
	switch env.Observed {
	case EnvRunning:
		return Decision{Action: ActionNone, Reason: "converged: running"}

	case EnvPending:
		// Never provisioned. provision clones the repo from scratch.
		return Decision{Action: ActionEnqueue, Task: TaskProvision, Reason: "desired=running, never provisioned"}

	case EnvStopped:
		// Has a snapshot; start hydrates it onto whichever worker placement
		// picks. Compute is disposable, so this need not be the original node.
		return Decision{Action: ActionEnqueue, Task: TaskStart, Reason: "desired=running, resuming from snapshot"}

	case EnvFailed:
		// The one cycle in the graph. Bound it.
		if env.RecoverAttempts >= MaxRecoverAttempts {
			return Decision{Action: ActionPark, Reason: fmt.Sprintf(
				"recovery budget exhausted after %d attempts across the fleet; "+
					"the failure follows the environment, not the worker", env.RecoverAttempts)}
		}
		return Decision{Action: ActionEnqueue, Task: TaskRecover, Reason: fmt.Sprintf(
			"desired=running, recovering (attempt %d/%d)", env.RecoverAttempts+1, MaxRecoverAttempts)}
	}
	return Decision{Action: ActionNone, Reason: "no rule matched for desired=running"}
}

func reconcileToStopped(env Env) Decision {
	switch env.Observed {
	case EnvStopped:
		return Decision{Action: ActionNone, Reason: "converged: stopped"}

	case EnvRunning:
		return Decision{Action: ActionEnqueue, Task: TaskStop, Reason: "desired=stopped"}

	case EnvPending:
		// No compute was ever allocated, so there is nothing for a worker to
		// do. Settling directly avoids scheduling a task whose entire body is
		// an early return.
		return Decision{Action: ActionSettle, Settle: EnvStopped,
			Reason: "desired=stopped and no compute was ever allocated"}

	case EnvFailed:
		// A failed environment holds no compute, so "stopped" is already
		// satisfied in every way the user cares about. Critically, we do NOT
		// try to recover it: the user asked for it to be off. Recovering here
		// would fight the user's intent forever.
		return Decision{Action: ActionNone,
			Reason: "failed environment holds no compute; desired=stopped is satisfied"}
	}
	return Decision{Action: ActionNone, Reason: "no rule matched for desired=stopped"}
}

// Converged reports whether an environment needs no further work. Used by the
// reconciler to skip rows cheaply and by tests to assert the loop reaches a
// fixed point.
func Converged(env Env, hasActiveTask bool, now time.Time) bool {
	d := Reconcile(env, hasActiveTask, now)
	return d.Action == ActionNone
}
