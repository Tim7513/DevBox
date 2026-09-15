package state

import (
	"fmt"
	"time"
)

// EnvState is the observed lifecycle state of an environment — what the system
// believes is actually true right now, as reported by workers.
type EnvState string

const (
	EnvPending      EnvState = "pending"
	EnvProvisioning EnvState = "provisioning"
	EnvRunning      EnvState = "running"
	EnvStopping     EnvState = "stopping"
	EnvStopped      EnvState = "stopped"
	EnvFailed       EnvState = "failed"
	EnvDestroyed    EnvState = "destroyed"
)

// Valid reports whether s is a known observed state.
func (s EnvState) Valid() bool {
	switch s {
	case EnvPending, EnvProvisioning, EnvRunning, EnvStopping, EnvStopped, EnvFailed, EnvDestroyed:
		return true
	}
	return false
}

// Transient reports whether the environment is mid-flight. The reconciler never
// issues new work against a transient environment; it waits for the in-flight
// task to settle. This is the single most important anti-oscillation rule: a
// desired-state flip while a provision is running must not cancel and restart
// it in a loop.
func (s EnvState) Transient() bool {
	return s == EnvProvisioning || s == EnvStopping
}

// HoldsCompute reports whether the environment is expected to have a container
// and a worker binding. Used to decide whether a dead worker orphans it.
func (s EnvState) HoldsCompute() bool {
	return s == EnvProvisioning || s == EnvRunning || s == EnvStopping
}

// DesiredState is the target the user (or admin) has asked for. It is written
// only by user-facing API calls, never by the reconciler — keeping desired
// under user control and observed under system control is what makes the loop
// well-founded.
type DesiredState string

const (
	DesireRunning   DesiredState = "running"
	DesireStopped   DesiredState = "stopped"
	DesireDestroyed DesiredState = "destroyed"
)

// Valid reports whether d is a known desired state.
func (d DesiredState) Valid() bool {
	switch d {
	case DesireRunning, DesireStopped, DesireDestroyed:
		return true
	}
	return false
}

// EnvEventKind enumerates observed-state events, all originating from real
// worker reports or the reaper — never from a user request.
type EnvEventKind string

const (
	// EnvEvProvisionStarted: a worker began hydrating and building.
	EnvEvProvisionStarted EnvEventKind = "provision_started"
	// EnvEvRunning: the container is up and the preview port is bound.
	EnvEvRunning EnvEventKind = "running"
	// EnvEvStopStarted: a worker began quiescing and snapshotting.
	EnvEvStopStarted EnvEventKind = "stop_started"
	// EnvEvStopped: snapshot uploaded and verified, container gone.
	EnvEvStopped EnvEventKind = "stopped"
	// EnvEvFailed: the container crashed, or a task dead-lettered.
	EnvEvFailed EnvEventKind = "failed"
	// EnvEvOrphaned: the worker holding this environment was declared dead.
	// Distinct from EnvEvFailed because the environment did nothing wrong and
	// recovery is expected rather than exceptional.
	EnvEvOrphaned EnvEventKind = "orphaned"
	// EnvEvDestroyed: compute released and local volume removed.
	EnvEvDestroyed EnvEventKind = "destroyed"
)

// Env is the subset of the environments row the FSM reasons about.
type Env struct {
	Observed EnvState
	Desired  DesiredState
	// WorkerID is the worker currently hosting this environment, if any.
	WorkerID string
	// SnapshotID is the last fully-uploaded, checksum-verified snapshot. Only
	// ever advanced by a completed snapshot task.
	SnapshotID string
	// RecoverAttempts counts consecutive automatic recoveries. Bounded so an
	// environment that fails on every worker cannot bounce around the fleet
	// forever; see MaxRecoverAttempts.
	RecoverAttempts int
	// ObservedSince is when Observed last actually changed. The reconciler uses
	// it to detect an environment stranded in a transient state; see
	// StuckThreshold. Self-transitions do not reset it, because an idempotent
	// re-report is not evidence of progress.
	ObservedSince time.Time
}

// MaxRecoverAttempts bounds automatic recovery.
//
// Rationale: recovery moves an environment to a *different* worker, so the
// first retry genuinely tests the "bad node" hypothesis. By the third failure
// on three different workers the fault is almost certainly the environment
// itself (a build that OOMs, a start command that exits), and continuing to
// rehydrate a multi-hundred-megabyte snapshot across the fleet burns real
// capacity to no purpose. At that point we stop, park the environment in
// failed with the last real error, and let the user look at it — the AI
// debugger has everything it needs by then.
const MaxRecoverAttempts = 3

// EnvEvent is an input to the environment FSM.
type EnvEvent struct {
	Kind EnvEventKind
	// WorkerID is the worker reporting, for events that bind compute.
	WorkerID string
	// SnapshotID is set on EnvEvStopped once the upload is verified.
	SnapshotID string
	// Err is the real failure reason surfaced to the user.
	Err string
}

// EnvResult is the outcome of applying an environment event.
type EnvResult struct {
	Next    Env
	Effects []Effect
	Audit   AuditRecord
}

// envTransitions is the legal observed-state transition table. Anything absent
// is rejected rather than coerced, so a worker reporting nonsense (a "running"
// for an environment we already destroyed) surfaces as an error instead of
// resurrecting a terminal row.
var envTransitions = map[EnvEventKind]map[EnvState]EnvState{
	EnvEvProvisionStarted: {
		EnvPending: EnvProvisioning,
		EnvStopped: EnvProvisioning,
		EnvFailed:  EnvProvisioning, // recovery path
	},
	EnvEvRunning: {
		EnvProvisioning: EnvRunning,
		EnvRunning:      EnvRunning, // idempotent re-report; see note below
	},
	EnvEvStopStarted: {
		EnvRunning:      EnvStopping,
		EnvProvisioning: EnvStopping,
	},
	EnvEvStopped: {
		EnvStopping: EnvStopped,
		EnvPending:  EnvStopped, // nothing was ever started
		EnvStopped:  EnvStopped, // idempotent
	},
	EnvEvFailed: {
		EnvPending:      EnvFailed,
		EnvProvisioning: EnvFailed,
		EnvRunning:      EnvFailed,
		EnvStopping:     EnvFailed,
	},
	EnvEvOrphaned: {
		EnvProvisioning: EnvFailed,
		EnvRunning:      EnvFailed,
		EnvStopping:     EnvFailed,
	},
	EnvEvDestroyed: {
		EnvPending:      EnvDestroyed,
		EnvProvisioning: EnvDestroyed,
		EnvRunning:      EnvDestroyed,
		EnvStopping:     EnvDestroyed,
		EnvStopped:      EnvDestroyed,
		EnvFailed:       EnvDestroyed,
		EnvDestroyed:    EnvDestroyed, // idempotent
	},
}

// ApplyEnv is the environment state machine. Pure; see ApplyTask.
//
// Note on idempotent self-transitions (running→running, destroyed→destroyed):
// these are permitted because task delivery is at-least-once. A worker that
// successfully reports "running" and then has its completion retried must not
// produce an error that gets recorded as a failure. Self-transitions are
// accepted but produce no effects and no audit churn beyond the record itself.
func ApplyEnv(cur Env, ev EnvEvent, now time.Time) (EnvResult, error) {
	if !cur.Observed.Valid() {
		return EnvResult{}, &EnvTransitionError{From: cur.Observed, Event: ev.Kind,
			Cause: ErrInvalidEvent, Msg: "unknown current state"}
	}
	if cur.Observed == EnvDestroyed && ev.Kind != EnvEvDestroyed {
		return EnvResult{}, &EnvTransitionError{From: cur.Observed, Event: ev.Kind,
			Cause: ErrIllegalTransition, Msg: "environment is destroyed"}
	}

	table, ok := envTransitions[ev.Kind]
	if !ok {
		return EnvResult{}, &EnvTransitionError{From: cur.Observed, Event: ev.Kind,
			Cause: ErrInvalidEvent, Msg: "unknown event kind"}
	}
	to, ok := table[cur.Observed]
	if !ok {
		return EnvResult{}, &EnvTransitionError{From: cur.Observed, Event: ev.Kind,
			Cause: ErrIllegalTransition, Msg: "no such transition"}
	}

	next := cur
	next.Observed = to
	if to != cur.Observed {
		next.ObservedSince = now
	}
	var effects []Effect
	reason := string(ev.Kind)
	actor := "worker:" + ev.WorkerID

	switch ev.Kind {
	case EnvEvProvisionStarted:
		next.WorkerID = ev.WorkerID
		if cur.Observed == EnvFailed {
			// This provision is a recovery; count it so MaxRecoverAttempts can
			// bite. A user-initiated start from stopped does not count.
			next.RecoverAttempts = cur.RecoverAttempts + 1
			reason = fmt.Sprintf("recovery attempt %d/%d on worker %s",
				next.RecoverAttempts, MaxRecoverAttempts, ev.WorkerID)
			actor = "scheduler:reaper"
		} else {
			reason = "provisioning on worker " + ev.WorkerID
		}

	case EnvEvRunning:
		next.WorkerID = ev.WorkerID
		// Reaching running is the only thing that proves recovery worked, so
		// this is where the counter resets. Resetting it on the *attempt*
		// instead would defeat the bound entirely.
		next.RecoverAttempts = 0
		reason = "container running on worker " + ev.WorkerID

	case EnvEvStopped:
		if ev.SnapshotID != "" {
			// Enforces the §17 invariant at the type level: the FSM only ever
			// advances the pointer from an event that carries a verified id.
			next.SnapshotID = ev.SnapshotID
			reason = "stopped; snapshot " + ev.SnapshotID
		} else {
			reason = "stopped with no new snapshot"
		}
		next.WorkerID = ""
		effects = append(effects, ClearWorkerBinding{})

	case EnvEvFailed:
		next.WorkerID = ""
		reason = truncateReason("failed: " + ev.Err)
		effects = append(effects, ClearWorkerBinding{})

	case EnvEvOrphaned:
		next.WorkerID = ""
		reason = truncateReason(fmt.Sprintf("orphaned: worker %s declared dead", ev.WorkerID))
		actor = "scheduler:reaper"
		effects = append(effects, ClearWorkerBinding{})

	case EnvEvDestroyed:
		next.WorkerID = ""
		reason = "destroyed"
		effects = append(effects, ClearWorkerBinding{})

	case EnvEvStopStarted:
		reason = "stopping on worker " + cur.WorkerID
	}

	return EnvResult{
		Next:    next,
		Effects: effects,
		Audit:   AuditRecord{From: string(cur.Observed), To: string(to), Reason: reason, Actor: actor, At: now},
	}, nil
}

// EnvTransitionError is the environment analogue of TransitionError.
type EnvTransitionError struct {
	From  EnvState
	Event EnvEventKind
	Cause error
	Msg   string
}

func (e *EnvTransitionError) Error() string {
	return fmt.Sprintf("environment %s --%s--> rejected: %s (%v)", e.From, e.Event, e.Msg, e.Cause)
}

func (e *EnvTransitionError) Unwrap() error { return e.Cause }
