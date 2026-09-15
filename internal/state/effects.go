package state

import "time"

// Effect is a side effect the FSM decided on but did not perform. The caller
// must apply every returned effect in the same database transaction that
// persists the new state, which is what keeps capacity accounting and state
// transitions from drifting apart under concurrency or partial failure.
//
// Effects are a closed set (the unexported marker method), so a switch over
// them in the store layer is exhaustive by construction.
type Effect interface {
	isEffect()
}

// ReserveCapacity instructs the store to add this task's resource spec to the
// worker's allocated_* columns and to the owner's quota usage. The store must
// re-verify the capacity and quota invariants while holding the worker row
// lock; the FSM knows nothing about how much capacity exists.
type ReserveCapacity struct {
	WorkerID string
}

func (ReserveCapacity) isEffect() {}

// ReleaseCapacity instructs the store to subtract this task's spec from the
// worker's allocated_* columns and the owner's quota usage.
type ReleaseCapacity struct {
	WorkerID string
}

func (ReleaseCapacity) isEffect() {}

// CommitCapacity hands a provisional reservation from a completed placing task
// over to its environment. Numerically it changes nothing — allocated_* already
// includes the spec and must keep including it, because the container is now
// really running. It exists as an explicit effect rather than as an absence so
// that "the task ended and capacity stayed allocated" is a decision recorded in
// the audit log, not an omission a future reader has to infer.
type CommitCapacity struct {
	WorkerID string
}

func (CommitCapacity) isEffect() {}

// ScheduleRetry sets the backoff gate. The task is queued but invisible to
// claims until AvailableAt.
type ScheduleRetry struct {
	AvailableAt time.Time
	Attempt     int
}

func (ScheduleRetry) isEffect() {}

// DeadLetter marks the task as permanently failed and is the signal for the
// reconciler to move the owning environment to failed with a real error.
type DeadLetter struct {
	Reason string
}

func (DeadLetter) isEffect() {}

// EnqueueTask asks the store to create a follow-on task. Emitted by the
// environment FSM when converging observed state toward desired state. The
// store applies it idempotently: if an active task already exists for the
// environment, the partial unique index rejects the insert and the reconciler
// treats that as "already converging", which is what keeps the loop from
// oscillating.
type EnqueueTask struct {
	Type TaskType
	// Reason is recorded on the task and in the audit log so an operator can
	// tell a user-requested stop from a reaper-driven recovery.
	Reason string
}

func (EnqueueTask) isEffect() {}

// ClearWorkerBinding detaches an environment from the worker it was running on.
// Emitted when an environment stops, fails, or is orphaned by a dead worker, so
// that placement is free to choose any healthy worker next time.
type ClearWorkerBinding struct{}

func (ClearWorkerBinding) isEffect() {}

// AuditRecord is the append-only transition record written alongside every
// state change. Tests and the AI debugger both read these, so the reason string
// is expected to be human-meaningful, not a constant.
type AuditRecord struct {
	From   string
	To     string
	Reason string
	Actor  string
	At     time.Time
}
