// Package state holds the two coupled lifecycle state machines that govern
// DevBox: the task FSM (units of work in the durable queue) and the environment
// FSM (what the user actually sees).
//
// Everything here is a pure function of (current snapshot, event, now). No I/O,
// no clock reads, no database. The caller is responsible for persisting the
// returned state and effects inside a single transaction; this package's job is
// to make illegal transitions impossible to express and to keep the guard logic
// (fencing tokens, attempt budgets, capacity release) in one auditable place.
package state

import (
	"errors"
	"fmt"
	"time"
)

// TaskState is the persisted lifecycle state of a queue task.
type TaskState string

const (
	// TaskQueued means the task is eligible for a worker to claim, subject to
	// AvailableAt (the backoff gate).
	TaskQueued TaskState = "queued"
	// TaskLeased means a worker has been granted the task but has not yet
	// reported that execution began.
	TaskLeased TaskState = "leased"
	// TaskRunning means a worker has acknowledged the lease and is executing.
	TaskRunning TaskState = "running"
	// TaskSucceeded is terminal.
	TaskSucceeded TaskState = "succeeded"
	// TaskFailed is a non-terminal resting state: the retry scheduler moves it
	// back to queued, or to dead once the attempt budget is exhausted.
	TaskFailed TaskState = "failed"
	// TaskDead is terminal: the dead-letter state.
	TaskDead TaskState = "dead"
)

// Terminal reports whether no further transition is possible.
func (s TaskState) Terminal() bool {
	return s == TaskSucceeded || s == TaskDead
}

// Active reports whether the task occupies the per-environment serialization
// slot. The partial unique index in the schema is defined over exactly this
// predicate, so the two definitions must not drift.
func (s TaskState) Active() bool {
	return s == TaskQueued || s == TaskLeased || s == TaskRunning
}

// Valid reports whether s is a state this package knows about.
func (s TaskState) Valid() bool {
	switch s {
	case TaskQueued, TaskLeased, TaskRunning, TaskSucceeded, TaskFailed, TaskDead:
		return true
	}
	return false
}

// TaskType identifies the kind of work a task performs. The type determines the
// attempt budget and whether a failure is worth retrying.
type TaskType string

const (
	TaskProvision TaskType = "provision"
	TaskStart     TaskType = "start"
	TaskStop      TaskType = "stop"
	TaskSnapshot  TaskType = "snapshot"
	TaskDestroy   TaskType = "destroy"
	TaskRecover   TaskType = "recover"
)

// Valid reports whether t is a known task type.
func (t TaskType) Valid() bool {
	switch t {
	case TaskProvision, TaskStart, TaskStop, TaskSnapshot, TaskDestroy, TaskRecover:
		return true
	}
	return false
}

// Places reports whether this task type acquires compute for its environment.
//
// The distinction drives the whole reservation lifecycle. A placing task takes
// a *provisional* reservation when it is leased; on success that reservation is
// handed over to the environment (whose container now genuinely occupies the
// resources) and on failure it is rolled back.
func (t TaskType) Places() bool {
	return t == TaskProvision || t == TaskStart || t == TaskRecover
}

// Releases reports whether this task type gives compute back. A releasing task
// takes no reservation of its own — it runs on the worker already hosting the
// environment — and frees the environment's reservation when it succeeds.
func (t TaskType) Releases() bool {
	return t == TaskStop || t == TaskDestroy
}

// MaxAttempts is the attempt budget per task type.
//
// The budgets differ on purpose. provision and recover pull a repo and a
// snapshot over the network, so transient failure is common and worth retrying
// several times. destroy gets a generous budget because giving up on it leaks a
// container and its reserved capacity — we would rather retry a no-op than
// strand resources. start/stop are cheap local Docker calls: if they fail three
// times the worker is genuinely sick and the reaper should move the environment
// elsewhere rather than grinding here.
func (t TaskType) MaxAttempts() int {
	switch t {
	case TaskProvision, TaskRecover:
		return 5
	case TaskDestroy:
		return 8
	case TaskSnapshot:
		return 4
	case TaskStart, TaskStop:
		return 3
	}
	return 3
}

// TaskEventKind enumerates the events that can move a task.
type TaskEventKind string

const (
	// EvLease is the scheduler granting the task to a worker. Carries the new
	// fencing token.
	EvLease TaskEventKind = "lease"
	// EvStart is the worker reporting that execution has begun.
	EvStart TaskEventKind = "start"
	// EvSucceed is the worker reporting successful completion.
	EvSucceed TaskEventKind = "succeed"
	// EvFail is the worker reporting failure, or the scheduler recording one.
	EvFail TaskEventKind = "fail"
	// EvLeaseExpire is the reaper reclaiming a task whose lease ran out (the
	// worker died, stalled, or was partitioned).
	EvLeaseExpire TaskEventKind = "lease_expire"
	// EvRetry is the retry scheduler returning a failed task to the queue.
	EvRetry TaskEventKind = "retry"
	// EvDeadLetter is the retry scheduler giving up on a failed task.
	EvDeadLetter TaskEventKind = "dead_letter"
)

// Task is the subset of the tasks row the FSM reasons about.
type Task struct {
	Type         TaskType
	State        TaskState
	Attempts     int
	MaxAttempts  int
	FencingToken int64
	WorkerID     string // empty when unassigned
	// Reserved records whether this task currently holds a capacity/quota
	// reservation on WorkerID. Leasing takes the reservation; every path out of
	// leased/running must give it back exactly once.
	Reserved bool
}

// TaskEvent is an input to the task FSM.
type TaskEvent struct {
	Kind TaskEventKind
	// FencingToken is the token the caller believes is current. For EvLease it
	// is the new token being issued and must strictly exceed the stored one.
	// For every worker-originated event it must equal the stored token.
	FencingToken int64
	// WorkerID is the worker the lease is being granted to (EvLease only).
	WorkerID string
	// Err carries the failure reason for EvFail.
	Err string
	// Retryable lets a worker mark a failure as permanent (e.g. the repo does
	// not exist), short-circuiting the attempt budget straight to dead-letter.
	Retryable bool
	// Backoff is the delay the retry scheduler computed for EvRetry.
	Backoff time.Duration
}

// Sentinel errors returned by ApplyTask. Callers distinguish these because they
// mean very different things operationally: a stale fence is a partitioned
// worker doing the right thing and being safely ignored, while an illegal
// transition is a bug.
var (
	// ErrIllegalTransition means the event is not defined for the current state.
	ErrIllegalTransition = errors.New("state: illegal transition")
	// ErrStaleFencingToken means the caller is acting on a lease that has been
	// superseded. This is the split-brain guard: a resurrected or partitioned
	// worker hits this and must abort.
	ErrStaleFencingToken = errors.New("state: stale fencing token")
	// ErrInvalidEvent means the event itself is malformed.
	ErrInvalidEvent = errors.New("state: invalid event")
)

// TransitionError wraps a sentinel with the context needed to debug it.
type TransitionError struct {
	From  TaskState
	Event TaskEventKind
	Cause error
	Msg   string
}

func (e *TransitionError) Error() string {
	if e.Msg != "" {
		return fmt.Sprintf("task %s --%s--> rejected: %s (%v)", e.From, e.Event, e.Msg, e.Cause)
	}
	return fmt.Sprintf("task %s --%s--> rejected: %v", e.From, e.Event, e.Cause)
}

func (e *TransitionError) Unwrap() error { return e.Cause }

// TaskResult is the outcome of applying an event: the next persisted shape of
// the task plus the side effects the caller must apply in the same transaction.
type TaskResult struct {
	Next    Task
	Effects []Effect
	// Audit is the transition to append to audit_log.
	Audit AuditRecord
}

// ApplyTask is the task state machine. It is a pure function: same inputs, same
// outputs, no clock and no I/O.
//
// now is passed in rather than read so that backoff deadlines are testable and
// so the caller can use the database's transaction timestamp, keeping the
// persisted deadline consistent with the row it is written beside.
func ApplyTask(cur Task, ev TaskEvent, now time.Time) (TaskResult, error) {
	if !cur.State.Valid() {
		return TaskResult{}, &TransitionError{From: cur.State, Event: ev.Kind, Cause: ErrInvalidEvent, Msg: "unknown current state"}
	}
	if cur.State.Terminal() {
		return TaskResult{}, &TransitionError{From: cur.State, Event: ev.Kind, Cause: ErrIllegalTransition, Msg: "task is terminal"}
	}

	switch ev.Kind {
	case EvLease:
		return applyLease(cur, ev, now)
	case EvStart:
		return applyStart(cur, ev, now)
	case EvSucceed:
		return applySucceed(cur, ev, now)
	case EvFail:
		return applyFail(cur, ev, now)
	case EvLeaseExpire:
		return applyLeaseExpire(cur, ev, now)
	case EvRetry:
		return applyRetry(cur, ev, now)
	case EvDeadLetter:
		return applyDeadLetter(cur, ev, now)
	}
	return TaskResult{}, &TransitionError{From: cur.State, Event: ev.Kind, Cause: ErrInvalidEvent, Msg: "unknown event kind"}
}

func applyLease(cur Task, ev TaskEvent, now time.Time) (TaskResult, error) {
	if cur.State != TaskQueued {
		return TaskResult{}, &TransitionError{From: cur.State, Event: ev.Kind, Cause: ErrIllegalTransition,
			Msg: "only a queued task may be leased"}
	}
	if ev.WorkerID == "" {
		return TaskResult{}, &TransitionError{From: cur.State, Event: ev.Kind, Cause: ErrInvalidEvent,
			Msg: "lease requires a worker"}
	}
	// A new lease must carry a strictly higher token than any lease before it.
	// This is what makes the previous holder's writes rejectable forever after.
	if ev.FencingToken <= cur.FencingToken {
		return TaskResult{}, &TransitionError{From: cur.State, Event: ev.Kind, Cause: ErrStaleFencingToken,
			Msg: fmt.Sprintf("new token %d does not exceed current %d", ev.FencingToken, cur.FencingToken)}
	}

	next := cur
	next.State = TaskLeased
	next.FencingToken = ev.FencingToken
	next.WorkerID = ev.WorkerID

	// Only a placing task reserves. A stop or destroy runs on the worker that
	// is already hosting the environment and consumes nothing new; making it
	// reserve would double-count the environment against its own worker and
	// could make a full node refuse to free itself — a deadlock where the only
	// way out of "full" is blocked by "full".
	var effects []Effect
	if cur.Type.Places() {
		next.Reserved = true
		effects = append(effects, ReserveCapacity{WorkerID: ev.WorkerID})
	}

	return TaskResult{
		Next:    next,
		Effects: effects,
		Audit:   AuditRecord{From: string(cur.State), To: string(next.State), Reason: "leased to worker", Actor: "scheduler", At: now},
	}, nil
}

func applyStart(cur Task, ev TaskEvent, now time.Time) (TaskResult, error) {
	if cur.State != TaskLeased {
		return TaskResult{}, &TransitionError{From: cur.State, Event: ev.Kind, Cause: ErrIllegalTransition,
			Msg: "only a leased task may start"}
	}
	if err := checkFence(cur, ev); err != nil {
		return TaskResult{}, err
	}
	next := cur
	next.State = TaskRunning
	next.Attempts = cur.Attempts + 1
	return TaskResult{
		Next:  next,
		Audit: AuditRecord{From: string(cur.State), To: string(next.State), Reason: "worker began execution", Actor: "worker:" + cur.WorkerID, At: now},
	}, nil
}

func applySucceed(cur Task, ev TaskEvent, now time.Time) (TaskResult, error) {
	if cur.State != TaskRunning && cur.State != TaskLeased {
		return TaskResult{}, &TransitionError{From: cur.State, Event: ev.Kind, Cause: ErrIllegalTransition,
			Msg: "only a leased or running task may succeed"}
	}
	if err := checkFence(cur, ev); err != nil {
		return TaskResult{}, err
	}
	next := cur
	next.State = TaskSucceeded
	next.Reserved = false

	var effects []Effect
	switch {
	case cur.Type.Places():
		// The container is now running and genuinely occupying these
		// resources. The reservation does not go back to the pool — ownership
		// moves from the task to the environment, which holds it until a stop
		// or destroy gives it up. Releasing here would let the scheduler
		// re-sell capacity that is physically in use, which is the overcommit
		// bug this whole model exists to prevent.
		effects = append(effects, CommitCapacity{WorkerID: cur.WorkerID})
	case cur.Type.Releases():
		// The container is gone, so the environment's long-held reservation
		// returns to the pool now.
		effects = append(effects, ReleaseCapacity{WorkerID: cur.WorkerID})
	}
	// The worker binding stays on a successful placing task: the environment
	// lives there now. It is cleared by the environment FSM when compute ends.
	if !cur.Type.Places() {
		next.WorkerID = ""
	}

	return TaskResult{
		Next:    next,
		Effects: effects,
		Audit:   AuditRecord{From: string(cur.State), To: string(next.State), Reason: "completed", Actor: "worker:" + cur.WorkerID, At: now},
	}, nil
}

func applyFail(cur Task, ev TaskEvent, now time.Time) (TaskResult, error) {
	if cur.State != TaskRunning && cur.State != TaskLeased {
		return TaskResult{}, &TransitionError{From: cur.State, Event: ev.Kind, Cause: ErrIllegalTransition,
			Msg: "only a leased or running task may fail"}
	}
	if err := checkFence(cur, ev); err != nil {
		return TaskResult{}, err
	}
	next := cur
	next.State = TaskFailed
	next.WorkerID = ""
	next.Reserved = false
	// Capacity comes back immediately on failure. Holding a reservation across
	// the backoff window would let a crash-looping environment pin a worker's
	// resources for the whole retry schedule.
	return TaskResult{
		Next:    next,
		Effects: releaseIfReserved(cur),
		Audit: AuditRecord{From: string(cur.State), To: string(next.State),
			Reason: truncateReason("failed: " + ev.Err), Actor: "worker:" + cur.WorkerID, At: now},
	}, nil
}

func applyLeaseExpire(cur Task, ev TaskEvent, now time.Time) (TaskResult, error) {
	if cur.State != TaskLeased && cur.State != TaskRunning {
		return TaskResult{}, &TransitionError{From: cur.State, Event: ev.Kind, Cause: ErrIllegalTransition,
			Msg: "only a leased or running task can have its lease expire"}
	}
	// Deliberately no fence check: the reaper acts on behalf of the system
	// precisely because the lease holder has stopped talking to us. The holder
	// is neutralised by the *next* lease carrying a higher token, not by this
	// transition.
	next := cur
	next.State = TaskFailed
	next.WorkerID = ""
	next.Reserved = false
	// An expired lease consumes an attempt. Without this a worker that reliably
	// dies mid-task would be handed the same task forever.
	if cur.State == TaskLeased {
		// The worker never reported starting, so applyStart never incremented.
		next.Attempts = cur.Attempts + 1
	}
	return TaskResult{
		Next:    next,
		Effects: releaseIfReserved(cur),
		Audit: AuditRecord{From: string(cur.State), To: string(next.State),
			Reason: "lease expired; reclaimed by reaper", Actor: "scheduler:reaper", At: now},
	}, nil
}

func applyRetry(cur Task, ev TaskEvent, now time.Time) (TaskResult, error) {
	if cur.State != TaskFailed {
		return TaskResult{}, &TransitionError{From: cur.State, Event: ev.Kind, Cause: ErrIllegalTransition,
			Msg: "only a failed task may be retried"}
	}
	budget := cur.MaxAttempts
	if budget <= 0 {
		budget = cur.Type.MaxAttempts()
	}
	if cur.Attempts >= budget {
		return TaskResult{}, &TransitionError{From: cur.State, Event: ev.Kind, Cause: ErrIllegalTransition,
			Msg: fmt.Sprintf("attempt budget exhausted (%d/%d); dead-letter instead", cur.Attempts, budget)}
	}
	next := cur
	next.State = TaskQueued
	next.FencingToken = cur.FencingToken // preserved: the next lease must exceed it
	next.WorkerID = ""
	next.Reserved = false
	return TaskResult{
		Next:    next,
		Effects: []Effect{ScheduleRetry{AvailableAt: now.Add(ev.Backoff), Attempt: cur.Attempts}},
		Audit: AuditRecord{From: string(cur.State), To: string(next.State),
			Reason: fmt.Sprintf("retry %d/%d in %s", cur.Attempts+1, budget, ev.Backoff), Actor: "scheduler:retry", At: now},
	}, nil
}

func applyDeadLetter(cur Task, ev TaskEvent, now time.Time) (TaskResult, error) {
	if cur.State != TaskFailed {
		return TaskResult{}, &TransitionError{From: cur.State, Event: ev.Kind, Cause: ErrIllegalTransition,
			Msg: "only a failed task may be dead-lettered"}
	}
	next := cur
	next.State = TaskDead
	next.WorkerID = ""
	next.Reserved = false
	return TaskResult{
		Next:    next,
		Effects: []Effect{DeadLetter{Reason: ev.Err}},
		Audit: AuditRecord{From: string(cur.State), To: string(next.State),
			Reason: truncateReason("dead-lettered: " + ev.Err), Actor: "scheduler:retry", At: now},
	}, nil
}

// checkFence enforces that a worker-originated event carries the token of the
// lease that is currently valid. Anything else is a straggler.
func checkFence(cur Task, ev TaskEvent) error {
	if ev.FencingToken != cur.FencingToken {
		return &TransitionError{From: cur.State, Event: ev.Kind, Cause: ErrStaleFencingToken,
			Msg: fmt.Sprintf("event token %d != current %d", ev.FencingToken, cur.FencingToken)}
	}
	return nil
}

// releaseIfReserved rolls back a provisional reservation exactly once.
//
// It fires only for a placing task that failed or lost its lease: the compute
// it was reserving never came into existence, so the capacity goes back to the
// pool. A releasing task never holds a provisional reservation (see
// applyLease), so a failed stop correctly leaves the environment's long-held
// capacity allocated — the container is, after all, still there.
func releaseIfReserved(cur Task) []Effect {
	if !cur.Reserved || cur.WorkerID == "" {
		return nil
	}
	return []Effect{ReleaseCapacity{WorkerID: cur.WorkerID}}
}

// ShouldDeadLetter reports whether a failed task has exhausted its budget and
// must go to the dead-letter state rather than being retried. The retry
// scheduler calls this to choose between EvRetry and EvDeadLetter.
func ShouldDeadLetter(cur Task, permanentFailure bool) bool {
	if cur.State != TaskFailed {
		return false
	}
	if permanentFailure {
		return true
	}
	budget := cur.MaxAttempts
	if budget <= 0 {
		budget = cur.Type.MaxAttempts()
	}
	return cur.Attempts >= budget
}

const maxReasonLen = 480

func truncateReason(s string) string {
	if len(s) <= maxReasonLen {
		return s
	}
	return s[:maxReasonLen] + "…"
}
