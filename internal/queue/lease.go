package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/timmai/devbox/internal/state"
	"github.com/timmai/devbox/internal/store"
)

// Renew extends a lease, but only for the worker that currently holds it.
//
// The fencing check is the whole point. A worker that was partitioned long
// enough for the reaper to reclaim its task will find its token is no longer
// current and must abort whatever it is doing — its replacement is already
// running. Renew failing is not a transient error to retry; it is an
// instruction to stop.
func (q *Queue) Renew(ctx context.Context, leaseID uuid.UUID, fencingToken int64) (time.Time, error) {
	var expiresAt time.Time
	err := q.db.InTx(ctx, store.TxOptions{}, func(tx pgx.Tx) error {
		var current int64
		var taskState string
		err := tx.QueryRow(ctx, `
			SELECT fencing_token, state FROM tasks
			WHERE lease_id = $1
			FOR UPDATE`, leaseID).Scan(&current, &taskState)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrLeaseNotFound
		}
		if err != nil {
			return fmt.Errorf("lock lease: %w", err)
		}
		if current != fencingToken {
			return fmt.Errorf("%w: have %d, current is %d", ErrStaleFence, fencingToken, current)
		}
		if !state.TaskState(taskState).Active() {
			return fmt.Errorf("%w: task is %s", ErrLeaseNotFound, taskState)
		}

		return tx.QueryRow(ctx, `
			UPDATE tasks
			SET lease_expires_at = now() + $2::interval, updated_at = now()
			WHERE lease_id = $1
			RETURNING lease_expires_at`, leaseID, q.LeaseTTL.String()).Scan(&expiresAt)
	})
	return expiresAt, err
}

// Start records that a worker began executing a leased task.
func (q *Queue) Start(ctx context.Context, leaseID uuid.UUID, fencingToken int64) error {
	return q.db.InTx(ctx, store.TxOptions{}, func(tx pgx.Tx) error {
		cur, err := lockTaskByLease(ctx, tx, leaseID)
		if err != nil {
			return err
		}
		if cur.snap.FencingToken != fencingToken {
			return fmt.Errorf("%w: have %d, current is %d", ErrStaleFence, fencingToken, cur.snap.FencingToken)
		}

		res, err := state.ApplyTask(cur.snap, state.TaskEvent{
			Kind: state.EvStart, FencingToken: fencingToken}, time.Now())
		if err != nil {
			return err
		}

		if _, err := tx.Exec(ctx, `
			UPDATE tasks SET state = 'running', attempts = $2, updated_at = now()
			WHERE id = $1`, cur.id, res.Next.Attempts); err != nil {
			return fmt.Errorf("mark task running: %w", err)
		}
		return store.AppendAudit(ctx, tx, "task", cur.id.String(),
			res.Audit.From, res.Audit.To, res.Audit.Actor, res.Audit.Reason)
	})
}

// CompleteResult is how a worker reports the outcome of a task.
type CompleteResult struct {
	Success bool
	// Err is the real failure reason, surfaced to the user and the AI debugger.
	Err string
	// Permanent marks a failure that retrying cannot fix (a repo that does not
	// exist, a build that cannot succeed). Skips the attempt budget and goes
	// straight to dead-letter.
	Permanent bool
}

// Complete resolves a leased task and applies the capacity consequences.
//
// On success of a placing task the reservation is *committed* — it stays
// allocated because the container is now genuinely running. On failure it is
// rolled back. On success of a releasing task the environment's long-held
// capacity is freed. Getting this wrong in either direction either leaks
// capacity forever or lets the scheduler oversell a node; see
// state.TestReservationLifecycleByTaskType.
func (q *Queue) Complete(ctx context.Context, leaseID uuid.UUID, fencingToken int64, result CompleteResult) error {
	return q.db.InTx(ctx, store.TxOptions{}, func(tx pgx.Tx) error {
		cur, err := lockTaskByLease(ctx, tx, leaseID)
		if err != nil {
			return err
		}
		if cur.snap.FencingToken != fencingToken {
			return fmt.Errorf("%w: have %d, current is %d", ErrStaleFence, fencingToken, cur.snap.FencingToken)
		}

		ev := state.TaskEvent{Kind: state.EvFail, FencingToken: fencingToken, Err: result.Err}
		if result.Success {
			ev = state.TaskEvent{Kind: state.EvSucceed, FencingToken: fencingToken}
		}
		res, err := state.ApplyTask(cur.snap, ev, time.Now())
		if err != nil {
			return err
		}

		if err := q.applyCapacityEffects(ctx, tx, res.Effects, cur); err != nil {
			return err
		}

		if _, err := tx.Exec(ctx, `
			UPDATE tasks
			SET state = $2, last_error = $3,
			    lease_id = NULL, assigned_worker_id = NULL, lease_expires_at = NULL,
			    updated_at = now()
			WHERE id = $1`, cur.id, string(res.Next.State), nullIfEmpty(result.Err)); err != nil {
			return fmt.Errorf("resolve task: %w", err)
		}
		if err := store.AppendAudit(ctx, tx, "task", cur.id.String(),
			res.Audit.From, res.Audit.To, res.Audit.Actor, res.Audit.Reason); err != nil {
			return err
		}

		// A failed task is resolved immediately into its next disposition —
		// requeued with backoff, or dead-lettered — so there is no window in
		// which a failed task sits without a plan. That window would show up as
		// an environment stuck in "failed" while the retry budget still had
		// room, which looks exactly like a lost task.
		if !result.Success {
			return q.scheduleRetryOrDeadLetter(ctx, tx, cur, res.Next, result)
		}
		return nil
	})
}

func (q *Queue) scheduleRetryOrDeadLetter(ctx context.Context, tx pgx.Tx, cur lockedTask, next state.Task, result CompleteResult) error {
	if state.ShouldDeadLetter(next, result.Permanent) {
		res, err := state.ApplyTask(next, state.TaskEvent{Kind: state.EvDeadLetter, Err: result.Err}, time.Now())
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE tasks SET state = 'dead', updated_at = now() WHERE id = $1`, cur.id); err != nil {
			return fmt.Errorf("dead-letter task: %w", err)
		}
		return store.AppendAudit(ctx, tx, "task", cur.id.String(),
			res.Audit.From, res.Audit.To, res.Audit.Actor, res.Audit.Reason)
	}

	delay := q.Backoff.Delay(next.Attempts, jitter())
	res, err := state.ApplyTask(next, state.TaskEvent{Kind: state.EvRetry, Backoff: delay}, time.Now())
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE tasks
		SET state = 'queued', available_at = now() + $2::interval, updated_at = now()
		WHERE id = $1`, cur.id, delay.String()); err != nil {
		return fmt.Errorf("requeue task: %w", err)
	}
	return store.AppendAudit(ctx, tx, "task", cur.id.String(),
		res.Audit.From, res.Audit.To, res.Audit.Actor, res.Audit.Reason)
}

// ReapExpiredLeases reclaims tasks whose lease ran out and returns how many.
//
// Called by the scheduler's reaper loop. It deliberately does NOT check fencing
// tokens: it acts precisely because the lease holder has stopped talking to us.
// The holder is neutralised by the next lease carrying a higher token, not by
// this reclamation — which is why a partitioned worker that comes back finds
// every one of its writes rejected instead of corrupting the new attempt.
func (q *Queue) ReapExpiredLeases(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		limit = 100
	}
	var reaped int

	err := q.db.InTx(ctx, store.TxOptions{}, func(tx pgx.Tx) error {
		reaped = 0
		rows, err := tx.Query(ctx, `
			SELECT t.id, t.type, t.state, t.attempts, t.max_attempts,
			       t.fencing_token, t.assigned_worker_id,
			       e.spec_cpu_millicores, e.spec_mem_mb, e.spec_disk_mb
			FROM tasks t
			JOIN environments e ON e.id = t.environment_id
			WHERE t.state IN ('leased', 'running')
			  AND t.lease_expires_at < now()
			ORDER BY t.lease_expires_at
			FOR UPDATE OF t SKIP LOCKED
			LIMIT $1`, limit)
		if err != nil {
			return fmt.Errorf("scan expired leases: %w", err)
		}

		var expired []lockedTask
		for rows.Next() {
			var lt lockedTask
			var typ, st string
			var workerID *uuid.UUID
			if err := rows.Scan(&lt.id, &typ, &st, &lt.snap.Attempts, &lt.snap.MaxAttempts,
				&lt.snap.FencingToken, &workerID,
				&lt.specCPU, &lt.specMem, &lt.specDisk); err != nil {
				rows.Close()
				return fmt.Errorf("scan expired lease row: %w", err)
			}
			lt.snap.Type = state.TaskType(typ)
			lt.snap.State = state.TaskState(st)
			lt.snap.Reserved = lt.snap.Type.Places()
			if workerID != nil {
				lt.workerID = *workerID
				lt.snap.WorkerID = workerID.String()
			}
			expired = append(expired, lt)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}

		for _, lt := range expired {
			res, err := state.ApplyTask(lt.snap, state.TaskEvent{Kind: state.EvLeaseExpire}, time.Now())
			if err != nil {
				return fmt.Errorf("reap task %s: %w", lt.id, err)
			}
			if err := q.applyCapacityEffects(ctx, tx, res.Effects, lt); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `
				UPDATE tasks
				SET state = 'failed', attempts = $2,
				    last_error = 'lease expired; worker stopped reporting',
				    lease_id = NULL, assigned_worker_id = NULL, lease_expires_at = NULL,
				    updated_at = now()
				WHERE id = $1`, lt.id, res.Next.Attempts); err != nil {
				return fmt.Errorf("mark reaped task failed: %w", err)
			}
			if err := store.AppendAudit(ctx, tx, "task", lt.id.String(),
				res.Audit.From, res.Audit.To, res.Audit.Actor, res.Audit.Reason); err != nil {
				return err
			}
			if err := q.scheduleRetryOrDeadLetter(ctx, tx, lt, res.Next,
				CompleteResult{Err: "lease expired; worker stopped reporting"}); err != nil {
				return err
			}
			reaped++
		}
		return nil
	})
	return reaped, err
}

// lockedTask is a task row locked FOR UPDATE, with the fields needed to run the
// FSM and apply capacity effects.
type lockedTask struct {
	id       uuid.UUID
	envID    uuid.UUID
	workerID uuid.UUID
	snap     state.Task
	specCPU  int
	specMem  int
	specDisk int
}

func lockTaskByLease(ctx context.Context, tx pgx.Tx, leaseID uuid.UUID) (lockedTask, error) {
	var lt lockedTask
	var typ, st string
	var workerID *uuid.UUID
	err := tx.QueryRow(ctx, `
		SELECT t.id, t.environment_id, t.type, t.state, t.attempts, t.max_attempts,
		       t.fencing_token, t.assigned_worker_id,
		       e.spec_cpu_millicores, e.spec_mem_mb, e.spec_disk_mb
		FROM tasks t
		JOIN environments e ON e.id = t.environment_id
		WHERE t.lease_id = $1
		FOR UPDATE OF t`, leaseID).
		Scan(&lt.id, &lt.envID, &typ, &st, &lt.snap.Attempts, &lt.snap.MaxAttempts,
			&lt.snap.FencingToken, &workerID, &lt.specCPU, &lt.specMem, &lt.specDisk)
	if errors.Is(err, pgx.ErrNoRows) {
		return lt, ErrLeaseNotFound
	}
	if err != nil {
		return lt, fmt.Errorf("lock task by lease: %w", err)
	}
	lt.snap.Type = state.TaskType(typ)
	lt.snap.State = state.TaskState(st)
	lt.snap.Reserved = lt.snap.Type.Places()
	if workerID != nil {
		lt.workerID = *workerID
		lt.snap.WorkerID = workerID.String()
	}
	return lt, nil
}

// applyCapacityEffects translates the FSM's decisions into allocation
// arithmetic. The switch is exhaustive over the closed Effect set, so a new
// effect cannot be silently ignored here.
func (q *Queue) applyCapacityEffects(ctx context.Context, tx pgx.Tx, effects []state.Effect, lt lockedTask) error {
	for _, e := range effects {
		switch e.(type) {
		case state.ReleaseCapacity:
			if lt.workerID == uuid.Nil {
				continue
			}
			if err := releaseWorker(ctx, tx, lt.workerID, lt.specCPU, lt.specMem, lt.specDisk); err != nil {
				return err
			}
		case state.CommitCapacity:
			// Numerically a no-op: allocated_* already includes this spec and
			// must keep including it, because the container is now running.
		case state.ReserveCapacity:
			if err := reserveWorker(ctx, tx, lt.workerID, lt.specCPU, lt.specMem, lt.specDisk); err != nil {
				return err
			}
		}
	}
	return nil
}

func nullIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
