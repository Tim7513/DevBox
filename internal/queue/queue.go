// Package queue is the durable task queue, implemented on PostgreSQL.
//
// There is no external broker. Durability, ordering, at-least-once delivery,
// leasing, and the atomic capacity/quota reservation all come from a handful of
// carefully written SQL statements plus the constraints in migration 0001. The
// two statements that carry the weight are the SELECT ... FOR UPDATE SKIP
// LOCKED that hands a task to exactly one consumer, and the reservation
// UPDATE whose CHECK constraint makes overcommit impossible rather than
// unlikely.
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

// Queue provides durable task operations.
type Queue struct {
	db *store.DB
	// LeaseTTL is how long a granted lease is valid before the reaper may
	// reclaim it. See ADR 0002 for how this interacts with the heartbeat
	// interval and why the value is what it is.
	LeaseTTL time.Duration
	// Backoff is the retry schedule applied to failed tasks.
	Backoff state.Backoff
	// now is injectable so tests can drive lease expiry without sleeping.
	// Production always uses the database clock for anything persisted; this is
	// only used for comparisons in Go.
	now func() time.Time
}

// New constructs a Queue.
func New(db *store.DB, leaseTTL time.Duration) *Queue {
	return &Queue{db: db, LeaseTTL: leaseTTL, Backoff: state.DefaultBackoff, now: time.Now}
}

// Errors callers distinguish.
var (
	// ErrNoWork means there was nothing claimable for this worker right now.
	// Not an error condition — the normal outcome of a quiet poll.
	ErrNoWork = errors.New("queue: no claimable work")
	// ErrEnvironmentBusy means an active task already exists for the
	// environment. Enqueue returns it instead of violating the one-active-task
	// index, so callers can treat "already converging" as success.
	ErrEnvironmentBusy = errors.New("queue: environment already has an active task")
	// ErrStaleFence means the caller's fencing token is not the current one.
	// The worker holding it must abort immediately.
	ErrStaleFence = errors.New("queue: stale fencing token")
	// ErrLeaseNotFound means the lease does not exist or has already been
	// resolved.
	ErrLeaseNotFound = errors.New("queue: lease not found")
	// ErrQuotaExceeded means placing this task would push the owner past their
	// quota.
	ErrQuotaExceeded = errors.New("queue: user quota exceeded")
)

// EnqueueParams describes a task to create.
type EnqueueParams struct {
	EnvironmentID  uuid.UUID
	Type           state.TaskType
	IdempotencyKey string
	Priority       int
	// AvailableAt gates the task; zero means immediately.
	AvailableAt time.Time
	Reason      string
}

// EnqueueResult reports what happened.
type EnqueueResult struct {
	TaskID uuid.UUID
	// Created is false when an existing task was returned instead, either
	// because the idempotency key was already used or because the environment
	// already had an active task.
	Created bool
}

// Enqueue creates a task, idempotently.
//
// Two different database constraints can reject the insert, and they mean
// different things:
//
//   - tasks_idempotency_key: this exact request was already submitted. Return
//     the original task. This is what makes a client retry (or a double-clicked
//     button) produce one environment rather than two.
//   - tasks_one_active_per_environment: a *different* operation is already in
//     flight for this environment. Return ErrEnvironmentBusy so the reconciler
//     backs off rather than fighting it.
//
// Both are handled by catching the violation rather than by checking first,
// because a check-then-insert has a race window and the constraint does not.
func (q *Queue) Enqueue(ctx context.Context, p EnqueueParams) (EnqueueResult, error) {
	if p.IdempotencyKey == "" {
		return EnqueueResult{}, fmt.Errorf("queue: idempotency key is required")
	}
	if !p.Type.Valid() {
		return EnqueueResult{}, fmt.Errorf("queue: invalid task type %q", p.Type)
	}

	availableAt := p.AvailableAt
	if availableAt.IsZero() {
		availableAt = time.Now()
	}

	var res EnqueueResult
	err := q.db.InTx(ctx, store.TxOptions{}, func(tx pgx.Tx) error {
		res = EnqueueResult{}
		var id uuid.UUID
		err := tx.QueryRow(ctx, `
			INSERT INTO tasks (environment_id, type, state, idempotency_key,
			                   priority, attempts, max_attempts, available_at)
			VALUES ($1, $2, 'queued', $3, $4, 0, $5, $6)
			RETURNING id`,
			p.EnvironmentID, string(p.Type), p.IdempotencyKey,
			p.Priority, p.Type.MaxAttempts(), availableAt).Scan(&id)

		if err != nil {
			constraint, isUnique := store.IsUniqueViolation(err)
			if !isUnique {
				return fmt.Errorf("insert task: %w", err)
			}
			switch constraint {
			case "tasks_idempotency_key_key":
				// Same request, seen before. Hand back the original.
				if err := tx.QueryRow(ctx,
					`SELECT id FROM tasks WHERE idempotency_key = $1`, p.IdempotencyKey).Scan(&id); err != nil {
					return fmt.Errorf("resolve duplicate idempotency key: %w", err)
				}
				res = EnqueueResult{TaskID: id, Created: false}
				return nil
			case "tasks_one_active_per_environment":
				return ErrEnvironmentBusy
			default:
				return fmt.Errorf("insert task: %w", err)
			}
		}

		reason := p.Reason
		if reason == "" {
			reason = "enqueued " + string(p.Type)
		}
		if err := store.AppendAudit(ctx, tx, "task", id.String(), "", "queued", "scheduler", reason); err != nil {
			return err
		}
		res = EnqueueResult{TaskID: id, Created: true}
		return nil
	})
	if err != nil {
		return EnqueueResult{}, err
	}
	return res, nil
}

// Lease is a granted unit of work.
type Lease struct {
	TaskID        uuid.UUID
	LeaseID       uuid.UUID
	EnvironmentID uuid.UUID
	Type          state.TaskType
	Attempt       int
	FencingToken  int64
	ExpiresAt     time.Time

	RepoURL    string
	RepoRef    string
	SnapshotID *uuid.UUID
	SpecCPU    int
	SpecMemMB  int
	SpecDiskMB int
}

// Claim grants at most one task to a worker.
//
// This is the transaction the whole system's correctness rests on (§33.1). In
// one atomic step it: verifies the worker is still schedulable, picks a task
// that fits the worker's *remaining* capacity, verifies and reserves against
// the owner's quota, increments the worker's allocation, and marks the task
// leased with a strictly higher fencing token.
//
// Three mechanisms combine to make it race-free:
//
//   - FOR UPDATE SKIP LOCKED on the candidate task means two concurrent
//     claimers never consider the same row; the loser skips to the next.
//   - FOR UPDATE on the worker row serializes all allocation arithmetic for
//     that node, and the workers_no_overcommit CHECK constraint turns any
//     mistake into an aborted transaction rather than an overbooked node.
//   - FOR UPDATE on the quotas row serializes admission per user, which closes
//     the read-then-write window that would otherwise let two concurrent
//     creates both observe "quota has room" and both proceed.
//
// The isolation level is Serializable on top of all three. The row locks make
// conflicts rare enough that serialization failures are unusual; Serializable
// is there to catch any predicate read a future change forgets to lock.
func (q *Queue) Claim(ctx context.Context, workerID uuid.UUID) (*Lease, error) {
	var lease *Lease

	err := q.db.InTx(ctx, store.TxOptions{Isolation: pgx.Serializable}, func(tx pgx.Tx) error {
		lease = nil

		// 1. Lock the worker and read its live allocation. A draining or dead
		// worker gets nothing; that is how drain sheds load without a separate
		// mechanism.
		var capCPU, capMem, capDisk, allocCPU, allocMem, allocDisk int
		err := tx.QueryRow(ctx, `
			SELECT capacity_cpu_millicores, capacity_mem_mb, capacity_disk_mb,
			       allocated_cpu_millicores, allocated_mem_mb, allocated_disk_mb
			FROM workers
			WHERE id = $1 AND state = 'active'
			FOR UPDATE`, workerID).Scan(&capCPU, &capMem, &capDisk, &allocCPU, &allocMem, &allocDisk)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNoWork // not registered, draining, or dead
		}
		if err != nil {
			return fmt.Errorf("lock worker: %w", err)
		}

		freeCPU, freeMem, freeDisk := capCPU-allocCPU, capMem-allocMem, capDisk-allocDisk

		// 2. Pick the best candidate that this worker can actually run.
		//
		// A placing task needs room for the environment's spec. A releasing
		// task (stop/destroy) needs no room but must run where the environment
		// actually is — or anywhere, if it was never placed, since then it is a
		// no-op. Expressing both in one query keeps the claim a single round
		// trip and a single lock ordering.
		var (
			taskID   uuid.UUID
			envID    uuid.UUID
			taskType string
			attempts int
			fencing  int64
			ownerID  uuid.UUID
			repoURL  string
			repoRef  string
			snapID   *uuid.UUID
			specCPU  int
			specMem  int
			specDisk int
		)
		err = tx.QueryRow(ctx, `
			SELECT t.id, t.environment_id, t.type, t.attempts, t.fencing_token,
			       e.user_id, e.repo_url, e.repo_ref, e.current_snapshot_id,
			       e.spec_cpu_millicores, e.spec_mem_mb, e.spec_disk_mb
			FROM tasks t
			JOIN environments e ON e.id = t.environment_id
			WHERE t.state = 'queued'
			  AND t.available_at <= now()
			  AND (
			        -- placing: must fit in this worker's remaining capacity
			        (t.type IN ('provision', 'start', 'recover')
			           AND e.spec_cpu_millicores <= $2
			           AND e.spec_mem_mb         <= $3
			           AND e.spec_disk_mb        <= $4)
			     OR -- releasing/other: must run where the environment lives
			        (t.type IN ('stop', 'destroy', 'snapshot')
			           AND (e.current_worker_id = $1 OR e.current_worker_id IS NULL))
			      )
			ORDER BY t.priority DESC, t.available_at, t.created_at
			FOR UPDATE OF t SKIP LOCKED
			LIMIT 1`,
			workerID, freeCPU, freeMem, freeDisk).
			Scan(&taskID, &envID, &taskType, &attempts, &fencing,
				&ownerID, &repoURL, &repoRef, &snapID, &specCPU, &specMem, &specDisk)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNoWork
		}
		if err != nil {
			return fmt.Errorf("select claimable task: %w", err)
		}

		tt := state.TaskType(taskType)

		// 3. For a placing task, reserve against the owner's quota and the
		// worker's capacity. Both are locked; both are checked by constraints.
		if tt.Places() {
			if err := q.reserveQuota(ctx, tx, ownerID, envID, specCPU, specMem, specDisk); err != nil {
				return err
			}
			if err := reserveWorker(ctx, tx, workerID, specCPU, specMem, specDisk); err != nil {
				return err
			}
		}

		// 4. Run the transition through the FSM so the guards live in one place.
		cur := state.Task{
			Type: tt, State: state.TaskQueued, Attempts: attempts,
			FencingToken: fencing,
		}
		newToken := fencing + 1
		if _, err := state.ApplyTask(cur,
			state.TaskEvent{Kind: state.EvLease, FencingToken: newToken, WorkerID: workerID.String()},
			time.Now()); err != nil {
			return fmt.Errorf("lease transition rejected: %w", err)
		}

		// 5. Mark it leased. The state='queued' predicate in the WHERE clause
		// is a belt-and-braces guard: the row lock already guarantees nobody
		// else changed it, but if that ever stops being true this UPDATE
		// affects zero rows instead of stealing someone else's task.
		leaseID := uuid.New()
		var expiresAt time.Time
		err = tx.QueryRow(ctx, `
			UPDATE tasks
			SET state = 'leased',
			    lease_id = $2,
			    assigned_worker_id = $3,
			    fencing_token = $4,
			    lease_expires_at = now() + $5::interval,
			    updated_at = now()
			WHERE id = $1 AND state = 'queued'
			RETURNING lease_expires_at`,
			taskID, leaseID, workerID, newToken, q.LeaseTTL.String()).Scan(&expiresAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNoWork
		}
		if err != nil {
			return fmt.Errorf("mark task leased: %w", err)
		}

		if err := store.AppendAudit(ctx, tx, "task", taskID.String(), "queued", "leased",
			"scheduler", fmt.Sprintf("leased to worker %s (fence %d)", workerID, newToken)); err != nil {
			return err
		}

		lease = &Lease{
			TaskID: taskID, LeaseID: leaseID, EnvironmentID: envID, Type: tt,
			Attempt: attempts + 1, FencingToken: newToken, ExpiresAt: expiresAt,
			RepoURL: repoURL, RepoRef: repoRef, SnapshotID: snapID,
			SpecCPU: specCPU, SpecMemMB: specMem, SpecDiskMB: specDisk,
		}
		return nil
	})

	if errors.Is(err, ErrNoWork) {
		return nil, ErrNoWork
	}
	if err != nil {
		return nil, err
	}
	return lease, nil
}

// reserveQuota locks the owner's quota row and verifies that adding this
// environment keeps them inside every limit.
//
// Usage is *derived* from the environments table rather than kept in a counter
// column. A counter would be faster but can drift from reality after a crash or
// a missed decrement, and a quota that silently drifts is worse than one that
// costs an index scan. The partial index environments_quota_usage_idx keeps
// this cheap by covering only the rows that count.
//
// The row lock is what makes concurrent admission safe: without it, two
// simultaneous creates both read "2 of 3 used" and both proceed to 4.
func (q *Queue) reserveQuota(ctx context.Context, tx pgx.Tx, ownerID, envID uuid.UUID, cpu, mem, disk int) error {
	var maxEnvs, maxCPU, maxMem, maxDisk int
	err := tx.QueryRow(ctx, `
		SELECT max_concurrent_envs, max_cpu_millicores, max_mem_mb, max_disk_mb
		FROM quotas WHERE user_id = $1
		FOR UPDATE`, ownerID).Scan(&maxEnvs, &maxCPU, &maxMem, &maxDisk)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("no quota row for user %s", ownerID)
	}
	if err != nil {
		return fmt.Errorf("lock quota: %w", err)
	}

	// Count everything live *except* this environment — it may already be
	// counted (a restart of an environment that is merely stopped), and
	// double-counting it would make a user's last slot unusable.
	var nEnvs, useCPU, useMem, useDisk int
	err = tx.QueryRow(ctx, `
		SELECT count(*),
		       coalesce(sum(spec_cpu_millicores), 0),
		       coalesce(sum(spec_mem_mb), 0),
		       coalesce(sum(spec_disk_mb), 0)
		FROM environments
		WHERE user_id = $1
		  AND id <> $2
		  AND observed_state NOT IN ('destroyed', 'failed', 'stopped')`,
		ownerID, envID).Scan(&nEnvs, &useCPU, &useMem, &useDisk)
	if err != nil {
		return fmt.Errorf("compute quota usage: %w", err)
	}

	switch {
	case nEnvs+1 > maxEnvs:
		return fmt.Errorf("%w: concurrent environments %d/%d", ErrQuotaExceeded, nEnvs+1, maxEnvs)
	case useCPU+cpu > maxCPU:
		return fmt.Errorf("%w: cpu %d/%d millicores", ErrQuotaExceeded, useCPU+cpu, maxCPU)
	case useMem+mem > maxMem:
		return fmt.Errorf("%w: memory %d/%d MB", ErrQuotaExceeded, useMem+mem, maxMem)
	case useDisk+disk > maxDisk:
		return fmt.Errorf("%w: disk %d/%d MB", ErrQuotaExceeded, useDisk+disk, maxDisk)
	}
	return nil
}

// reserveWorker increments a worker's allocation. The workers_no_overcommit
// CHECK constraint is the real guard: if this would exceed capacity the
// statement raises rather than writing, so overcommit is impossible even if the
// candidate-selection query above is ever wrong.
func reserveWorker(ctx context.Context, tx pgx.Tx, workerID uuid.UUID, cpu, mem, disk int) error {
	_, err := tx.Exec(ctx, `
		UPDATE workers
		SET allocated_cpu_millicores = allocated_cpu_millicores + $2,
		    allocated_mem_mb         = allocated_mem_mb + $3,
		    allocated_disk_mb        = allocated_disk_mb + $4,
		    version = version + 1,
		    updated_at = now()
		WHERE id = $1`, workerID, cpu, mem, disk)
	if err != nil {
		if constraint, ok := store.IsCheckViolation(err); ok && constraint == "workers_no_overcommit" {
			// Reached only if the selection query and the constraint disagree,
			// which would be a bug. Surfacing it as "no work" keeps the worker
			// polling safely instead of crash-looping, and the metric makes it
			// visible.
			return ErrNoWork
		}
		return fmt.Errorf("reserve worker capacity: %w", err)
	}
	return nil
}

// releaseWorker returns capacity to a worker, clamping at zero.
//
// The GREATEST(0, ...) is deliberate. A negative allocation would violate the
// no-overcommit constraint and wedge the worker permanently: every subsequent
// claim and release against it would abort. Clamping means a double-release bug
// costs us some accounting accuracy rather than taking a node out of service,
// and the reconciler's periodic recompute repairs the drift.
func releaseWorker(ctx context.Context, tx pgx.Tx, workerID uuid.UUID, cpu, mem, disk int) error {
	_, err := tx.Exec(ctx, `
		UPDATE workers
		SET allocated_cpu_millicores = GREATEST(0, allocated_cpu_millicores - $2),
		    allocated_mem_mb         = GREATEST(0, allocated_mem_mb - $3),
		    allocated_disk_mb        = GREATEST(0, allocated_disk_mb - $4),
		    version = version + 1,
		    updated_at = now()
		WHERE id = $1`, workerID, cpu, mem, disk)
	if err != nil {
		return fmt.Errorf("release worker capacity: %w", err)
	}
	return nil
}
