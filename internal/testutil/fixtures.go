package testutil

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/timmai/devbox/internal/store"
)

// Spec is an environment's resource request.
type Spec struct {
	CPUMillicores int
	MemMB         int
	DiskMB        int
}

// SmallSpec is the default environment size used across tests: deliberately a
// clean divisor of the worker capacities below so that "how many fit" is
// obvious in assertions rather than something the reader has to compute.
var SmallSpec = Spec{CPUMillicores: 1000, MemMB: 1024, DiskMB: 4096}

// Quota mirrors the quotas table.
type Quota struct {
	MaxConcurrentEnvs int
	MaxCPUMillicores  int
	MaxMemMB          int
	MaxDiskMB         int
	MaxCreatesPerMin  int
}

// GenerousQuota is large enough not to interfere with tests that are about
// something other than quota.
var GenerousQuota = Quota{MaxConcurrentEnvs: 100, MaxCPUMillicores: 1_000_000,
	MaxMemMB: 1_000_000, MaxDiskMB: 10_000_000, MaxCreatesPerMin: 1000}

// CreateUser inserts a user with the given quota and returns their id.
func CreateUser(t *testing.T, db *store.DB, q Quota) uuid.UUID {
	t.Helper()
	ctx := t.Context()

	var id uuid.UUID
	email := "user-" + uuid.New().String()[:8] + "@example.test"
	if err := db.Pool.QueryRow(ctx, `
		INSERT INTO users (email, password_hash, role)
		VALUES ($1, 'argon2id$placeholder', 'user') RETURNING id`, email).Scan(&id); err != nil {
		t.Fatalf("create user: %v", err)
	}
	if _, err := db.Pool.Exec(ctx, `
		INSERT INTO quotas (user_id, max_concurrent_envs, max_cpu_millicores,
		                    max_mem_mb, max_disk_mb, max_env_creates_per_min)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		id, q.MaxConcurrentEnvs, q.MaxCPUMillicores, q.MaxMemMB, q.MaxDiskMB, q.MaxCreatesPerMin); err != nil {
		t.Fatalf("create quota: %v", err)
	}
	return id
}

// CreateWorker inserts an active worker with the given capacity.
func CreateWorker(t *testing.T, db *store.DB, capacity Spec) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	host := "worker-" + uuid.New().String()[:8]
	if err := db.Pool.QueryRow(t.Context(), `
		INSERT INTO workers (hostname, advertise_addr, state,
		                     capacity_cpu_millicores, capacity_mem_mb, capacity_disk_mb)
		VALUES ($1, $2, 'active', $3, $4, $5) RETURNING id`,
		host, "http://"+host+":8081", capacity.CPUMillicores, capacity.MemMB, capacity.DiskMB).Scan(&id); err != nil {
		t.Fatalf("create worker: %v", err)
	}
	return id
}

// CreateEnvironment inserts an environment owned by userID.
func CreateEnvironment(t *testing.T, db *store.DB, userID uuid.UUID, spec Spec) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	if err := db.Pool.QueryRow(t.Context(), `
		INSERT INTO environments (user_id, name, repo_url, repo_ref,
		                          spec_cpu_millicores, spec_mem_mb, spec_disk_mb)
		VALUES ($1, $2, 'https://github.com/example/repo', 'main', $3, $4, $5)
		RETURNING id`,
		userID, "env-"+uuid.New().String()[:8], spec.CPUMillicores, spec.MemMB, spec.DiskMB).Scan(&id); err != nil {
		t.Fatalf("create environment: %v", err)
	}
	return id
}

// WorkerAllocation reads a worker's current allocation — the number the
// no-overcommit invariant is about.
func WorkerAllocation(t *testing.T, db *store.DB, workerID uuid.UUID) Spec {
	t.Helper()
	var s Spec
	if err := db.Pool.QueryRow(t.Context(), `
		SELECT allocated_cpu_millicores, allocated_mem_mb, allocated_disk_mb
		FROM workers WHERE id = $1`, workerID).Scan(&s.CPUMillicores, &s.MemMB, &s.DiskMB); err != nil {
		t.Fatalf("read worker allocation: %v", err)
	}
	return s
}

// SetEnvObservedState forces an environment's observed state, for tests that
// need to start from a particular point without driving the whole lifecycle.
func SetEnvObservedState(t *testing.T, db *store.DB, envID uuid.UUID, observed string, workerID *uuid.UUID) {
	t.Helper()
	if _, err := db.Pool.Exec(t.Context(), `
		UPDATE environments
		SET observed_state = $2, current_worker_id = $3, observed_since = now(), version = version + 1
		WHERE id = $1`, envID, observed, workerID); err != nil {
		t.Fatalf("set observed state: %v", err)
	}
}

// ExpireLease backdates a task's lease so the reaper will collect it, without
// making the test sleep for the TTL.
func ExpireLease(t *testing.T, db *store.DB, taskID uuid.UUID) {
	t.Helper()
	if _, err := db.Pool.Exec(t.Context(), `
		UPDATE tasks SET lease_expires_at = now() - interval '1 second' WHERE id = $1`, taskID); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
}

// CountTasks counts an environment's tasks in the given states.
func CountTasks(ctx context.Context, db *store.DB, envID uuid.UUID, states ...string) (int, error) {
	var n int
	err := db.Pool.QueryRow(ctx, `
		SELECT count(*) FROM tasks WHERE environment_id = $1 AND state = ANY($2)`,
		envID, states).Scan(&n)
	return n, err
}
