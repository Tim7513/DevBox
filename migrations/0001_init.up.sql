-- DevBox initial schema.
--
-- Postgres is the system of record, the durable task queue, the leader-election
-- primitive, and the optimistic-concurrency mechanism. Invariants that matter
-- for correctness are expressed as constraints here rather than as application
-- checks, so that a buggy or racing writer fails loudly instead of silently
-- overcommitting the fleet.

CREATE EXTENSION IF NOT EXISTS citext;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ---------------------------------------------------------------------------
-- users & quotas
-- ---------------------------------------------------------------------------

CREATE TABLE users (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    email         citext NOT NULL UNIQUE,
    -- NULL for accounts created via GitHub OAuth that never set a password.
    password_hash text,
    github_id     bigint UNIQUE,
    role          text NOT NULL DEFAULT 'user',
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT users_role_valid CHECK (role IN ('user', 'admin')),
    -- An account with no password and no GitHub identity can never be logged
    -- into; it is always a bug to create one.
    CONSTRAINT users_has_credential CHECK (password_hash IS NOT NULL OR github_id IS NOT NULL)
);

CREATE TABLE quotas (
    user_id                 uuid PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    max_concurrent_envs     int NOT NULL DEFAULT 3,
    max_cpu_millicores      int NOT NULL DEFAULT 4000,
    max_mem_mb              int NOT NULL DEFAULT 8192,
    max_disk_mb             int NOT NULL DEFAULT 20480,
    max_env_creates_per_min int NOT NULL DEFAULT 10,
    updated_at              timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT quotas_non_negative CHECK (
        max_concurrent_envs >= 0 AND max_cpu_millicores >= 0 AND
        max_mem_mb >= 0 AND max_disk_mb >= 0 AND max_env_creates_per_min >= 0
    )
);

-- ---------------------------------------------------------------------------
-- workers
-- ---------------------------------------------------------------------------

CREATE TABLE workers (
    id                        uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    hostname                  text NOT NULL,
    advertise_addr            text NOT NULL,
    state                     text NOT NULL DEFAULT 'active',

    capacity_cpu_millicores   int NOT NULL,
    capacity_mem_mb           int NOT NULL,
    capacity_disk_mb          int NOT NULL,

    allocated_cpu_millicores  int NOT NULL DEFAULT 0,
    allocated_mem_mb          int NOT NULL DEFAULT 0,
    allocated_disk_mb         int NOT NULL DEFAULT 0,

    -- Bumped on every registration. A worker that restarts gets a new epoch,
    -- which invalidates anything the previous incarnation was doing.
    epoch                     bigint NOT NULL DEFAULT 1,
    labels                    jsonb NOT NULL DEFAULT '{}'::jsonb,

    last_heartbeat_at         timestamptz NOT NULL DEFAULT now(),
    version                   bigint NOT NULL DEFAULT 1,
    registered_at             timestamptz NOT NULL DEFAULT now(),
    updated_at                timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT workers_state_valid CHECK (state IN ('active', 'draining', 'dead')),

    -- THE no-overcommit invariant (§33.1). The placement transaction increments
    -- allocated_* while holding this row's lock; if a concurrent scheduler,
    -- a retry, or a bug ever tries to push a worker past its capacity, the
    -- transaction aborts here rather than quietly overbooking the node.
    CONSTRAINT workers_no_overcommit CHECK (
        allocated_cpu_millicores >= 0 AND allocated_cpu_millicores <= capacity_cpu_millicores AND
        allocated_mem_mb         >= 0 AND allocated_mem_mb         <= capacity_mem_mb AND
        allocated_disk_mb        >= 0 AND allocated_disk_mb        <= capacity_disk_mb
    ),
    CONSTRAINT workers_capacity_positive CHECK (
        capacity_cpu_millicores > 0 AND capacity_mem_mb > 0 AND capacity_disk_mb > 0
    )
);

-- Placement scans for schedulable workers ordered by free capacity.
CREATE INDEX workers_schedulable_idx ON workers (state, last_heartbeat_at)
    WHERE state = 'active';

-- ---------------------------------------------------------------------------
-- snapshots
-- ---------------------------------------------------------------------------
-- Content-addressed and never mutated. A row exists only once the upload is
-- complete and the checksum verified, which is what lets
-- environments.current_snapshot_id be trusted as "the last good state".

CREATE TABLE snapshots (
    id                 uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    environment_id     uuid NOT NULL,
    object_key         text NOT NULL,
    size_bytes         bigint NOT NULL,
    checksum           text NOT NULL,
    parent_snapshot_id uuid REFERENCES snapshots(id),
    created_at         timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT snapshots_size_non_negative CHECK (size_bytes >= 0),
    CONSTRAINT snapshots_checksum_present CHECK (length(checksum) > 0)
);

CREATE INDEX snapshots_environment_idx ON snapshots (environment_id, created_at DESC);

-- ---------------------------------------------------------------------------
-- environments
-- ---------------------------------------------------------------------------

CREATE TABLE environments (
    id                    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id               uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name                  text NOT NULL,
    repo_url              text NOT NULL,
    repo_ref              text NOT NULL DEFAULT 'HEAD',

    desired_state         text NOT NULL DEFAULT 'running',
    observed_state        text NOT NULL DEFAULT 'pending',
    -- When observed_state last actually changed. Drives the reconciler's
    -- stranded-environment backstop (state.StuckThreshold).
    observed_since        timestamptz NOT NULL DEFAULT now(),

    spec_cpu_millicores   int NOT NULL,
    spec_mem_mb           int NOT NULL,
    spec_disk_mb          int NOT NULL,

    current_worker_id     uuid REFERENCES workers(id) ON DELETE SET NULL,
    current_snapshot_id   uuid REFERENCES snapshots(id),
    container_id          text,
    preview_internal_port int,

    -- Counts consecutive automatic recoveries; reset on reaching running.
    recover_attempts      int NOT NULL DEFAULT 0,
    last_error            text,

    version               bigint NOT NULL DEFAULT 1,
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT environments_desired_valid CHECK (
        desired_state IN ('running', 'stopped', 'destroyed')),
    CONSTRAINT environments_observed_valid CHECK (
        observed_state IN ('pending', 'provisioning', 'running', 'stopping',
                           'stopped', 'failed', 'destroyed')),
    CONSTRAINT environments_spec_positive CHECK (
        spec_cpu_millicores > 0 AND spec_mem_mb > 0 AND spec_disk_mb > 0),
    CONSTRAINT environments_recover_attempts_non_negative CHECK (recover_attempts >= 0),
    CONSTRAINT environments_port_valid CHECK (
        preview_internal_port IS NULL OR
        (preview_internal_port > 0 AND preview_internal_port <= 65535))
);

ALTER TABLE snapshots
    ADD CONSTRAINT snapshots_environment_fk
    FOREIGN KEY (environment_id) REFERENCES environments(id) ON DELETE CASCADE;

CREATE INDEX environments_user_idx ON environments (user_id, created_at DESC);
CREATE INDEX environments_worker_idx ON environments (current_worker_id)
    WHERE current_worker_id IS NOT NULL;

-- The reconciler scans for environments that are not converged. Excluding
-- destroyed rows keeps the scan proportional to live environments rather than
-- to all environments ever created.
CREATE INDEX environments_reconcile_idx ON environments (observed_state, desired_state)
    WHERE observed_state <> 'destroyed';

-- Quota admission sums the specs of a user's live environments. This index
-- makes that sum an index-only scan over just the rows that count.
CREATE INDEX environments_quota_usage_idx ON environments (user_id)
    WHERE observed_state NOT IN ('destroyed', 'failed', 'stopped');

-- ---------------------------------------------------------------------------
-- tasks — the durable queue
-- ---------------------------------------------------------------------------

CREATE TABLE tasks (
    id                 uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    environment_id     uuid NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
    type               text NOT NULL,
    state              text NOT NULL DEFAULT 'queued',

    -- Client-supplied for environment creation, server-derived otherwise.
    -- The unique constraint is what makes duplicate submission a no-op rather
    -- than a second environment (§14).
    idempotency_key    text NOT NULL UNIQUE,

    priority           int NOT NULL DEFAULT 0,
    attempts           int NOT NULL DEFAULT 0,
    max_attempts       int NOT NULL,

    -- The backoff gate. A queued task is invisible to claims until now().
    available_at       timestamptz NOT NULL DEFAULT now(),

    lease_id           uuid,
    assigned_worker_id uuid REFERENCES workers(id) ON DELETE SET NULL,
    lease_expires_at   timestamptz,
    -- Monotonic per task. Every re-lease strictly increases it, which is what
    -- makes a previous holder's writes rejectable forever after.
    fencing_token      bigint NOT NULL DEFAULT 0,

    last_error         text,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT tasks_type_valid CHECK (
        type IN ('provision', 'start', 'stop', 'snapshot', 'destroy', 'recover')),
    CONSTRAINT tasks_state_valid CHECK (
        state IN ('queued', 'leased', 'running', 'succeeded', 'failed', 'dead')),
    CONSTRAINT tasks_attempts_bounded CHECK (attempts >= 0 AND max_attempts > 0),
    CONSTRAINT tasks_fencing_token_non_negative CHECK (fencing_token >= 0),

    -- A leased or running task must carry the full lease tuple; a queued or
    -- terminal one must not. This prevents the half-leased states that make
    -- reasoning about ownership impossible.
    CONSTRAINT tasks_lease_consistent CHECK (
        (state IN ('leased', 'running')) =
        (lease_id IS NOT NULL AND assigned_worker_id IS NOT NULL AND lease_expires_at IS NOT NULL)
    )
);

-- THE per-environment serialization invariant (§15). At most one active task
-- per environment, enforced by the database, so operations on one environment
-- can never run concurrently no matter how many schedulers or API replicas are
-- racing. The predicate must stay in lockstep with state.TaskState.Active.
CREATE UNIQUE INDEX tasks_one_active_per_environment
    ON tasks (environment_id)
    WHERE state IN ('queued', 'leased', 'running');

-- The claim query: eligible tasks ordered by priority then age.
CREATE INDEX tasks_claimable_idx ON tasks (priority DESC, available_at, created_at)
    WHERE state = 'queued';

-- The reaper's scan for leases that have run out.
CREATE INDEX tasks_lease_expiry_idx ON tasks (lease_expires_at)
    WHERE state IN ('leased', 'running');

-- Dead-letter inspection and the admin queue endpoint.
CREATE INDEX tasks_deadletter_idx ON tasks (updated_at DESC) WHERE state = 'dead';
CREATE INDEX tasks_environment_idx ON tasks (environment_id, created_at DESC);

-- ---------------------------------------------------------------------------
-- audit_log — append-only transition record
-- ---------------------------------------------------------------------------
-- Written in the same transaction as every state change. Tests assert against
-- it and the AI debugger reads it, so it is load-bearing, not decoration.

CREATE TABLE audit_log (
    id         bigserial PRIMARY KEY,
    entity     text NOT NULL,
    entity_id  uuid NOT NULL,
    from_state text NOT NULL,
    to_state   text NOT NULL,
    actor      text NOT NULL,
    reason     text NOT NULL,
    at         timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT audit_entity_valid CHECK (entity IN ('environment', 'task', 'worker'))
);

CREATE INDEX audit_entity_idx ON audit_log (entity, entity_id, at DESC);
CREATE INDEX audit_at_idx ON audit_log (at DESC);
