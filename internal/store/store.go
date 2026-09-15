package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DB is the shared connection pool. Every service holds exactly one.
type DB struct {
	Pool *pgxpool.Pool
}

// Open creates a pool and verifies connectivity.
//
// maxConns matters more than it looks: the placement transaction takes row
// locks, so an oversized pool lets more writers pile up behind the same worker
// row and turns lock contention into connection exhaustion. Callers should size
// it to their concurrency, not to the database's max_connections.
func Open(ctx context.Context, dsn string, maxConns int32) (*DB, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &DB{Pool: pool}, nil
}

// Close releases the pool.
func (db *DB) Close() {
	if db != nil && db.Pool != nil {
		db.Pool.Close()
	}
}

// MigrateUp applies pending migrations using a dedicated connection from the
// pool. The connection is held for the duration because the advisory lock is
// session-scoped.
func (db *DB) MigrateUp(ctx context.Context) (int, error) {
	conn, err := db.Pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()
	return Migrate(ctx, conn.Conn())
}

// SchemaVersion reports the highest applied migration. Used by /readyz so a
// service that booted against an un-migrated database reports unready instead
// of serving 500s.
func (db *DB) SchemaVersion(ctx context.Context) (int, error) {
	var v int
	err := db.Pool.QueryRow(ctx, `SELECT coalesce(max(version), 0) FROM schema_migrations`).Scan(&v)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == UndefinedTable {
			return 0, nil
		}
		return 0, err
	}
	return v, nil
}

// Postgres error codes this codebase reacts to by name rather than by string
// matching, because the difference between them changes behaviour: a
// serialization failure should be retried, a unique violation usually means
// "someone else already did this" and is success, and a check violation means
// an invariant was about to be broken and must never be retried blindly.
const (
	SerializationFailure = "40001"
	DeadlockDetected     = "40P01"
	UniqueViolation      = "23505"
	CheckViolation       = "23514"
	ForeignKeyViolation  = "23503"
	UndefinedTable       = "42P01"
)

// IsRetryable reports whether err is a transient concurrency conflict that the
// same transaction can simply be re-run against.
//
// Deliberately narrow. Check violations are excluded: they mean the no-overcommit
// or lease-consistency invariant was about to be violated, and retrying would
// just hit it again — the caller needs to re-derive its inputs, not retry.
func IsRetryable(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == SerializationFailure || pgErr.Code == DeadlockDetected
}

// IsUniqueViolation reports whether err is a unique-constraint violation, and
// on which constraint. The constraint name is the useful part: a collision on
// tasks_idempotency_key means a duplicate submission (benign), while one on
// tasks_one_active_per_environment means the environment is already busy (also
// benign, but a different decision).
func IsUniqueViolation(err error) (constraint string, ok bool) {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == UniqueViolation {
		return pgErr.ConstraintName, true
	}
	return "", false
}

// IsCheckViolation reports whether err is a check-constraint violation and
// which constraint failed.
func IsCheckViolation(err error) (constraint string, ok bool) {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == CheckViolation {
		return pgErr.ConstraintName, true
	}
	return "", false
}

// TxOptions configures InTx.
type TxOptions struct {
	// Isolation defaults to ReadCommitted. The placement transaction uses
	// Serializable; see internal/scheduler for why row locks alone are not
	// enough there.
	Isolation pgx.TxIsoLevel
	// MaxRetries bounds serialization-failure retries. Zero means
	// DefaultTxRetries.
	MaxRetries int
	// AccessMode defaults to ReadWrite.
	AccessMode pgx.TxAccessMode
}

// DefaultTxRetries bounds automatic retry of serialization failures.
//
// Under Serializable isolation a contended placement transaction can abort
// several times in a row when many schedulers target the same worker. Five
// attempts absorbs realistic contention; beyond that the caller is better off
// surfacing the conflict (and incrementing devbox_reservation_conflicts_total)
// than spinning, because sustained conflict means the fleet is saturated and
// the honest answer to the user is backpressure, not a longer wait.
const DefaultTxRetries = 5

// InTx runs fn inside a transaction, retrying serialization failures with a
// short backoff and rolling back on any error or panic.
//
// fn must be idempotent with respect to its own effects, because it may run
// more than once. It must not capture results into outer variables before the
// commit succeeds — assign them only from the final successful call, or read
// them back after InTx returns.
func (db *DB) InTx(ctx context.Context, opts TxOptions, fn func(pgx.Tx) error) error {
	iso := opts.Isolation
	if iso == "" {
		iso = pgx.ReadCommitted
	}
	mode := opts.AccessMode
	if mode == "" {
		mode = pgx.ReadWrite
	}
	retries := opts.MaxRetries
	if retries <= 0 {
		retries = DefaultTxRetries
	}

	var lastErr error
	for attempt := 0; attempt < retries; attempt++ {
		if attempt > 0 {
			// Brief, growing pause so competing transactions do not lockstep
			// into each other's retry immediately.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * 2 * time.Millisecond):
			}
		}

		err := db.runOnce(ctx, iso, mode, fn)
		if err == nil {
			return nil
		}
		lastErr = err
		if !IsRetryable(err) {
			return err
		}
	}
	return fmt.Errorf("transaction still conflicting after %d attempts: %w", retries, lastErr)
}

func (db *DB) runOnce(ctx context.Context, iso pgx.TxIsoLevel, mode pgx.TxAccessMode, fn func(pgx.Tx) error) (err error) {
	tx, err := db.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: iso, AccessMode: mode})
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			// Roll back with a context that outlives a cancelled caller, so a
			// panic cannot leave the transaction open holding row locks.
			tx.Rollback(context.WithoutCancel(ctx))
			panic(p)
		}
		if err != nil {
			tx.Rollback(context.WithoutCancel(ctx))
		}
	}()

	if err = fn(tx); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// AppendAudit writes one transition record. Always called inside the same
// transaction as the state change it describes, so the log cannot disagree with
// the rows it documents.
func AppendAudit(ctx context.Context, tx pgx.Tx, entity, entityID, from, to, actor, reason string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO audit_log (entity, entity_id, from_state, to_state, actor, reason)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		entity, entityID, from, to, actor, reason)
	if err != nil {
		return fmt.Errorf("append audit: %w", err)
	}
	return nil
}
