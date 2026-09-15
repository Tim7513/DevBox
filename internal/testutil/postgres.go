// Package testutil provides the real-infrastructure harness the integration and
// fault suites run against.
//
// Nothing here mocks Postgres, Redis, MinIO, or Docker. The invariants this
// project cares about — SKIP LOCKED handing a row to exactly one consumer, a
// CHECK constraint refusing an overcommit, a row lock serialising concurrent
// quota admission — are properties of the real database. A fake would assert
// only that the fake behaves like the fake.
package testutil

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/timmai/devbox/internal/store"
)

var (
	sharedPG     *tcpostgres.PostgresContainer
	sharedDSN    string
	sharedPGOnce sync.Once
	sharedPGErr  error
)

// PostgresDSN starts one Postgres container for the whole test binary and
// returns its DSN.
//
// One container per package rather than per test: starting Postgres costs a
// couple of seconds, and the concurrency tests need to run many times to be
// meaningful. Isolation between tests comes from NewDB giving each test its own
// freshly-migrated database inside that container, which is both faster and a
// stronger guarantee than truncating shared tables.
//
// Set DEVBOX_TEST_DSN to point the suite at an already-running Postgres (the
// compose stack, or CI's service container) and skip container management.
func PostgresDSN(t *testing.T) string {
	t.Helper()
	if dsn := os.Getenv("DEVBOX_TEST_DSN"); dsn != "" {
		return dsn
	}

	sharedPGOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
			tcpostgres.WithDatabase("devbox_test"),
			tcpostgres.WithUsername("devbox"),
			tcpostgres.WithPassword("devbox"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).WithStartupTimeout(2*time.Minute)),
		)
		if err != nil {
			sharedPGErr = fmt.Errorf("start postgres container: %w", err)
			return
		}
		dsn, err := container.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			sharedPGErr = fmt.Errorf("postgres connection string: %w", err)
			return
		}
		sharedPG, sharedDSN = container, dsn
	})

	if sharedPGErr != nil {
		t.Fatalf("test infrastructure unavailable: %v\n"+
			"Integration tests require a working Docker daemon (colima start), "+
			"or set DEVBOX_TEST_DSN to an existing Postgres.", sharedPGErr)
	}
	return sharedDSN
}

// NewDB returns a store.DB backed by a fresh, fully-migrated database, dropped
// when the test finishes.
func NewDB(t *testing.T) *store.DB {
	t.Helper()
	ctx := t.Context()

	adminDSN := PostgresDSN(t)
	admin, err := store.Open(ctx, adminDSN, 4)
	if err != nil {
		t.Fatalf("connect to postgres: %v", err)
	}
	defer admin.Close()

	// A unique database per test keeps concurrency tests honest: one test's
	// leftover rows cannot make another's "no double-claim" assertion pass or
	// fail for the wrong reason.
	dbName := "devbox_" + uuid.New().String()[:8]
	if _, err := admin.Pool.Exec(ctx, `CREATE DATABASE "`+dbName+`"`); err != nil {
		t.Fatalf("create test database: %v", err)
	}

	dsn := replaceDBName(adminDSN, dbName)
	db, err := store.Open(ctx, dsn, 16)
	if err != nil {
		t.Fatalf("connect to test database: %v", err)
	}
	if _, err := db.MigrateUp(ctx); err != nil {
		db.Close()
		t.Fatalf("migrate test database: %v", err)
	}

	t.Cleanup(func() {
		db.Close()
		// Best-effort drop on a background context: the test's context is
		// already cancelled by the time cleanup runs.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if a, err := store.Open(cleanupCtx, adminDSN, 2); err == nil {
			defer a.Close()
			a.Pool.Exec(cleanupCtx,
				`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1`, dbName)
			a.Pool.Exec(cleanupCtx, `DROP DATABASE IF EXISTS "`+dbName+`"`)
		}
	})
	return db
}

// replaceDBName swaps the database component of a postgres:// DSN.
func replaceDBName(dsn, name string) string {
	// DSNs here are always postgres://user:pass@host:port/db?params.
	schemeEnd := len("postgres://")
	rest := dsn[schemeEnd:]
	slash := -1
	for i := 0; i < len(rest); i++ {
		if rest[i] == '/' {
			slash = i
			break
		}
	}
	if slash < 0 {
		return dsn + "/" + name
	}
	tail := rest[slash+1:]
	query := ""
	for i := 0; i < len(tail); i++ {
		if tail[i] == '?' {
			query = tail[i:]
			break
		}
	}
	return dsn[:schemeEnd] + rest[:slash+1] + name + query
}
