// Package store owns all database access: connection pooling, migrations, and
// the transaction helpers that the queue and scheduler build their invariants
// on top of.
package store

import (
	"context"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/timmai/devbox/migrations"
)

// Migration is one numbered schema change.
type Migration struct {
	Version int
	Name    string
	Up      string
	Down    string
}

// LoadMigrations reads the embedded migration files and returns them in
// ascending version order.
//
// Files are named NNNN_name.up.sql / NNNN_name.down.sql. Every up migration
// must have a matching down migration — an irreversible schema change is a
// thing you want to discover while writing it, not during an incident.
func LoadMigrations() ([]Migration, error) {
	entries, err := fs.ReadDir(migrations.FS, ".")
	if err != nil {
		return nil, fmt.Errorf("read embedded migrations: %w", err)
	}

	byVersion := map[int]*Migration{}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".sql") {
			continue
		}
		idx := strings.Index(name, "_")
		if idx < 0 {
			return nil, fmt.Errorf("migration %q: expected NNNN_name.{up,down}.sql", name)
		}
		version, err := strconv.Atoi(name[:idx])
		if err != nil {
			return nil, fmt.Errorf("migration %q: bad version prefix: %w", name, err)
		}

		body, err := migrations.FS.ReadFile(name)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", name, err)
		}

		m := byVersion[version]
		if m == nil {
			m = &Migration{Version: version, Name: strings.TrimSuffix(name[idx+1:], ".sql")}
			byVersion[version] = m
		}
		switch {
		case strings.HasSuffix(name, ".up.sql"):
			m.Up = string(body)
			m.Name = strings.TrimSuffix(name[idx+1:], ".up.sql")
		case strings.HasSuffix(name, ".down.sql"):
			m.Down = string(body)
		default:
			return nil, fmt.Errorf("migration %q: must end in .up.sql or .down.sql", name)
		}
	}

	out := make([]Migration, 0, len(byVersion))
	for _, m := range byVersion {
		if m.Up == "" {
			return nil, fmt.Errorf("migration %d (%s): missing .up.sql", m.Version, m.Name)
		}
		if m.Down == "" {
			return nil, fmt.Errorf("migration %d (%s): missing .down.sql", m.Version, m.Name)
		}
		out = append(out, *m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out, nil
}

// migrateLockKey is the advisory lock migrations serialize on. Distinct from
// the scheduler's leader-election key (see internal/scheduler) so a migrating
// process and a scheduling leader never block each other.
const migrateLockKey int64 = 0x6D696772_6174696F

// Migrate applies every migration newer than the recorded version.
//
// It takes a session-level advisory lock first, so that N API servers and
// schedulers booting simultaneously — which is the normal case under compose
// and Kubernetes — do not race to apply the same DDL. The losers block, then
// observe the work is already done and return.
//
// Each migration runs in its own transaction alongside its schema_migrations
// insert, so a failure part-way through a sequence leaves the database at a
// clean, known version rather than half-applied.
func Migrate(ctx context.Context, conn *pgx.Conn) (applied int, err error) {
	migs, err := LoadMigrations()
	if err != nil {
		return 0, err
	}

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrateLockKey); err != nil {
		return 0, fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		if _, uerr := conn.Exec(context.WithoutCancel(ctx),
			`SELECT pg_advisory_unlock($1)`, migrateLockKey); uerr != nil && err == nil {
			err = fmt.Errorf("release migration lock: %w", uerr)
		}
	}()

	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    int PRIMARY KEY,
			name       text NOT NULL,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
		return 0, fmt.Errorf("create schema_migrations: %w", err)
	}

	var current int
	if err := conn.QueryRow(ctx,
		`SELECT coalesce(max(version), 0) FROM schema_migrations`).Scan(&current); err != nil {
		return 0, fmt.Errorf("read current version: %w", err)
	}

	for _, m := range migs {
		if m.Version <= current {
			continue
		}
		if err := applyOne(ctx, conn, m); err != nil {
			return applied, err
		}
		applied++
	}
	return applied, nil
}

func applyOne(ctx context.Context, conn *pgx.Conn, m Migration) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration %d: %w", m.Version, err)
	}
	defer tx.Rollback(context.WithoutCancel(ctx))

	if _, err := tx.Exec(ctx, m.Up); err != nil {
		return fmt.Errorf("apply migration %d (%s): %w", m.Version, m.Name, err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`, m.Version, m.Name); err != nil {
		return fmt.Errorf("record migration %d: %w", m.Version, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration %d: %w", m.Version, err)
	}
	return nil
}

// Rollback reverts migrations down to (and excluding) targetVersion. Used by
// tests and by the runbook's recovery procedure; never called automatically.
func Rollback(ctx context.Context, conn *pgx.Conn, targetVersion int) (reverted int, err error) {
	migs, err := LoadMigrations()
	if err != nil {
		return 0, err
	}
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrateLockKey); err != nil {
		return 0, fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		if _, uerr := conn.Exec(context.WithoutCancel(ctx),
			`SELECT pg_advisory_unlock($1)`, migrateLockKey); uerr != nil && err == nil {
			err = fmt.Errorf("release migration lock: %w", uerr)
		}
	}()

	for i := len(migs) - 1; i >= 0; i-- {
		m := migs[i]
		if m.Version <= targetVersion {
			break
		}
		var exists bool
		if err := conn.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`, m.Version).Scan(&exists); err != nil {
			return reverted, err
		}
		if !exists {
			continue
		}

		tx, err := conn.Begin(ctx)
		if err != nil {
			return reverted, err
		}
		if _, err := tx.Exec(ctx, m.Down); err != nil {
			tx.Rollback(context.WithoutCancel(ctx))
			return reverted, fmt.Errorf("revert migration %d (%s): %w", m.Version, m.Name, err)
		}
		if _, err := tx.Exec(ctx, `DELETE FROM schema_migrations WHERE version = $1`, m.Version); err != nil {
			tx.Rollback(context.WithoutCancel(ctx))
			return reverted, err
		}
		if err := tx.Commit(ctx); err != nil {
			return reverted, err
		}
		reverted++
	}
	return reverted, nil
}
