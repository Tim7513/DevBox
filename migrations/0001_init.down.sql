-- Reverse of 0001_init.up.sql. Dropped in dependency order; snapshots and
-- environments reference each other so the FK added after both exist is
-- dropped first.

ALTER TABLE IF EXISTS snapshots DROP CONSTRAINT IF EXISTS snapshots_environment_fk;

DROP TABLE IF EXISTS audit_log;
DROP TABLE IF EXISTS tasks;
DROP TABLE IF EXISTS environments;
DROP TABLE IF EXISTS snapshots;
DROP TABLE IF EXISTS workers;
DROP TABLE IF EXISTS quotas;
DROP TABLE IF EXISTS users;
