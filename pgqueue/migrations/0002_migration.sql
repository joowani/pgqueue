-- `pg_monitor` reads the columns a metrics exporter counts and watches, and no longer the documents applications store
-- in them. Migration 1 granted it every column of every table: job payloads, results, errors and metadata, dedupe keys,
-- cron definitions, and workers' stats and metadata, any of which can hold whatever an application puts there, such as
-- tokens or personal data, kept a week after a failure by default. `pg_monitor` is the role monitoring agents are
-- routinely granted, and it reads no table data anywhere else. The grants below still cover what migration 1 meant it
-- to watch: queue depth, worker leases and cron schedules, which is everything `Queue::counts` reads.

-- A role that holds privileges on these tables without being able to act as their owner may still run GRANT and
-- REVOKE, which then only warn and change nothing, so this migration would be recorded as applied with `pg_monitor`
-- still reading every document. Such a role is refused up front, naming the role to apply it as, and the outcome is
-- checked again at the end. Only what this migration changes is checked: the tables, and default privileges that
-- grant `pg_monitor` SELECT on tables.
DO $$
DECLARE
    required regrole;
BEGIN
    SELECT role INTO required FROM (
        SELECT c.relowner::regrole AS role FROM pg_class c
        WHERE c.relnamespace = 'pgqueue'::regnamespace AND c.relkind = 'r' AND NOT pg_has_role(c.relowner, 'USAGE')
        UNION ALL
        SELECT d.defaclrole::regrole FROM pg_default_acl d
        WHERE d.defaclnamespace = 'pgqueue'::regnamespace AND d.defaclobjtype = 'r'
          AND EXISTS (SELECT 1 FROM aclexplode(d.defaclacl) a WHERE a.grantee = 'pg_monitor'::regrole)
          AND NOT pg_has_role(d.defaclrole, 'USAGE')
    ) AS blocking
    LIMIT 1;
    IF required IS NOT NULL THEN
        RAISE EXCEPTION 'pgqueue migration 2 narrows pg_monitor''s access and must be applied as % or a role with its '
                        'privileges, not as %; run pgqueue-migrate as that role', required, current_user;
    END IF;
END $$;

REVOKE SELECT ON ALL TABLES IN SCHEMA pgqueue FROM pg_monitor;

-- Every role's default privileges, not only those of the role applying this: whoever applied migration 1 granted
-- `pg_monitor` SELECT on tables they would create later, and that need not be the same role.
DO $$
DECLARE
    creator regrole;
BEGIN
    FOR creator IN
        SELECT DISTINCT d.defaclrole::regrole
        FROM pg_default_acl d, aclexplode(d.defaclacl) a
        WHERE d.defaclnamespace = 'pgqueue'::regnamespace AND d.defaclobjtype = 'r'
          AND a.grantee = 'pg_monitor'::regrole
    LOOP
        EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %s IN SCHEMA pgqueue REVOKE SELECT ON TABLES FROM pg_monitor',
                       creator);
    END LOOP;
END $$;

GRANT USAGE ON SCHEMA pgqueue TO pg_monitor;

GRANT SELECT (
    id, queue, name, status, priority, attempts, refunds, max_attempts, timeout_ms, retry_delay_ms, backoff,
    result_ttl_ms, failed_ttl_ms, scheduled_at, enqueued_at, started_at, touched_at, completed_at, expires_at,
    worker_id, kind, cron_expr, retried_at, awaited
) ON pgqueue.jobs TO pg_monitor;

GRANT SELECT (id, queue, started_at, heartbeat_at, expires_at, accepting) ON pgqueue.workers TO pg_monitor;

GRANT SELECT (
    queue, name, expression, revision, misfire_policy, grace_ms, next_run_at, created_at, updated_at
) ON pgqueue.cron_schedules TO pg_monitor;

GRANT SELECT (queue, scheduled_at, expires_at) ON pgqueue.cron_occurrences TO pg_monitor;

GRANT SELECT ON pgqueue.migrations TO pg_monitor;

-- The outcome, checked rather than assumed, on `pg_monitor`'s own grants: access it has through PUBLIC or another role
-- is not this migration's to change.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_class c, aclexplode(c.relacl) a
        WHERE c.relnamespace = 'pgqueue'::regnamespace AND c.relkind = 'r' AND c.relname <> 'migrations'
          AND a.grantee = 'pg_monitor'::regrole
    ) OR EXISTS (
        SELECT 1 FROM pg_attribute att JOIN pg_class c ON c.oid = att.attrelid, aclexplode(att.attacl) a
        WHERE c.relnamespace = 'pgqueue'::regnamespace AND a.grantee = 'pg_monitor'::regrole
          AND (c.relname, att.attname) IN (
              ('jobs', 'payload'), ('jobs', 'result'), ('jobs', 'error'), ('jobs', 'meta'), ('jobs', 'dedupe_key'),
              ('workers', 'stats'), ('workers', 'metadata'), ('cron_schedules', 'definition'),
              ('cron_schedules', 'dedupe_key'), ('cron_occurrences', 'dedupe_key'))
    ) OR EXISTS (
        SELECT 1 FROM pg_default_acl d, aclexplode(d.defaclacl) a
        WHERE d.defaclnamespace = 'pgqueue'::regnamespace AND d.defaclobjtype = 'r'
          AND a.grantee = 'pg_monitor'::regrole
    ) THEN
        RAISE EXCEPTION 'pgqueue migration 2 could not narrow pg_monitor''s access as %; run pgqueue-migrate as the '
                        'owner of the pgqueue schema and its tables', current_user;
    END IF;
END $$;
