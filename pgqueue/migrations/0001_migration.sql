CREATE TABLE pgqueue.jobs (
    id             uuid PRIMARY KEY DEFAULT uuidv7(),
    -- 255 bytes is `MAX_INDEXED_KEY_BYTES`, the bound `JobRequest::validate`
    -- and `validate_queue_name` hold every Rust writer to (a cron name caps at
    -- 250 only because its derived dedupe key is `cron:{name}` — the stored
    -- columns share one limit). The database repeats it because foreign SQL
    -- writers exist by design (see the enqueue-lock fallback in `database.rs`),
    -- and without it the failure was deferred: the dedupe index is partial, so
    -- a terminal row carrying an oversized key landed silently, and the first
    -- `retry_job_occurrence` to copy that key onto a live row raised `54000`
    -- from inside the B-tree — permanently, for that job. `queue` and `name`
    -- sit in full indexes, so their oversized writes at least failed at
    -- insert, but only past the ~2704-byte tuple limit and with the same
    -- internals error. `octet_length`, not `length`: the Rust limits are byte
    -- lengths. `queue` and `name` must also be non-empty, exactly as the
    -- validators require; an empty dedupe key has no Rust-side rule, so it
    -- gets none here.
    dedupe_key     text
        CHECK (dedupe_key IS NULL OR octet_length(dedupe_key) <= 255),
    queue          text NOT NULL CHECK (octet_length(queue) BETWEEN 1 AND 255),
    name           text NOT NULL CHECK (octet_length(name) BETWEEN 1 AND 255),
    payload        jsonb NOT NULL DEFAULT 'null',
    status         text NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'running', 'aborting', 'complete', 'failed', 'aborted')),
    priority       smallint NOT NULL DEFAULT 0,
    attempts       integer NOT NULL DEFAULT 0,
    max_attempts   integer NOT NULL DEFAULT 1,
    -- NULL = no timeout. Zero and negative values have no encoding — the macro
    -- already reads `timeout_ms = 0` as "unlimited" — and `JobRow::timeout`
    -- would decode either as a zero-length deadline, which cancels every
    -- attempt before its handler runs a statement.
    --
    -- The upper bound is `MAX_DURATION_MS`, the same window `validate_duration`
    -- holds every Rust writer to. Unbounded, `pgqueue.job_is_stuck` computed
    -- `timeout_ms + grace_ms` in bigint over *every* active row of a queue, so
    -- one row near `bigint`'s ceiling raised `22003` and took stuck-job
    -- recovery down for that whole queue, permanently.
    timeout_ms     bigint
        CHECK (timeout_ms IS NULL OR timeout_ms BETWEEN 1 AND 3153600000000),
    -- The same window `timeout_ms` and `result_ttl_ms` carry, and for the same
    -- reason. Every Rust reader launders this column today — `retry_delay_for`
    -- clamps at zero and every backoff arm ends in `.min(MAX_DURATION)` — so a
    -- negative or near-`bigint` value is currently reinterpreted rather than
    -- raised. That is exactly the deferred failure the sibling bounds exist to
    -- remove: the value is accepted, silently means something else, and the
    -- first statement to compute `now() + retry_delay_ms * interval
    -- '1 millisecond'` in SQL — which the two batch requeues are one edit away
    -- from — raises `22003`/`22008` for the whole queue instead.
    retry_delay_ms bigint NOT NULL DEFAULT 0
        CHECK (retry_delay_ms BETWEEN 0 AND 3153600000000),
    -- The `JobRetryBackoff` tag. A value this build cannot decode is claimed
    -- and then dropped by `Decode` (see `job.rs`), because the dequeue
    -- statement has already committed by the time the client reads the row;
    -- refusing to store one keeps that fallback for rows that predate this
    -- check or come from a newer version, rather than a way to write new ones.
    -- `COALESCE`, not a bare `IN`: `->>` answers NULL for a missing key and for
    -- every non-object, and a NULL predicate is not FALSE, so `{"delay":5}`
    -- and a bare `null` would both satisfy the check they are written to fail.
    backoff        jsonb NOT NULL DEFAULT '{"type":"none"}'
        CHECK (COALESCE(backoff ->> 'type', '') IN ('none', 'exponential')),
    -- NULL = keep forever, 0 = delete on finish. A negative value has no
    -- encoding, and `JobRetention::from_result_ttl_ms` would decode it as a
    -- live retention rather than an immediate delete. The upper bound is
    -- `MAX_DURATION_MS` for the same reason as `timeout_ms` above: finishing a
    -- row whose retention was near `bigint`'s ceiling raised `22008` from the
    -- `now() + (result_ttl_ms * interval '1 millisecond')` that finish and
    -- abort both compute.
    result_ttl_ms  bigint
        CHECK (result_ttl_ms IS NULL OR result_ttl_ms BETWEEN 0 AND 3153600000000),
    -- Retention for a row that finishes `failed` or `aborted`, with `result_ttl_ms`'s encoding and bounds. It is a
    -- separate clock because the two outcomes are read at different times: a result is collected within moments of
    -- the finish by whoever waited for it, while a failure is investigated by an operator after the fact, often a
    -- night later. One shared retention made the default that suits results (minutes) silently purge every failure
    -- before anyone looked, and `retry_job` then had no row left to retry.
    failed_ttl_ms  bigint
        CHECK (failed_ttl_ms IS NULL OR failed_ttl_ms BETWEEN 0 AND 3153600000000),
    scheduled_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
    enqueued_at    timestamptz NOT NULL DEFAULT clock_timestamp(),
    started_at     timestamptz,
    touched_at     timestamptz,
    completed_at   timestamptz,
    expires_at     timestamptz,
    result         jsonb,
    error          text CHECK (octet_length(error) <= 1048576),
    meta           jsonb NOT NULL DEFAULT '{}',
    worker_id      uuid,
    kind           text NOT NULL DEFAULT 'job' CHECK (kind IN ('job', 'cron')),
    cron_expr      text,
    retried_at     timestamptz,
    -- Whether a caller is waiting on this job's completion channel. A finish
    -- emits its completion `NOTIFY` only for a row that carries this flag:
    -- PostgreSQL serializes every notifying commit cluster-wide behind one lock
    -- (`PreCommit_Notify`), so a notification nobody listens for costs every
    -- other committing writer, not just this one. `enqueue_and_wait` sets it
    -- with the insert; `JobHandle::wait` sets it before subscribing.
    awaited        boolean NOT NULL DEFAULT false,
    -- Bounds for the attempt counters, closing the last numeric column a foreign SQL writer could poison a
    -- queue through (foreign writers exist by design — see the enqueue-lock fallback in `database.rs` — and
    -- the text, duration, and timestamp columns carry their bounds elsewhere in this table). The dequeue claim
    -- computes `attempts + 1` in `integer` over whichever queued rows sort first, so one hand-written row with
    -- `attempts = 2147483647` raised `22003` from inside the claim, rolled back the whole batch, and — sitting
    -- at the front of the dequeue order — was selected again by every retry: all matching work behind it
    -- stopped until the row was repaired by hand.
    --
    -- The bounds mirror what every Rust writer already holds:
    --
    --   * `max_attempts` is 1..=2147483646 — `JobConfig::validate` refuses `i32::MAX`, and the shutdown
    --     requeue's attempt refund saturates at the same ceiling, so a claim of a row at the cap still writes
    --     `attempts + 1 <= 2147483647` without overflowing.
    --   * `attempts` is 0..=`max_attempts`, and strictly below it while the row is `queued`: every organic
    --     path into `queued` (enqueue, retry, refund, manual retry, cron publish) leaves at least one attempt
    --     to spend, which is exactly what makes the claim's increment safe. Terminal and running rows may sit
    --     at the maximum, because their last claim is the one that took them there.
    CONSTRAINT jobs_attempts_range_check CHECK (
        max_attempts BETWEEN 1 AND 2147483646
        AND attempts BETWEEN 0 AND max_attempts
        AND (status <> 'queued' OR attempts < max_attempts)
    ),
    -- The clock stuck-job recovery reads. `pgqueue.job_is_stuck` tests
    -- `started_at` on its timeout trigger and `COALESCE(touched_at, started_at)`
    -- on its liveness one, so an active row carrying neither answers the whole
    -- predicate NULL — and `WHERE NULL` is not TRUE, so the sweeper's scan never
    -- selects the row, phase one never marks it, and phase two never takes it.
    -- It stays `running` for ever, holding its dedupe key — which silently
    -- deduplicates every re-enqueue and every cron occurrence under it, so a
    -- schedule keyed on `cron:{name}` simply stops — with nothing on
    -- `WorkerComponent::Sweeper` to say so.
    --
    -- `started_at` has no default, so an ops script, a restore, or a
    -- half-finished backfill that writes a `running` row without naming the
    -- column lands exactly that. Every Rust writer already sets the pair with
    -- the status: the dequeue claim stamps `started_at` and `touched_at` in the
    -- statement that writes `running`, and every requeue clears `started_at` in
    -- the statement that writes `queued`. So this binds only the foreign SQL
    -- writers the rest of this table's checks exist for, and it fails at the
    -- write rather than at the recovery.
    --
    -- `Queue::abort_job` remains the repair for a row that predates this check:
    -- the abort stamps `touched_at`, which is the clock the second trigger
    -- reads, so the next sweep finishes the row and releases its key.
    CONSTRAINT jobs_active_started_at_check CHECK (
        status NOT IN ('running', 'aborting') OR started_at IS NOT NULL
    ),
    -- PostgreSQL accepts infinity and instants after Jiff's maximum.
    CONSTRAINT jobs_timestamps_jiff_range_check CHECK (
        isfinite(scheduled_at) AND scheduled_at < TIMESTAMPTZ '9999-12-30 22:00:01+00'
        AND isfinite(enqueued_at) AND enqueued_at < TIMESTAMPTZ '9999-12-30 22:00:01+00'
        AND (started_at IS NULL OR
             (isfinite(started_at) AND started_at < TIMESTAMPTZ '9999-12-30 22:00:01+00'))
        AND (touched_at IS NULL OR
             (isfinite(touched_at) AND touched_at < TIMESTAMPTZ '9999-12-30 22:00:01+00'))
        AND (completed_at IS NULL OR
             (isfinite(completed_at) AND completed_at < TIMESTAMPTZ '9999-12-30 22:00:01+00'))
        AND (expires_at IS NULL OR
             (isfinite(expires_at) AND expires_at < TIMESTAMPTZ '9999-12-30 22:00:01+00'))
        AND (retried_at IS NULL OR
             (isfinite(retried_at) AND retried_at < TIMESTAMPTZ '9999-12-30 22:00:01+00'))
    )
);

CREATE UNIQUE INDEX jobs_dedupe_key_idx ON pgqueue.jobs (queue, dedupe_key)
    WHERE dedupe_key IS NOT NULL AND status IN ('queued', 'running', 'aborting');
-- The only dequeue index, and deliberately so: with no name-leading dequeue
-- index, this ordered walk is the only *index-ordered* access path for the
-- claim, so a prepared statement settling into the generic plan has no rival
-- index to defect to. A sequential scan plus sort always exists in principle;
-- the claim's plan-shape test is what pins the planner to the walk. Workers
-- do not filter by job name — a worker handles every name enqueued on its
-- queue.
CREATE INDEX jobs_dequeue_idx ON pgqueue.jobs (queue, priority, scheduled_at, id)
    WHERE status = 'queued';
CREATE INDEX jobs_expires_idx ON pgqueue.jobs (queue, expires_at, id)
    WHERE expires_at IS NOT NULL;
CREATE INDEX jobs_page_idx ON pgqueue.jobs (queue, enqueued_at DESC, id DESC);
CREATE INDEX jobs_dashboard_status_page_idx ON pgqueue.jobs
    (queue, kind, status, enqueued_at DESC, id DESC);
CREATE INDEX jobs_dashboard_name_page_idx ON pgqueue.jobs
    (queue, kind, name, status, enqueued_at DESC, id DESC);
-- The job-name typeahead uses a loose index scan over case-folded names.
-- `text_pattern_ops` provides the `~>=~`/`~>~` comparison operators and `~<~`
-- ordering that let those parameterized prefix bounds remain index conditions
-- under a generic prepared-statement plan.
CREATE INDEX jobs_dashboard_name_prefix_idx ON pgqueue.jobs
    (queue, kind, status, lower(name) text_pattern_ops, enqueued_at DESC, id DESC);
CREATE INDEX jobs_dashboard_ready_idx ON pgqueue.jobs (queue, scheduled_at, id)
    WHERE status = 'queued';
CREATE INDEX jobs_dashboard_terminal_idx ON pgqueue.jobs
    (queue, status, completed_at DESC, id DESC) WHERE status IN ('failed', 'aborted');
-- Active jobs: the sweeper scans them per queue, the dashboard probes single
-- statuses. Both fit one partial index.
CREATE INDEX jobs_active_idx ON pgqueue.jobs (queue, status)
    WHERE status IN ('running', 'aborting');

-- Cron occurrence identity outlives the job row so result retention (including
-- immediate deletion) cannot make a completed or aborted occurrence eligible
-- for enqueue again. Claims only need to survive the scheduler's maximum
-- backfill grace, then the sweeper removes them.
-- The indexed text columns carry `pgqueue.jobs`'s bounds, for the reason that
-- table states them: foreign SQL writers exist by design, and `(queue,
-- dedupe_key, scheduled_at)` is this table's primary key, so an oversized pair
-- fails from inside the B-tree with an opaque `54000` — here on the *scheduler's*
-- insert rather than on the operator's, taking that cron down.
--
-- Exactly `pgqueue.jobs`'s bounds, including where they are *loose*:
-- `dedupe_key` has an upper bound and no lower one, because no Rust writer
-- refuses an empty key. `JobBuilder::dedupe_key("")` on a cron template is a
-- degenerate configuration, but it is one this crate accepts on `jobs`, and a
-- constraint only this table carried would fail it here as an `Error::Db` the
-- scheduler retries for ever — a permanently degraded worker where the same key
-- on a plain job simply works.
CREATE TABLE pgqueue.cron_occurrences (
    queue        text NOT NULL CHECK (octet_length(queue) BETWEEN 1 AND 255),
    dedupe_key   text NOT NULL CHECK (octet_length(dedupe_key) <= 255),
    scheduled_at timestamptz NOT NULL,
    expires_at   timestamptz NOT NULL,
    CONSTRAINT cron_occurrences_timestamps_jiff_range_check CHECK (
        isfinite(scheduled_at) AND scheduled_at < TIMESTAMPTZ '9999-12-30 22:00:01+00'
        AND isfinite(expires_at) AND expires_at < TIMESTAMPTZ '9999-12-30 22:00:01+00'
    ),
    PRIMARY KEY (queue, dedupe_key, scheduled_at)
);

CREATE INDEX cron_occurrences_expiry_idx
    ON pgqueue.cron_occurrences (queue, expires_at);

-- `queue`, `dedupe_key` and `name` are bounded for the reason given above
-- `pgqueue.cron_occurrences`; `(queue, dedupe_key)` is this table's primary key
-- too. `expression` is not indexed, so it needs no bound of its own.
CREATE TABLE pgqueue.cron_schedules (
    queue          text NOT NULL CHECK (octet_length(queue) BETWEEN 1 AND 255),
    dedupe_key     text NOT NULL CHECK (octet_length(dedupe_key) <= 255),
    name           text NOT NULL CHECK (octet_length(name) BETWEEN 1 AND 255),
    expression     text NOT NULL,
    definition     jsonb NOT NULL,
    revision       bigint NOT NULL CHECK (revision >= 0),
    misfire_policy text NOT NULL CHECK (misfire_policy IN ('skip', 'fire_once')),
    grace_ms       bigint CHECK (grace_ms IS NULL OR grace_ms >= 0),
    next_run_at    timestamptz NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT cron_schedules_timestamps_jiff_range_check CHECK (
        isfinite(next_run_at) AND next_run_at < TIMESTAMPTZ '9999-12-30 22:00:01+00'
        AND isfinite(created_at) AND created_at < TIMESTAMPTZ '9999-12-30 22:00:01+00'
        AND isfinite(updated_at) AND updated_at < TIMESTAMPTZ '9999-12-30 22:00:01+00'
    ),
    PRIMARY KEY (queue, dedupe_key)
);

-- No due-time index: every access to this table is by its primary key
-- `(queue, dedupe_key)`, and the `next_run_at <= now()` test is a filter on an
-- already-located row. An index on `next_run_at` would only add write cost to
-- the two statements that advance it on every tick.

CREATE TABLE pgqueue.workers (
    id           uuid PRIMARY KEY,
    -- Bounded like every other indexed queue name: this one sits in
    -- `workers_queue_idx`, with the same exposure to a foreign SQL writer.
    queue        text NOT NULL CHECK (octet_length(queue) BETWEEN 1 AND 255),
    stats        jsonb NOT NULL DEFAULT '{}',
    metadata     jsonb,
    started_at   timestamptz NOT NULL DEFAULT now(),
    heartbeat_at timestamptz NOT NULL DEFAULT now(),
    expires_at   timestamptz NOT NULL,
    accepting    boolean NOT NULL DEFAULT true,
    CONSTRAINT workers_timestamps_jiff_range_check CHECK (
        isfinite(started_at) AND started_at < TIMESTAMPTZ '9999-12-30 22:00:01+00'
        AND isfinite(heartbeat_at) AND heartbeat_at < TIMESTAMPTZ '9999-12-30 22:00:01+00'
        AND isfinite(expires_at) AND expires_at < TIMESTAMPTZ '9999-12-30 22:00:01+00'
    )
);

CREATE INDEX workers_queue_idx ON pgqueue.workers (queue, expires_at, id);
CREATE INDEX workers_dashboard_page_idx ON pgqueue.workers (queue, started_at, id);

-- Two independent, *additive* reasons an attempt is recoverable:
--
--   1. Its configured timeout elapsed. This bounds an attempt even while its
--      worker is alive and healthy.
--   2. Its owner is provably gone — the `pgqueue.workers` lease that covered
--      the attempt lapsed, and has stayed lapsed for the liveness grace.
--
-- Gating (2) on the absence of a timeout made the two mutually exclusive, so
-- setting `timeout_ms` *weakened* crash recovery: a SIGKILLed worker's hour-long
-- attempt stayed `running` for the full hour, holding its dedupe key (which
-- silently deduplicates every re-enqueue and cron occurrence) and leaving
-- `abort_job` stranded in `aborting` with nobody alive to finish it.
--
-- The grace in (2) is measured from the *lease*, not from the attempt. Both
-- clocks are needed: `COALESCE(touched_at, started_at)` is the only one a
-- leaseless consumer has, but on its own it made a long attempt sweepable the
-- instant its owner missed one heartbeat window — a workers-row lock wait, a
-- pool stall, a GC pause or a failover was enough to cancel and re-run work
-- that was still in flight, where before it had the whole `timeout + grace`.
-- Waiting `grace_ms` past the lease's `expires_at` gives a stalled heartbeat the
-- same cushion a slow finish gets. `expires_at > now()` implies
-- `expires_at + grace_ms > now()` for a non-negative grace, so this single
-- predicate still carries the "no live lease" requirement; `sweep_grace` is
-- validated non-negative before it ever reaches here.
--
-- Worker rows are purged on the same grace (see `Sweeper::purge_worker_leases`)
-- so a lease that has just lapsed is still on disk to be seen — a deleted row
-- is indistinguishable from one that lapsed an hour ago.
--
-- This opens no double-execution hole: every caller re-checks for a live lease
-- and guards on `attempts`/`worker_id`, so a resurrected owner's `finish` is a
-- no-op.
--
-- The lease is a *parameter*, not a lookup inside the body. `inline_function`
-- refuses to inline any SQL function whose body has a sublink, and this one is
-- applied to every `running`/`aborting` row of a queue by
-- `Sweeper::recover_stuck_jobs` — unbounded by the sweep batch size, and with an
-- `ORDER BY` that forbids an early exit. As an opaque call it built the whole
-- `pgqueue.jobs` tuple as a composite datum per row and re-ran the lease lookup
-- per row: measured over 20,000 active rows and 50 leases at 180 ms / 20,484
-- buffers, against 6.6 ms / 460 for the same predicate inlined over a hashed
-- `LEFT JOIN`. `pgqueue.workers.id` is the primary key, so that join yields at
-- most one row and a NULL `lease_expires_at` is exactly `NOT EXISTS`.
--
-- `COALESCE(lease_expires_at, '-infinity')` rather than `IS NULL OR ...` so the
-- parameter is used once: `inline_function` declines when a parameter used more
-- than once is passed an expensive argument, and the callers that are already
-- keyed by id pass a correlated subquery (an UPDATE's target table cannot be
-- referenced from a join in its own FROM clause).
CREATE FUNCTION pgqueue.job_is_stuck(
    j                pgqueue.jobs,
    grace_ms         bigint,
    lease_expires_at timestamptz
)
RETURNS boolean
LANGUAGE sql
STABLE
AS $$
    SELECT (j.timeout_ms IS NOT NULL
            AND j.started_at + ((j.timeout_ms + grace_ms) * interval '1 millisecond') < now())
        OR (COALESCE(j.touched_at, j.started_at)
                + (grace_ms * interval '1 millisecond') < now()
            AND COALESCE(lease_expires_at, '-infinity')
                + (grace_ms * interval '1 millisecond') <= now())
$$;

-- The job listing's keyset pagination, newest first, in two functions rather
-- than one: per-status laterals riding `jobs_dashboard_status_page_idx` here,
-- and `jobs_dashboard_name_page_idx` in the `_by_name` variant below. Inlined by
-- the planner, so the caller keeps the index scan.
--
-- Two functions because one cannot serve both. A single
-- `(p_name IS NULL OR j.name = p_name)` is not an equality the planner can turn
-- into an index condition unless it folds the parameter into a constant, which
-- a generic plan — the plan sqlx's prepared statements settle into — never
-- does. Measured on PostgreSQL 18.4 over 350,000 retained rows, the dashboard's
-- six-status name-filtered page under `force_generic_plan`:
-- `Index Scan using jobs_dashboard_status_page_idx ...
-- Filter: (($4 IS NULL) OR (name = $4)) ... Rows Removed by Filter: 35556`,
-- 29,422 buffers and 105 ms, growing linearly with retention and unbounded under
-- `JobRetention::Forever` — against the pool the worker dequeues and finalizes
-- with. Split, the same page is an
-- `Index Only Scan using jobs_dashboard_name_page_idx` with the name in the
-- Index Cond: 374 buffers and 0.9 ms. Under `plan_cache_mode = auto` the custom
-- plan happened to cost less and held the fast path off the floor; an operator
-- setting `force_generic_plan` — a common cure for planning overhead — lost it.
--
-- `p_queue` and `p_kind` select the partition to page through and `p_statuses`
-- names the statuses to union, so all three are required: a NULL `p_statuses`
-- drives `unnest` to zero rows rather than skipping the filter. The cursor pair
-- is the optional one — pass NULL to skip it. `p_limit` bounds each status's
-- lateral; the caller re-applies it to the union.
CREATE FUNCTION pgqueue.job_page_keys(
    p_queue     text,
    p_kind      text,
    p_statuses  text[],
    p_cursor_at timestamptz,
    p_cursor_id uuid,
    p_limit     bigint
)
RETURNS TABLE (enqueued_at timestamptz, id uuid)
LANGUAGE sql
STABLE
AS $$
    SELECT candidate.enqueued_at, candidate.id
    FROM unnest(p_statuses) AS requested(status)
    CROSS JOIN LATERAL (
        SELECT j.enqueued_at, j.id
        FROM pgqueue.jobs j
        WHERE j.queue = p_queue
          AND j.kind = p_kind
          AND j.status = requested.status
          AND (p_cursor_at IS NULL OR (j.enqueued_at, j.id) < (p_cursor_at, p_cursor_id))
        ORDER BY j.enqueued_at DESC, j.id DESC
        LIMIT p_limit
    ) candidate
$$;

-- The same page with the listing's `?name=` filter applied. `p_name` is
-- required and its comparison is unconditional, which is the whole point: it is
-- then an index condition on `jobs_dashboard_name_page_idx` under a generic plan
-- rather than a filter over every row the status index already had to read.
CREATE FUNCTION pgqueue.job_page_keys_by_name(
    p_queue     text,
    p_kind      text,
    p_statuses  text[],
    p_name      text,
    p_cursor_at timestamptz,
    p_cursor_id uuid,
    p_limit     bigint
)
RETURNS TABLE (enqueued_at timestamptz, id uuid)
LANGUAGE sql
STABLE
AS $$
    SELECT candidate.enqueued_at, candidate.id
    FROM unnest(p_statuses) AS requested(status)
    CROSS JOIN LATERAL (
        SELECT j.enqueued_at, j.id
        FROM pgqueue.jobs j
        WHERE j.queue = p_queue
          AND j.kind = p_kind
          AND j.name = p_name
          AND j.status = requested.status
          AND (p_cursor_at IS NULL OR (j.enqueued_at, j.id) < (p_cursor_at, p_cursor_id))
        ORDER BY j.enqueued_at DESC, j.id DESC
        LIMIT p_limit
    ) candidate
$$;

-- Read access for the built-in monitoring role, so an operator's metrics
-- exporter can watch queue depth, worker leases and cron schedules without a
-- custom grant per deployment. `pg_monitor` is the role PostgreSQL ships for
-- exactly this, and it carries no write privilege here. The default-privileges
-- grant covers tables a later migration creates under the same owner.
GRANT USAGE ON SCHEMA pgqueue TO pg_monitor;
GRANT SELECT ON ALL TABLES IN SCHEMA pgqueue TO pg_monitor;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgqueue GRANT SELECT ON TABLES TO pg_monitor;
