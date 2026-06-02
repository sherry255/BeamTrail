CREATE TABLE IF NOT EXISTS workflow_events (
  run_id bytea NOT NULL,
  event_seq bigint NOT NULL,
  event_type text NOT NULL,
  step_id text,
  step_version integer,
  idempotency_key bytea,
  payload bytea NOT NULL,
  fencing_token bigint,
  occurred_at_ms bigint NOT NULL,
  PRIMARY KEY (run_id, event_seq)
);

CREATE INDEX IF NOT EXISTS workflow_events_type_idx
  ON workflow_events (event_type, occurred_at_ms);

CREATE TABLE IF NOT EXISTS workflow_runs (
  run_id bytea PRIMARY KEY,
  created_at_ms bigint NOT NULL,
  updated_at_ms bigint NOT NULL,
  status text NOT NULL DEFAULT 'running',
  terminal boolean NOT NULL DEFAULT false,
  next_retry_at_ms bigint
);

-- Recovery-scan projection columns. The event log stays authoritative; these
-- are a derived index so scans avoid replaying every historical run.
-- Defaults over-approximate (running / non-terminal) so a run is never silently
-- excluded from recovery before its projection is written. Existing deployments
-- pick the columns up via ADD COLUMN IF NOT EXISTS; legacy terminal rows keep
-- the default until normalized by beamtrail_postgres_storage:backfill_run_projections/0.
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'running';
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS terminal boolean NOT NULL DEFAULT false;
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS next_retry_at_ms bigint;

INSERT INTO workflow_runs (run_id, created_at_ms, updated_at_ms)
SELECT run_id, MIN(occurred_at_ms), MAX(occurred_at_ms)
FROM workflow_events
GROUP BY run_id
ON CONFLICT (run_id) DO NOTHING;

-- Partial index supporting the recovery scan: keyset pagination over run_id
-- among non-terminal runs only.
CREATE INDEX IF NOT EXISTS workflow_runs_recoverable_idx
  ON workflow_runs (run_id) WHERE terminal = false;

CREATE TABLE IF NOT EXISTS workflow_snapshots (
  run_id bytea PRIMARY KEY,
  snapshot_seq bigint NOT NULL,
  snapshot_revision integer NOT NULL,
  state bytea NOT NULL,
  written_at_ms bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS workflow_leases (
  run_id bytea PRIMARY KEY,
  owner_node bytea NOT NULL,
  lease_until_ms bigint NOT NULL,
  fencing_token bigint NOT NULL,
  acquired_at_ms bigint NOT NULL,
  updated_at_ms bigint NOT NULL
);

-- Supports the recovery scan's lease-expiry join (lease_until_ms <= now).
CREATE INDEX IF NOT EXISTS workflow_leases_until_idx
  ON workflow_leases (lease_until_ms);
