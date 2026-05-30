CREATE TABLE IF NOT EXISTS workflow_events (
  run_id bytea NOT NULL,
  event_seq bigint NOT NULL,
  event_type text NOT NULL,
  step_id text,
  step_version integer,
  idempotency_key text,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  occurred_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, event_seq)
);

CREATE INDEX IF NOT EXISTS workflow_events_type_idx
  ON workflow_events (event_type, occurred_at);

CREATE TABLE IF NOT EXISTS workflow_snapshots (
  run_id bytea PRIMARY KEY,
  snapshot_seq bigint NOT NULL,
  snapshot_revision integer NOT NULL,
  state jsonb NOT NULL,
  written_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS workflow_leases (
  run_id bytea PRIMARY KEY,
  owner_node text NOT NULL,
  lease_until timestamptz NOT NULL,
  fencing_token bigint NOT NULL,
  scanner_cursor bigint NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS workflow_instances (
  run_id bytea PRIMARY KEY,
  workflow text NOT NULL,
  status text NOT NULL,
  current_step text,
  last_event_seq bigint NOT NULL,
  next_retry_at timestamptz,
  failure jsonb,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS step_attempts (
  run_id bytea NOT NULL,
  step_id text NOT NULL,
  attempt integer NOT NULL,
  step_version integer NOT NULL,
  idempotency_key text NOT NULL,
  status text NOT NULL,
  started_event_seq bigint NOT NULL,
  completed_event_seq bigint,
  reason jsonb,
  result jsonb,
  PRIMARY KEY (run_id, step_id, attempt)
);
