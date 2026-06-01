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
  updated_at_ms bigint NOT NULL
);

INSERT INTO workflow_runs (run_id, created_at_ms, updated_at_ms)
SELECT run_id, MIN(occurred_at_ms), MAX(occurred_at_ms)
FROM workflow_events
GROUP BY run_id
ON CONFLICT (run_id) DO NOTHING;

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
