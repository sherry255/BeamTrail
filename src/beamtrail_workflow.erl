-module(beamtrail_workflow).

-export_type([run_id/0, step_id/0, step_version/0]).

-type run_id() :: binary().
-type step_id() :: atom().
-type step_version() :: non_neg_integer().

-callback steps(Input :: term()) -> [step_id()].
-callback step_version(StepId :: step_id()) -> step_version().
-callback retry_policy(StepId :: step_id()) ->
    #{max_attempts := pos_integer(),
      backoff_ms := non_neg_integer(),
      retryable_errors := [term()]}.
-callback timeout_ms(StepId :: step_id()) -> timeout().
-callback idempotency_key(RunId :: run_id(), StepId :: step_id(), Input :: term()) -> term().
-callback execute(StepId :: step_id(), StepVersion :: step_version(), Input :: term(), Context :: map()) ->
    {ok, term()} | {error, term()}.
