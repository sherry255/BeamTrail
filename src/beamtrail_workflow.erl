-module(beamtrail_workflow).

-export_type([run_id/0, step_id/0, step_version/0,
              retry_policy/0, execution_context/0]).

-type run_id() :: binary().
-type step_id() :: atom().
-type step_version() :: non_neg_integer().
-type retry_policy() ::
    #{max_attempts := pos_integer(),
      backoff_ms := non_neg_integer(),
      retryable_errors := [atom()]}.
-type execution_context() ::
    #{run_id := run_id(),
      step_id := step_id(),
      step_version := step_version(),
      attempt := pos_integer(),
      idempotency_key := term()}.

-callback steps(Input :: term()) -> [step_id()].
-callback step_version(StepId :: step_id()) -> step_version().
-callback retry_policy(StepId :: step_id()) -> retry_policy().
-callback timeout_ms(StepId :: step_id()) -> timeout().
-callback idempotency_key(RunId :: run_id(), StepId :: step_id(), Input :: term()) -> term().
-callback execute(StepId :: step_id(), StepVersion :: step_version(), Input :: term(),
                  Context :: execution_context()) ->
    {ok, term()} | {error, term()}.

%% Optional. Total wall-clock budget for the whole workflow instance,
%% measured from workflow.instance.created.occurred_at. Defaults to
%% infinity (no workflow-level timeout). When the runtime dispatches and
%% finds the budget exceeded, it emits workflow.failed with reason
%% workflow_timeout instead of running the next step.
-optional_callbacks([workflow_timeout_ms/0]).
-callback workflow_timeout_ms() -> timeout().
