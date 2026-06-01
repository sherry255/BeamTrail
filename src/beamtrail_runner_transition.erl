-module(beamtrail_runner_transition).

%% Boundary used by beamtrail_run. The runner owns process lifetime and timers;
%% durable event transitions are still delegated to beamtrail until the shared
%% dispatch path is split further.

-export([load_state/1, next_action/2, next_action/3,
         execute_attempt/1, finish_attempt/4, finish_attempt/5]).

load_state(RunId) ->
    beamtrail:load_runner_state(RunId).

next_action(RunId, Lease) ->
    beamtrail:next_runner_action(RunId, Lease).

next_action(RunId, Lease, State) ->
    beamtrail:next_runner_action(RunId, Lease, State).

execute_attempt(ExecSpec) when is_map(ExecSpec) ->
    Workflow = maps:get(workflow, ExecSpec),
    safe_execute(Workflow,
                 maps:get(step_id, ExecSpec),
                 maps:get(step_version, ExecSpec),
                 maps:get(input, ExecSpec),
                 maps:get(context, ExecSpec)).

finish_attempt(RunId, Lease, Attempt, Result) ->
    beamtrail:finish_runner_attempt(RunId, Lease, Attempt, Result).

finish_attempt(RunId, Lease, Attempt, Result, State) ->
    beamtrail:finish_runner_attempt(RunId, Lease, Attempt, Result, State).

safe_execute(Workflow, StepId, StepVersion, Input, Context) ->
    try Workflow:execute(StepId, StepVersion, Input, Context) of
        {ok, _Value} = Ok -> Ok;
        {error, _Reason} = Error -> Error;
        Other -> {error, {bad_return, Other}}
    catch
        Class:Reason:_Stacktrace ->
            {error, #{class => Class, reason => Reason}}
    end.
