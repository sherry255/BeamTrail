-module(beamtrail_decider).

-export([decide/1, legacy_decide/1]).

decide(State) ->
    case maps:get(decider, State, legacy) of
        module ->
            Workflow = maps:get(workflow, State),
            decide_with_workflow(State, Workflow);
        legacy ->
            {ok, legacy_decide(State)};
        _Other ->
            {ok, legacy_decide(State)}
    end.

legacy_decide(State) ->
    case maps:get(current_step, State, undefined) of
        undefined ->
            complete;
        StepId ->
            {run_step, StepId, maps:get(input, State)}
    end.

decide_with_workflow(State, Workflow) ->
    View = decision_view(State),
    try Workflow:decide(View) of
        Command ->
            validate_command(Command, State)
    catch
        Class:Reason:_Stacktrace ->
            {error, #{reason => bad_workflow_callback,
                      callback => decide,
                      callback_error => #{callback => decide,
                                          class => Class,
                                          reason => Reason}}}
    end.

decision_view(State) ->
    #{run_id => maps:get(run_id, State),
      workflow => maps:get(workflow, State),
      input => maps:get(input, State),
      steps => maps:get(steps, State, []),
      status => maps:get(status, State),
      current_step => maps:get(current_step, State, undefined),
      results => maps:get(results, State, []),
      workflow_result => maps:get(workflow_result, State, undefined),
      attempts => maps:get(attempts, State, []),
      failure => maps:get(failure, State, undefined)}.

validate_command(complete, _State) ->
    {ok, complete};
validate_command({complete, _Result} = Command, _State) ->
    {ok, Command};
validate_command({fail, _Reason} = Command, _State) ->
    {ok, Command};
validate_command({run_step, StepId}, State) ->
    validate_run_step({run_step, StepId, maps:get(input, State)}, State);
validate_command({run_step, _StepId, _StepInput} = Command, State) ->
    validate_run_step(Command, State);
validate_command(Command, _State) ->
    invalid_command(Command).

validate_run_step({run_step, StepId, _StepInput} = Command, State) ->
    case lists:member(StepId, maps:get(steps, State, [])) of
        true -> {ok, Command};
        false -> invalid_command(Command)
    end.

invalid_command(Command) ->
    {error, #{reason => invalid_decider_command, command => Command}}.
