-module(beamtrail_decider).

-export([legacy_decide/1]).

legacy_decide(State) ->
    case maps:get(current_step, State, undefined) of
        undefined ->
            complete;
        StepId ->
            {run_step, StepId, maps:get(input, State)}
    end.
