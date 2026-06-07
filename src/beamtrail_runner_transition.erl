-module(beamtrail_runner_transition).

%% Boundary used by beamtrail_run. The runner owns process lifetime and timers;
%% durable event transitions live in beamtrail_transition.

-export([load_state/1, next_action/2, next_action/3,
         finish_attempt/4, finish_attempt/5,
         complete_effect/4, complete_effect/5]).

load_state(RunId) ->
    ok = beamtrail_config:ensure_storage(),
    beamtrail_state:load(RunId, beamtrail_config:storage()).

next_action(RunId, Lease) ->
    case load_state(RunId) of
        {ok, State} ->
            case next_action(RunId, Lease, State) of
                {ok, {execute, Attempt, Effect, _State1}} ->
                    {ok, {execute, Attempt, Effect}};
                Other ->
                    Other
            end;
        {error, _} = Error ->
            Error
    end.

next_action(RunId, Lease, State) ->
    beamtrail_transition:dispatch_locked(RunId, State, Lease,
                                         #{runner_mode => prepare,
                                           runner_state => State}).

finish_attempt(RunId, Lease, Attempt, Result) ->
    complete_effect(RunId, Lease, Attempt, Result).

finish_attempt(RunId, Lease, Attempt, Result, State) ->
    complete_effect(RunId, Lease, Attempt, Result, State).

complete_effect(RunId, Lease, Attempt, Result) ->
    beamtrail_transition:complete_effect(RunId, Lease, Attempt, Result).

complete_effect(RunId, Lease, Attempt, Result, State) ->
    beamtrail_transition:complete_effect(RunId, Lease, Attempt, Result, State).
