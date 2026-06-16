-module(beamtrail_external_worker).

-export([run_once/2, run_once/3]).

%% Minimal in-process external-effect worker loop.
%%
%% run_once/2 claims one visible external effect, passes the claimed effect map
%% to the handler, and completes it with the returned {ok, Result} or
%% {error, Reason}. Handler exceptions are intentionally allowed to escape so
%% the claim can expire and the effect can be retried by another worker.

-define(RETRY_SLEEP_MS, 50).
-define(DEFAULT_RETRY_MS, 5000).

-type owner() :: term().
-type effect() :: map().
-type worker_result() :: {ok, term()} | {error, term()}.
-type handler() :: fun((effect()) -> worker_result()).
-type options() :: map().
-type run_once_result() ::
    {ok, idle}
    | {ok, #{run_id := beamtrail:run_id(),
             effect_id := term(),
             state := beamtrail:state()}}
    | {error, term()}.

-spec run_once(owner(), handler()) -> run_once_result().
run_once(Owner, Handler) when is_function(Handler, 1) ->
    run_once(Owner, Handler, #{}).

-spec run_once(owner(), handler(), options()) -> run_once_result().
run_once(Owner, Handler, Options) when is_function(Handler, 1), is_map(Options) ->
    case beamtrail:list_pending_effects() of
        {ok, Effects} ->
            run_first_visible(Effects, Owner, Handler, Options);
        {error, _} = Error ->
            Error
    end.

run_first_visible([], _Owner, _Handler, _Options) ->
    {ok, idle};
run_first_visible([Effect | Rest], Owner, Handler, Options) ->
    case effect_matches(Effect, Options) of
        false ->
            run_first_visible(Rest, Owner, Handler, Options);
        true ->
            RunId = maps:get(run_id, Effect),
            EffectId = maps:get(effect_id, Effect),
            case validate_effect_options(Effect, Options) of
                ok ->
                    case claim_with_retry(RunId, EffectId, Owner,
                                          ?DEFAULT_RETRY_MS) of
                        {ok, Claim} ->
                            ClaimedEffect = maps:merge(Effect, Claim),
                            complete_claimed_effect(RunId, EffectId, Handler,
                                                    ClaimedEffect, Options);
                        {skip, _Reason} ->
                            run_first_visible(Rest, Owner, Handler, Options);
                        {error, _} = Error ->
                            Error
                    end;
                {error, _} = Error ->
                    Error
            end
    end.

effect_matches(Effect, Options) ->
    matches_value_list(maps:get(effect_type, Effect), maps:get(effect_types, Options, all))
        andalso
    matches_value_list(maps:get(step_id, Effect, undefined), maps:get(step_ids, Options, all)).

matches_value_list(_Value, all) ->
    true;
matches_value_list(Value, Values) when is_list(Values) ->
    lists:member(Value, Values).

validate_effect_options(Effect, #{renew_claim := true} = Options) ->
    case maps:get(claim_renew_interval_ms, Options, undefined) of
        undefined ->
            ok;
        Ms when is_integer(Ms), Ms > 0 ->
            validate_claim_renew_interval(Ms, maps:get(visibility_timeout_ms,
                                                       Effect, undefined));
        _ ->
            {error, {bad_worker_option, claim_renew_interval_ms}}
    end;
validate_effect_options(_Effect, _Options) ->
    ok.

validate_claim_renew_interval(_Ms, infinity) ->
    ok;
validate_claim_renew_interval(Ms, VisibilityTimeoutMs)
  when is_integer(VisibilityTimeoutMs), Ms < VisibilityTimeoutMs ->
    ok;
validate_claim_renew_interval(_Ms, _VisibilityTimeoutMs) ->
    {error, {bad_worker_option, claim_renew_interval_ms}}.

complete_claimed_effect(RunId, EffectId, Handler, ClaimedEffect, Options) ->
    ClaimToken = maps:get(claim_token, ClaimedEffect),
    RenewPid = maybe_start_claim_renewer(RunId, EffectId, ClaimToken,
                                         ClaimedEffect, Options),
    try run_handler(Handler, ClaimedEffect) of
        {ok, _} = Result ->
            complete_with_retry(RunId, EffectId, ClaimToken, Result,
                                ?DEFAULT_RETRY_MS);
        {error, _} = Result ->
            complete_with_retry(RunId, EffectId, ClaimToken, Result,
                                ?DEFAULT_RETRY_MS)
    after
        stop_claim_renewer(RenewPid)
    end.

run_handler(Handler, Effect) ->
    case Handler(Effect) of
        {ok, _} = Ok ->
            Ok;
        {error, _} = Error ->
            Error;
        Other ->
            error({bad_worker_result, Other})
    end.

maybe_start_claim_renewer(_RunId, _EffectId, _ClaimToken, _ClaimedEffect,
                          #{renew_claim := false}) ->
    undefined;
maybe_start_claim_renewer(RunId, EffectId, ClaimToken, ClaimedEffect,
                          #{renew_claim := true} = Options) ->
    IntervalMs = claim_renew_interval_ms(ClaimedEffect, Options),
    Parent = self(),
    spawn_link(
      fun() ->
              claim_renew_loop(Parent, RunId, EffectId, ClaimToken, IntervalMs)
      end);
maybe_start_claim_renewer(_RunId, _EffectId, _ClaimToken, _ClaimedEffect,
                          _Options) ->
    undefined.

claim_renew_interval_ms(_ClaimedEffect, #{claim_renew_interval_ms := Ms})
  when is_integer(Ms), Ms > 0 ->
    Ms;
claim_renew_interval_ms(#{visibility_timeout_ms := Ms}, _Options)
  when is_integer(Ms), Ms > 2 ->
    max(1, Ms div 3);
claim_renew_interval_ms(_ClaimedEffect, _Options) ->
    5000.

claim_renew_loop(Parent, RunId, EffectId, ClaimToken, IntervalMs) ->
    receive
        stop ->
            ok
    after IntervalMs ->
        case beamtrail:renew_effect_claim(RunId, EffectId, ClaimToken) of
            {ok, ignored} ->
                Parent ! {beamtrail_external_worker_claim_renew_stopped,
                          EffectId, ignored},
                ok;
            {ok, _Claim} ->
                claim_renew_loop(Parent, RunId, EffectId, ClaimToken, IntervalMs);
            {error, Reason} ->
                Parent ! {beamtrail_external_worker_claim_renew_failed,
                          EffectId, Reason},
                ok
        end
    end.

stop_claim_renewer(undefined) ->
    ok;
stop_claim_renewer(Pid) when is_pid(Pid) ->
    Pid ! stop,
    drain_claim_renewer_messages().

drain_claim_renewer_messages() ->
    receive
        {beamtrail_external_worker_claim_renew_stopped, _EffectId, _Reason} ->
            drain_claim_renewer_messages();
        {beamtrail_external_worker_claim_renew_failed, _EffectId, _Reason} ->
            drain_claim_renewer_messages()
    after 0 ->
        ok
    end.

claim_with_retry(_RunId, _EffectId, _Owner, RemainingMs)
  when RemainingMs =< 0 ->
    {error, claim_timeout};
claim_with_retry(RunId, EffectId, Owner, RemainingMs) ->
    case beamtrail:claim_effect(RunId, EffectId, Owner) of
        {ok, #{claim_token := _}} = Ok ->
            Ok;
        {ok, ignored} ->
            {skip, ignored};
        {error, leased} ->
            timer:sleep(?RETRY_SLEEP_MS),
            claim_with_retry(RunId, EffectId, Owner,
                             RemainingMs - ?RETRY_SLEEP_MS);
        {error, not_visible} ->
            {skip, not_visible};
        {error, claimed} ->
            {skip, claimed};
        {error, _} = Error ->
            Error
    end.

complete_with_retry(_RunId, _EffectId, _ClaimToken, _Result, RemainingMs)
  when RemainingMs =< 0 ->
    {error, complete_timeout};
complete_with_retry(RunId, EffectId, ClaimToken, Result, RemainingMs) ->
    case beamtrail:complete_effect(RunId, EffectId, ClaimToken, Result) of
        {ok, State} when is_map(State) ->
            {ok, #{run_id => RunId,
                   effect_id => EffectId,
                   state => State}};
        {ok, ignored} ->
            {ok, idle};
        {error, leased} ->
            timer:sleep(?RETRY_SLEEP_MS),
            complete_with_retry(RunId, EffectId, ClaimToken, Result,
                                RemainingMs - ?RETRY_SLEEP_MS);
        {error, _} = Error ->
            Error
    end.
