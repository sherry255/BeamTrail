-module(beamtrail_external_worker).

-export([run_once/2]).

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
-type run_once_result() ::
    {ok, idle}
    | {ok, #{run_id := beamtrail:run_id(),
             effect_id := term(),
             state := beamtrail:state()}}
    | {error, term()}.

-spec run_once(owner(), handler()) -> run_once_result().
run_once(Owner, Handler) when is_function(Handler, 1) ->
    case beamtrail:list_pending_effects() of
        {ok, Effects} ->
            run_first_visible(Effects, Owner, Handler);
        {error, _} = Error ->
            Error
    end.

run_first_visible([], _Owner, _Handler) ->
    {ok, idle};
run_first_visible([Effect | Rest], Owner, Handler) ->
    RunId = maps:get(run_id, Effect),
    EffectId = maps:get(effect_id, Effect),
    case claim_with_retry(RunId, EffectId, Owner, ?DEFAULT_RETRY_MS) of
        {ok, Claim} ->
            ClaimedEffect = maps:merge(Effect, Claim),
            complete_claimed_effect(RunId, EffectId, Handler, ClaimedEffect);
        {skip, _Reason} ->
            run_first_visible(Rest, Owner, Handler);
        {error, _} = Error ->
            Error
    end.

complete_claimed_effect(RunId, EffectId, Handler, ClaimedEffect) ->
    case run_handler(Handler, ClaimedEffect) of
        {ok, _} = Result ->
            complete_with_retry(RunId, EffectId,
                                maps:get(claim_token, ClaimedEffect),
                                Result,
                                ?DEFAULT_RETRY_MS);
        {error, _} = Result ->
            complete_with_retry(RunId, EffectId,
                                maps:get(claim_token, ClaimedEffect),
                                Result,
                                ?DEFAULT_RETRY_MS)
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
