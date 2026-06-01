-module(beamtrail_run).
-behaviour(gen_statem).

%% Active BEAM-side owner for a single workflow run. The event log remains the
%% source of truth; this process only keeps the hot path alive between dispatch
%% calls and retry timers while holding a renewable storage lease.

-export([start_link/1, dispatch/1, dispatch/2, info/1]).
-export([callback_mode/0, init/1, handle_event/4, terminate/3, code_change/4]).

start_link(RunId) ->
    gen_statem:start_link(?MODULE, RunId, []).

dispatch(Pid) ->
    dispatch(Pid, undefined).

dispatch(Pid, undefined) ->
    gen_statem:cast(Pid, dispatch);
dispatch(Pid, Lease) when is_map(Lease) ->
    gen_statem:cast(Pid, {dispatch, Lease}).

info(Pid) ->
    gen_statem:call(Pid, info, 1000).

callback_mode() ->
    handle_event_function.

init(RunId) ->
    {ok, idle, #{run_id => RunId,
                 lease => undefined,
                 retry_timer => undefined,
                 heartbeat_timer => undefined}}.

handle_event(cast, dispatch, _StateName, Data) ->
    run_once(Data);
handle_event(cast, {dispatch, Lease}, _StateName, Data) ->
    run_once(seed_lease(Lease, Data));
handle_event({call, From}, info, StateName, Data) ->
    {next_state, StateName, Data,
     [{reply, From, info_map(StateName, Data)}]};
handle_event(internal, dispatch_now, _StateName, Data) ->
    run_once(Data);
handle_event(info, {retry_due, Ref}, _StateName,
             #{retry_timer := {Ref, _TRef, _DueAt}} = Data) ->
    run_once(Data#{retry_timer := undefined});
handle_event(info, {retry_due, _Ref}, StateName, Data) ->
    {next_state, StateName, Data};
handle_event(info, {lease_heartbeat, Ref}, StateName,
             #{heartbeat_timer := {Ref, _TRef, _DueAt}} = Data) ->
    renew_lease(StateName, Data#{heartbeat_timer := undefined});
handle_event(info, {lease_heartbeat, _Ref}, StateName, Data) ->
    {next_state, StateName, Data};
handle_event(_, _, StateName, Data) ->
    {next_state, StateName, Data}.

terminate(_, _, Data) ->
    cancel_timer(maps:get(retry_timer, Data, undefined)),
    cancel_timer(maps:get(heartbeat_timer, Data, undefined)),
    ok.

code_change(_, StateName, Data, _) ->
    {ok, StateName, Data}.

run_once(Data0) ->
    Data1 = cancel_retry_timer(Data0),
    case ensure_lease(Data1) of
        {ok, #{run_id := RunId, lease := Lease} = Data2} ->
            case beamtrail:dispatch(RunId, Lease) of
                {ok, State} ->
                    after_dispatch(State, Data2);
                {error, _Reason} ->
                    {stop, normal, Data2}
            end;
        {error, _Reason} ->
            {stop, normal, Data1}
    end.

after_dispatch(State, Data) ->
    case maps:get(status, State, undefined) of
        completed ->
            {stop, normal, Data};
        failed ->
            case maps:get(terminal, State, false) of
                true -> {stop, normal, Data};
                false -> schedule_immediate(Data)
            end;
        retrying ->
            schedule_retry_or_dispatch(State, Data);
        _ ->
            schedule_immediate(Data)
    end.

schedule_retry_or_dispatch(State, Data) ->
    Now = erlang:system_time(millisecond),
    case maps:get(next_retry_at, State, 0) of
        NextRetryAt when is_integer(NextRetryAt), NextRetryAt > Now ->
            Data1 = schedule_retry_timer(NextRetryAt - Now, Data),
            Data2 = schedule_heartbeat(Data1),
            {next_state, waiting_retry, Data2};
        _ ->
            schedule_immediate(Data)
    end.

schedule_immediate(Data) ->
    {next_state, active, Data, [{next_event, internal, dispatch_now}]}.

ensure_lease(#{lease := undefined, run_id := RunId} = Data) ->
    Owner = #{node => node(), pid => self(), runner => ?MODULE},
    case beamtrail_lease_manager:acquire(RunId, Owner) of
        {ok, Lease} -> {ok, Data#{lease := Lease}};
        {error, _} = Error -> Error
    end;
ensure_lease(#{lease := _Lease} = Data) ->
    {ok, Data}.

seed_lease(undefined, Data) ->
    Data;
seed_lease(Lease, #{lease := undefined} = Data) when is_map(Lease) ->
    Data#{lease := Lease};
seed_lease(_Lease, Data) ->
    Data.

renew_lease(StateName, #{run_id := RunId, lease := Lease} = Data)
  when is_map(Lease) ->
    case beamtrail_lease_manager:renew(
           RunId, maps:get(fencing_token, Lease),
           beamtrail_lease_manager:default_ttl_ms()) of
        {ok, Renewed} ->
            {next_state, StateName, schedule_heartbeat(Data#{lease := Renewed})};
        {error, _} ->
            {stop, normal, Data}
    end;
renew_lease(_StateName, Data) ->
    {stop, normal, Data}.

schedule_retry_timer(DelayMs, Data) ->
    Data1 = cancel_retry_timer(Data),
    Ref = make_ref(),
    Delay = max(0, DelayMs),
    DueAt = erlang:system_time(millisecond) + Delay,
    TRef = erlang:send_after(Delay, self(), {retry_due, Ref}),
    Data1#{retry_timer := {Ref, TRef, DueAt}}.

schedule_heartbeat(#{lease := undefined} = Data) ->
    Data;
schedule_heartbeat(#{lease := Lease} = Data) when is_map(Lease) ->
    Data1 = cancel_heartbeat_timer(Data),
    Ref = make_ref(),
    Delay = lease_heartbeat_interval_ms(),
    DueAt = erlang:system_time(millisecond) + Delay,
    TRef = erlang:send_after(Delay, self(), {lease_heartbeat, Ref}),
    Data1#{heartbeat_timer := {Ref, TRef, DueAt}}.

cancel_retry_timer(Data) ->
    cancel_timer(maps:get(retry_timer, Data, undefined)),
    Data#{retry_timer := undefined}.

cancel_heartbeat_timer(Data) ->
    cancel_timer(maps:get(heartbeat_timer, Data, undefined)),
    Data#{heartbeat_timer := undefined}.

cancel_timer(undefined) ->
    ok;
cancel_timer({_Ref, TRef}) ->
    erlang:cancel_timer(TRef),
    ok;
cancel_timer({_Ref, TRef, _DueAt}) ->
    erlang:cancel_timer(TRef),
    ok.

lease_heartbeat_interval_ms() ->
    max(1, min(5000, beamtrail_lease_manager:default_ttl_ms() div 3)).

info_map(StateName, Data) ->
    Lease = maps:get(lease, Data, undefined),
    #{run_id => maps:get(run_id, Data),
      pid => self(),
      status => StateName,
      fencing_token => lease_field(Lease, fencing_token),
      lease_until => lease_field(Lease, lease_until),
      retry_due_at => timer_due_at(maps:get(retry_timer, Data, undefined)),
      retry_due_in_ms => timer_due_in_ms(maps:get(retry_timer, Data, undefined)),
      heartbeat_due_at => timer_due_at(maps:get(heartbeat_timer, Data, undefined)),
      heartbeat_due_in_ms => timer_due_in_ms(maps:get(heartbeat_timer, Data, undefined))}.

lease_field(undefined, _Key) ->
    undefined;
lease_field(Lease, Key) when is_map(Lease) ->
    maps:get(Key, Lease, undefined).

timer_due_at({_Ref, _TRef, DueAt}) ->
    DueAt;
timer_due_at(_) ->
    undefined.

timer_due_in_ms({_Ref, _TRef, DueAt}) ->
    max(0, DueAt - erlang:system_time(millisecond));
timer_due_in_ms(_) ->
    undefined.
