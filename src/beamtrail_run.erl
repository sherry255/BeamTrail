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
    process_flag(trap_exit, true),
    {ok, idle, #{run_id => RunId,
                 lease => undefined,
                 retry_due_at => undefined,
                 heartbeat_timer => undefined,
                 dispatch => undefined,
                 attempt => undefined}}.

handle_event(cast, dispatch, executing, Data) ->
    {keep_state, Data};
handle_event(cast, {dispatch, _Lease}, executing, Data) ->
    {keep_state, Data};
handle_event(cast, dispatch, _StateName, Data) ->
    start_dispatch(Data);
handle_event(cast, {dispatch, Lease}, _StateName, Data) ->
    start_dispatch(seed_lease(Lease, Data));
handle_event({call, From}, info, StateName, Data) ->
    {keep_state, Data, [{reply, From, info_map(StateName, Data)}]};
handle_event(internal, dispatch_now, executing, Data) ->
    {keep_state, Data};
handle_event(internal, dispatch_now, _StateName, Data) ->
    start_dispatch(Data);
handle_event(state_timeout, retry_due, waiting_retry, Data) ->
    start_dispatch(Data#{retry_due_at := undefined});
handle_event(state_timeout, retry_due, _StateName, Data) ->
    {keep_state, Data};
handle_event(info, {step_result, Ref, Result}, _StateName,
             #{dispatch := {Ref, _Pid}} = Data) ->
    handle_step_result(Result, clear_step_execution(Data));
handle_event(info, {step_result, _Ref, _Result}, _StateName, Data) ->
    {keep_state, Data};
handle_event(state_timeout, {step_timeout, Ref}, _StateName,
             #{dispatch := {Ref, Pid}} = Data) ->
    exit(Pid, kill),
    handle_step_result({error, timeout}, clear_step_execution(Data));
handle_event(state_timeout, {step_timeout, _Ref}, _StateName, Data) ->
    {keep_state, Data};
handle_event(info, {'EXIT', Pid, normal}, _StateName,
             #{dispatch := {_Ref, Pid}} = Data) ->
    {keep_state, Data};
handle_event(info, {'EXIT', Pid, Reason}, _StateName,
             #{dispatch := {_Ref, Pid}} = Data) ->
    handle_step_result({error, #{class => exit, reason => Reason}},
                       clear_step_execution(Data));
handle_event(info, {'EXIT', _Pid, _Reason}, _StateName, Data) ->
    {keep_state, Data};
handle_event({timeout, lease_heartbeat}, {lease_heartbeat, Ref}, StateName,
             #{heartbeat_timer := {Ref, _DueAt}} = Data) ->
    renew_lease(StateName, Data#{heartbeat_timer := undefined});
handle_event({timeout, lease_heartbeat}, {lease_heartbeat, _Ref}, _StateName, Data) ->
    {keep_state, Data};
handle_event(_, _, _StateName, Data) ->
    {keep_state, Data}.

terminate(_, _, Data) ->
    cancel_dispatch(maps:get(dispatch, Data, undefined)),
    ok.

code_change(_, StateName, Data, _) ->
    {ok, StateName, Data}.

start_dispatch(Data0) ->
    Data1 = cancel_heartbeat_timer(clear_retry_due(Data0)),
    case ensure_lease(Data1) of
        {ok, #{run_id := RunId, lease := Lease} = Data2} ->
            case beamtrail_runner_transition:next_action(RunId, Lease) of
                {ok, {execute, Attempt, ExecSpec}} ->
                    start_step_execution(Attempt, ExecSpec, Data2);
                {ok, State} ->
                    after_dispatch(State, Data2);
                {error, _Reason} ->
                    {stop, normal, Data2}
            end;
        {error, _Reason} ->
            {stop, normal, Data1}
    end.

start_step_execution(Attempt, ExecSpec, Data0) ->
    Ref = make_ref(),
    Parent = self(),
    Pid = spawn_link(
            fun() ->
                    Parent ! {step_result, Ref,
                              beamtrail_runner_transition:execute_attempt(ExecSpec)}
            end),
    Data1 = schedule_heartbeat(Data0),
    Data2 = Data1#{dispatch := {Ref, Pid},
                   attempt := Attempt},
    {next_state, executing, Data2,
     heartbeat_timeout_action(Data2) ++
     step_timeout_action(maps:get(timeout_ms, ExecSpec, infinity), Ref)}.

handle_step_result(Result, #{run_id := RunId, lease := Lease, attempt := Attempt} = Data)
  when is_map(Attempt) ->
    case beamtrail_runner_transition:finish_attempt(RunId, Lease, Attempt, Result) of
        {ok, State} ->
            after_dispatch(State, Data#{attempt := undefined});
        {error, _Reason} ->
            {stop, normal, Data#{attempt := undefined}}
    end;
handle_step_result(_Result, Data) ->
    {stop, normal, Data}.

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
            Data1 = set_retry_due(NextRetryAt, Data),
            Data2 = schedule_heartbeat(Data1),
            {next_state, waiting_retry, Data2,
             heartbeat_timeout_action(Data2) ++ retry_timeout_action(Data2)};
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

renew_lease(_StateName, #{run_id := RunId, lease := Lease} = Data)
  when is_map(Lease) ->
    case beamtrail_lease_manager:renew(
           RunId, maps:get(fencing_token, Lease),
           beamtrail_lease_manager:default_ttl_ms()) of
        {ok, Renewed} ->
            Data1 = schedule_heartbeat(Data#{lease := Renewed}),
            {keep_state, Data1, heartbeat_timeout_action(Data1)};
        {error, _} ->
            {stop, normal, Data}
    end;
renew_lease(_StateName, Data) ->
    {stop, normal, Data}.

set_retry_due(DueAt, Data) ->
    Data#{retry_due_at := DueAt}.

schedule_heartbeat(#{lease := undefined} = Data) ->
    Data;
schedule_heartbeat(#{lease := Lease} = Data) when is_map(Lease) ->
    Data1 = cancel_heartbeat_timer(Data),
    Ref = make_ref(),
    Delay = lease_heartbeat_interval_ms(),
    DueAt = erlang:system_time(millisecond) + Delay,
    Data1#{heartbeat_timer := {Ref, DueAt}}.

step_timeout_action(infinity, _Ref) ->
    [];
step_timeout_action(undefined, _Ref) ->
    [];
step_timeout_action(TimeoutMs, Ref) when is_integer(TimeoutMs), TimeoutMs >= 0 ->
    [{state_timeout, TimeoutMs, {step_timeout, Ref}}].

retry_timeout_action(#{retry_due_at := DueAt}) when is_integer(DueAt) ->
    Delay = max(0, DueAt - erlang:system_time(millisecond)),
    [{state_timeout, Delay, retry_due}];
retry_timeout_action(_Data) ->
    [].

heartbeat_timeout_action(#{heartbeat_timer := {Ref, DueAt}}) when is_integer(DueAt) ->
    Delay = max(0, DueAt - erlang:system_time(millisecond)),
    [{{timeout, lease_heartbeat}, Delay, {lease_heartbeat, Ref}}];
heartbeat_timeout_action(_Data) ->
    [{{timeout, lease_heartbeat}, cancel}].

clear_retry_due(Data) ->
    Data#{retry_due_at := undefined}.

cancel_heartbeat_timer(Data) ->
    Data#{heartbeat_timer := undefined}.

clear_step_execution(Data) ->
    Data#{dispatch := undefined}.

cancel_dispatch(undefined) ->
    ok;
cancel_dispatch({_Ref, Pid}) when is_pid(Pid) ->
    exit(Pid, kill),
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
      dispatch_pid => dispatch_pid(maps:get(dispatch, Data, undefined)),
      retry_due_at => maps:get(retry_due_at, Data, undefined),
      retry_due_in_ms => due_in_ms(maps:get(retry_due_at, Data, undefined)),
      heartbeat_due_at => timer_due_at(maps:get(heartbeat_timer, Data, undefined)),
      heartbeat_due_in_ms => timer_due_in_ms(maps:get(heartbeat_timer, Data, undefined))}.

lease_field(undefined, _Key) ->
    undefined;
lease_field(Lease, Key) when is_map(Lease) ->
    maps:get(Key, Lease, undefined).

timer_due_at({_Ref, DueAt}) ->
    DueAt;
timer_due_at(_) ->
    undefined.

timer_due_in_ms({_Ref, DueAt}) ->
    max(0, DueAt - erlang:system_time(millisecond));
timer_due_in_ms(_) ->
    undefined.

due_in_ms(DueAt) when is_integer(DueAt) ->
    max(0, DueAt - erlang:system_time(millisecond));
due_in_ms(_) ->
    undefined.

dispatch_pid({_Ref, Pid}) ->
    Pid;
dispatch_pid(_) ->
    undefined.
