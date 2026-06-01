-module(beamtrail_executor).

-export([execute_attempt/1, execute_attempt/3]).

execute_attempt(ExecSpec) when is_map(ExecSpec) ->
    Workflow = maps:get(workflow, ExecSpec),
    safe_execute(Workflow,
                 maps:get(step_id, ExecSpec),
                 maps:get(step_version, ExecSpec),
                 maps:get(input, ExecSpec),
                 maps:get(context, ExecSpec)).

execute_attempt(RunId, Lease, ExecSpec) when is_map(Lease), is_map(ExecSpec) ->
    Timeout = maps:get(timeout_ms, ExecSpec),
    Fun = fun() -> execute_attempt(ExecSpec) end,
    run_with_timeout_and_lease_heartbeat(RunId, Lease, Fun, Timeout).

safe_execute(Workflow, StepId, StepVersion, Input, Context) ->
    try Workflow:execute(StepId, StepVersion, Input, Context) of
        {ok, _Value} = Ok -> Ok;
        {error, _Reason} = Error -> Error;
        Other -> {error, {bad_return, Other}}
    catch
        Class:Reason:_Stacktrace ->
            {error, #{class => Class, reason => Reason}}
    end.

run_with_timeout_and_lease_heartbeat(RunId, Lease, Fun, TimeoutMs) ->
    Parent = self(),
    ResultRef = make_ref(),
    StopRef = make_ref(),
    ExecPid = spawn(fun() -> Parent ! {ResultRef, execute, guarded_execute(Fun)} end),
    HeartbeatPid =
        spawn(fun() ->
                      ParentRef = erlang:monitor(process, Parent),
                      lease_heartbeat_loop(RunId, Lease, StopRef, ParentRef, Parent)
              end),
    TimeoutRef = arm_attempt_timeout(ResultRef, TimeoutMs),
    receive
        {ResultRef, execute, Result} ->
            cancel_attempt_timeout(TimeoutRef),
            HeartbeatPid ! {StopRef, stop},
            Result;
        {ResultRef, timeout} ->
            exit(ExecPid, kill),
            HeartbeatPid ! {StopRef, stop},
            {error, timeout};
        {StopRef, lease_lost, Reason} ->
            cancel_attempt_timeout(TimeoutRef),
            exit(ExecPid, kill),
            {error, #{class => lease_lost, reason => Reason}}
    end.

guarded_execute(Fun) ->
    try Fun()
    catch
        Class:Reason:_Stacktrace ->
            {error, #{class => Class, reason => Reason}}
    end.

arm_attempt_timeout(_ResultRef, infinity) ->
    undefined;
arm_attempt_timeout(ResultRef, TimeoutMs)
  when is_integer(TimeoutMs), TimeoutMs >= 0 ->
    erlang:send_after(TimeoutMs, self(), {ResultRef, timeout}).

cancel_attempt_timeout(undefined) ->
    ok;
cancel_attempt_timeout(TimeoutRef) ->
    erlang:cancel_timer(TimeoutRef),
    ok.

lease_heartbeat_loop(RunId, Lease, StopRef, ParentRef, Parent) ->
    Interval = beamtrail_lease_manager:heartbeat_interval_ms(Lease),
    receive
        {StopRef, stop} ->
            erlang:demonitor(ParentRef, [flush]),
            ok;
        {'DOWN', ParentRef, process, _Pid, _Reason} ->
            ok
    after Interval ->
        case (beamtrail_config:storage()):renew_lease(
               RunId,
               beamtrail_lease_manager:fencing_token(Lease),
               beamtrail_lease_manager:ttl_ms(Lease)) of
            {ok, _Renewed} ->
                lease_heartbeat_loop(RunId, Lease, StopRef, ParentRef, Parent);
            {error, Reason} ->
                Parent ! {StopRef, lease_lost, Reason},
                ok
        end
    end.
