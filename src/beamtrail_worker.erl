-module(beamtrail_worker).

%% Transient worker that drives a single dispatch and exits. Spawned by
%% beamtrail_worker_sup. Errors are isolated to this worker so a slow or
%% crashing run never blocks the scanner.

-export([start_link/1, start_link/2, run/1, run/2]).

start_link(RunId) ->
    {ok, proc_lib:spawn_link(?MODULE, run, [RunId])}.

start_link(RunId, Lease) ->
    {ok, proc_lib:spawn_link(?MODULE, run, [RunId, Lease])}.

run(RunId) ->
    try beamtrail:dispatch(RunId) of
        _ -> ok
    catch
        Class:Reason:Stack ->
            error_logger:warning_msg(
              "beamtrail_worker dispatch failed for ~p: ~p:~p ~p~n",
              [RunId, Class, Reason, Stack])
    end.

run(RunId, Lease) ->
    try beamtrail:dispatch(RunId, Lease) of
        _ -> ok
    catch
        Class:Reason:Stack ->
            error_logger:warning_msg(
              "beamtrail_worker dispatch failed for ~p: ~p:~p ~p~n",
              [RunId, Class, Reason, Stack])
    end.
