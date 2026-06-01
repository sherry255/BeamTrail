-module(beamtrail_run_registry).
-behaviour(gen_server).

%% Local registry for active run processes. It keeps the fast path to one
%% runner per run on this BEAM node; storage leases and fencing remain the
%% correctness boundary across node loss or split ownership.

-export([start_link/0, dispatch/1, dispatch/2, lookup/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

dispatch(RunId) ->
    dispatch(RunId, undefined).

dispatch(RunId, Lease) ->
    gen_server:call(?MODULE, {dispatch, RunId, Lease}, 30000).

lookup(RunId) ->
    gen_server:call(?MODULE, {lookup, RunId}).

init([]) ->
    {ok, #{runs => #{}, refs => #{}}}.

handle_call({dispatch, RunId, Lease}, _From, State) ->
    case find_live_runner(RunId, State) of
        {ok, Pid, State1} ->
            beamtrail_run:dispatch(Pid, Lease),
            {reply, {ok, Pid}, State1};
        not_found ->
            case start_runner(RunId, Lease, State) of
                {ok, Pid, State1} -> {reply, {ok, Pid}, State1};
                {error, _} = Error -> {reply, Error, State}
            end
    end;
handle_call({lookup, RunId}, _From, State) ->
    case find_live_runner(RunId, State) of
        {ok, Pid, State1} -> {reply, {ok, Pid}, State1};
        not_found -> {reply, not_found, State}
    end;
handle_call(_, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info({'DOWN', Ref, process, _Pid, _Reason}, State) ->
    {noreply, remove_ref(Ref, State)};
handle_info(_, State) ->
    {noreply, State}.

terminate(_, _State) ->
    ok.

code_change(_, State, _) ->
    {ok, State}.

find_live_runner(RunId, #{runs := Runs} = State) ->
    case maps:find(RunId, Runs) of
        {ok, {Pid, _Ref}} ->
            case is_process_alive(Pid) of
                true -> {ok, Pid, State};
                false -> not_found
            end;
        error ->
            not_found
    end.

start_runner(RunId, Lease, State) ->
    case whereis(beamtrail_run_sup) of
        undefined ->
            {error, run_supervisor_not_started};
        _ ->
            case beamtrail_run_sup:start_run(RunId) of
                {ok, Pid} ->
                    State1 = track(RunId, Pid, State),
                    beamtrail_run:dispatch(Pid, Lease),
                    {ok, Pid, State1};
                {ok, Pid, _Info} ->
                    State1 = track(RunId, Pid, State),
                    beamtrail_run:dispatch(Pid, Lease),
                    {ok, Pid, State1};
                {error, _} = Error ->
                    Error
            end
    end.

track(RunId, Pid, #{runs := Runs, refs := Refs} = State) ->
    Ref = erlang:monitor(process, Pid),
    State#{runs := maps:put(RunId, {Pid, Ref}, Runs),
           refs := maps:put(Ref, RunId, Refs)}.

remove_ref(Ref, #{runs := Runs, refs := Refs} = State) ->
    case maps:find(Ref, Refs) of
        {ok, RunId} ->
            Runs1 =
                case maps:find(RunId, Runs) of
                    {ok, {_Pid, Ref}} -> maps:remove(RunId, Runs);
                    _ -> Runs
                end,
            State#{runs := Runs1,
                   refs := maps:remove(Ref, Refs)};
        error ->
            State
    end.
