-module(beamtrail_postgres_pool).
-behaviour(gen_server).

-export([start_link/0, checkout/0, checkin/1, info/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

checkout() ->
    gen_server:call(?MODULE, checkout, infinity).

checkin(Connection) ->
    gen_server:cast(?MODULE, {checkin, Connection}).

info() ->
    gen_server:call(?MODULE, info).

init([]) ->
    Size = beamtrail_postgres_config:pool_size(),
    case connect_initial(Size, [], #{}, #{}) of
        {ok, Connections, Refs, ConnRefs} ->
            {ok, #{size => Size,
                   available => Connections,
                   busy => #{},
                   refs => Refs,
                   conn_refs => ConnRefs,
                   owners => #{},
                   waiting => queue:new(),
                   checkouts => 0}};
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call(checkout, From, State) ->
    case maps:get(available, State) of
        [Connection | Rest] ->
            State1 = assign_connection(Connection, From,
                                       State#{available := Rest}),
            {reply, {ok, Connection}, State1};
        [] ->
            Waiting = queue:in(From, maps:get(waiting, State)),
            {noreply, State#{waiting := Waiting}}
    end;
handle_call(info, _From, State) ->
    Reply = #{size => maps:get(size, State),
              available => length(maps:get(available, State)),
              busy => maps:size(maps:get(busy, State)),
              waiting => queue:len(maps:get(waiting, State)),
              checkouts => maps:get(checkouts, State)},
    {reply, Reply, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast({checkin, Connection}, State) ->
    case maps:is_key(Connection, maps:get(busy, State)) of
        true ->
            {noreply, return_connection(Connection, State)};
        false ->
            {noreply, State}
    end;
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', Ref, process, _Pid, _Reason}, State) ->
    case maps:take(Ref, maps:get(refs, State)) of
        {Connection, Refs1} ->
            Available = lists:delete(Connection, maps:get(available, State)),
            {Busy, Owners} = remove_busy_connection(Connection, State),
            State1 = State#{refs := Refs1,
                            conn_refs := maps:remove(Connection,
                                                     maps:get(conn_refs, State)),
                            owners := Owners,
                            available := Available,
                            busy := Busy},
            {noreply, maybe_replace_connection(State1)};
        error ->
            case maps:take(Ref, maps:get(owners, State)) of
                {Connection, Owners1} ->
                    State1 = drop_owned_connection(Connection, Owners1, State),
                    {noreply, maybe_replace_connection(State1)};
                error ->
                    {noreply, State}
            end
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    lists:foreach(fun close_connection/1, all_connections(State)),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

connect_initial(0, Connections, Refs, ConnRefs) ->
    {ok, Connections, Refs, ConnRefs};
connect_initial(N, Connections, Refs, ConnRefs) ->
    case connect_one() of
        {ok, Connection, Ref} ->
            connect_initial(N - 1, [Connection | Connections],
                            maps:put(Ref, Connection, Refs),
                            maps:put(Connection, Ref, ConnRefs));
        {error, Reason} ->
            lists:foreach(fun close_connection/1, Connections),
            {error, Reason}
    end.

connect_one() ->
    case beamtrail_postgres_config:connection() of
        {ok, Config} ->
            case epgsql:connect(Config) of
                {ok, Connection} ->
                    Ref = erlang:monitor(process, Connection),
                    {ok, Connection, Ref};
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

return_connection(Connection, State) ->
    {Busy, Owners} = remove_busy_connection(Connection, State),
    case queue:out(maps:get(waiting, State)) of
        {{value, From}, Waiting1} ->
            State1 = assign_connection(Connection, From,
                                       State#{busy := Busy,
                                              owners := Owners,
                                              waiting := Waiting1}),
            gen_server:reply(From, {ok, Connection}),
            State1;
        {empty, Waiting1} ->
            State#{busy := Busy,
                   owners := Owners,
                   waiting := Waiting1,
                   available := [Connection | maps:get(available, State)]}
    end.

maybe_replace_connection(State) ->
    case total_connections(State) < maps:get(size, State) of
        true ->
            case connect_one() of
                {ok, Connection, Ref} ->
                    State1 = State#{refs := maps:put(Ref, Connection,
                                                     maps:get(refs, State)),
                                    conn_refs := maps:put(Connection, Ref,
                                                         maps:get(conn_refs,
                                                                  State))},
                    return_new_connection(Connection, State1);
                {error, _Reason} ->
                    State
            end;
        false ->
            State
    end.

return_new_connection(Connection, State) ->
    case queue:out(maps:get(waiting, State)) of
        {{value, From}, Waiting1} ->
            State1 = assign_connection(Connection, From,
                                       State#{waiting := Waiting1}),
            gen_server:reply(From, {ok, Connection}),
            State1;
        {empty, Waiting1} ->
            State#{waiting := Waiting1,
                   available := [Connection | maps:get(available, State)]}
    end.

assign_connection(Connection, From, State) ->
    OwnerRef = erlang:monitor(process, owner_pid(From)),
    State#{busy := maps:put(Connection, OwnerRef, maps:get(busy, State)),
           owners := maps:put(OwnerRef, Connection, maps:get(owners, State)),
           checkouts := maps:get(checkouts, State) + 1}.

remove_busy_connection(Connection, State) ->
    Busy0 = maps:get(busy, State),
    case maps:take(Connection, Busy0) of
        {OwnerRef, Busy1} ->
            erlang:demonitor(OwnerRef, [flush]),
            {Busy1, maps:remove(OwnerRef, maps:get(owners, State))};
        error ->
            {Busy0, maps:get(owners, State)}
    end.

drop_owned_connection(Connection, Owners, State) ->
    case maps:take(Connection, maps:get(conn_refs, State)) of
        {ConnRef, ConnRefs1} ->
            erlang:demonitor(ConnRef, [flush]),
            close_connection(Connection),
            State#{busy := maps:remove(Connection, maps:get(busy, State)),
                   owners := Owners,
                   refs := maps:remove(ConnRef, maps:get(refs, State)),
                   conn_refs := ConnRefs1};
        error ->
            State#{busy := maps:remove(Connection, maps:get(busy, State)),
                   owners := Owners}
    end.

owner_pid({Pid, _Tag}) when is_pid(Pid) ->
    Pid.

total_connections(State) ->
    length(maps:get(available, State)) + maps:size(maps:get(busy, State)).

all_connections(State) ->
    maps:keys(maps:get(busy, State)) ++ maps:get(available, State).

close_connection(Connection) ->
    catch epgsql:close(Connection),
    ok.
