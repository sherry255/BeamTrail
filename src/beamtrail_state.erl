-module(beamtrail_state).

-export([load/2, maybe_snapshot/4, apply_event/2]).

-define(SNAPSHOT_EVERY, 5).
-define(SNAPSHOT_REVISION, 1).

load(RunId, Storage) ->
    case Storage:read_snapshot(RunId) of
        {ok, Snapshot} ->
            case snapshot_revision_compatible(Snapshot) of
                true -> load_from_snapshot(RunId, Snapshot, Storage);
                false -> load_from_events(RunId, Storage)
            end;
        not_found ->
            load_from_events(RunId, Storage);
        {error, _} = Error ->
            Error
    end.

load_from_snapshot(RunId, Snapshot, Storage) ->
    SnapshotSeq = maps:get(snapshot_seq, Snapshot),
    case Storage:read_events(RunId, SnapshotSeq + 1, infinity) of
        {ok, TailEvents} ->
            State = beamtrail_reducer:from_snapshot_and_events(
                      maps:get(state, Snapshot), TailEvents),
            {ok, enrich_version_migration(State)};
        {error, _} = Error ->
            Error
    end.

load_from_events(RunId, Storage) ->
    case Storage:events(RunId) of
        {ok, Events} ->
            {ok, enrich_version_migration(beamtrail_reducer:from_events(Events))};
        {error, _} = Error ->
            Error
    end.

snapshot_revision_compatible(Snapshot) ->
    maps:get(snapshot_revision, Snapshot, 0) =:= ?SNAPSHOT_REVISION.

maybe_snapshot(RunId, State, Force, Storage) ->
    Seq = maps:get(last_event_seq, State, 0),
    ShouldWrite = Force orelse (Seq > 0 andalso Seq rem ?SNAPSHOT_EVERY =:= 0),
    case ShouldWrite of
        true ->
            case Storage:write_snapshot(RunId, State, Seq, ?SNAPSHOT_REVISION) of
                ok ->
                    beamtrail_telemetry:execute([beamtrail, snapshot, written],
                                                #{count => 1},
                                                #{run_id => RunId,
                                                  snapshot_seq => Seq}),
                    ok;
                {error, _} = Error ->
                    Error
            end;
        false ->
            ok
    end.

apply_event(State, Event) ->
    enrich_version_migration(beamtrail_reducer:apply_event(State, Event)).

enrich_version_migration(State = #{workflow := undefined}) ->
    State#{migration_required_for_version_change => false};
enrich_version_migration(State) ->
    Workflow = maps:get(workflow, State),
    Attempts = maps:get(attempts, State, []),
    MigrationRequired =
        lists:any(
          fun(Attempt) ->
                  StepId = maps:get(step_id, Attempt),
                  RecordedVersion = maps:get(step_version, Attempt),
                  current_step_version(Workflow, StepId) =/= RecordedVersion
          end,
          Attempts),
    State#{migration_required_for_version_change => MigrationRequired}.

current_step_version(Workflow, StepId) ->
    try Workflow:step_version(StepId) of
        Version -> Version
    catch
        _:_ -> undefined
    end.
