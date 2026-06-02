-module(beamtrail_state).

-export([load/2, maybe_snapshot/4, apply_event/2,
         snapshot_policy/0, snapshot_every/0, snapshot_revision/0,
         snapshot_schema/0,
         snapshot_revision_compatible/1]).

-define(SNAPSHOT_EVERY, 5).
-define(SNAPSHOT_SCHEMA_VERSION, 1).

snapshot_policy() ->
    #{revision => snapshot_revision(),
      every_events => snapshot_every(),
      forced_by => [terminal_transition, recovery_marker]}.

snapshot_schema() ->
    (snapshot_schema_definition())#{revision => snapshot_revision()}.

snapshot_schema_definition() ->
    #{schema_version => ?SNAPSHOT_SCHEMA_VERSION,
      state_keys =>
          [attempt_counts,
           attempts,
           completed_steps,
           created_at,
           current_step,
           failure,
           input,
           last_event_seq,
           migration_required_for_version_change,
           next_retry_at,
           parked,
           parked_at,
           parked_reason,
           pending_attempt,
           run_id,
           status,
           steps,
           terminal,
           workflow],
      attempt_keys => beamtrail_reducer:attempt_keys()}.

snapshot_every() ->
    ?SNAPSHOT_EVERY.

snapshot_revision() ->
    %% Derive the persisted revision from the explicit schema contract so
    %% schema edits automatically invalidate older snapshots.
    <<Raw:32, _/binary>> = erlang:md5(term_to_binary(snapshot_schema_fingerprint_term())),
    (Raw band 16#7fffffff) + 1.

snapshot_schema_fingerprint_term() ->
    Schema = snapshot_schema_definition(),
    {maps:get(schema_version, Schema),
     maps:get(state_keys, Schema),
     maps:get(attempt_keys, Schema)}.

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
    maps:get(snapshot_revision, Snapshot, 0) =:= snapshot_revision().

maybe_snapshot(RunId, State, Force, Storage) ->
    Seq = maps:get(last_event_seq, State, 0),
    ShouldWrite = Force orelse (Seq > 0 andalso Seq rem snapshot_every() =:= 0),
    case ShouldWrite of
        true ->
            case Storage:write_snapshot(RunId, State, Seq, snapshot_revision()) of
                ok ->
                    beamtrail_telemetry:execute([beamtrail, snapshot, written],
                                                #{count => 1},
                                                #{run_id => RunId,
                                                  snapshot_seq => Seq}),
                    ok;
                {error, Reason} ->
                    beamtrail_telemetry:execute([beamtrail, snapshot, write_failed],
                                                #{count => 1},
                                                #{run_id => RunId,
                                                  snapshot_seq => Seq,
                                                  reason => Reason}),
                    ok
            end;
        false ->
            ok
    end.

apply_event(State, Event) ->
    enrich_version_migration(beamtrail_reducer:apply_event(State, Event)).

enrich_version_migration(State = #{workflow := undefined}) ->
    State#{migration_required_for_version_change => false};
enrich_version_migration(State) ->
    %% Gate only the in-flight (pending) attempt: that is the one recovery
    %% would re-execute at its recorded step_version, so a divergence from the
    %% currently deployed code is the genuine migration hazard. Completed
    %% attempts are never re-executed and their steps receive the original
    %% workflow input (not prior results), so a version bump to a finished
    %% step must not wedge an otherwise-progressable run. This matches the
    %% documented replay policy ("recover and retry in-flight work with
    %% recorded step_version").
    Workflow = maps:get(workflow, State),
    MigrationRequired =
        case maps:get(pending_attempt, State, undefined) of
            #{step_id := StepId, step_version := RecordedVersion} ->
                current_step_version(Workflow, StepId) =/= RecordedVersion;
            _ ->
                false
        end,
    State#{migration_required_for_version_change => MigrationRequired}.

current_step_version(Workflow, StepId) ->
    try Workflow:step_version(StepId) of
        Version -> Version
    catch
        _:_ -> undefined
    end.
