#!/usr/bin/env sh
set -eu

ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"
CONTAINER="${BEAMTRAIL_DEMO_CONTAINER:-beamtrail-crash-demo-pg}"
PORT="${BEAMTRAIL_DEMO_PG_PORT:-55432}"
RUN_ID="${BEAMTRAIL_DEMO_RUN_ID:-crash-demo-$(date +%s)}"
SCENARIO="${1:-${BEAMTRAIL_DEMO_SCENARIO:-attempt}}"
WORKDIR="${TMPDIR:-/tmp}/beamtrail-crash-demo-${RUN_ID}"
DEMO_EBIN="${WORKDIR}/ebin"
MODE_FILE="${WORKDIR}/mode"
MARKER_FILE="${WORKDIR}/attempts.log"

cleanup() {
    if [ "${KEEP_BEAMTRAIL_DEMO:-0}" != "1" ]; then
        docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
        rm -rf "$WORKDIR"
    fi
}

trap cleanup EXIT INT TERM

wait_for_postgres() {
    i=0
    while [ "$i" -lt 60 ]; do
        if docker exec "$CONTAINER" pg_isready -U beamtrail -d beamtrail >/dev/null 2>&1; then
            return 0
        fi
        i=$((i + 1))
        sleep 0.2
    done
    echo "PostgreSQL did not become ready." >&2
    return 1
}

wait_for_marker() {
    pattern="$1"
    description="$2"
    i=0
    while [ "$i" -lt 60 ]; do
        if [ -f "$MARKER_FILE" ] && grep -q "$pattern" "$MARKER_FILE"; then
            return 0
        fi
        i=$((i + 1))
        sleep 0.2
    done
    echo "Timed out waiting for marker: ${description}." >&2
    return 1
}

code_path_args() {
    for ebin in "$ROOT"/_build/default/lib/*/ebin; do
        printf ' -pa %s' "$ebin"
    done
}

run_erl_foreground() {
    erl -noshell -pa "$DEMO_EBIN" $(code_path_args) -eval "$1"
}

run_erl_background() {
    erl -noshell -pa "$DEMO_EBIN" $(code_path_args) -eval "$1" &
    VM_PID=$!
}

mkdir -p "$DEMO_EBIN"
echo block > "$MODE_FILE"
: > "$MARKER_FILE"

echo "==> Starting disposable PostgreSQL on port ${PORT}"
docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
docker run --name "$CONTAINER" \
    -e POSTGRES_USER=beamtrail \
    -e POSTGRES_PASSWORD=beamtrail \
    -e POSTGRES_DB=beamtrail \
    -p "${PORT}:5432" \
    -d postgres:16 >/dev/null
wait_for_postgres

echo "==> Compiling BeamTrail and demo modules"
(cd "$ROOT" && rebar3 compile >/dev/null)
erlc -pa "$ROOT/_build/default/lib/beamtrail/ebin" \
     -pa "$ROOT/_build/default/lib/epgsql/ebin" \
     -o "$DEMO_EBIN" \
     "$ROOT"/examples/crash_recovery/src/*.erl

run_attempt_scenario() {
    echo "==> Starting VM 1 and blocking inside the workflow step"
    run_erl_background "bt_crash_demo:start_and_block(\"${RUN_ID}\", \"${MODE_FILE}\", \"${MARKER_FILE}\", \"${PORT}\")."
    wait_for_marker '^started ' "attempt start"

    echo "==> Killing VM 1 while attempt 1 is still open"
    kill -KILL "$VM_PID" >/dev/null 2>&1 || true
    wait "$VM_PID" 2>/dev/null || true

    echo complete > "$MODE_FILE"
    echo "==> Starting VM 2 and waiting for recovery"
    run_erl_foreground "bt_crash_demo:recover(\"${RUN_ID}\", \"${MODE_FILE}\", \"${MARKER_FILE}\", \"${PORT}\")."
}

run_approval_scenario() {
    APPROVAL_OK_RUN_ID="${RUN_ID}-approval-ok"
    APPROVAL_TIMEOUT_RUN_ID="${RUN_ID}-approval-timeout"
    MARKER_FILE="${WORKDIR}/approval.log"
    : > "$MARKER_FILE"

    echo "==> Approval scenario A: kill VM while waiting, then approve after restart"
    run_erl_background "bt_crash_demo:start_approval_wait(\"${APPROVAL_OK_RUN_ID}\", \"${MARKER_FILE}\", \"${PORT}\", 60000)."
    wait_for_marker "waiting run=${APPROVAL_OK_RUN_ID}" "approval wait"
    echo "==> Killing VM 1 while approval run is waiting"
    kill -KILL "$VM_PID" >/dev/null 2>&1 || true
    wait "$VM_PID" 2>/dev/null || true
    echo "==> Starting VM 2 and delivering approval signal"
    run_erl_foreground "bt_crash_demo:approve_after_restart(\"${APPROVAL_OK_RUN_ID}\", \"${MARKER_FILE}\", \"${PORT}\")."

    echo "==> Approval scenario B: kill VM while waiting, then let deadline fire"
    run_erl_background "bt_crash_demo:start_approval_wait(\"${APPROVAL_TIMEOUT_RUN_ID}\", \"${MARKER_FILE}\", \"${PORT}\", 300)."
    wait_for_marker "waiting run=${APPROVAL_TIMEOUT_RUN_ID}" "approval timeout wait"
    echo "==> Killing VM 3 before the approval deadline is processed"
    kill -KILL "$VM_PID" >/dev/null 2>&1 || true
    wait "$VM_PID" 2>/dev/null || true
    sleep 0.5
    echo "==> Starting VM 4 and letting scanner-driven timer recovery fire"
    run_erl_foreground "bt_crash_demo:timeout_after_restart(\"${APPROVAL_TIMEOUT_RUN_ID}\", \"${MARKER_FILE}\", \"${PORT}\")."
}

case "$SCENARIO" in
    attempt)
        run_attempt_scenario
        ;;
    approval)
        run_approval_scenario
        ;;
    *)
        echo "Unknown scenario: ${SCENARIO}. Use attempt or approval." >&2
        exit 64
        ;;
esac

echo
echo "Demo completed. Set KEEP_BEAMTRAIL_DEMO=1 to keep ${WORKDIR} and ${CONTAINER}."
