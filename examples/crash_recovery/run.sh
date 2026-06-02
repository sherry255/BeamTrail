#!/usr/bin/env sh
set -eu

ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"
CONTAINER="${BEAMTRAIL_DEMO_CONTAINER:-beamtrail-crash-demo-pg}"
PORT="${BEAMTRAIL_DEMO_PG_PORT:-55432}"
RUN_ID="${BEAMTRAIL_DEMO_RUN_ID:-crash-demo-$(date +%s)}"
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
    i=0
    while [ "$i" -lt 60 ]; do
        if [ -f "$MARKER_FILE" ] && grep -q '^started ' "$MARKER_FILE"; then
            return 0
        fi
        i=$((i + 1))
        sleep 0.2
    done
    echo "The blocking step did not start." >&2
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

echo "==> Starting VM 1 and blocking inside the workflow step"
run_erl_background "bt_crash_demo:start_and_block(\"${RUN_ID}\", \"${MODE_FILE}\", \"${MARKER_FILE}\", \"${PORT}\")."
wait_for_marker

echo "==> Killing VM 1 while attempt 1 is still open"
kill -KILL "$VM_PID" >/dev/null 2>&1 || true
wait "$VM_PID" 2>/dev/null || true

echo complete > "$MODE_FILE"
echo "==> Starting VM 2 and waiting for recovery"
run_erl_foreground "bt_crash_demo:recover(\"${RUN_ID}\", \"${MODE_FILE}\", \"${MARKER_FILE}\", \"${PORT}\")."

echo
echo "Demo completed. Set KEEP_BEAMTRAIL_DEMO=1 to keep ${WORKDIR} and ${CONTAINER}."
