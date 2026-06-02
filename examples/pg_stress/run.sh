#!/usr/bin/env sh
set -eu

ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"
CONTAINER="${BEAMTRAIL_STRESS_CONTAINER:-beamtrail-pg-stress}"
PORT="${BEAMTRAIL_STRESS_PG_PORT:-55432}"
RUNS="${BEAMTRAIL_STRESS_RUNS:-32}"
SLEEP_MS="${BEAMTRAIL_STRESS_SLEEP_MS:-25}"
POOL_SIZE="${BEAMTRAIL_STRESS_POOL_SIZE:-5}"
WORKDIR="${TMPDIR:-/tmp}/beamtrail-pg-stress"
DEMO_EBIN="${WORKDIR}/ebin"

cleanup() {
    if [ "${KEEP_BEAMTRAIL_STRESS:-0}" != "1" ]; then
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

code_path_args() {
    for ebin in "$ROOT"/_build/default/lib/*/ebin; do
        printf ' -pa %s' "$ebin"
    done
}

mkdir -p "$DEMO_EBIN"

echo "==> Starting disposable PostgreSQL on port ${PORT}"
docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
docker run --name "$CONTAINER" \
    -e POSTGRES_USER=beamtrail \
    -e POSTGRES_PASSWORD=beamtrail \
    -e POSTGRES_DB=beamtrail \
    -p "${PORT}:5432" \
    -d postgres:16 >/dev/null
wait_for_postgres

echo "==> Compiling BeamTrail and stress modules"
(cd "$ROOT" && rebar3 compile >/dev/null)
erlc -pa "$ROOT/_build/default/lib/beamtrail/ebin" \
     -pa "$ROOT/_build/default/lib/epgsql/ebin" \
     -o "$DEMO_EBIN" \
     "$ROOT"/examples/pg_stress/src/*.erl

echo "==> Running ${RUNS} workflows with pool_size=${POOL_SIZE}, sleep_ms=${SLEEP_MS}"
erl -noshell -pa "$DEMO_EBIN" $(code_path_args) \
    -eval "bt_pg_stress:run(\"${RUNS}\", \"${SLEEP_MS}\", \"${POOL_SIZE}\", \"${PORT}\")."

echo
echo "Stress run completed. Set KEEP_BEAMTRAIL_STRESS=1 to keep ${WORKDIR} and ${CONTAINER}."
