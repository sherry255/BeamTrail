#!/usr/bin/env sh
set -eu

ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"
CONTAINER="${BEAMTRAIL_EXTERNAL_WORKER_CONTAINER:-beamtrail-external-worker-pg}"
PORT="${BEAMTRAIL_EXTERNAL_WORKER_PG_PORT:-55432}"
RUN_ID="${BEAMTRAIL_EXTERNAL_WORKER_RUN_ID:-external-worker-$(date +%s)}"
WORKDIR="${TMPDIR:-/tmp}/beamtrail-external-worker-${RUN_ID}"
DEMO_EBIN="${WORKDIR}/ebin"

cleanup() {
    if [ "${KEEP_BEAMTRAIL_EXTERNAL_WORKER:-0}" != "1" ]; then
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

echo "==> Compiling BeamTrail and external-worker modules"
(cd "$ROOT" && rebar3 compile >/dev/null)
erlc -pa "$ROOT/_build/default/lib/beamtrail/ebin" \
     -pa "$ROOT/_build/default/lib/epgsql/ebin" \
     -o "$DEMO_EBIN" \
     "$ROOT"/examples/external_worker/src/*.erl

echo "==> Running external worker handoff demo"
erl -noshell -pa "$DEMO_EBIN" $(code_path_args) \
    -eval "bt_external_worker_demo:run(\"${RUN_ID}\", \"${PORT}\")."

echo
echo "External worker demo completed. Set KEEP_BEAMTRAIL_EXTERNAL_WORKER=1 to keep ${WORKDIR} and ${CONTAINER}."

