#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

compose_file="${ROOT_DIR}/integration/docker-compose.yml"
runs_dir="${ROOT_DIR}/integration/server-runs"
server_log="${ROOT_DIR}/integration/server-e2e.log"
server_pid=""

function cleanup() {
  if [ -n "${server_pid}" ] && kill -0 "${server_pid}" >/dev/null 2>&1; then
    kill "${server_pid}" >/dev/null 2>&1 || true
    wait "${server_pid}" >/dev/null 2>&1 || true
  fi
  docker compose -f "${compose_file}" down -v >/dev/null 2>&1 || true
}

function wait_for_es() {
  local url="$1"
  local name="$2"
  for _ in {1..60}; do
    if curl -s "${url}" | grep -q "You Know, for Search"; then
      return 0
    fi
    sleep 2
  done
  echo "Elasticsearch ${name} did not become ready" >&2
  return 1
}

function wait_for_server() {
  for _ in {1..60}; do
    if curl -s -o /dev/null -w "%{http_code}" "http://localhost:18182/" | grep -q "200"; then
      return 0
    fi
    sleep 1
  done
  echo "Server did not become ready" >&2
  return 1
}

function wait_for_count() {
  local url="$1"
  local expected="$2"
  local count="0"
  for _ in {1..60}; do
    curl -s -X POST "${url%/*}/_refresh" >/dev/null || true
    count=$(curl -s "${url}/_count" | grep -oE '"count":[0-9]+' | awk -F: '{print $2}')
    if [ "${count}" = "${expected}" ]; then
      return 0
    fi
    sleep 1
  done
  echo "Unexpected count for ${url}: ${count}, expected ${expected}" >&2
  return 1
}

trap cleanup EXIT

docker compose -f "${compose_file}" up -d

wait_for_es "http://localhost:9201" "source"
wait_for_es "http://localhost:9202" "destination"

rm -rf "${runs_dir}" "${server_log}"
mkdir -p "${runs_dir}"

curl -s -X PUT "http://localhost:9201/wizard-ticket-000001" \
  -H "Content-Type: application/json" \
  -d '{
    "settings": { "index": { "number_of_shards": 1, "number_of_replicas": 0 } },
    "mappings": {
      "properties": {
        "join": { "type": "join", "relations": { "parent": "child" } },
        "whenInserted": { "type": "date" },
        "message": { "type": "text" }
      }
    }
  }' >/dev/null

curl -s -X POST "http://localhost:9201/wizard-ticket-000001/_bulk" \
  -H "Content-Type: application/x-ndjson" \
  --data-binary $'{"index":{"_id":"p1"}}\n{"join":{"name":"parent"},"whenInserted":"2024-01-01T00:00:00Z","message":"parent one"}\n{"index":{"_id":"c1","routing":"p1"}}\n{"join":{"name":"child","parent":"p1"},"whenInserted":"2024-01-02T00:00:00Z","message":"child one"}\n{"index":{"_id":"p2"}}\n{"join":{"name":"parent"},"whenInserted":"2024-02-01T00:00:00Z","message":"parent two"}\n{"index":{"_id":"c2","routing":"p2"}}\n{"join":{"name":"child","parent":"p2"},"whenInserted":"2024-02-02T00:00:00Z","message":"child two"}\n' >/dev/null

curl -s -X POST "http://localhost:9201/wizard-ticket-000001/_refresh" >/dev/null
curl -s -X POST "http://localhost:9201/_aliases" \
  -H "Content-Type: application/json" \
  -d '{"actions":[{"add":{"index":"wizard-ticket-000001","alias":"wizard-ticket-old"}}]}' >/dev/null

curl -s -X PUT "http://localhost:9202/wizard-ticket-new-000001" \
  -H "Content-Type: application/json" \
  -d '{
    "settings": { "index": { "number_of_shards": 2, "number_of_replicas": 0 } },
    "mappings": {
      "dynamic": true,
      "properties": {
        "join": { "type": "join", "relations": { "parent": "child" } },
        "whenInserted": { "type": "date" },
        "message": { "type": "text" }
      }
    }
  }' >/dev/null

curl -s -X POST "http://localhost:9202/_aliases" \
  -H "Content-Type: application/json" \
  -d '{"actions":[{"add":{"index":"wizard-ticket-new-000001","alias":"wizard-ticket","is_write_index":true}}]}' >/dev/null

if [ ! -x "${ROOT_DIR}/target/debug/es-copy-indices" ] || [ ! -x "${ROOT_DIR}/target/debug/es-copy-indices-server" ]; then
  cargo build --bin es-copy-indices --bin es-copy-indices-server
fi

"${ROOT_DIR}/target/debug/es-copy-indices-server" \
  --bind 127.0.0.1:18182 \
  --base-path / \
  --main-config "${ROOT_DIR}/integration/server-main.toml" \
  --env-templates "${ROOT_DIR}/integration/server-templates" \
  --es-copy-indices-path "${ROOT_DIR}/target/debug/es-copy-indices" \
  --runs-dir "${runs_dir}" \
  --max-concurrent-jobs 5 \
  > "${server_log}" 2>&1 &
server_pid="$!"

wait_for_server

create_headers="$(mktemp)"
curl -s -D "${create_headers}" -o /tmp/wizard-e2e-create.out \
  -H "Content-Type: application/json" \
  -X POST "http://localhost:18182/runs/wizard" \
  -d '{
    "src_endpoint_id": "e2e_source",
    "dst_endpoint_id": "e2e_destination",
    "dry_run": false,
    "defaults": {
      "buffer_size": 2,
      "copy_content": true,
      "copy_mapping": false,
      "delete_if_exists": false,
      "routing_field": "/join/parent",
      "write_existing": true,
      "split_field": "whenInserted",
      "split_parts": 2,
      "number_of_replicas": 0,
      "number_of_shards": 1,
      "alias_enabled": false,
      "alias_remove_if_exists": false
    },
    "rename": { "pattern": "", "replace": "", "prefix": "", "suffix": "" },
    "alias": { "enabled": false, "pattern": "", "replace": "", "prefix": "", "suffix": "" },
    "items": [
      {
        "source_name": "wizard-ticket-old",
        "dest_base_name": "wizard-ticket",
        "alias_base_name": null,
        "overrides": null
      }
    ]
  }'

location=$(awk 'tolower($1) == "location:" {print $2}' "${create_headers}" | tr -d '\r' | tail -n1)
rm -f "${create_headers}"
if [ -z "${location}" ]; then
  echo "Wizard create did not return redirect" >&2
  cat /tmp/wizard-e2e-create.out >&2
  exit 1
fi
run_id="${location##*/}"

test -d "${runs_dir}/${run_id}"

config_count=$(find "${runs_dir}/${run_id}/configs" -type f -name '*.toml' | wc -l | tr -d ' ')
if [ "${config_count}" != "3" ]; then
  echo "Expected 3 split configs, got ${config_count}" >&2
  find "${runs_dir}/${run_id}/configs" -type f -name '*.toml' -print >&2
  exit 1
fi

if ! grep -R 'routing_field = "/join/parent"' "${runs_dir}/${run_id}/configs" >/dev/null; then
  echo "routing_field missing in generated configs" >&2
  exit 1
fi
if ! grep -R 'name_of_copy = "wizard-ticket"' "${runs_dir}/${run_id}/configs" >/dev/null; then
  echo "destination alias missing in generated configs" >&2
  exit 1
fi
if grep -R '\[indices.alias\]' "${runs_dir}/${run_id}/configs" >/dev/null; then
  echo "Wizard write-existing run must not create alias config" >&2
  exit 1
fi
if ! grep -R 'copy_mapping = false' "${runs_dir}/${run_id}/configs" >/dev/null; then
  echo "copy_mapping=false missing in generated configs" >&2
  exit 1
fi
if ! grep -R 'delete_if_exists = false' "${runs_dir}/${run_id}/configs" >/dev/null; then
  echo "delete_if_exists=false missing in generated configs" >&2
  exit 1
fi

curl -s -X POST "http://localhost:18182/runs/${run_id}/stages/copy_wizard_ticket_old/start" >/dev/null

wait_for_count "http://localhost:9202/wizard-ticket" "4"

state=$(curl -s "http://localhost:9202/_alias/wizard-ticket")
if ! printf '%s' "${state}" | grep -q '"is_write_index":true'; then
  echo "Destination write alias was not preserved" >&2
  echo "${state}" >&2
  exit 1
fi

echo "Wizard E2E test OK"
