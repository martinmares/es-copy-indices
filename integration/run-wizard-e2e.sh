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
    count=$(curl -s "${url}/_count" | jq -r '.count // 0')
    if [ "${count}" = "${expected}" ]; then
      return 0
    fi
    sleep 1
  done
  echo "Unexpected count for ${url}: ${count}, expected ${expected}" >&2
  return 1
}

function seed_alias_graph_index() {
  local index_name="$1"
  local month="$2"
  local shards="$3"
  curl -s -X PUT "http://localhost:9201/${index_name}" \
    -H "Content-Type: application/json" \
    -d "{
      \"settings\": { \"index\": { \"number_of_shards\": ${shards}, \"number_of_replicas\": 0 } },
      \"mappings\": { \"properties\": { \"whenInserted\": { \"type\": \"date\" }, \"message\": { \"type\": \"keyword\" } } }
    }" >/dev/null
  curl -s -X POST "http://localhost:9201/${index_name}/_bulk" \
    -H "Content-Type: application/x-ndjson" \
    --data-binary $'{"index":{"_id":"1"}}\n{"whenInserted":"2024-'"${month}"$'-01T00:00:00Z","message":"one"}\n{"index":{"_id":"2"}}\n{"whenInserted":"2024-'"${month}"$'-02T00:00:00Z","message":"two"}\n' >/dev/null
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

seed_alias_graph_index "wizard-order-000001" "01" "1"
seed_alias_graph_index "wizard-order-000002" "02" "4"
seed_alias_graph_index "wizard-order-000003" "03" "2"
curl -s -X POST "http://localhost:9201/_aliases" \
  -H "Content-Type: application/json" \
  -d '{"actions":[
    {"add":{"index":"wizard-order-000001","alias":"wizard-order","is_write_index":true}},
    {"add":{"index":"wizard-order-000002","alias":"wizard-order","is_write_index":false}},
    {"add":{"index":"wizard-order-000003","alias":"wizard-order","is_write_index":false}},
    {"add":{"index":"wizard-order-000001","alias":"wizard-order-active","is_write_index":true}},
    {"add":{"index":"wizard-order-000002","alias":"wizard-order-active","is_write_index":false}}
  ]}' >/dev/null

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

cargo build --bin es-copy-indices --bin es-copy-indices-server

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

alias_headers="$(mktemp)"
curl -s -D "${alias_headers}" -o /tmp/wizard-alias-graph-create.out \
  -H "Content-Type: application/json" \
  -X POST "http://localhost:18182/runs/wizard" \
  -d '{
    "src_endpoint_id": "e2e_source",
    "dst_endpoint_id": "e2e_destination",
    "dry_run": false,
    "defaults": {
      "buffer_size": 2,
      "copy_content": true,
      "copy_mapping": true,
      "delete_if_exists": false,
      "routing_field": null,
      "write_existing": false,
      "split_field": "whenInserted",
      "split_parts": 2,
      "number_of_replicas": 0,
      "number_of_shards": null,
      "alias_enabled": true,
      "alias_remove_if_exists": false
    },
    "rename": { "pattern": "", "replace": "", "prefix": "", "suffix": "" },
    "alias": { "enabled": true, "pattern": "", "replace": "", "prefix": "", "suffix": "" },
    "items": [
      {
        "source_name": "wizard-order",
        "dest_base_name": "wizard-order",
        "alias_base_name": "wizard-order",
        "overrides": null
      },
      {
        "source_name": "wizard-order-active",
        "dest_base_name": "wizard-order-active",
        "alias_base_name": "wizard-order-active",
        "overrides": null
      }
    ]
  }'

alias_location=$(awk 'tolower($1) == "location:" {print $2}' "${alias_headers}" | tr -d '\r' | tail -n1)
rm -f "${alias_headers}"
if [ -z "${alias_location}" ]; then
  echo "Alias graph Wizard create did not return redirect" >&2
  cat /tmp/wizard-alias-graph-create.out >&2
  exit 1
fi
alias_run_id="${alias_location##*/}"
alias_config_dir="${runs_dir}/${alias_run_id}/configs"

alias_config_count=$(find "${alias_config_dir}" -type f -name '*.toml' | wc -l | tr -d ' ')
if [ "${alias_config_count}" != "9" ]; then
  echo "Expected three split configs per deduplicated physical index, got ${alias_config_count}" >&2
  find "${alias_config_dir}" -type f -name '*.toml' -print >&2
  exit 1
fi
if grep -R '__expanded_' "${alias_config_dir}" >/dev/null; then
  echo "Deduplicated Wizard configs must not contain fallback expanded names" >&2
  exit 1
fi
if [ "$(grep -R -l '\[\[indices.aliases\]\]' "${alias_config_dir}" | wc -l | tr -d ' ')" != "6" ]; then
  echo "Expected all split configs of two physical indices to contain multiple aliases" >&2
  exit 1
fi
for expected in \
  "copy_wizard_order_000001_split_1.toml:1" \
  "copy_wizard_order_000002_split_1.toml:4" \
  "copy_wizard_order_000003_split_1.toml:2"; do
  config_name="${expected%%:*}"
  shard_count="${expected##*:}"
  if ! grep -q "number_of_shards = ${shard_count}" "${alias_config_dir}/${config_name}"; then
    echo "Source shard count was not inherited for ${config_name}" >&2
    exit 1
  fi
done

for stage_id in copy_wizard_order_000001 copy_wizard_order_000002 copy_wizard_order_000003; do
  curl -s -X POST "http://localhost:18182/runs/${alias_run_id}/stages/${stage_id}/start" >/dev/null
done

wait_for_count "http://localhost:9202/wizard-order-000001" "2"
wait_for_count "http://localhost:9202/wizard-order-000002" "2"
wait_for_count "http://localhost:9202/wizard-order-000003" "2"

for expected in \
  "wizard-order-000001:1" \
  "wizard-order-000002:4" \
  "wizard-order-000003:2"; do
  index_name="${expected%%:*}"
  shard_count="${expected##*:}"
  actual_shards=$(curl -s "http://localhost:9202/${index_name}/_settings" | jq -r ".[\"${index_name}\"].settings.index.number_of_shards")
  if [ "${actual_shards}" != "${shard_count}" ]; then
    echo "Unexpected destination shard count for ${index_name}: ${actual_shards}" >&2
    exit 1
  fi
done

order_alias=$(curl -s "http://localhost:9202/_alias/wizard-order")
active_alias=$(curl -s "http://localhost:9202/_alias/wizard-order-active")
if [ "$(printf '%s' "${order_alias}" | jq 'length')" != "3" ]; then
  echo "Main alias does not point to all three destination indices" >&2
  echo "${order_alias}" >&2
  exit 1
fi
if [ "$(printf '%s' "${active_alias}" | jq 'length')" != "2" ]; then
  echo "Active alias does not preserve its two-index subset" >&2
  echo "${active_alias}" >&2
  exit 1
fi
if [ "$(printf '%s' "${order_alias}" | jq -r '.["wizard-order-000001"].aliases["wizard-order"].is_write_index')" != "true" ]; then
  echo "Main alias write index was not preserved" >&2
  exit 1
fi
if [ "$(printf '%s' "${active_alias}" | jq -r '.["wizard-order-000001"].aliases["wizard-order-active"].is_write_index')" != "true" ]; then
  echo "Active alias write index was not preserved" >&2
  exit 1
fi

echo "Wizard E2E test OK"
