#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

compose_file="${ROOT_DIR}/integration/docker-compose.yml"
backup_dir="${ROOT_DIR}/integration/backup"

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

docker compose -f "${compose_file}" up -d

wait_for_es "http://localhost:9201" "source"
wait_for_es "http://localhost:9202" "destination"

${ROOT_DIR}/integration/seed.sh

if [ ! -x "${ROOT_DIR}/target/debug/es-copy-indices" ]; then
  cargo build --bin es-copy-indices
fi

rm -rf "${backup_dir}"
mkdir -p "${backup_dir}"

${ROOT_DIR}/target/debug/es-copy-indices -c "${ROOT_DIR}/integration/copy.toml"

curl -s -X POST "http://localhost:9202/test-index-copy/_refresh" >/dev/null
count_copy=0
for _ in {1..20}; do
  count_copy=$(curl -s "http://localhost:9202/test-index-copy/_count" | grep -oE '"count":[0-9]+' | awk -F: '{print $2}')
  if [ "${count_copy}" = "3" ]; then
    break
  fi
  sleep 1
done
if [ "${count_copy}" != "3" ]; then
  echo "Unexpected count for copy index: ${count_copy}" >&2
  exit 1
fi

${ROOT_DIR}/target/debug/es-copy-indices -c "${ROOT_DIR}/integration/copy-routing.toml"

curl -s -X POST "http://localhost:9202/pc-index-copy/_refresh" >/dev/null
count_pc_copy=0
for _ in {1..20}; do
  count_pc_copy=$(curl -s "http://localhost:9202/pc-index-copy/_count" | grep -oE '"count":[0-9]+' | awk -F: '{print $2}')
  if [ "${count_pc_copy}" = "2" ]; then
    break
  fi
  sleep 1
done
if [ "${count_pc_copy}" != "2" ]; then
  echo "Unexpected count for pc-index copy: ${count_pc_copy}" >&2
  exit 1
fi

${ROOT_DIR}/target/debug/es-copy-indices -c "${ROOT_DIR}/integration/backup.toml"

test -f "${backup_dir}/test-index/metadata.json"
test -f "${backup_dir}/test-index/mappings.json"
test -f "${backup_dir}/test-index/settings.json"
ls "${backup_dir}/test-index/data/"*.jsonl.zst >/dev/null

${ROOT_DIR}/target/debug/es-copy-indices -c "${ROOT_DIR}/integration/restore.toml"

curl -s -X POST "http://localhost:9202/test-index-restore/_refresh" >/dev/null
count_restore=0
for _ in {1..20}; do
  count_restore=$(curl -s "http://localhost:9202/test-index-restore/_count" | grep -oE '"count":[0-9]+' | awk -F: '{print $2}')
  if [ "${count_restore}" = "3" ]; then
    break
  fi
  sleep 1
done
if [ "${count_restore}" != "3" ]; then
  echo "Unexpected count for restore index: ${count_restore}" >&2
  exit 1
fi

${ROOT_DIR}/target/debug/es-copy-indices -c "${ROOT_DIR}/integration/backup-routing.toml"
${ROOT_DIR}/target/debug/es-copy-indices -c "${ROOT_DIR}/integration/restore-routing.toml"

curl -s -X POST "http://localhost:9202/pc-index-restore/_refresh" >/dev/null
count_pc_restore=0
for _ in {1..20}; do
  count_pc_restore=$(curl -s "http://localhost:9202/pc-index-restore/_count" | grep -oE '"count":[0-9]+' | awk -F: '{print $2}')
  if [ "${count_pc_restore}" = "2" ]; then
    break
  fi
  sleep 1
done
if [ "${count_pc_restore}" != "2" ]; then
  echo "Unexpected count for pc-index restore: ${count_pc_restore}" >&2
  exit 1
fi

docker compose -f "${compose_file}" down -v

echo "Integration test OK"
