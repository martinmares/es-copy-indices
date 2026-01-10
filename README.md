# es-copy-indices

CLI utility for copying Elasticsearch indices between clusters, including mappings, settings, aliases, and documents. It supports both the classic scroll API and the newer PIT-based "scrolling search" (PIT + `search_after`).

## Features
- Copy mappings and settings to the destination index (or use a custom JSON file).
- Copy documents with scroll API or scrolling search; optional auto-fallback.
- Optional alias creation/removal.
- Supports wildcard index patterns (`multiple = true`).
- Basic auth and custom root certificates for HTTPS.
- Audit log for bulk requests and responses.

## Requirements
- Rust toolchain (stable).
- Access to source and destination Elasticsearch clusters.

## Build
```bash
cargo build --release
```

## Usage
```bash
RUST_LOG=info ./target/release/es-copy-indices -c ./conf/main.toml
```

## Server Mode (`es-copy-indices-server`)
The server wraps `es-copy-indices` with a web UI for running many jobs in parallel, splitting large indices by date, and tracking logs/progress. It reads endpoints from a main config file and index templates from a directory.

### Build
```bash
cargo build --bin es-copy-indices-server
```

### Run
```bash
./target/debug/es-copy-indices-server \
  --main-config ./conf/main-server.toml \
  --env-templates ./conf/templates \
  --ca-path ./certs \
  --runs-dir ./runs \
  --bind 0.0.0.0:8080
```

### main-server.toml
```toml
[[endpoints]]
  name = "REF prostředí"
  url = "http://celzisr401.server.cetin:9200"
  prefix = "tsm-ref"
  number_of_replicas = 0
  keep_alive = "10m"
  auth = { username = "empty", password = "empty" }

[[endpoints]]
  name = "TEST prostředí"
  url = "http://celzist401.server.cetin:9200"
  prefix = "tsm-test"
  number_of_replicas = 0
  keep_alive = "10m"
  auth = { username = "empty", password = "empty" }
```

Notes:
- `auth` is optional. If omitted, no basic auth is used.
- `prefix` is concatenated directly (`prefix + index_name`) to match legacy index naming.
- `number_of_replicas` and `keep_alive` are per-endpoint defaults used when generating the final TOML for jobs.

HTTPS example with self-signed certs:
```toml
[[endpoints]]
  name = "PROD TLS"
  url = "https://es-prod.local:9200"
  prefix = "tsm"
  number_of_replicas = 1
  keep_alive = "10m"
  auth = { username = "elastic", password = "secret" }
```

Run with CA bundle:
```bash
./target/debug/es-copy-indices-server \
  --main-config ./conf/main-server.toml \
  --env-templates ./conf/templates \
  --ca-path ./certs
```

If you must bypass TLS validation for percentile queries:
```bash
./target/debug/es-copy-indices-server \
  --main-config ./conf/main-server.toml \
  --env-templates ./conf/templates \
  --insecure
```

### Templates directory (`--env-templates`)
Each template file contains only `[[indices]]` (no `[env]`). Optional metadata lives under `[global]`.

Example `./conf/templates/normal.toml`:
```toml
[global]
  name = "Zero replicas"

[[indices]]
  name = "ticket"
  buffer_size = 2500
  number_of_shards = 10
  routing_field = "/joinField/parent"

  [indices.split]
    field_name = "whenInserted"
    number_of_parts = 10
```

The template name shown in UI is:
- `global.name` if present,
- otherwise the file name.

### UI flow
- Home page lists runs and updates via SSE (no page refresh).
- Create New Run opens a modal where you select:
  - source endpoint
  - destination endpoint
  - template
- Optional Dry run skips `es-copy-indices` execution and marks jobs succeeded (for testing).
- If source == destination, a warning is shown (allowed).
- Run details show `SRC → DST + template` and per-endpoint settings.
- Each run card has a Remove button (with confirmation). Running jobs cannot be removed.
- Run page actions: Export run ZIP (configs/logs/metadata) and Retry Failed jobs.
- Logs stream via SSE with ANSI color support, per-stream filtering, and copy-to-clipboard.
- Exported ZIP strips ANSI escape codes from `runs/<id>/logs/*.log` for readability.
- Status page shows CPU/memory and load graphs for host + process.

### Support helpers
- Export run ZIP to share configs/logs/metadata with L2 support.
- Retry Failed to re-run only the failed jobs without rebuilding the run.
- Dry run to validate pipelines without touching Elasticsearch.
- Log filter + copy to clipboard for fast diagnostics.

### Server CLI reference (selected)
Flags are strict CLI-only (no env fallbacks).

- `--main-config PATH`: main config with endpoints.
- `--env-templates DIR`: templates directory.
- `--es-copy-indices-path PATH`: explicit path to the `es-copy-indices` binary (default resolves from `PATH`).
- `--from-index-name-suffix SUFFIX`: adds `-SUFFIX` to source index name.
- `--index-copy-suffix SUFFIX`: adds `-SUFFIX` to destination index name.
- `--alias-suffix SUFFIX`: adds `-SUFFIX` to alias name.
- `--alias-remove-if-exists`: if set, alias removal is enabled (default false).
- `--audit`: if set, audit logging is enabled (default false).
- `--timestamp STRING`: override timestamp used in `name_of_copy`.
- `--ca-path DIR`: PEM directory for HTTPS.
- `--insecure`: disable TLS verify for percentile queries (useful with self-signed).
- `--runs-dir DIR`: store run history/logs (default `./runs`).
- `--base-path PATH`: reverse-proxy base path (e.g. `/es-copy-indices`).

### Troubleshooting
- `percentile request failed: 404` usually means the source index name is wrong:
  - wrong endpoint,
  - wrong prefix,
  - or `--from-index-name-suffix` is set incorrectly.
- If templates directory is empty or invalid, the server will refuse to start.
- If `--base-path` is set, open `http://host:port/<base-path>` (no trailing slash).

## How It Works
1) Detects server info for both source and destination clusters.
2) Optionally creates the destination index with mappings and settings.
3) Reads documents from the source index (scroll API or PIT-based search).
4) Writes documents to the destination index in bulk.
5) Optionally updates aliases.

## Configuration (TOML)
The config is loaded from a TOML file passed via `-c/--config`.

### Minimal Example
```toml
[[endpoints]]
name = "source"
url = "https://source-es:9200"

[[endpoints]]
name = "dest"
url = "https://dest-es:9200"

[[indices]]
name = "my-index"
from = "source"
to = "dest"
buffer_size = 1000
copy_mapping = true
copy_content = true
```

### Full Example
```toml
[[endpoints]]
name = "source"
url = "https://source-es:9200"
timeout = 90

[endpoints.basic_auth]
username = "elastic"
password = "changeme"

# Optional: directory with PEM certificates
# root_certificates = "/path/to/certs"

[[endpoints]]
name = "dest"
url = "https://dest-es:9200"

[[indices]]
name = "my-index"
from = "source"
to = "dest"
buffer_size = 1000
keep_alive = "5m"
copy_mapping = true
copy_content = true
scroll_mode = "scroll_api" # scroll_api | scrolling_search | auto

# Optional: name for the destination index
name_of_copy = "my-index-copy"

# Optional: copy multiple indices by pattern
# multiple = true

# Optional: alias management
[indices.alias]
name = "my-index-alias"
remove_if_exists = true

# Optional: custom query/sort/doc_type/mapping
[indices.custom]
query = "{ \"match_all\": {} }"
sort = "{ \"_doc\": \"asc\" }"
doc_type = "_doc"
mapping = "conf/custom-mapping.json"

# Optional: routing-based pre-create
# routing_field = "/parent/id"
# pre_create_doc_ids = true
# pre_create_doc_source = "{ \"esCopyIndicesPreCreatedParent\": true }"

# Optional: override shard/replica settings for the destination
# number_of_shards = 1
# number_of_replicas = 1

[audit]
file_name = "audit/es-copy-indices.log"
enabled = true
```

## Configuration Reference

### endpoints
- `name` (string, required): Identifier used by indices (`from`/`to`).
- `url` (string, required): Elasticsearch base URL.
- `timeout` (number, optional, default 90): HTTP timeout in seconds.
- `basic_auth` (object, optional):
  - `username` (string, required)
  - `password` (string, optional)
- `root_certificates` (string, optional): Directory with PEM files to trust.

### indices
- `name` (string, required): Source index name or pattern.
- `from` (string, required): Endpoint name of the source cluster.
- `to` (string, required): Endpoint name of the destination cluster.
- `name_of_copy` (string, optional): Destination index name.
- `multiple` (bool, optional, default false): Treat `name` as a wildcard pattern and copy all matches.
- `buffer_size` (number, required): Batch size per request.
- `keep_alive` (string, optional, default `5m`): Scroll or PIT keep-alive.
- `copy_mapping` (bool, required): Copy mappings and settings.
- `copy_content` (bool, required): Copy documents.
- `scroll_mode` (string, optional, default `scroll_api`): `scroll_api`, `scrolling_search`, or `auto`.
- `routing_field` (string, optional): JSON Pointer to routing id (RFC 6901).
- `pre_create_doc_ids` (bool, optional, default true): Pre-create routed parent docs.
- `pre_create_doc_source` (string, optional, default `{}`): Source JSON for pre-created docs.
- `number_of_shards` (number, optional, default 1): Override shard count in destination settings.
- `number_of_replicas` (number, optional, default 1): Override replica count in destination settings.
- `custom` (object, optional):
  - `query` (string, optional): JSON string used as query.
  - `sort` (string, optional): JSON string used as sort.
  - `doc_type` (string, optional): Legacy doc type for ES <= 7.
  - `mapping` (string, optional): Path to a JSON file containing `settings` and `mappings`.
- `alias` (object, optional):
  - `name` (string, required): Alias name.
  - `remove_if_exists` (bool, optional, default false): Remove alias from other indices first.
- `delete_if_exists` (bool, optional): Defined in config, not currently used in code.

### audit
- `file_name` (string, required): Path to the audit log file.
- `enabled` (bool, optional, default false): Defined in config but not currently enforced in code.

## Scroll Modes
- `scroll_api`: Classic Elasticsearch scroll API (default).
- `scrolling_search`: PIT + `search_after` ("scrolling search").
- `auto`: Tries scrolling search first, falls back to scroll API when PIT is not supported.

The selected mode is logged at startup for each index copy.

## Custom Mapping File
`indices.custom.mapping` must point to a JSON file that includes `settings` and `mappings`, for example:
```json
{
  "settings": {
    "index": {
      "number_of_shards": 1,
      "number_of_replicas": 1
    }
  },
  "mappings": {
    "properties": {
      "message": { "type": "text" }
    }
  }
}
```

## Examples

### Copy a single index
```toml
[[indices]]
name = "logs-2024-05"
from = "source"
to = "dest"
buffer_size = 2000
copy_mapping = true
copy_content = true
```

### Copy multiple indices by pattern
```toml
[[indices]]
name = "logs-*"
multiple = true
from = "source"
to = "dest"
buffer_size = 1000
copy_mapping = true
copy_content = true
```

### Use PIT-based scrolling search
```toml
[[indices]]
name = "events"
from = "source"
to = "dest"
buffer_size = 5000
copy_mapping = true
copy_content = true
scroll_mode = "scrolling_search"
```

## Logging and Troubleshooting
- Use `RUST_LOG=info` or `RUST_LOG=debug` for more detail.
- Audit log captures bulk requests and responses when `[audit]` is present.
