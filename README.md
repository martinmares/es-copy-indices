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

