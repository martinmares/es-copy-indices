#!/bin/bash
set -euo pipefail

ES_SOURCE="http://localhost:9201"

curl -s -X PUT "${ES_SOURCE}/test-index" \
  -H "Content-Type: application/json" \
  -d '{
    "settings": { "index": { "number_of_shards": 1, "number_of_replicas": 0 } },
    "mappings": {
      "properties": {
        "message": { "type": "text" },
        "timestamp": { "type": "date" }
      }
    }
  }' >/dev/null

curl -s -X POST "${ES_SOURCE}/test-index/_bulk" \
  -H "Content-Type: application/x-ndjson" \
  --data-binary $'{"index":{"_id":"1"}}\n{"message":"one","timestamp":"2024-01-01T00:00:00Z"}\n{"index":{"_id":"2"}}\n{"message":"two","timestamp":"2024-01-02T00:00:00Z"}\n{"index":{"_id":"3"}}\n{"message":"three","timestamp":"2024-01-03T00:00:00Z"}\n' >/dev/null

curl -s -X POST "${ES_SOURCE}/test-index/_refresh" >/dev/null

curl -s -X POST "${ES_SOURCE}/_aliases" \
  -H "Content-Type: application/json" \
  -d '{
    "actions": [
      { "add": { "index": "test-index", "alias": "test-index-alias" } }
    ]
  }' >/dev/null

curl -s -X PUT "${ES_SOURCE}/pc-index" \
  -H "Content-Type: application/json" \
  -d '{
    "settings": { "index": { "number_of_shards": 1, "number_of_replicas": 0 } },
    "mappings": {
      "properties": {
        "join": {
          "type": "join",
          "relations": { "parent": "child" }
        },
        "message": { "type": "text" }
      }
    }
  }' >/dev/null

curl -s -X POST "${ES_SOURCE}/pc-index/_doc/p1" \
  -H "Content-Type: application/json" \
  -d '{"join":{"name":"parent"},"message":"parent"}' >/dev/null

curl -s -X POST "${ES_SOURCE}/pc-index/_doc/c1?routing=p1" \
  -H "Content-Type: application/json" \
  -d '{"join":{"name":"child","parent":"p1"},"message":"child"}' >/dev/null

curl -s -X POST "${ES_SOURCE}/pc-index/_refresh" >/dev/null

curl -s -X POST "${ES_SOURCE}/_aliases" \
  -H "Content-Type: application/json" \
  -d '{
    "actions": [
      { "add": { "index": "pc-index", "alias": "pc-index-alias" } }
    ]
  }' >/dev/null
