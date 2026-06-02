# loge

> "Let's capture your logs!"

`loge` captures logs into compressed SQLite files and lets you query them by time
range, labels, and log-line content. It speaks a Grafana-Loki-style push API.

## How it works

```
POST /api/v1/push ──► bucket workers batch ──► flush to <bucket>.sqlite.zst
        │                                              │
        └──► write-ahead log (fsync) ──► 200 OK        ▼
                                              background compactor merges
                                              many small files into larger
                                              segment-*.sqlite.zst with a
                                              trigram line index + time index
```

- **Ingest** is batched in memory and flushed to small, immutable, zstd-compressed
  SQLite files. With the write-ahead log enabled (default) each payload is fsynced
  before `/push` is acknowledged, so an acknowledged log is not lost on a crash
  (at-least-once; replayed on restart). A checkpoint keeps the log bounded: once a
  payload is durably in a queryable segment, its log record is pruned, so the WAL
  only ever holds the un-flushed tail (seconds of data) rather than the whole
  session.
- **Compaction** runs in the background, merging the many small flush files into
  fewer, larger, time-local segments and building the expensive indexes once per
  segment instead of once per flush.
- **Queries** prune files whose stored time bounds fall outside the requested
  window, match labels with `json_extract`, accelerate line search with the
  segment trigram index (falling back to a scan on freshly flushed files), and
  merge results newest-first across files.

## HTTP API

### `POST /api/v1/push`

Accepts a stream payload as `application/json`, `application/msgpack`, or
`application/protobuf`:

```json
{ "streams": [ { "stream": { "app": "web" }, "values": [ ["<unix_ns>", "log line"] ] } ] }
```

Returns `200 OK` when the payload is durably logged (write-ahead log enabled), or
`202 Accepted` when running with `--durable=false` (async, lost on crash). Invalid
payloads (no streams, missing labels/values) return `400`.

### `GET /api/v1/labels`

Returns the set of label keys across all files: `{ "status": "success", "data": [...] }`.

### `POST /api/v1/query`

Body (all fields optional):

```json
{
  "start": 1700000000000000000,
  "end":   1700000009000000000,
  "matchers": [ { "name": "app", "value": "web", "type": "=" } ],
  "line": "error",
  "limit": 100
}
```

`start`/`end` are inclusive nanosecond timestamps (`0` = unbounded). Matcher `type`
is one of `=`, `!=`, `=~`, `!~` (regex matchers are applied in Go). `line` is a
substring filter on the log line. Response:

```json
{ "status": "success", "data": [ { "timestamp": 1700000000000000000, "line": "...", "labels": { "app": "web" } } ] }
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | `3000` | HTTP server port |
| `--buckets` | `4` | number of in-memory bucket workers |
| `--payload-size` | `1000` | batch size before a forced flush |
| `--output-path` | `tmp/` | directory for the SQLite files |
| `--flush-interval` | `1s` | how often a bucket flushes a non-empty batch |
| `--compact-interval` | `30s` | how often to merge small files into segments (`0` disables) |
| `--compact-min-files` | `8` | minimum flush files before a compaction pass runs |
| `--durable` | `true` | fsync each payload to the write-ahead log before acknowledging |
| `--checkpoint-interval` | `2s` | how often to fsync new segments and prune the write-ahead log |
| `--drop-on-backpressure` | `false` | drop instead of blocking when the flush pipeline is saturated (ignored when `--durable`) |

## Development

```sh
task test    # ginkgo -tags fts5 -race
task bench    # k6 push benchmarks (msgpack/json/protobuf)
task server   # run a local server
```
