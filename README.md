# loge

> "Let's capture your logs!"

`loge` captures logs into compressed SQLite files and lets you query them by
time range, labels, and log-line content. It speaks a Grafana-Loki-style push
API and ships a `loge search` CLI for querying from the terminal.

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

- **Ingest** is batched in memory and flushed to small, immutable,
  zstd-compressed SQLite files. With the write-ahead log enabled (default) each
  payload is fsynced before `/push` is acknowledged, so an acknowledged log is
  not lost on a crash (at-least-once; replayed on restart). A checkpoint keeps
  the log bounded: once a payload is durably in a queryable segment, its log
  record is pruned, so the WAL only ever holds the un-flushed tail (seconds of
  data) rather than the whole session.
- **Compaction** runs in the background, merging the many small flush files into
  fewer, larger, time-local segments and building the expensive indexes once per
  segment instead of once per flush. Each segment is recorded in a small local
  **catalog** (`catalog.sqlite`) with its time bounds and label keys.
- **Queries** ask the catalog for the segments whose time bounds overlap the
  requested window (pruning the rest without opening any file), match labels
  with `json_extract`, accelerate line search with the segment trigram index,
  fan out across the surviving segments in parallel, and merge results
  newest-first.
- **S3 tiering** (optional): segments older than `--s3-rotate-age` are rotated
  to S3 and the catalog is flipped to point at their public URL; recent segments
  stay local. Queries read cold (S3) segments back over HTTP via the
  seekable-zstd VFS, fetching only the bytes an indexed query needs (with
  decoded frames cached). Pruning means most queries touch S3 zero times; an S3
  outage degrades a query to its local results rather than failing. Ingest
  durability is unaffected — S3 only ever receives already-durable, compacted
  segments.

## HTTP API

### `POST /api/v1/push`

Accepts a stream payload as `application/json`, `application/msgpack`, or
`application/protobuf`:

```json
{
  "streams": [
    { "stream": { "app": "web" }, "values": [["<unix_ns>", "log line"]] }
  ]
}
```

Returns `200 OK` when the payload is durably logged (write-ahead log enabled),
or `202 Accepted` when running with `--durable=false` (async, lost on crash).
Invalid payloads (no streams, missing labels/values) return `400`.

### `GET /api/v1/labels`

Returns the set of label keys across all files:
`{ "status": "success", "data": [...] }`.

### `POST /api/v1/query`

Body (all fields optional):

```json
{
  "start": 1700000000000000000,
  "end": 1700000009000000000,
  "matchers": [{ "name": "app", "value": "web", "type": "=" }],
  "line": "error",
  "limit": 100
}
```

`start`/`end` are inclusive nanosecond timestamps (`0` = unbounded). Matcher
`type` is one of `=`, `!=`, `=~`, `!~` (regex matchers are applied in Go).
`line` is a substring filter on the log line. Response:

```json
{
  "status": "success",
  "data": [
    {
      "timestamp": 1700000000000000000,
      "line": "...",
      "labels": { "app": "web" }
    }
  ]
}
```

Instead of structured `matchers`/`line`, you can pass a single LogQL-style
selector as `query` (e.g. `{ "query": "{app=\"web\"} |= \"error\"" }`); the
server parses it into matchers + a line filter, the same syntax `loge search`
and the web UI use.

When `--api-key` is set, this endpoint (and `/labels`, `/stats`, `/search/plan`)
requires `Authorization: Bearer <key>`; ingest (`/push`) stays open.

## Web UI

The server hosts a small web UI at `/` so you can query logs from a browser when
the CLI isn't handy. It runs the same LogQL selectors, supports light/dark mode,
and can live-tail by polling. When `--api-key` is set it prompts for the key
(stored in the browser and sent as a bearer token); otherwise it's open. The UI
is built from `web/` into `webdist/` and embedded into the binary, so no extra
serving is needed — just open `http://localhost:3000/`.

## Command-line search

`loge search` queries a running server over the `POST /api/v1/query` API using a
LogQL-style selector, so you can read logs from the terminal without crafting
JSON. It builds the same request the API consumes, so its results match the
server.

```sh
loge search '{app="web", level=~"err.*"} |= "timeout"' --addr http://localhost:3000 --since 1h
```

The positional selector has two parts:

- **Stream-label matchers** in `{ ... }` — comma-separated `name op "value"`
  pairs, where `op` is one of `=`, `!=`, `=~`, `!~` (the same operators as the
  query API; `=~`/`!~` are regex). These filter the JSON stream labels. The
  block may be empty (`{}`) or omitted when only a keyword filter is given.
- An optional **keyword filter** `|= "term"` — a substring matched against the
  log line via the trigram (FTS5) index. Only `|=` is supported (regex/negated
  line filters are not), and `term` must be at least 3 characters so it can use
  the index.

| Flag       | Default                 | Description                                                              |
| ---------- | ----------------------- | ------------------------------------------------------------------------ |
| `--addr`   | `http://localhost:3000` | base URL of the running loge server                                      |
| `--since`  | –                       | relative window start, e.g. `1h`, `30m` (from now)                       |
| `--until`  | –                       | relative window end, e.g. `5m` ago (from now)                            |
| `--start`  | –                       | absolute window start: RFC3339 or unix nanoseconds (overrides `--since`) |
| `--end`    | –                       | absolute window end: RFC3339 or unix nanoseconds (overrides `--until`)   |
| `--limit`  | `100`                   | max results (server caps at 5000)                                        |
| `--output` | `text`                  | output format: `text` or `json`                                          |

```sh
# all "web" logs in the last hour, human-readable
loge search '{app="web"}' --since 1h

# errors containing a keyword, as JSON, over an absolute window
loge search '{level=~"err.*"} |= "connection refused"' \
  --start 2026-06-01T00:00:00Z --end 2026-06-02T00:00:00Z --output json

# keyword-only search across all streams
loge search '|= "panic"'
```

## Server flags

The server is the default command, so `loge` and `loge serve` are equivalent.

| Flag                     | Default | Description                                                                                            |
| ------------------------ | ------- | ------------------------------------------------------------------------------------------------------ |
| `--port`                 | `3000`  | HTTP server port                                                                                       |
| `--buckets`              | `4`     | number of in-memory bucket workers                                                                     |
| `--payload-size`         | `1000`  | batch size before a forced flush                                                                       |
| `--output-path`          | `tmp/`  | directory for the SQLite files                                                                         |
| `--flush-interval`       | `1s`    | how often a bucket flushes a non-empty batch                                                           |
| `--compact-interval`     | `30s`   | how often to merge small files into segments (`0` disables)                                            |
| `--compact-min-files`    | `8`     | minimum flush files before a compaction pass runs                                                      |
| `--durable`              | `true`  | fsync each payload to the write-ahead log before acknowledging                                         |
| `--checkpoint-interval`  | `2s`    | how often to fsync new segments and prune the write-ahead log                                          |
| `--drop-on-backpressure` | `false` | drop instead of blocking when the flush pipeline is saturated (ignored when `--durable`)               |
| `--query-concurrency`    | `8`     | max segments a query opens in parallel                                                                 |
| `--api-key`              | `""`    | shared secret (bearer token) required to read/query and to load the web UI's data; empty disables auth |
| `--s3-bucket`            | `""`    | S3 bucket to rotate old segments into (empty disables S3 tiering)                                      |
| `--s3-prefix`            | `loge/` | key prefix for uploaded segments                                                                       |
| `--s3-endpoint`          | `""`    | custom S3 endpoint (e.g. MinIO); empty uses AWS                                                        |
| `--s3-region`            | `""`    | S3 region (else from the AWS environment)                                                              |
| `--s3-force-path-style`  | `false` | path-style addressing (needed for MinIO)                                                               |
| `--s3-read-url-base`     | `""`    | public/CDN base URL reads are served from; empty derives it                                            |
| `--s3-acl`               | `""`    | canned ACL for uploads (e.g. `public-read`); empty relies on bucket policy                             |
| `--s3-rotate-age`        | `1h`    | rotate local segments older than this to S3                                                            |
| `--s3-rotate-interval`   | `1m`    | how often the rotation loop runs                                                                       |
| `--s3-rotate-grace`      | `1m`    | keep a rotated segment's local copy this long before deleting it                                       |
| `--s3-frame-cache-size`  | `512`   | per-file decoded zstd frames cached for segment reads (each frame is ~64 KiB)                          |

S3 reads are **public, path-based** HTTP GETs (auth must not ride in the URL
query string — `go-sqlite3` strips it), so the bucket/prefix must be readable
without signed query params (public objects, a bucket policy, or a CDN). Upload
credentials come from the standard AWS chain (env / shared config / IAM role).

## Development

```sh
task test    # ginkgo -tags fts5 -race
task bench    # k6 push benchmarks (msgpack/json/protobuf)
task web     # bundle the web UI (web/ -> webdist/, esbuild)
task server   # run a local server (rebuilds the web UI first)

go build -tags fts5 -o loge ./loge/main.go   # build the `loge` binary (serve + search)
```

The web UI bundle in `webdist/` is committed so `go build`/`go install` work
without a Node toolchain. After editing anything under `web/`, run `task web`
and commit the regenerated `webdist/`.
