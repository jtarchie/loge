# Benchmarking loge

Two layers of benchmark live here:

- **Ingest micro/throughput** — the original k6 push scripts (`streams.*.js`) and the
  Go `benchmark_test.go` benches, driven by `task bench`.
- **Real-world end-to-end** — deploy loge to **Fly.io** with **Tigris** (Fly's
  S3-compatible store) as the cold tier, drive it with the realistic Go generator
  (`cmd/loge-loadgen`) + the k6 **query** scenarios (`benchmark/query/*.js`), and
  measure ingest, hot-vs-cold search latency, and the **cold-start catalog rebuild**.

The realistic generator uses **bounded label cardinality** (`app, env, level, host,
service`) and **real words** in the lines — unlike the old random-key generator, which
exploded label cardinality and made FTS meaningless. High-cardinality IDs
(`trace_id`, `user_id`) live in the log **line**, not the labels, which is how you should
model them in loge — and makes them needle-in-haystack keyword targets.

## ⚠️ loge reads cold segments via public, unauthenticated HTTP

The seekable-zstd VFS fetches cold segments with plain range `GET`s to
`readURLBase + "/" + key` — **no auth, no query string** (go-sqlite3 strips it). So the
Tigris bucket must serve objects **publicly**. The entrypoint uploads with
`--s3-acl public-read` and reads from `https://<bucket>.t3.tigrisfiles.io`. **Your cold
log segments become world-readable.** That's fine for synthetic benchmark data; a real
deployment needs a CDN/auth-proxy or a change to loge's read model.

---

## Local quickstart (no cloud)

Validates the whole loop — ingest → realistic logs → label/keyword/window search — in
one terminal, no Fly/Tigris:

```sh
go build -tags fts5 -o /tmp/loge ./loge/main.go
/tmp/loge --port 6500 --buckets 2 --payload-size 200 --compact-interval 3s --output-path /tmp/loge-data &

task bench:loadgen TARGET=http://localhost:6500 DURATION=30s RATE=8000
task bench:stats                      # local vs remote segment counts (remote=0 locally)
task bench:query                      # runs label/hot/cold/keyword k6 (needs k6 installed)
/tmp/loge search '{app="search"} |= "connection refused"' --addr http://localhost:6500
```

(S3 tiering is off locally — `segments_remote` stays 0. Use the Fly path below to
exercise cold reads and the rebuild.)

---

## Real-world run on Fly.io + Tigris

### 0. Prereqs
`flyctl` (logged in), `k6`, and Docker (Fly builds remotely, so local Docker is optional).

### 1. Provision

```sh
fly launch --no-deploy --copy-config --name loge-bench      # uses fly.toml
fly volumes create loge_data --region iad --size 20         # hot tier (size for the hot window)
fly storage create                                          # provisions Tigris; sets
   # BUCKET_NAME, AWS_ENDPOINT_URL_S3, AWS_REGION, AWS_ACCESS_KEY_ID/SECRET as app secrets
```

**Make the bucket public** so loge can read segments back. Either set it in the Tigris
dashboard (Fly dashboard → Tigris → your bucket → public), or with the AWS CLI against
the Tigris endpoint:

```sh
export AWS_ENDPOINT_URL_S3=https://fly.storage.tigris.dev   # values from `fly storage create`
aws s3api put-bucket-acl --bucket "$BUCKET_NAME" --acl public-read --endpoint-url $AWS_ENDPOINT_URL_S3
```

(The server also sets `--s3-acl public-read` per object, so a public bucket is belt-and-suspenders.)

### 2. Deploy loge

```sh
task bench:fly:deploy            # fly deploy
curl -s https://loge-bench.fly.dev/api/v1/labels    # should return []
```

`fly.toml` sets an aggressive `S3_ROTATE_AGE=2m` so segments tier to Tigris *during* the
run — that's what creates the hot/cold split the search benchmark needs.

### 3. Ingest

Either run the generator **locally** against the public URL (simple, lower throughput):

```sh
task bench:loadgen TARGET=https://loge-bench.fly.dev RATE=20000 DURATION=30m
```

…or, for production-like throughput, run it as a **one-off Fly machine in the same
region**, pushing over the private network:

```sh
fly deploy --dockerfile Dockerfile.loadgen --build-only --push -a loge-bench   # prints an image ref
fly machine run <image-ref> -a loge-bench --region iad --vm-cpus 4 -- \
  --target http://loge-bench.internal:8080 --rate 50000 --workers 16 --duration 30m
```

Watch tiering as it happens:

```sh
watch -n5 'task bench:stats BASE=https://loge-bench.fly.dev'
# segments_remote climbs as segments age past S3_ROTATE_AGE and rotate to Tigris
```

### 4. Search benchmark

Once `segments_remote > 0` (a real hot/cold split exists):

```sh
task bench:query BASE=https://loge-bench.fly.dev
```

- **label.js** — `{app=…, level=~"err.*"}` matching.
- **hot.js** — recent window → local segments only.
- **cold.js** — auto-targets the oldest data (via `/api/v1/stats` `min_timestamp`) → Tigris range GETs. Compare p95 to hot.
- **keyword.js** — FTS trigram substring search (`connection refused`, `/checkout`, …).

Capture each script's k6 summary (p50/p95/p99 + error rate). The **hot vs cold p95
delta** and Tigris GET/egress (Tigris dashboard) are the headline numbers.

### 5. Cold-start rebuild (the new feature, in the cloud)

Prove a fresh node rebuilds its catalog from Tigris with **zero per-file opens**:

```sh
# Wipe the hot volume's catalog and restart, OR launch a fresh machine on an empty volume:
fly ssh console -a loge-bench -C "sh -c 'rm -f /data/catalog.sqlite*'"
fly machine restart <machine-id> -a loge-bench

fly logs -a loge-bench | grep "rediscovered remote segments"   # ReconcileRemote ran
time curl -s https://loge-bench.fly.dev/api/v1/stats           # segments_remote restored from the listing
task bench:query BASE=https://loge-bench.fly.dev               # cold/keyword queries work immediately
```

Record **boot → first-successful-query** time and the rebuild log line.

### 6. Metrics to collect

| Metric | Where |
|--------|-------|
| Ingest lines/s, MB/s, errors | `loge-loadgen` summary |
| Query p50/p95/p99 per scenario | k6 end-of-run summary |
| Hot vs cold latency delta | compare `hot.js` vs `cold.js` p95 |
| Tigris GET count + egress | Tigris dashboard (egress is the real $ cost — reads are public GETs) |
| Local vs remote segments, rows, span | `GET /api/v1/stats` |
| Hot-tier disk vs cold-tier size → compression ratio | `fly ssh console -C "du -sh /data"` vs Tigris bucket size |
| Cold-start rebuild time | `fly logs` + `time` on first query |

### 7. Teardown

```sh
task bench:fly:teardown            # fly apps destroy loge-bench
fly volumes destroy <vol-id>
fly storage destroy <bucket>       # or keep for re-runs
```

---

## Files

- `cmd/loge-loadgen/` — realistic, CGO-free Go log generator.
- `benchmark/query/{label,hot,cold,keyword}.js` — k6 query scenarios.
- `Dockerfile` / `scripts/entrypoint.sh` — loge server image (CGO + fts5; env → flags).
- `Dockerfile.loadgen` — static loadgen image.
- `fly.toml` — loge app (volume hot tier, Tigris cold tier, aggressive rotation).
- `GET /api/v1/stats` — catalog tiering summary used by `cold.js` and `task bench:stats`.
