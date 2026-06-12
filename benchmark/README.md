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
go build -o /tmp/loge ./loge/main.go
/tmp/loge --port 6500 --buckets 2 --payload-size 200 --compact-interval 3s --output-path /tmp/loge-data &

task bench:loadgen TARGET=http://localhost:6500 DURATION=30s RATE=8000
task bench:stats                      # local vs remote segment counts (remote=0 locally)
task bench:query                      # runs label/hot/cold/keyword k6 (needs k6 installed)
/tmp/loge search '{app="search"} |= "connection refused"' --addr http://localhost:6500
```

(S3 tiering is off locally — `segments_remote` stays 0. Use the Fly path below to
exercise cold reads and the rebuild.)

---

## Profiling (pprof)

The server exposes `net/http/pprof` on a **loopback-only** listener when started with
`--pprof-port <port>` (off by default — heap profiles can expose log contents, so it
never binds a public interface; on Fly use `fly proxy 6060:6060` to reach it).
`--pprof-block-rate` / `--pprof-mutex-fraction` additionally enable block/mutex
contention sampling — useful for the channel-based buckets and the query fan-out.

Automated captures (timestamped output under `profiles/`, gitignored):

```sh
task profile:ingest                   # cpu/heap/allocs/goroutine/mutex/block under loadgen
                                      # ingest + fast compaction (vars: RATE, DURATION, SECONDS)
task profile:query                    # seed with loadgen, then capture under k6 hot.js query
                                      # load (vars: SEED_RATE, SEED_DURATION, SECONDS)
task profile:bench BENCH=BenchmarkBucketsParallelWithBackpressure   # go test -cpuprofile/-memprofile
task profile:bench DIR=worker BENCH=BenchmarkEntries                # worker module benches
task profile:web                      # open the newest cpu.pprof in the pprof web UI
```

Benchmark regression loop (local, benchstat-based):

```sh
task bench:save                       # write profiles/bench/baseline.txt (root + worker, -count 6)
# ...make changes...
task bench:diff                       # re-run and benchstat against the baseline
```

For the client-side (`--local`) search path, profile the sizebench harness directly:

```sh
go test -tags "fts5 sqlite_dbstat sizebench" -run TestQueryLatency -cpuprofile cpu.pprof
```

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
# ⚠️ `fly deploy --dockerfile X --build-only` IGNORES --dockerfile and builds the
# Dockerfile from fly.toml (the server!). Confirm the image you get is ~6 MB, not ~46 MB.
# Build the loadgen image by pointing fly.toml's [build] at it, then reverting:
sed -i.bak 's#dockerfile = "Dockerfile"#dockerfile = "Dockerfile.loadgen"#' fly.toml
fly deploy --build-only --push -a loge-bench        # prints an image ref; expect "image size: ~6.6 MB"
mv fly.toml.bak fly.toml                             # revert

# Run it. --entrypoint forces the loadgen binary (belt-and-suspenders vs the wrong image).
# Target the server's SPECIFIC machine (<id>.vm.<app>.internal), NOT loge-bench.internal —
# the app-wide name round-robins across every machine, including the loadgen machine itself.
fly machine run <image-ref> -a loge-bench --region iad --vm-cpu-kind performance --vm-cpus 4 \
  --entrypoint /usr/local/bin/loge-loadgen -- \
  --target http://<server-machine-id>.vm.loge-bench.internal:8080 \
  --rate 50000 --workers 16 --duration 30m
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

## Sample run + findings

Both runs are on the same hardware — one Fly `iad` `performance-2x` (2 CPU / 4 GB),
20 GB volume, public Tigris bucket, ~4 min ingest at a 20k-line/s target. "Run 1" is
the original; "Run 2" is the 2026-06-12 re-run after the query-path rework (drop FTS5
entirely → sequential LIKE + binary-fuse trigram segment pruning, decode-after-merge-cap).

- **Ingest** held steady across the rework: Run 1 **4.78 M lines @ ~19,900 lines/s**
  (948 MB wire, 0 errors); Run 2 **4.77 M lines @ 19,875 lines/s** (947.5 MB wire, 0
  errors). The control workload is unchanged, so the query deltas below are apples-to-apples.
- **Tiering / compression:** segments zstd-compress to ~**4.2×** vs wire JSON (Run 2:
  947.5 MB wire → 228 MB across 12 Tigris segments; hot tier after full rotation is just
  the catalog, ~1 MB). Cold reads over the public bucket worked with no degradation.

### Per-query latency, single VU (p95) — before → after

| Scenario | Run 1 (before) | Run 2 (after) |
|---|---|---|
| label, unbounded (`{app=…, level=~"err.*"}`) | 84 ms | 85 ms |
| hot window (recent → local segments) | 55 ms | 73 ms |
| cold window over Tigris (oldest data) | 74 ms | 75 ms |
| keyword, bounded to a 5 m window | 130 ms – 1.2 s | **182 ms** |
| **keyword, UNBOUNDED across Tigris segments** | **~37 s** | **84 ms** |

✅ **The keyword cliff is gone.** Run 1's pathology — unbounded FTS keyword search across
multiple Tigris segments taking **~37 s** (many small *random* range-GETs into a remote
trigram index) — is **~84 ms in Run 2 (~440× faster)**, now in the same ballpark as a
label scan. **FTS5 was dropped entirely** (an SQLite FTS5 trigram index cost ~3.3× the
segment size yet lost to a plain scan): keyword search is now a sequential `LIKE`, with a
small serialized **binary-fuse trigram filter** per segment that prunes whole segments a
keyword can't be in. That turns cold keyword search into large contiguous reads instead of
many random index lookups over HTTP. Run 2 was
also *harder* — 10–12 of 12 segments were already on Tigris during the suite (vs 3 of 5 in
Run 1), so most of these numbers are against an almost-entirely-remote store and still
sub-100 ms. Time-bounding keyword search is still good hygiene (it prunes remote segments
outright), but it is no longer load-bearing for correctness of latency.

### Cold-start catalog rebuild (Run 2)

A fresh machine on an **empty volume** rebuilt its catalog purely from the Tigris listing —
log line `rediscovered remote segments from S3 listing segments=12`, no per-file opens. The
in-process rebuild finished **< 2 s** after boot (≈ 24 s end-to-end including Fly machine
provisioning); first cold-window query **1.3 s**, first keyword query **2.5 s** (both warm
the seekable-zstd frame cache), steady-state thereafter.

### Max-throughput "hammer" (Run 2)

Pushing as hard as the 2-CPU server will go, via an in-region loadgen machine over the
private 6PN network (so the client/uplink is not the bottleneck):

| Loadgen | Target rate | Achieved | Errors |
|---|---|---|---|
| `performance-4x`, 24 workers | 100k lines/s | **92,516 lines/s** (18.4 MB/s) | 0 |
| `performance-8x`, 48 workers | 400k lines/s | **111,182 lines/s** (22.1 MB/s) | 0 |

**The ceiling for this hardware is ~110k lines/s (~22 MB/s) with zero data loss.** Past
that, the **flusher applies backpressure** (`flusher backpressure, blocking until flush
completes`) — it *blocks* ingest rather than dropping, so the effective accept-rate
plateaus near 110k/s no matter how hard you push (a 4× higher target yielded only ~20%
more throughput). Every push still returned HTTP 2xx (the generator counts only 2xx as
success), and no machine restarted/OOM'd. The catalog's `rows_total` *lagged* the accepted
volume during the burst (the flush→segment→catalog pipeline can't keep pace at 110k/s on
2 CPUs) and then **drained back up** once load stopped — i.e. the **accept ceiling
(~110k/s) is higher than the sustainable segment/catalog rate**. For sustained ingest
well above ~100k lines/s, scale CPUs (flush writes + background compaction/zstd are the
CPU bottleneck) or run
multiple machines; the backpressure path guarantees you lose nothing while you do.

---

## Files

- `cmd/loge-loadgen/` — realistic, CGO-free Go log generator.
- `benchmark/query/{label,hot,cold,keyword}.js` — k6 query scenarios.
- `Dockerfile` / `scripts/entrypoint.sh` — loge server image (CGO; env → flags).
- `Dockerfile.loadgen` — static loadgen image.
- `fly.toml` — loge app (volume hot tier, Tigris cold tier, aggressive rotation).
- `GET /api/v1/stats` — catalog tiering summary used by `cold.js` and `task bench:stats`.
