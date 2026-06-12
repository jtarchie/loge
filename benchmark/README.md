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
well above ~100k lines/s, scale CPUs or run multiple machines; the backpressure path
guarantees you lose nothing while you do. (Run 3 below profiles *which* CPU work is the
ceiling — it is not the part you would guess.)

### Run 3 — tuning the flush path + a CPU profile (2026-06-12)

The ingest knobs added after Run 2 — `--flush-compression` (flush-tier zstd level),
`--flush-workers` / `--flush-queue` (flush+compress pool sizing, independent of bucket
count) — plus a loopback `--pprof-port` are wired through the Fly entrypoint
(`FLUSH_COMPRESSION` / `FLUSH_WORKERS` / `FLUSH_QUEUE` / `PPROF_PORT` env in `fly.toml`).

**Tuned, same 2-CPU hardware** (`FLUSH_COMPRESSION=fastest`, `FLUSH_WORKERS=4`,
`FLUSH_QUEUE=16`), hammered at a 400k-line/s target:

| Config | Achieved | Errors |
|---|---|---|
| Run 2 defaults (better zstd, 2 flush workers) | 111,182 lines/s | 0 |
| Run 3 tuned (fastest zstd, 4 flush workers) | 102,636 lines/s | 0 |

**The flush tuning did *not* raise the 2-CPU ceiling** (102k ≈ 111k, within run/region
noise — Run 3 ran in `ord`, Run 2 in `iad`). A 30-second CPU profile captured mid-hammer
(`fly ssh console` on the machine → `curl localhost:6060/debug/pprof/profile`, since the
listener is loopback-only) shows why — the cycles go almost entirely to two places, and
neither is flush compression:

| CPU cost (cumulative) | Share |
|---|---|
| **JSON decode of the push body** (`loge.bind` → goccy/go-json; mostly `runtime.memmove` + string decode) | **~40%** |
| **SQLite via cgo** — flush `INSERT`s + the compaction merge reading rows back (`cgocall`, `sqlite3_step`) | **~28%** |
| zstd **flush-tier** compression (`fastest`) | **~3%** |
| zstd **compaction** recompression (`Best`) | ~9% |

So `--flush-compression` was tuning a ~3% slice, which is why the needle did not move:
**the real backpressure ceiling on this workload is the request-path JSON decode plus the
SQLite insert/compaction**, not flush/compress.

**Scaling out the bottleneck** — same tuned config on **`performance-8x` (8 CPU / 16 GB,
`FLUSH_WORKERS=8`)**, hammered at 600k:

| Server | Achieved | Errors | vs 2-CPU |
|---|---|---|---|
| perf-2x (2 CPU) | 102,636 lines/s | 0 | — |
| perf-8x (8 CPU) | **316,167 lines/s** (62.8 MB/s) | 66 / 75,885 (0.087%) | **~3.1×** |

~3.1× throughput for 4× the CPUs (sub-linear: SQLite's per-DB locking + the cgo boundary +
GC don't parallelize perfectly), and the **profile keeps the same shape** — JSON decode
~37%, SQLite insert ~29%, flush-tier zstd still ~3%. The flusher *still* logs blocking
backpressure even at 8 CPU, and a tiny 0.087% of pushes exceeded the loadgen's 30 s timeout
under it (still lossless by design — it blocks, never drops).

**Takeaways for where backpressure work actually pays off:**
- The flush knobs (`--flush-compression` / `--flush-workers` / `--flush-queue`) are
  *enablers*, not the lever: on 2 CPUs the decode+insert path saturates the cores, so
  cheaper flush compression just frees cycles decode immediately reclaims. They earn their
  keep only once you add CPUs (8 flush workers on 8 cores keep flush off the critical path).
- The highest-leverage real change is **cutting ingest decode cost**: accept msgpack on the
  push path (loge already decodes it; the realistic loadgen only emits JSON) or optimize the
  JSON bind path — that ~40% is the single biggest slice and is pure request-path CPU.
- Next after that is the **SQLite insert/compaction** (~28%): larger batched inserts, or
  trimming compaction's read-back/recompression cost.
- Throughput scales ~linearly-ish with CPUs, so "add cores" remains the simplest lever for
  raw ingest headroom.

### Run 4 — the decode win (2026-06-12)

The Run 3 profile pointed straight at the request-path JSON decode (~40% of CPU). A
local decoder bake-off (`jsonbench_test.go`) found goccy/go-json was already the fastest
library available, but that `bind()` was calling its **streaming** `Decoder` — which is
~10× slower than `Unmarshal` over a contiguous buffer. The fix (read the body, then
`json.Unmarshal`) is a couple of lines, no new dependency.

Re-hammered on the **same `performance-2x`** with the same 400k-target hammer:

| | Achieved | Errors |
|---|---|---|
| Run 3 (streaming decode) | 102,636 lines/s | 0 |
| **Run 4 (ReadAll + Unmarshal)** | **141,039 lines/s** (28 MB/s) | 0 |

**+37% ingest throughput on identical hardware**, still zero data loss. The fresh CPU
profile confirms the mechanism and the shifted bottleneck:

| CPU cost (cumulative) | Run 3 | Run 4 |
|---|---|---|
| JSON decode (`loge.bind`) | ~40% | **~14%** |
| `runtime.memmove` (flat — streaming byte-shuffle) | ~31% | **~2%** |
| **SQLite via cgo** (flush `INSERT`s + compaction) | ~28% | **~42%** (now #1) |

The gain is sub-proportional to the decode saving (Amdahl): freeing ~26% of CPU lifts
throughput ~37% because the remaining work — now dominated by the **SQLite insert path** —
becomes the ceiling. That's the next lever (batched inserts / lighter compaction), followed
still by msgpack ingest to shrink decode further.

### Run 5 — attacking the SQLite cost (2026-06-12)

Run 4 left SQLite-via-cgo as the #1 cost (~42%), split between the flush **write** and the
compaction **merge**. Profiling separated the two and only one had a SQLite-specific win:

- **Compaction merge — the win.** `copyFileInto` copied every source row into Go
  (`rows.Next`/`Scan`) and re-`Exec`'d it into the segment — two cgo crossings per row, the
  `SQLiteRows.Next` the profile flagged. Replaced with a SQLite-native merge: **`ATTACH` each
  source** (a read-only zstd-compressed db, reachable via `file:<abs>?vfs=zstd&immutable=1` —
  the ATTACH URI resolves the same zstd VFS the reads use) and two **`INSERT...SELECT`s** that
  offset label ids in SQL, so the whole copy runs in SQLite's C core with no per-row round-trip.
  Local benchmark (`BenchmarkCompactMerge`, 16×2000 rows): **~305 ms → ~225 ms (−26%)** and
  **986k → 130k allocs/op (−87%)** — far less GC pressure.
- **Flush write — already optimal.** The flush insert is b-tree-bound, not crossing-bound:
  raising the multi-row `INSERT` batch size 8× (`maxBatchInsert` 500→4000, 8× fewer statements)
  changed nothing (`BenchmarkFlush` ~43 ms either way). Multi-row `INSERT` in a `journal_mode=OFF`
  transaction is already the fast path; reverted.

**Live (same perf-2x, same 400k hammer as Run 4):**

| | Achieved | `SQLiteRows.Next` (compaction read) |
|---|---|---|
| Run 4 | 141,039 lines/s | ~12% |
| **Run 5** | **146,466 lines/s** (+3.9%) | **~9%** |

The live lift is modest because compaction is a **background** task — its CPU only intermittently
competes with the continuous foreground flush+decode, so cutting it nudges the ceiling rather
than moving it like the decode fix did. The local merge numbers are the cleaner measure of the
change. Profiling also surfaced the next compaction-read hotspot: `buildLineFilter` still scans
every line in Go to hash trigrams (the remaining ~9%); shrinking that, plus msgpack ingest for
decode, are the open levers.

---

## Files

- `cmd/loge-loadgen/` — realistic, CGO-free Go log generator.
- `benchmark/query/{label,hot,cold,keyword}.js` — k6 query scenarios.
- `Dockerfile` / `scripts/entrypoint.sh` — loge server image (CGO; env → flags).
- `Dockerfile.loadgen` — static loadgen image.
- `fly.toml` — loge app (volume hot tier, Tigris cold tier, aggressive rotation).
- `GET /api/v1/stats` — catalog tiering summary used by `cold.js` and `task bench:stats`.
