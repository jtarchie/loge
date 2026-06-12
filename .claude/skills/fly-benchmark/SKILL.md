---
name: fly-benchmark
description: Run loge's real-world end-to-end benchmark on Fly.io + Tigris — provision, deploy, ingest, measure hot-vs-cold query latency, test the cold-start catalog rebuild, hammer for max ingest throughput, and tear down. Use when asked to "run the fly benchmark", "rerun the fly.io benchmark", "benchmark loge on fly", "hammer the fly server", or to produce before/after numbers for benchmark/README.md.
---

# loge Fly.io + Tigris benchmark — runbook

This is the operational companion to [benchmark/README.md](../../../benchmark/README.md). The
README explains *what* and *why*; this skill is the *exact sequence that works*, including the
gotchas that cost time on previous runs. Read the README's "Sample run + findings" for the
reference numbers any new run should be compared against.

## What this measures
1. **Ingest** throughput (realistic bounded-cardinality generator, JSON wire format).
2. **Hot vs cold query latency** (local segments vs Tigris range-GETs), single-VU p95.
3. **Cold-start catalog rebuild** from the S3 listing (zero per-file opens).
4. **Max ingest ceiling** ("hammer") via an in-region loadgen machine.
5. **Compression** (wire JSON → zstd segments on Tigris).

## Prereqs
- `flyctl` logged in (`fly auth whoami`), `k6`, `aws` CLI. Docker NOT required (Fly builds remotely).
- Run all `fly` commands with `-a loge-bench`. Run query/loadgen `task` commands from repo root.
- Naming: app `loge-bench`, volume `loge_data`, region `iad`. Pick a **unique** bucket name
  per run (Tigris names are global); a dated name like `loge-bench-cold-YYYYMMDD` works.

---

## Phase 0 — provision (~2 min)

```sh
fly apps create loge-bench --org personal
fly volumes create loge_data --region iad --size 20 -a loge-bench --yes
fly storage create -a loge-bench -n loge-bench-cold-YYYYMMDD -p -y   # -p = PUBLIC bucket (required!)
```

- `fly storage create` **stages the S3 secrets** (`BUCKET_NAME`, `AWS_*`) on the app for the
  first deploy. Capture the printed `AWS_ACCESS_KEY_ID/SECRET` + `BUCKET_NAME` — you need them
  for the `aws s3 ls` compression check later.
- **The bucket MUST be public** (`-p`). loge reads cold segments via plain unauthenticated HTTP
  range-GETs (`go-sqlite3` strips query strings, so presigned URLs can't work). Without `-p`,
  cold reads 403 and `cold.js`/`keyword.js` fail.

## Phase 1 — deploy (~3 min, remote build)

```sh
fly deploy -a loge-bench                       # builds Dockerfile (server) from fly.toml
```

Wait for DNS + health, then confirm. **DNS can lag ~30–60 s after deploy** (`curl` exits 6 until
it resolves). Poll instead of one-shotting:

```sh
until curl -sf https://loge-bench.fly.dev/api/v1/labels >/dev/null; do sleep 10; done
curl -s https://loge-bench.fly.dev/api/v1/stats      # rows_total:0 to start
```

`fly.toml` sets `S3_ROTATE_AGE=2m` / `S3_ROTATE_INTERVAL=30s` / `COMPACT_INTERVAL=15s` so
segments tier to Tigris *during* the run — that's what creates the hot/cold split.

Grab the server machine id now — you need it for the hammer's private DNS target:
```sh
SERVER_ID=$(fly machine list -a loge-bench | grep -oE '^[ ]*[a-f0-9]{14}' | tr -d ' ' | head -1)
```

## Phase 2 — control ingest (4 min)

Reproduces the README baseline (apples-to-apples for the query deltas). Run from repo root:

```sh
task bench:loadgen TARGET=https://loge-bench.fly.dev RATE=20000 DURATION=4m
```
Expect **~19,900 lines/s, 0 errors, ~948 MB wire**. (`task bench:loadgen` builds & runs
`cmd/loge-loadgen` locally; a home uplink sustains 20k/s fine.)

## Phase 3 — wait for tiering

```sh
until curl -s https://loge-bench.fly.dev/api/v1/stats | grep -qE '"segments_remote":[1-9]'; do sleep 15; done
curl -s https://loge-bench.fly.dev/api/v1/stats      # segments_remote climbs as segments age past 2m
```

## Phase 4 — query benchmark (single VU)

Single-VU p95 is the comparison methodology (isolates per-query latency, not QPS). Keep the
server otherwise idle so latencies aren't skewed. Run hot → label → cold, then keyword:

```sh
VUS=1 BASE=https://loge-bench.fly.dev k6 run benchmark/query/hot.js
VUS=1 BASE=https://loge-bench.fly.dev k6 run benchmark/query/label.js
VUS=1 BASE=https://loge-bench.fly.dev k6 run benchmark/query/cold.js
VUS=1 BASE=https://loge-bench.fly.dev DURATION=60s k6 run benchmark/query/keyword.js
```
Pull `http_req_duration … p(95)=…` from each summary. Reference (after the query-path rework):
hot ~73 ms, label ~85 ms, cold ~75 ms, **keyword unbounded ~84 ms** (was ~37 s — the cliff is
gone). For bounded-keyword numbers, POST `/api/v1/query` with `{"line":"connection refused",
"start":<min>,"end":<min+300e9>,"limit":100}` (min/max from `/api/v1/stats`).

## Phase 5 — cold-start catalog rebuild

The README suggests `rm -f /data/catalog.sqlite*` over SSH — **the Claude auto-permission
classifier blocks remote `rm` as destructive**, and it's risky. Use the empty-volume clone
instead (same code path, nothing deleted):

```sh
fly machine clone $SERVER_ID -a loge-bench --region iad --name rebuild-test --detach
# note the NEW machine id it prints (e.g. d8d0...); poll IT specifically:
until curl -s -H "fly-force-instance-id: <NEW_ID>" https://loge-bench.fly.dev/api/v1/stats \
      | grep -q '"segments_remote":12'; do sleep 1; done
fly logs -a loge-bench --machine <NEW_ID> --no-tail | grep "rediscovered remote segments"
# cleanup: destroy the clone AND its orphaned empty volume
fly machine destroy <NEW_ID> -a loge-bench --force
fly volumes list -a loge-bench           # find the unattached loge_data volume the clone made
fly volumes destroy <orphan-vol-id> -a loge-bench --yes
```
Expect: `rediscovered remote segments from S3 listing segments=12`, in-process rebuild < 2 s,
~24 s clone→ready incl. provisioning; first cold query ~1.3 s, first keyword ~2.5 s.

## Phase 6 — hammer (max ingest) ⚠️ most gotcha-prone

Drive the server from an **in-region loadgen machine** over the private network (your home
uplink can't saturate it). Three landmines, all hit on previous runs:

**Gotcha A — building the loadgen image.** `fly deploy --dockerfile Dockerfile.loadgen
--build-only` **silently ignores `--dockerfile`** and builds `Dockerfile` (the server) from
fly.toml. You get a 46 MB image that runs `entrypoint.sh` (a second loge server), pushes 0
rows, and never exits. Build it by swapping fly.toml's `[build] dockerfile`, then reverting:

```sh
sed -i.bak 's#dockerfile = "Dockerfile"#dockerfile = "Dockerfile.loadgen"#' fly.toml
fly deploy --build-only --push -a loge-bench      # MUST print "image size: ~6.6 MB" (not ~46 MB)
mv fly.toml.bak fly.toml                          # revert (or: git checkout fly.toml)
LOADGEN_IMG=registry.fly.io/loge-bench:deployment-<the-id-it-printed>
```

**Gotcha B — DNS round-robin.** `loge-bench.internal` resolves to **every** app machine,
including the loadgen machine itself (which isn't listening on 8080 → connection refused for
those batches). Target the server's **specific** machine DNS: `<SERVER_ID>.vm.loge-bench.internal`.

**Gotcha C — wrong-image safety net.** Force the entrypoint so a stray server image fails fast
instead of silently running a server: `--entrypoint /usr/local/bin/loge-loadgen`.

```sh
fly machine run $LOADGEN_IMG -a loge-bench --region iad \
  --vm-cpu-kind performance --vm-cpus 8 --vm-memory 16384 \
  --entrypoint /usr/local/bin/loge-loadgen --name hammer \
  -- --target http://$SERVER_ID.vm.loge-bench.internal:8080 \
     --rate 400000 --workers 48 --duration 90s
```
The loadgen prints its summary **only at the end**, in the machine's `fly logs` (it's a one-off
machine, not local). Poll until stopped, then read it:
```sh
HID=$(fly machine list -a loge-bench | grep hammer | grep -oE '[a-f0-9]{14}' | head -1)
until fly machine status $HID -a loge-bench | grep -qiE 'state:[[:space:]]*stopped'; do sleep 8; done
fly logs -a loge-bench --machine $HID --no-tail | grep -iE 'summary|lines:|errors|bytes'
fly machine destroy $HID -a loge-bench --force
```

**Interpreting results (don't misread these):**
- Reference ceiling on `performance-2x` (2 CPU): **~110k lines/s, ~22 MB/s, 0 errors.** A 100k
  target gave ~92k/s; a 400k target gave only ~111k/s — it *plateaus*.
- Past the ceiling the server logs `flusher backpressure, blocking until flush completes` — it
  **blocks ingest, never drops**. All pushes still return 2xx (the generator counts only 2xx as
  success, so "0 errors" is real acceptance, not optimistic counting).
- `rows_total` will **lag far behind** the lines pushed during the burst (the flush→segment→
  catalog pipeline can't keep 110k/s on 2 CPUs; `/data` balloons to ~1 GB of buffered data).
  This is **catalog lag, not data loss** — confirm by re-sampling `/api/v1/stats` a minute after
  load stops and watching `rows_total` climb back up. CPU bottleneck is the flush SQLite
  writes + background compaction (timestamp index, binary-fuse trigram filter, zstd) — note
  there is **no FTS index** anymore; keyword search is a `LIKE` scan + trigram segment filter.

## Phase 7 — compression metrics

```sh
fly ssh console -a loge-bench -C "du -sh /data"     # hot tier (≈ catalog only after full rotation)
AWS_ACCESS_KEY_ID=<id> AWS_SECRET_ACCESS_KEY=<secret> AWS_REGION=auto \
  aws s3 ls s3://<bucket> --recursive --summarize --endpoint-url https://fly.storage.tigris.dev | tail -2
```
Control run reference: 947.5 MB wire → 228 MB across 12 Tigris segments ≈ **4.2×** compression.

## Phase 8 — teardown (do this promptly — it's what costs money)

```sh
fly apps destroy loge-bench --yes                  # also destroys the attached volume
fly storage destroy <bucket> --yes                 # Tigris bucket
fly storage list | grep loge-bench || echo "clean" # verify gone
```
Destroying the app removes its machine + attached volume. **Orphaned volumes from `fly machine
clone` (Phase 5) are NOT** — destroy those separately first, or they linger and bill. Pushed
loadgen/server images in the registry are negligible and can be left.

---

## Quick reference — gotcha cheat sheet
| Symptom | Cause | Fix |
|---|---|---|
| loadgen pushes 0 rows, machine never exits | built/ran the 46 MB server image | swap fly.toml `[build]`, verify ~6.6 MB; `--entrypoint /usr/local/bin/loge-loadgen` |
| ~half the hammer batches error | `loge-bench.internal` round-robins to loadgen machine | target `<SERVER_ID>.vm.loge-bench.internal` |
| `curl` exits 6 right after deploy | DNS not propagated | poll `until curl -sf …/labels` |
| cold reads 403 | bucket not public | `fly storage create -p` (or `aws s3api put-bucket-acl --acl public-read`) |
| remote `rm /data/catalog.*` denied | auto-permission classifier blocks destructive SSH | use `fly machine clone` on empty volume instead |
| `rows_total` ≪ lines pushed | flush/catalog lag under overload (not loss) | re-sample after load stops; it drains back up |
| `min_timestamp` flat / no remote segments | rotation age is 2m; not enough time | wait `until segments_remote >= 1` |
