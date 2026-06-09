//go:build sizebench

// Size-measurement harness for segment storage. It is build-tagged out of the
// normal suite; run it explicitly:
//
//	go test -tags "fts5 sqlite_dbstat sizebench" -run SegmentSize -v .
//
// It builds one representative segment from a fixed corpus, prints the
// uncompressed per-object byte breakdown (via the dbstat vtab) and the
// compressed .sqlite.zst size for a sweep of page_size / frame_size / zstd level
// / index variants, and dumps EXPLAIN QUERY PLAN for the canonical queries so we
// can confirm idx_streams_label_id is unused. Every number here is for picking
// the production defaults in bucket.go / compactor.go; nothing asserts.
package loge

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg"
	_ "github.com/jtarchie/sqlitezstd" // registers the read-only "zstd" VFS
	"github.com/klauspost/compress/zstd"
)

type corpusRow struct {
	timestamp int64
	line      string
	labelID   int // index into labelPayloads
}

// buildCorpus returns n rows spread across a small set of distinct label sets
// with semi-repetitive structured log lines — the shape real logs have, so the
// label-dedup and FTS effects are visible.
func buildCorpus(n int) ([]corpusRow, []string) {
	rng := rand.New(rand.NewSource(42)) //nolint:gosec // deterministic corpus

	apps := []string{"api", "web", "worker", "db", "cache", "gateway"}
	envs := []string{"prod", "staging"}

	var labelPayloads []string
	for _, app := range apps {
		for _, env := range envs {
			labelPayloads = append(labelPayloads, MarshalLabels(map[string]string{
				"app": app, "env": env, "region": "us-east-1",
			}))
		}
	}

	methods := []string{"GET", "POST", "PUT", "DELETE"}
	paths := []string{"/api/v1/users", "/api/v1/orders", "/api/v1/items", "/healthz", "/metrics"}
	upstreams := []string{"db-primary", "db-replica", "cache-01", "auth-svc"}
	templates := []func() string{
		func() string {
			return fmt.Sprintf(`level=info msg="request completed" method=%s path=%s status=%d duration=%dms bytes=%d`,
				methods[rng.Intn(len(methods))], paths[rng.Intn(len(paths))], 200+rng.Intn(5)*100, rng.Intn(800), rng.Intn(50000))
		},
		func() string {
			return fmt.Sprintf(`level=warn msg="slow query" query_id=%d duration=%dms rows=%d`, rng.Intn(1_000_000), 500+rng.Intn(5000), rng.Intn(10000))
		},
		func() string {
			return fmt.Sprintf(`level=error msg="connection refused" upstream=%s retry=%d`, upstreams[rng.Intn(len(upstreams))], rng.Intn(5))
		},
		func() string {
			return fmt.Sprintf(`level=debug msg="cache lookup" key=user:%d hit=%t`, rng.Intn(100000), rng.Intn(2) == 0)
		},
	}

	rows := make([]corpusRow, n)
	base := int64(1_700_000_000_000_000_000)
	for i := range rows {
		rows[i] = corpusRow{
			timestamp: base + int64(i)*1_000_000, // ~1ms apart, monotonic
			line:      templates[rng.Intn(len(templates))](),
			labelID:   rng.Intn(len(labelPayloads)),
		}
	}
	return rows, labelPayloads
}

type variant struct {
	name        string
	pageSize    int
	fts         bool
	tsIndex     bool
	labelIDIdx  bool
	dedupLabels bool
	labelPairs  bool // build an inverted label_pairs(key,value,label_id) index
}

// buildSegmentDB writes a plain (uncompressed) .sqlite mirroring compactor.merge,
// parameterized so we can A/B individual structures. Returns the file path.
func buildSegmentDB(t *testing.T, dir string, v variant, rows []corpusRow, labelPayloads []string) string {
	t.Helper()
	path := filepath.Join(dir, v.name+".sqlite")

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec(fmt.Sprintf(`
		PRAGMA page_size = %d;
		PRAGMA journal_mode = OFF;
		PRAGMA synchronous = OFF;
		CREATE TABLE labels (id INTEGER PRIMARY KEY, payload BLOB) STRICT;
		CREATE TABLE streams (id INTEGER PRIMARY KEY, timestamp INTEGER, line TEXT, label_id INTEGER) STRICT;
		CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT) STRICT, WITHOUT ROWID;
	`, v.pageSize)); err != nil {
		t.Fatalf("schema: %v", err)
	}

	tx, _ := db.Begin()

	// labels: one row per (label set occurrence). Without dedup we emulate the
	// current compactor, which copies a label row per push-stream per flush; we
	// approximate that by writing one label row per distinct (labelID) *per 200
	// rows* so duplication is realistic. With dedup, one row per distinct set.
	labelRowID := map[int]int64{}
	var nextLabelRow int64
	streamLabelRow := make([]int64, len(rows))
	// inserted records every (rowID, payload) actually written so label_pairs can
	// be built from the real rows (the non-dedup path writes the same set many times).
	type insertedLabel struct {
		id      int64
		payload string
	}
	var inserted []insertedLabel
	if v.dedupLabels {
		for i, r := range rows {
			id, ok := labelRowID[r.labelID]
			if !ok {
				nextLabelRow++
				id = nextLabelRow
				labelRowID[r.labelID] = id
				if _, err := tx.Exec(`INSERT INTO labels (id, payload) VALUES (?, jsonb(?))`, id, labelPayloads[r.labelID]); err != nil {
					t.Fatalf("label: %v", err)
				}
				inserted = append(inserted, insertedLabel{id, labelPayloads[r.labelID]})
			}
			streamLabelRow[i] = id
		}
	} else {
		// Emulate per-flush duplication: a new label row every time the label set
		// "reappears after a flush boundary" (every 200 rows).
		lastFlush := map[int]int{}
		for i, r := range rows {
			flush := i / 200
			if prev, ok := lastFlush[r.labelID]; !ok || prev != flush {
				nextLabelRow++
				labelRowID[r.labelID] = nextLabelRow
				lastFlush[r.labelID] = flush
				if _, err := tx.Exec(`INSERT INTO labels (id, payload) VALUES (?, jsonb(?))`, nextLabelRow, labelPayloads[r.labelID]); err != nil {
					t.Fatalf("label: %v", err)
				}
				inserted = append(inserted, insertedLabel{nextLabelRow, labelPayloads[r.labelID]})
			}
			streamLabelRow[i] = labelRowID[r.labelID]
		}
	}

	stmt, _ := tx.Prepare(`INSERT INTO streams (timestamp, line, label_id) VALUES (?, ?, ?)`)
	for i, r := range rows {
		if _, err := stmt.Exec(r.timestamp, r.line, streamLabelRow[i]); err != nil {
			t.Fatalf("stream: %v", err)
		}
	}
	_ = stmt.Close()

	_, _ = tx.Exec(`INSERT INTO metadata (key, value) VALUES ('minTimestamp', ?), ('maxTimestamp', ?)`,
		rows[0].timestamp, rows[len(rows)-1].timestamp)

	if v.tsIndex {
		if _, err := tx.Exec(`CREATE INDEX idx_streams_timestamp ON streams(timestamp)`); err != nil {
			t.Fatalf("ts idx: %v", err)
		}
	}
	if v.labelIDIdx {
		if _, err := tx.Exec(`CREATE INDEX idx_streams_label_id ON streams(label_id)`); err != nil {
			t.Fatalf("label idx: %v", err)
		}
	}
	if v.fts {
		if _, err := tx.Exec(`CREATE VIRTUAL TABLE line_search USING fts5(line, content='', columnsize=0, tokenize="trigram");
			INSERT INTO line_search(rowid, line) SELECT id, line FROM streams;`); err != nil {
			t.Fatalf("fts: %v", err)
		}
	}

	if v.labelPairs {
		// Inverted index: one row per (key, value, label_id). The composite PK is
		// the index a `key=value` matcher probes; reaching the matching streams then
		// needs idx_streams_label_id, so build it too.
		if _, err := tx.Exec(`CREATE TABLE label_pairs (key TEXT, value TEXT, label_id INTEGER, PRIMARY KEY (key, value, label_id)) WITHOUT ROWID`); err != nil {
			t.Fatalf("label_pairs: %v", err)
		}
		stmt, err := tx.Prepare(`INSERT OR IGNORE INTO label_pairs (key, value, label_id) VALUES (?, ?, ?)`)
		if err != nil {
			t.Fatalf("label_pairs prep: %v", err)
		}
		for _, l := range inserted {
			var kv map[string]string
			if err := json.Unmarshal([]byte(l.payload), &kv); err != nil {
				t.Fatalf("label_pairs decode: %v", err)
			}
			for k, val := range kv {
				if _, err := stmt.Exec(k, val, l.id); err != nil {
					t.Fatalf("label_pairs insert: %v", err)
				}
			}
		}
		_ = stmt.Close()
		if _, err := tx.Exec(`CREATE INDEX IF NOT EXISTS idx_streams_label_id ON streams(label_id)`); err != nil {
			t.Fatalf("label_id idx: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return path
}

// dbstatBreakdown returns per-object uncompressed byte sizes, or nil if the
// dbstat vtab is not compiled in.
func dbstatBreakdown(t *testing.T, path string) map[string]int64 {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil
	}
	defer func() { _ = db.Close() }()

	rows, err := db.Query(`SELECT name, SUM(pgsize) FROM dbstat GROUP BY name ORDER BY 2 DESC`)
	if err != nil {
		t.Logf("dbstat unavailable (build with -tags sqlite_dbstat): %v", err)
		return nil
	}
	defer func() { _ = rows.Close() }()

	out := map[string]int64{}
	for rows.Next() {
		var name string
		var size int64
		_ = rows.Scan(&name, &size)
		out[name] = size
	}
	return out
}

// compressedSize compresses a copy of plainPath via the production path with the
// given encoder pool and returns the resulting .sqlite.zst byte count.
func compressedSize(t *testing.T, plainPath string, pool *sync.Pool) int64 {
	t.Helper()
	tmp := plainPath + ".cmp"
	if err := copyFile(plainPath, tmp); err != nil {
		t.Fatalf("copy: %v", err)
	}
	if err := compressWithPool(tmp, pool); err != nil {
		t.Fatalf("compress: %v", err)
	}
	defer func() { _ = os.Remove(tmp + ".zst") }()
	return fileSize(t, tmp+".zst")
}

// readerOnly hides WriteTo/ReadFrom so io.CopyBuffer actually uses the supplied
// buffer (and thus the seekable writer makes one frame per buffer-sized chunk).
// Without this, *os.File.WriteTo drives the copy and the buffer size is ignored.
type readerOnly struct{ r io.Reader }

func (r readerOnly) Read(p []byte) (int, error) { return r.r.Read(p) }

// compressForced compresses plainPath with a genuinely controlled frame size and
// returns the resulting .sqlite.zst byte count. frameSize<=0 uses the production
// path (os.File.WriteTo framing) for comparison.
func compressForced(t *testing.T, plainPath string, frameSize int, pool *sync.Pool) int64 {
	t.Helper()
	out := plainPath + fmt.Sprintf(".forced%d.zst", frameSize)

	outFile, err := os.Create(out)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() { _ = outFile.Close(); _ = os.Remove(out) }()

	in, err := os.Open(plainPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = in.Close() }()

	enc := pool.Get().(*zstd.Encoder)
	defer pool.Put(enc)
	enc.Reset(nil)

	w, err := seekable.NewWriter(outFile, enc)
	if err != nil {
		t.Fatalf("seekable: %v", err)
	}
	if frameSize <= 0 {
		if _, err := io.Copy(w, in); err != nil { // production-equivalent (WriteTo)
			t.Fatalf("copy: %v", err)
		}
	} else {
		buf := make([]byte, frameSize)
		if _, err := io.CopyBuffer(w, readerOnly{in}, buf); err != nil {
			t.Fatalf("copybuf: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	_ = outFile.Close()
	return fileSize(t, out)
}

// TestEndToEndSizes exercises the REAL production path (flusher -> compress ->
// Compactor.merge -> stripFTSForUpload) so the size win is not just the harness
// builder. It prints the local hot-tier segment size (kept indexed for fast
// local keyword search) and the cold-tier copy actually uploaded to S3.
func TestEndToEndSizes(t *testing.T) {
	rows, labelPayloads := buildCorpus(40_000)
	dir := t.TempDir()

	// Build several flush files via the real flusher + compress.
	const perFile = 4000
	idx := 0
	for start := 0; start < len(rows); start += perFile {
		end := start + perFile
		if end > len(rows) {
			end = len(rows)
		}
		payloads := corpusToPayloads(rows[start:end], labelPayloads)
		name, err := flusher(payloads, dir, idx)
		if err != nil {
			t.Fatalf("flush: %v", err)
		}
		if err := compress(name); err != nil {
			t.Fatalf("compress flush: %v", err)
		}
		idx++
	}

	// Real compaction into one segment (Best level, no label_id index).
	merged, err := NewCompactor(dir, 2, 256, 0).Compact()
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	t.Logf("merged %d flush files", merged)

	segs, _ := filepath.Glob(filepath.Join(dir, "segment-*.sqlite.zst"))
	if len(segs) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(segs))
	}
	seg := fileSize(t, segs[0])

	t.Logf("REAL end-to-end (40k rows): segment (no FTS, Best) = %s; hot and cold tiers are now the same file (uploaded as-is).", human(seg))
}

// corpusToPayloads groups consecutive rows that share a label set into Entries.
func corpusToPayloads(rows []corpusRow, labelPayloads []string) []Payload {
	// One Entry per row keeps it simple; the flusher batches them into one file.
	streams := make(Streams, 0, len(rows))
	for _, r := range rows {
		var labels map[string]string
		_ = json.Unmarshal([]byte(labelPayloads[r.labelID]), &labels)
		streams = append(streams, Entry{
			Stream: labels,
			Values: Values{Value{fmt.Sprintf("%d", r.timestamp), r.line}},
		})
	}
	return []Payload{{Streams: streams}}
}

func TestFrameSizeReal(t *testing.T) {
	rows, labelPayloads := buildCorpus(50_000)
	dir := t.TempDir()
	path := buildSegmentDB(t, dir, variant{name: "full", pageSize: 4096, fts: true, tsIndex: true}, rows, labelPayloads)

	t.Logf("REAL frame-size effect (full segment, 50k rows), Best level:")
	t.Logf("  production (WriteTo) : %s", human(compressForced(t, path, 0, bestEncoderPool)))
	for _, fs := range []int{4 * 1024, 8 * 1024, 32 * 1024, 128 * 1024, 512 * 1024, 4 * 1024 * 1024} {
		t.Logf("  forced %-8s     : %s", human64(int64(fs)), human(compressForced(t, path, fs, bestEncoderPool)))
	}
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()
	_, err = io.Copy(out, in)
	return err
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return info.Size()
}

func TestSegmentSizeBreakdown(t *testing.T) {
	const n = 100_000
	rows, labelPayloads := buildCorpus(n)
	dir := t.TempDir()

	// --- Baseline (current production shape: 4K pages, fts + both indexes) and
	// the post-change shape, plus single-component variants for honest
	// compressed attribution. ---
	variants := []variant{
		{name: "full_4k", pageSize: 4096, fts: true, tsIndex: true, labelIDIdx: true},
		{name: "full_16k", pageSize: 16384, fts: true, tsIndex: true, labelIDIdx: true},
		{name: "full_32k", pageSize: 32768, fts: true, tsIndex: true, labelIDIdx: true},
		{name: "no_labelidx_16k", pageSize: 16384, fts: true, tsIndex: true, labelIDIdx: false},
		{name: "no_fts_16k", pageSize: 16384, fts: false, tsIndex: true, labelIDIdx: false},
		{name: "dedup_16k", pageSize: 16384, fts: true, tsIndex: true, labelIDIdx: false, dedupLabels: true},
		{name: "dedup_nofts_16k", pageSize: 16384, fts: false, tsIndex: true, labelIDIdx: false, dedupLabels: true},
		{name: "streams_only_16k", pageSize: 16384, fts: false, tsIndex: false, labelIDIdx: false, dedupLabels: true},
	}

	t.Logf("corpus: %d rows, %d distinct label sets", n, len(labelPayloads))
	t.Logf("%-20s %12s | dbstat per-object (uncompressed)", "variant", "uncompressed")

	type result struct {
		name         string
		uncompressed int64
		better       int64
		best         int64
	}
	var results []result

	for _, v := range variants {
		path := buildSegmentDB(t, dir, v, rows, labelPayloads)
		uncompressed := fileSize(t, path)
		breakdown := dbstatBreakdown(t, path)

		results = append(results, result{
			name:         v.name,
			uncompressed: uncompressed,
			better:       compressedSize(t, path, betterEncoderPool),
			best:         compressedSize(t, path, bestEncoderPool),
		})

		var parts []string
		names := make([]string, 0, len(breakdown))
		for k := range breakdown {
			names = append(names, k)
		}
		sort.Slice(names, func(i, j int) bool { return breakdown[names[i]] > breakdown[names[j]] })
		for _, k := range names {
			parts = append(parts, fmt.Sprintf("%s=%s", k, human(breakdown[k])))
		}
		t.Logf("%-20s %12s | %s", v.name, human(uncompressed), strings.Join(parts, " "))
	}

	t.Logf("")
	t.Logf("Compressed .sqlite.zst (production framing) — Better / Best:")
	t.Logf("%-20s %12s %12s", "variant", "better", "best")
	for _, r := range results {
		t.Logf("%-20s %12s %12s", r.name, human(r.better), human(r.best))
	}

	// --- EXPLAIN QUERY PLAN on the full baseline: prove the label_id index is
	// never chosen and the timestamp index backs range+order. ---
	t.Logf("")
	t.Logf("EXPLAIN QUERY PLAN (full_4k baseline):")
	explain(t, filepath.Join(dir, "full_4k.sqlite"))
}

func explain(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	queries := map[string]string{
		"label matcher":  `SELECT s.timestamp, s.line, json(l.payload) FROM streams s JOIN labels l ON l.id = s.label_id WHERE json_extract(l.payload, '$."app"') = 'api' ORDER BY s.timestamp DESC, s.id DESC LIMIT 100`,
		"time range":     `SELECT s.timestamp, s.line FROM streams s JOIN labels l ON l.id = s.label_id WHERE s.timestamp >= 1700000000000000000 AND s.timestamp <= 1700000050000000000 ORDER BY s.timestamp DESC, s.id DESC LIMIT 100`,
		"keyword (fts)":  `SELECT s.timestamp, s.line FROM streams s JOIN labels l ON l.id = s.label_id JOIN line_search ON line_search.rowid = s.id WHERE line_search MATCH '"slow query"' AND s.line LIKE '%slow query%' ORDER BY s.timestamp DESC, s.id DESC LIMIT 100`,
		"keyword (like)": `SELECT s.timestamp, s.line FROM streams s JOIN labels l ON l.id = s.label_id WHERE s.line LIKE '%slow query%' ORDER BY s.timestamp DESC, s.id DESC LIMIT 100`,
	}
	names := []string{"label matcher", "time range", "keyword (fts)", "keyword (like)"}
	for _, name := range names {
		rows, err := db.Query("EXPLAIN QUERY PLAN " + queries[name])
		if err != nil {
			t.Logf("  %s: ERROR %v", name, err)
			continue
		}
		var detail []string
		for rows.Next() {
			var id, parent, notused int
			var text string
			_ = rows.Scan(&id, &parent, &notused, &text)
			detail = append(detail, text)
		}
		_ = rows.Close()
		t.Logf("  %-14s: %s", name, strings.Join(detail, " | "))
	}
}

func human(n int64) string { return human64(n) }
func human64(n int64) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%.2fMB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1fKB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%dB", n)
	}
}

// --- Query-latency benchmark -------------------------------------------------
//
// Answers two questions with numbers, on the real compressed read path:
//   1. Does the FTS index earn its size, or is a LIKE scan fast enough?
//   2. Is an inverted label index worth its size vs the json_extract scan?
//
// Run: go test -tags "fts5 sqlite_dbstat sizebench" -run QueryLatency -v -timeout 30m .

const (
	rareNeedle   = "qzx7rare"          // injected into ~1% of lines, uniformly
	commonNeedle = "request completed" // template 0 emits it (~25% of lines)
	absentNeedle = "nonexistentzzz"    // never present (filter prunes in prod)
	// sparseNeedle is the FTS best case / LIKE worst case: it occurs only ~30
	// times and ONLY in the oldest 10% of rows, so an ORDER BY timestamp DESC
	// LIMIT scan cannot short-circuit and must read almost the whole segment.
	sparseNeedle = "wq9sparseold"
	sparseCount  = 30
)

// buildCorpusCard is buildCorpus with a cardinality knob and embedded needles at
// known selectivity, so keyword/label scenarios have exact, repeatable hit rates.
func buildCorpusCard(n int, highCard bool) ([]corpusRow, []string) {
	rng := rand.New(rand.NewSource(42)) //nolint:gosec // deterministic corpus

	apps := []string{"api", "web", "worker", "db", "cache", "gateway"}
	envs := []string{"prod", "staging"}

	var labelPayloads []string
	if highCard {
		// ~2000 distinct sets: a specific pod selects 1/2000 of rows.
		const sets = 2000
		for i := 0; i < sets; i++ {
			labelPayloads = append(labelPayloads, MarshalLabels(map[string]string{
				"app":      apps[rng.Intn(len(apps))],
				"env":      envs[rng.Intn(len(envs))],
				"region":   "us-east-1",
				"pod":      fmt.Sprintf("pod-%d", i),
				"instance": fmt.Sprintf("i-%06d", rng.Intn(100000)),
			}))
		}
	} else {
		for _, app := range apps {
			for _, env := range envs {
				labelPayloads = append(labelPayloads, MarshalLabels(map[string]string{
					"app": app, "env": env, "region": "us-east-1",
				}))
			}
		}
	}

	methods := []string{"GET", "POST", "PUT", "DELETE"}
	paths := []string{"/api/v1/users", "/api/v1/orders", "/api/v1/items", "/healthz", "/metrics"}
	upstreams := []string{"db-primary", "db-replica", "cache-01", "auth-svc"}
	templates := []func() string{
		func() string {
			return fmt.Sprintf(`level=info msg="request completed" method=%s path=%s status=%d duration=%dms bytes=%d`,
				methods[rng.Intn(len(methods))], paths[rng.Intn(len(paths))], 200+rng.Intn(5)*100, rng.Intn(800), rng.Intn(50000))
		},
		func() string {
			return fmt.Sprintf(`level=warn msg="slow query" query_id=%d duration=%dms rows=%d`, rng.Intn(1_000_000), 500+rng.Intn(5000), rng.Intn(10000))
		},
		func() string {
			return fmt.Sprintf(`level=error msg="connection refused" upstream=%s retry=%d`, upstreams[rng.Intn(len(upstreams))], rng.Intn(5))
		},
		func() string {
			return fmt.Sprintf(`level=debug msg="cache lookup" key=user:%d hit=%t`, rng.Intn(100000), rng.Intn(2) == 0)
		},
	}

	rareEvery := n / 1000 // ~0.1% of rows carry the rare needle
	if rareEvery < 1 {
		rareEvery = 1
	}

	// Sparse needle: sparseCount occurrences confined to the oldest 10% of rows.
	sparseRows := map[int]struct{}{}
	oldEnd := n / 10
	if oldEnd < sparseCount {
		oldEnd = n
	}
	for k := 0; k < sparseCount && oldEnd > 0; k++ {
		sparseRows[k*oldEnd/sparseCount] = struct{}{}
	}

	rows := make([]corpusRow, n)
	base := int64(1_700_000_000_000_000_000)
	for i := range rows {
		line := templates[rng.Intn(len(templates))]()
		if i%rareEvery == 0 {
			line += " " + rareNeedle
		}
		if _, ok := sparseRows[i]; ok {
			line += " " + sparseNeedle
		}
		rows[i] = corpusRow{
			timestamp: base + int64(i)*1_000_000, // ~1ms apart, monotonic
			line:      line,
			labelID:   rng.Intn(len(labelPayloads)),
		}
	}
	return rows, labelPayloads
}

// keywordSQL mirrors buildQuery's keyword path: the literal LIKE always runs; the
// FTS join is added only when useFTS (local segments).
func keywordSQL(useFTS bool, term string, start, end int64, limit int) (string, []any) {
	var sb strings.Builder
	sb.WriteString(`SELECT s.timestamp, s.line, json(l.payload) FROM streams s JOIN labels l ON l.id = s.label_id`)
	var args []any
	if useFTS {
		sb.WriteString(` JOIN line_search ON line_search.rowid = s.id`)
	}
	sb.WriteString(` WHERE 1=1`)
	if useFTS {
		sb.WriteString(` AND line_search MATCH ?`)
		args = append(args, `"`+strings.ReplaceAll(term, `"`, `""`)+`"`)
	}
	if start != 0 {
		sb.WriteString(` AND s.timestamp >= ?`)
		args = append(args, start)
	}
	if end != 0 {
		sb.WriteString(` AND s.timestamp <= ?`)
		args = append(args, end)
	}
	sb.WriteString(` AND s.line LIKE ?`)
	args = append(args, "%"+term+"%")
	sb.WriteString(` ORDER BY s.timestamp DESC, s.id DESC LIMIT ?`)
	args = append(args, limit)
	return sb.String(), args
}

// labelSQL mirrors buildQuery's label path: json_extract post-filter, or an
// inverted label_pairs subquery when useLabelPairs.
func labelSQL(useLabelPairs bool, key, val, term string, start, end int64, limit int) (string, []any) {
	var sb strings.Builder
	sb.WriteString(`SELECT s.timestamp, s.line, json(l.payload) FROM streams s JOIN labels l ON l.id = s.label_id WHERE 1=1`)
	var args []any
	if useLabelPairs {
		sb.WriteString(` AND s.label_id IN (SELECT label_id FROM label_pairs WHERE key = ? AND value = ?)`)
		args = append(args, key, val)
	} else {
		sb.WriteString(` AND json_extract(l.payload, ?) = ?`)
		args = append(args, `$."`+strings.ReplaceAll(key, `"`, `""`)+`"`, val)
	}
	if term != "" {
		sb.WriteString(` AND s.line LIKE ?`)
		args = append(args, "%"+term+"%")
	}
	if start != 0 {
		sb.WriteString(` AND s.timestamp >= ?`)
		args = append(args, start)
	}
	if end != 0 {
		sb.WriteString(` AND s.timestamp <= ?`)
		args = append(args, end)
	}
	sb.WriteString(` ORDER BY s.timestamp DESC, s.id DESC LIMIT ?`)
	args = append(args, limit)
	return sb.String(), args
}

// buildZst builds a plain segment for v and compresses it (Best level) to a kept
// .sqlite.zst, returning its path.
func buildZst(t *testing.T, dir string, v variant, rows []corpusRow, labelPayloads []string) string {
	t.Helper()
	plain := buildSegmentDB(t, dir, v, rows, labelPayloads)
	if err := compressWithPool(plain, bestEncoderPool); err != nil {
		t.Fatalf("compress: %v", err)
	}
	return plain + ".zst"
}

func runQuery(db *sql.DB, query string, args []any) (int, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return 0, err
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return 0, err
	}
	holders := make([]any, len(cols))
	for i := range holders {
		var v any
		holders[i] = &v
	}

	n := 0
	for rows.Next() {
		if err := rows.Scan(holders...); err != nil {
			return n, err
		}
		n++
	}
	return n, rows.Err()
}

// measureCold copies the archive to a fresh name (so the sqlitezstd frame cache
// is empty) and times one full query — this captures FTS's frame-skipping
// advantage, which a warm decoded-frame cache would hide. Median of `copies`.
func measureCold(t *testing.T, zst, query string, args []any, copies int) (time.Duration, int) {
	t.Helper()
	var durs []time.Duration
	var rowsOut int
	for k := 0; k < copies; k++ {
		dst := strings.TrimSuffix(zst, ".zst") + fmt.Sprintf(".cold%d.zst", k)
		if err := copyFile(zst, dst); err != nil {
			t.Fatalf("copy: %v", err)
		}
		db, err := sql.Open("sqlite3", dst+"?vfs=zstd")
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		db.SetMaxOpenConns(1)
		start := time.Now()
		n, err := runQuery(db, query, args)
		el := time.Since(start)
		_ = db.Close()
		_ = os.Remove(dst)
		if err != nil {
			t.Fatalf("cold query: %v", err)
		}
		durs = append(durs, el)
		rowsOut = n
	}
	return medianDur(durs), rowsOut
}

// measureWarm times the query with the frame cache primed (steady state).
func measureWarm(t *testing.T, zst, query string, args []any, runs int) (time.Duration, int) {
	t.Helper()
	db, err := sql.Open("sqlite3", zst+"?vfs=zstd")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.SetMaxOpenConns(1)
	defer func() { _ = db.Close() }()

	n, err := runQuery(db, query, args) // prime
	if err != nil {
		t.Fatalf("warm prime: %v", err)
	}

	var durs []time.Duration
	for i := 0; i < runs; i++ {
		start := time.Now()
		if _, err := runQuery(db, query, args); err != nil {
			t.Fatalf("warm query: %v", err)
		}
		durs = append(durs, time.Since(start))
	}
	return medianDur(durs), n
}

func medianDur(d []time.Duration) time.Duration {
	if len(d) == 0 {
		return 0
	}
	s := append([]time.Duration(nil), d...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[len(s)/2]
}

func ms(d time.Duration) string { return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000) }

func TestQueryLatency(t *testing.T) {
	type corpusCfg struct {
		name     string
		n        int
		highCard bool
	}
	corpora := []corpusCfg{
		{"low_100k", 100_000, false},
		{"low_1M", 1_000_000, false},
		{"high_100k", 100_000, true},
		{"high_1M", 1_000_000, true},
	}

	const limit = 100

	for _, c := range corpora {
		rows, labelPayloads := buildCorpusCard(c.n, c.highCard)
		dir := t.TempDir()
		base := rows[0].timestamp
		span := rows[len(rows)-1].timestamp - base
		winStart := base + span*99/100 // most-recent ~1% window

		zfts := buildZst(t, dir, variant{name: "fts", pageSize: 4096, fts: true, tsIndex: true}, rows, labelPayloads)
		zno := buildZst(t, dir, variant{name: "nofts", pageSize: 4096, tsIndex: true}, rows, labelPayloads)
		zlp := buildZst(t, dir, variant{name: "lp", pageSize: 4096, tsIndex: true, labelPairs: true}, rows, labelPayloads)

		t.Logf("================ %s: %d rows, %d label sets ================", c.name, c.n, len(labelPayloads))
		t.Logf("segment size (.sqlite.zst, Best): fts=%s  nofts=%s  labelpairs=%s",
			human(fileSize(t, zfts)), human(fileSize(t, zno)), human(fileSize(t, zlp)))

		type run struct {
			scenario, variant, zst, sql string
			args                        []any
		}
		var runs []run
		add := func(scn, varn, zst, sqlText string, args []any) {
			runs = append(runs, run{scn, varn, zst, sqlText, args})
		}

		// Keyword scenarios: FTS vs LIKE-only.
		for _, kw := range []struct{ name, term string }{
			{"kw_rare", rareNeedle}, {"kw_common", commonNeedle}, {"kw_absent", absentNeedle},
			{"kw_sparse_old", sparseNeedle},
		} {
			s, a := keywordSQL(true, kw.term, 0, 0, limit)
			add(kw.name, "fts", zfts, s, a)
			s, a = keywordSQL(false, kw.term, 0, 0, limit)
			add(kw.name, "nofts", zno, s, a)
		}
		// Rare keyword constrained to a recent window (the live-tail case).
		s, a := keywordSQL(true, rareNeedle, winStart, 0, limit)
		add("kw_rare_tb", "fts", zfts, s, a)
		s, a = keywordSQL(false, rareNeedle, winStart, 0, limit)
		add("kw_rare_tb", "nofts", zno, s, a)

		// Label scenarios: json_extract scan vs label_pairs.
		labelKey, labelVal := "app", "api"
		if c.highCard {
			labelKey, labelVal = "pod", "pod-7" // selects ~1/2000 of rows
		}
		s, a = labelSQL(false, labelKey, labelVal, "", 0, 0, limit)
		add("label", "nofts(json)", zno, s, a)
		s, a = labelSQL(true, labelKey, labelVal, "", 0, 0, limit)
		add("label", "labelpairs", zlp, s, a)

		if c.highCard { // also a broad label (app=api spans many sets)
			s, a = labelSQL(false, "app", "api", "", 0, 0, limit)
			add("label_broad", "nofts(json)", zno, s, a)
			s, a = labelSQL(true, "app", "api", "", 0, 0, limit)
			add("label_broad", "labelpairs", zlp, s, a)
		}

		// Label + keyword combined.
		s, a = labelSQL(false, labelKey, labelVal, commonNeedle, 0, 0, limit)
		add("label+kw", "nofts(json)", zno, s, a)
		s, a = labelSQL(true, labelKey, labelVal, commonNeedle, 0, 0, limit)
		add("label+kw", "labelpairs", zlp, s, a)

		t.Logf("%-12s %-13s %10s %10s %7s", "scenario", "variant", "cold", "warm", "rows")
		for _, r := range runs {
			cold, n := measureCold(t, r.zst, r.sql, r.args, 3)
			warm, _ := measureWarm(t, r.zst, r.sql, r.args, 5)
			t.Logf("%-12s %-13s %10s %10s %7d", r.scenario, r.variant, ms(cold), ms(warm), n)
		}
	}
}
