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
	hot := fileSize(t, segs[0])

	// Cold-tier copy actually uploaded.
	id := filepath.Base(segs[0])
	stripped, err := stripFTSForUpload(dir, id, segs[0])
	if err != nil {
		t.Fatalf("strip: %v", err)
	}
	cold := fileSize(t, stripped)
	_ = os.Remove(stripped)

	t.Logf("REAL end-to-end (40k rows):")
	t.Logf("  local hot segment (indexed, Best) : %s", human(hot))
	t.Logf("  cold S3 copy (FTS-stripped)       : %s  (%.0f%% smaller)", human(cold), 100*(1-float64(cold)/float64(hot)))
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
