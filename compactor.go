package loge

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/jtarchie/loge/managers"
)

// buildLineFilter scans the segment's lines (in the open transaction) and builds
// a serialized trigram filter used to skip the whole segment for keyword queries
// it cannot match.
func buildLineFilter(tx *sql.Tx) ([]byte, error) {
	rows, err := tx.Query("SELECT line FROM streams")
	if err != nil {
		return nil, fmt.Errorf("could not read lines for filter: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	hashes := map[uint64]struct{}{}

	for rows.Next() {
		var line string

		if err := rows.Scan(&line); err != nil {
			return nil, fmt.Errorf("could not scan line: %w", err)
		}

		managers.AddTrigramHashes(line, hashes)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("could not read lines: %w", err)
	}

	return managers.BuildLineFilter(hashes)
}

const (
	// defaultCompactMinFiles is the number of small flush files that must
	// accumulate before a compaction pass merges them.
	defaultCompactMinFiles = 8
	// defaultCompactMaxFiles bounds how many files a single segment merges, so
	// one pass cannot produce an unbounded segment.
	defaultCompactMaxFiles = 128
	// defaultCompactInterval is how often the background loop looks for work.
	defaultCompactInterval = 30 * time.Second
)

// Compactor merges the many small per-flush SQLite files into fewer, larger
// time-local "segment" files, building the expensive trigram line index and
// the timestamp/label indexes once per segment instead of once per flush.
type Compactor struct {
	dir      string
	minFiles int
	maxFiles int
	interval time.Duration
	catalog  *managers.Catalog
}

// CompactorOption configures optional Compactor behaviour.
type CompactorOption func(*Compactor)

// WithCompactorCatalog records each new segment in the catalog so the query
// path can prune by time without opening files.
func WithCompactorCatalog(catalog *managers.Catalog) CompactorOption {
	return func(c *Compactor) {
		c.catalog = catalog
	}
}

// NewCompactor builds a Compactor for dir. Zero values select defaults.
func NewCompactor(dir string, minFiles, maxFiles int, interval time.Duration, opts ...CompactorOption) *Compactor {
	if minFiles <= 1 {
		minFiles = defaultCompactMinFiles
	}

	if maxFiles < minFiles {
		maxFiles = defaultCompactMaxFiles
	}

	if interval <= 0 {
		interval = defaultCompactInterval
	}

	compactor := &Compactor{dir: dir, minFiles: minFiles, maxFiles: maxFiles, interval: interval}

	for _, opt := range opts {
		opt(compactor)
	}

	return compactor
}

// Run compacts on a ticker until ctx is cancelled.
func (c *Compactor) Run(ctx context.Context) {
	timer := time.NewTimer(c.interval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if merged, err := c.Compact(); err != nil {
				slog.Error("compaction failed", slog.String("error", err.Error()))
			} else if merged > 0 {
				slog.Info("compacted files into a segment", slog.Int("files", merged))
			}

			timer.Reset(c.interval)
		}
	}
}

// Compact merges one batch of the oldest flush files into a single segment.
// It returns the number of source files merged (0 when there is nothing to do).
func (c *Compactor) Compact() (int, error) {
	candidates, err := filepath.Glob(filepath.Join(c.dir, "bucket-*.sqlite.zst"))
	if err != nil {
		return 0, fmt.Errorf("could not list flush files: %w", err)
	}

	if len(candidates) < c.minFiles {
		return 0, nil
	}

	// Oldest first: the nanosecond suffix in the filename tracks creation order,
	// so merging a contiguous batch keeps each segment time-local (which makes
	// metadata time-pruning effective).
	sort.Strings(candidates)

	if len(candidates) > c.maxFiles {
		candidates = candidates[:c.maxFiles]
	}

	if err := c.merge(candidates); err != nil {
		return 0, err
	}

	// Publish-new-then-delete-old: the segment .sqlite.zst already exists, so
	// removing the sources now never opens a query gap.
	for _, source := range candidates {
		if err := os.Remove(source); err != nil {
			slog.Warn("could not remove compacted source",
				slog.String("filename", source),
				slog.String("error", err.Error()),
			)
		}
	}

	return len(candidates), nil
}

// merge reads every source file and writes a single compressed segment with the
// trigram line index and supporting indexes built once.
func (c *Compactor) merge(sources []string) error {
	sealedAt := time.Now().UnixNano()
	// The published name encodes the segment's min/max timestamps, but those are
	// only known after the merge loop, so write to a provisional temp first and
	// compute the real base name below.
	tmp := filepath.Join(c.dir, fmt.Sprintf("segment-%d.sqlite.partial", sealedAt))

	finalized := false
	defer func() {
		if !finalized {
			_ = os.Remove(tmp)
		}
	}()

	client, err := sql.Open("sqlite3", tmp)
	if err != nil {
		return fmt.Errorf("could not open segment: %w", err)
	}
	defer func() {
		_ = client.Close()
	}()

	// The default 4 KiB page size compresses best here: the size harness found
	// larger pages slightly increase the compressed segment (more b-tree/FTS slack
	// per page), so we do not override it.
	_, err = client.Exec(`
		PRAGMA journal_mode = OFF;
		PRAGMA synchronous = OFF;
		PRAGMA locking_mode = EXCLUSIVE;
		PRAGMA temp_store = FILE;

		CREATE TABLE labels (id INTEGER PRIMARY KEY, payload BLOB) STRICT;
		CREATE TABLE streams (id INTEGER PRIMARY KEY, timestamp INTEGER, line TEXT, label_id INTEGER) STRICT;
		CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT) STRICT, WITHOUT ROWID;
	`)
	if err != nil {
		return fmt.Errorf("could not create segment schema: %w", err)
	}

	transaction, err := client.Begin()
	if err != nil {
		return fmt.Errorf("could not begin segment transaction: %w", err)
	}
	defer func() {
		_ = transaction.Rollback()
	}()

	minTimestamp := int64(math.MaxInt64)
	maxTimestamp := int64(math.MinInt64)
	streamCount := 0
	labelOffset := int64(0)

	for _, source := range sources {
		offset, fileMin, fileMax, count, err := copyFileInto(transaction, source, labelOffset)
		if err != nil {
			return fmt.Errorf("could not merge %q: %w", source, err)
		}

		labelOffset = offset
		streamCount += count

		if count > 0 {
			minTimestamp = min(minTimestamp, fileMin)
			maxTimestamp = max(maxTimestamp, fileMax)
		}
	}

	if streamCount == 0 {
		minTimestamp = 0
		maxTimestamp = 0
	}

	// Name the published segment after its data bounds so queries can prune it
	// (and a fresh catalog can be rebuilt from an S3 listing) without opening it.
	base := filepath.Join(c.dir, managers.FormatSegmentName(minTimestamp, maxTimestamp, sealedAt))

	// Build a trigram filter of the segment's lines so keyword queries can skip
	// this whole segment (zero file/S3 reads) when it can't contain the term.
	lineFilter, err := buildLineFilter(transaction)
	if err != nil {
		return fmt.Errorf("could not build line filter: %w", err)
	}

	if _, err := transaction.Exec(
		`INSERT INTO metadata (key, value) VALUES ('minTimestamp', ?), ('maxTimestamp', ?), ('lineFilter', ?);`,
		minTimestamp, maxTimestamp, base64.StdEncoding.EncodeToString(lineFilter),
	); err != nil {
		return fmt.Errorf("could not write segment metadata: %w", err)
	}

	// Build the heavy indexes once for the whole segment. idx_streams_timestamp
	// backs the time-range filter and the ORDER BY timestamp DESC LIMIT; no read
	// path filters streams by label_id (the query drives from streams and reaches
	// labels by primary key), so an index on label_id would be pure dead weight.
	if _, err := transaction.Exec(`
		CREATE INDEX idx_streams_timestamp ON streams(timestamp);
		CREATE VIRTUAL TABLE line_search USING fts5(line, content='', columnsize=0, tokenize="trigram");
		INSERT INTO line_search(rowid, line) SELECT id, line FROM streams;
	`); err != nil {
		return fmt.Errorf("could not build segment indexes: %w", err)
	}

	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("could not commit segment: %w", err)
	}

	if err := client.Close(); err != nil {
		return fmt.Errorf("could not close segment: %w", err)
	}

	if err := os.Rename(tmp, base); err != nil {
		return fmt.Errorf("could not finalize segment: %w", err)
	}
	finalized = true

	// compressSegment() turns base into base+".zst" and removes base, using the
	// higher-ratio segment encoder.
	if err := compressSegment(base); err != nil {
		return fmt.Errorf("could not compress segment: %w", err)
	}

	// fsync the published segment (and the directory) before the caller deletes
	// the source files, so the merged data is durable on its own first. This
	// keeps the write-ahead-log checkpoint correct: a source file is only
	// removed once the segment that replaces it is power-loss durable.
	if err := fsyncFile(base + ".zst"); err != nil {
		return fmt.Errorf("could not sync segment: %w", err)
	}

	if err := fsyncDir(c.dir); err != nil {
		return fmt.Errorf("could not sync segment directory: %w", err)
	}

	// Record the new segment in the catalog so queries can prune by time
	// without opening it (and so it becomes a rotation candidate).
	if c.catalog != nil {
		meta, err := managers.DeriveSegmentMeta(base + ".zst")
		if err != nil {
			return fmt.Errorf("could not derive segment metadata: %w", err)
		}

		meta.SealedAt = sealedAt

		if err := c.catalog.Upsert(meta); err != nil {
			return fmt.Errorf("could not catalog segment: %w", err)
		}
	}

	return nil
}

// copyFileInto copies one source file's labels and streams into the segment
// transaction, offsetting label ids so they do not collide across files. It
// returns the next label offset, the file's min/max timestamps, and its stream
// count.
func copyFileInto(tx *sql.Tx, source string, labelOffset int64) (int64, int64, int64, int, error) {
	src, err := sql.Open("sqlite3", source+"?vfs=zstd")
	if err != nil {
		return labelOffset, 0, 0, 0, fmt.Errorf("could not open source: %w", err)
	}
	src.SetMaxOpenConns(1)
	defer func() {
		_ = src.Close()
	}()

	maxOldID, err := copyLabels(tx, src, labelOffset)
	if err != nil {
		return labelOffset, 0, 0, 0, err
	}

	fileMin, fileMax, count, err := copyStreams(tx, src, labelOffset)
	if err != nil {
		return labelOffset, 0, 0, 0, err
	}

	return labelOffset + maxOldID, fileMin, fileMax, count, nil
}

func copyLabels(tx *sql.Tx, src *sql.DB, labelOffset int64) (int64, error) {
	rows, err := src.Query("SELECT id, payload FROM labels")
	if err != nil {
		return 0, fmt.Errorf("could not read labels: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	stmt, err := tx.Prepare("INSERT INTO labels (id, payload) VALUES (?, ?)")
	if err != nil {
		return 0, fmt.Errorf("could not prepare label insert: %w", err)
	}
	defer func() {
		_ = stmt.Close()
	}()

	var maxOldID int64

	for rows.Next() {
		var (
			id      int64
			payload []byte
		)

		if err := rows.Scan(&id, &payload); err != nil {
			return 0, fmt.Errorf("could not scan label: %w", err)
		}

		if _, err := stmt.Exec(id+labelOffset, payload); err != nil {
			return 0, fmt.Errorf("could not insert label: %w", err)
		}

		if id > maxOldID {
			maxOldID = id
		}
	}

	return maxOldID, rows.Err()
}

func copyStreams(tx *sql.Tx, src *sql.DB, labelOffset int64) (int64, int64, int, error) {
	rows, err := src.Query("SELECT timestamp, line, label_id FROM streams")
	if err != nil {
		return 0, 0, 0, fmt.Errorf("could not read streams: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	stmt, err := tx.Prepare("INSERT INTO streams (timestamp, line, label_id) VALUES (?, ?, ?)")
	if err != nil {
		return 0, 0, 0, fmt.Errorf("could not prepare stream insert: %w", err)
	}
	defer func() {
		_ = stmt.Close()
	}()

	fileMin := int64(math.MaxInt64)
	fileMax := int64(math.MinInt64)
	count := 0

	for rows.Next() {
		var (
			timestamp int64
			line      string
			labelID   int64
		)

		if err := rows.Scan(&timestamp, &line, &labelID); err != nil {
			return 0, 0, 0, fmt.Errorf("could not scan stream: %w", err)
		}

		if _, err := stmt.Exec(timestamp, line, labelID+labelOffset); err != nil {
			return 0, 0, 0, fmt.Errorf("could not insert stream: %w", err)
		}

		fileMin = min(fileMin, timestamp)
		fileMax = max(fileMax, timestamp)
		count++
	}

	return fileMin, fileMax, count, rows.Err()
}
