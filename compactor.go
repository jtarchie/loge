package loge

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/jtarchie/loge/managers"
)

// rowQuerier is the read subset shared by *sql.DB and *sql.Tx, so the filter
// builders work whether the merge runs in a transaction or on a pinned conn.
type rowQuerier interface {
	Query(query string, args ...any) (*sql.Rows, error)
}

// buildLineFilter scans the segment's lines and builds a serialized trigram
// filter used to skip the whole segment for keyword queries it cannot match.
func buildLineFilter(db rowQuerier) ([]byte, error) {
	rows, err := db.Query("SELECT line FROM streams")
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

// buildLabelFilter scans the segment's distinct label key=value pairs and builds
// a serialized filter used to skip the whole segment for equality matchers it
// cannot satisfy.
func buildLabelFilter(db rowQuerier) ([]byte, error) {
	rows, err := db.Query(`SELECT DISTINCT je.key, je.value FROM labels, json_each(labels.payload) je`)
	if err != nil {
		return nil, fmt.Errorf("could not read label pairs for filter: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	hashes := map[uint64]struct{}{}

	for rows.Next() {
		var key, value string

		if err := rows.Scan(&key, &value); err != nil {
			return nil, fmt.Errorf("could not scan label pair: %w", err)
		}

		managers.AddLabelPairHash(key, value, hashes)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("could not read label pairs: %w", err)
	}

	return managers.BuildLabelFilter(hashes)
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

	// Pin one connection: the merge ATTACHes each source database and runs
	// INSERT...SELECT against it, and ATTACH is connection-scoped, so every
	// statement must run on the same connection for the attached db to be visible.
	client.SetMaxOpenConns(1)

	// The default 4 KiB page size compresses best here: the size harness found
	// larger pages slightly increase the compressed segment (more b-tree slack
	// per page), so we do not override it. No wrapping transaction: ATTACH cannot
	// run inside one, and with journal_mode=OFF each statement's auto-commit is a
	// cheap no-op. File-level atomicity comes from writing to a .partial temp and
	// renaming on success, so a transaction is not needed for that either.
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

	// Merge each source entirely inside SQLite: ATTACH the (zstd-compressed,
	// read-only) source and INSERT...SELECT its rows, offsetting label ids so they
	// stay unique across files. This replaces a row-by-row Scan/Exec copy — a cgo
	// crossing each way per row — with two statements per file.
	labelOffset := int64(0)

	for _, source := range sources {
		maxOldID, err := copyFileInto(client, source, labelOffset)
		if err != nil {
			return fmt.Errorf("could not merge %q: %w", source, err)
		}

		labelOffset += maxOldID
	}

	// Recompute the segment's bounds once over the merged data (no longer tracked
	// per row during the copy). COALESCE yields 0/0 when no streams were merged,
	// matching the previous empty-segment behavior.
	var minTimestamp, maxTimestamp int64

	if err := client.QueryRow(
		`SELECT COALESCE(min(timestamp), 0), COALESCE(max(timestamp), 0) FROM streams`,
	).Scan(&minTimestamp, &maxTimestamp); err != nil {
		return fmt.Errorf("could not compute segment bounds: %w", err)
	}

	// Name the published segment after its data bounds so queries can prune it
	// (and a fresh catalog can be rebuilt from an S3 listing) without opening it.
	base := filepath.Join(c.dir, managers.FormatSegmentName(minTimestamp, maxTimestamp, sealedAt))

	// Build a trigram filter of the segment's lines so keyword queries can skip
	// this whole segment (zero file/S3 reads) when it can't contain the term.
	lineFilter, err := buildLineFilter(client)
	if err != nil {
		return fmt.Errorf("could not build line filter: %w", err)
	}

	// Build a filter of the segment's exact label key=value pairs so equality
	// matchers can skip this whole segment when it holds no matching stream.
	labelFilter, err := buildLabelFilter(client)
	if err != nil {
		return fmt.Errorf("could not build label filter: %w", err)
	}

	if _, err := client.Exec(
		`INSERT INTO metadata (key, value) VALUES ('minTimestamp', ?), ('maxTimestamp', ?), ('lineFilter', ?), ('labelFilter', ?);`,
		minTimestamp, maxTimestamp,
		base64.StdEncoding.EncodeToString(lineFilter),
		base64.StdEncoding.EncodeToString(labelFilter),
	); err != nil {
		return fmt.Errorf("could not write segment metadata: %w", err)
	}

	// Build the one index the read path needs: idx_streams_timestamp backs the
	// time-range filter and the ORDER BY timestamp DESC LIMIT. No FTS index is
	// built — benchmarks showed a trigram index costs ~3.3x the segment size yet
	// loses to a plain LIKE scan on the common/recent/time-bounded queries that
	// dominate (the timestamp-ordered scan short-circuits at LIMIT, while FTS must
	// materialize every match then sort); the binary-fuse line filter already
	// prunes whole segments that cannot contain a keyword. No label_id index
	// either: the query drives from streams and reaches labels by primary key.
	if _, err := client.Exec(`CREATE INDEX idx_streams_timestamp ON streams(timestamp);`); err != nil {
		return fmt.Errorf("could not build segment index: %w", err)
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

// copyFileInto merges one source file into the open segment connection by
// ATTACHing the source — a read-only, zstd-compressed SQLite db — and running
// INSERT...SELECT, so the entire copy happens inside SQLite's C core with no
// per-row round-trip into Go (the old path Scanned and re-Exec'd every row, a cgo
// crossing each way). Label ids (and the streams that reference them) shift by
// labelOffset so they stay unique across merged files; it returns this file's max
// label id so the caller can advance the offset.
func copyFileInto(db *sql.DB, source string, labelOffset int64) (int64, error) {
	// ATTACH needs an absolute URI (the compactor's CWD is not guaranteed) on the
	// zstd VFS. immutable=1 marks the source read-only so SQLite never tries to
	// create a lock/journal sidecar next to the compressed file.
	abs, err := filepath.Abs(source)
	if err != nil {
		return 0, fmt.Errorf("could not resolve source path %q: %w", source, err)
	}

	uri := fmt.Sprintf("file:%s?vfs=zstd&immutable=1", abs)

	if _, err := db.Exec(`ATTACH DATABASE ? AS src`, uri); err != nil {
		return 0, fmt.Errorf("could not attach source %q: %w", source, err)
	}
	defer func() {
		_, _ = db.Exec(`DETACH DATABASE src`)
	}()

	if _, err := db.Exec(
		`INSERT INTO labels (id, payload) SELECT id + ?, payload FROM src.labels`, labelOffset,
	); err != nil {
		return 0, fmt.Errorf("could not copy labels from %q: %w", source, err)
	}

	if _, err := db.Exec(
		`INSERT INTO streams (timestamp, line, label_id) SELECT timestamp, line, label_id + ? FROM src.streams`,
		labelOffset,
	); err != nil {
		return 0, fmt.Errorf("could not copy streams from %q: %w", source, err)
	}

	var maxOldID int64
	if err := db.QueryRow(`SELECT COALESCE(MAX(id), 0) FROM src.labels`).Scan(&maxOldID); err != nil {
		return 0, fmt.Errorf("could not read max label id from %q: %w", source, err)
	}

	return maxOldID, nil
}
