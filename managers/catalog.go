package managers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/georgysavva/scany/v2/sqlscan"
	_ "github.com/mattn/go-sqlite3"
)

// Segment location values.
const (
	LocationLocal  = "local"
	LocationRemote = "remote"
)

// SegmentMeta is one row of the catalog: a compacted segment and where it
// lives. A segment is queryable either at LocalPath (LocationLocal) or at
// RemoteURL over HTTP (LocationRemote).
type SegmentMeta struct {
	ID           string   `db:"id"`
	Location     string   `db:"location"`
	LocalPath    string   `db:"local_path"`
	RemoteURL    string   `db:"remote_url"`
	MinTimestamp int64    `db:"min_timestamp"`
	MaxTimestamp int64    `db:"max_timestamp"`
	RowCount     int64    `db:"row_count"`
	LabelKeysRaw string   `db:"label_keys"`
	SealedAt     int64    `db:"sealed_at"`
	UploadedAt   int64    `db:"uploaded_at"`
	LabelKeys    []string `db:"-"`
}

// Catalog is a small, local, authoritative index of every compacted segment
// (local or remote). The query path consults it to prune segments by time
// window without opening any file, which is what makes querying remote (S3)
// segments fast. It is fully rebuildable from the segment files on disk
// (see Reconcile), so it is a cache, never a source of truth for data.
type Catalog struct {
	db *sql.DB
	mu sync.Mutex
}

// OpenCatalog opens (creating if needed) the catalog database in dir.
func OpenCatalog(dir string) (*Catalog, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("could not create catalog directory: %w", err)
	}

	path := filepath.Join(dir, "catalog.sqlite")

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("could not open catalog: %w", err)
	}

	// One connection + a mutex keeps all access serialized and lock-free; the
	// catalog is tiny and accessed at low frequency (a few writes per
	// compaction/upload, one indexed read per query).
	db.SetMaxOpenConns(1)

	if _, err := db.Exec(`
		PRAGMA journal_mode = WAL;
		PRAGMA synchronous = NORMAL;
		PRAGMA busy_timeout = 5000;

		CREATE TABLE IF NOT EXISTS segments (
			id            TEXT PRIMARY KEY,
			location      TEXT NOT NULL,
			local_path    TEXT NOT NULL DEFAULT '',
			remote_url    TEXT NOT NULL DEFAULT '',
			min_timestamp INTEGER NOT NULL,
			max_timestamp INTEGER NOT NULL,
			row_count     INTEGER NOT NULL,
			label_keys    TEXT NOT NULL DEFAULT '[]',
			sealed_at     INTEGER NOT NULL,
			uploaded_at   INTEGER NOT NULL DEFAULT 0
		) STRICT;

		CREATE INDEX IF NOT EXISTS idx_segments_maxts ON segments(max_timestamp);
	`); err != nil {
		_ = db.Close()

		return nil, fmt.Errorf("could not initialize catalog: %w", err)
	}

	return &Catalog{db: db}, nil
}

// Close closes the catalog database.
func (c *Catalog) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.db.Close()
}

// Upsert inserts or replaces a segment row.
func (c *Catalog) Upsert(meta SegmentMeta) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	labelKeys := meta.LabelKeysRaw
	if labelKeys == "" {
		encoded, err := json.Marshal(meta.LabelKeys)
		if err != nil {
			return fmt.Errorf("could not encode label keys: %w", err)
		}

		labelKeys = string(encoded)
	}

	_, err := c.db.Exec(`
		INSERT INTO segments
			(id, location, local_path, remote_url, min_timestamp, max_timestamp, row_count, label_keys, sealed_at, uploaded_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			location = excluded.location,
			local_path = excluded.local_path,
			remote_url = excluded.remote_url,
			min_timestamp = excluded.min_timestamp,
			max_timestamp = excluded.max_timestamp,
			row_count = excluded.row_count,
			label_keys = excluded.label_keys,
			sealed_at = excluded.sealed_at,
			uploaded_at = excluded.uploaded_at;
	`, meta.ID, meta.Location, meta.LocalPath, meta.RemoteURL,
		meta.MinTimestamp, meta.MaxTimestamp, meta.RowCount, labelKeys, meta.SealedAt, meta.UploadedAt)
	if err != nil {
		return fmt.Errorf("could not upsert segment %q: %w", meta.ID, err)
	}

	return nil
}

// MarkRemote flips a segment to remote-resident: it is now served from
// remoteURL and the local copy may be removed.
func (c *Catalog) MarkRemote(id, remoteURL string, uploadedAt int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, err := c.db.Exec(`
		UPDATE segments SET location = ?, remote_url = ?, uploaded_at = ? WHERE id = ?;
	`, LocationRemote, remoteURL, uploadedAt, id)
	if err != nil {
		return fmt.Errorf("could not mark segment %q remote: %w", id, err)
	}

	return nil
}

// ClearLocalPath records that a remote segment no longer has a local copy.
func (c *Catalog) ClearLocalPath(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, err := c.db.Exec(`UPDATE segments SET local_path = '' WHERE id = ?;`, id)
	if err != nil {
		return fmt.Errorf("could not clear local path for %q: %w", id, err)
	}

	return nil
}

// Remove deletes a segment row.
func (c *Catalog) Remove(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, err := c.db.Exec(`DELETE FROM segments WHERE id = ?;`, id); err != nil {
		return fmt.Errorf("could not remove segment %q: %w", id, err)
	}

	return nil
}

// Overlapping returns the segments whose [min,max] timestamp range overlaps the
// requested [start,end] window (0 meaning unbounded), newest-first. This is the
// prune that avoids touching non-overlapping (remote) segments at all.
func (c *Catalog) Overlapping(start, end int64) ([]SegmentMeta, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	query := `SELECT * FROM segments WHERE 1 = 1`
	args := []any{}

	if start != 0 {
		query += ` AND max_timestamp >= ?`
		args = append(args, start)
	}

	if end != 0 {
		query += ` AND min_timestamp <= ?`
		args = append(args, end)
	}

	query += ` ORDER BY max_timestamp DESC`

	return c.scan(query, args...)
}

// LocalToRotate returns local segments sealed strictly before olderThan,
// oldest-first — the rotation candidates.
func (c *Catalog) LocalToRotate(olderThan int64) ([]SegmentMeta, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.scan(`
		SELECT * FROM segments
		WHERE location = 'local' AND local_path != '' AND sealed_at < ?
		ORDER BY sealed_at ASC;
	`, olderThan)
}

// RemoteWithLocalCopy returns remote segments that still have a local file and
// were uploaded before olderThan — eligible for local cleanup after the grace
// window.
func (c *Catalog) RemoteWithLocalCopy(olderThan int64) ([]SegmentMeta, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.scan(`
		SELECT * FROM segments
		WHERE location = 'remote' AND local_path != '' AND uploaded_at > 0 AND uploaded_at < ?
		ORDER BY uploaded_at ASC;
	`, olderThan)
}

// List returns every segment.
func (c *Catalog) List() ([]SegmentMeta, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.scan(`SELECT * FROM segments ORDER BY max_timestamp DESC;`)
}

// LabelKeys returns the union of label keys across all segments.
func (c *Catalog) LabelKeys() ([]string, error) {
	segments, err := c.List()
	if err != nil {
		return nil, err
	}

	seen := map[string]struct{}{}

	for _, segment := range segments {
		for _, key := range segment.LabelKeys {
			seen[key] = struct{}{}
		}
	}

	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	return keys, nil
}

// Reconcile makes the catalog consistent with the segment files actually on
// disk in dir: it adds any local segment missing from the catalog (deriving its
// metadata) and drops local rows whose file has vanished. Remote rows are left
// untouched (their authority is S3). This lets the catalog be rebuilt after a
// crash or out-of-band file changes.
func (c *Catalog) Reconcile(dir string) error {
	paths, err := filepath.Glob(filepath.Join(dir, "segment-*.sqlite.zst"))
	if err != nil {
		return fmt.Errorf("could not list segments: %w", err)
	}

	existing, err := c.List()
	if err != nil {
		return err
	}

	known := make(map[string]SegmentMeta, len(existing))
	for _, segment := range existing {
		known[segment.ID] = segment
	}

	onDisk := make(map[string]struct{}, len(paths))

	for _, path := range paths {
		id := filepath.Base(path)
		onDisk[id] = struct{}{}

		if prior, ok := known[id]; ok {
			// Already cataloged; make sure a present local file is recorded
			// (covers the grace window where a remote segment still has a local
			// copy), but never downgrade a remote segment back to local.
			if prior.LocalPath == "" {
				abs, _ := filepath.Abs(path)
				_ = c.setLocalPath(id, abs)
			}

			continue
		}

		meta, err := DeriveSegmentMeta(path)
		if err != nil {
			return fmt.Errorf("could not derive metadata for %q: %w", path, err)
		}

		if err := c.Upsert(meta); err != nil {
			return err
		}
	}

	// Drop local rows whose file is gone; keep remote rows regardless.
	for _, segment := range existing {
		if segment.Location != LocationLocal {
			continue
		}

		if _, ok := onDisk[segment.ID]; !ok {
			if err := c.Remove(segment.ID); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *Catalog) setLocalPath(id, localPath string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, err := c.db.Exec(`UPDATE segments SET local_path = ? WHERE id = ?;`, localPath, id)
	if err != nil {
		return fmt.Errorf("could not set local path for %q: %w", id, err)
	}

	return nil
}

// DeriveSegmentMeta opens a local segment and reads its metadata so a catalog
// row can be (re)built. The returned row is local.
func DeriveSegmentMeta(path string) (SegmentMeta, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return SegmentMeta{}, fmt.Errorf("could not resolve path: %w", err)
	}

	client, err := sql.Open("sqlite3", path+"?vfs=zstd")
	if err != nil {
		return SegmentMeta{}, fmt.Errorf("could not open segment: %w", err)
	}

	client.SetMaxOpenConns(1)
	defer func() {
		_ = client.Close()
	}()

	meta := SegmentMeta{
		ID:        filepath.Base(path),
		Location:  LocationLocal,
		LocalPath: abs,
	}

	if err := readSegmentMeta(client, &meta); err != nil {
		return SegmentMeta{}, err
	}

	return meta, nil
}

// DeriveRemoteSegmentMeta opens a remote segment over HTTP (via the seekable
// zstd VFS named vfsName) to read its metadata. It is the expensive fallback
// used by ReconcileRemote for legacy-named keys whose bounds cannot be parsed
// from the filename. The returned row is remote.
func DeriveRemoteSegmentMeta(remoteURL, vfsName string) (SegmentMeta, error) {
	if vfsName == "" {
		vfsName = "zstd"
	}

	client, err := sql.Open("sqlite3", remoteURL+"?vfs="+vfsName)
	if err != nil {
		return SegmentMeta{}, fmt.Errorf("could not open remote segment: %w", err)
	}

	client.SetMaxOpenConns(1)
	defer func() {
		_ = client.Close()
	}()

	meta := SegmentMeta{
		ID:        filepath.Base(remoteURL),
		Location:  LocationRemote,
		RemoteURL: remoteURL,
	}

	if err := readSegmentMeta(client, &meta); err != nil {
		return SegmentMeta{}, err
	}

	return meta, nil
}

// readSegmentMeta fills the time bounds, row count, and label keys of meta from
// an open segment database.
func readSegmentMeta(client *sql.DB, meta *SegmentMeta) error {
	if err := client.QueryRow(`SELECT value FROM metadata WHERE key = 'minTimestamp'`).Scan(&meta.MinTimestamp); err != nil {
		return fmt.Errorf("could not read minTimestamp: %w", err)
	}

	if err := client.QueryRow(`SELECT value FROM metadata WHERE key = 'maxTimestamp'`).Scan(&meta.MaxTimestamp); err != nil {
		return fmt.Errorf("could not read maxTimestamp: %w", err)
	}

	if err := client.QueryRow(`SELECT COUNT(*) FROM streams`).Scan(&meta.RowCount); err != nil {
		return fmt.Errorf("could not count streams: %w", err)
	}

	var labelKeys []string
	if err := sqlscan.Select(context.Background(), client, &labelKeys,
		`SELECT DISTINCT json_each.key FROM labels, json_each(labels.payload)`); err != nil {
		return fmt.Errorf("could not read label keys: %w", err)
	}

	sort.Strings(labelKeys)
	meta.LabelKeys = labelKeys

	encoded, err := json.Marshal(labelKeys)
	if err != nil {
		return fmt.Errorf("could not encode label keys: %w", err)
	}

	meta.LabelKeysRaw = string(encoded)

	return nil
}

// scan runs a query and decodes label keys. Caller holds c.mu.
func (c *Catalog) scan(query string, args ...any) ([]SegmentMeta, error) {
	var segments []SegmentMeta
	if err := sqlscan.Select(context.Background(), c.db, &segments, query, args...); err != nil {
		return nil, fmt.Errorf("could not read segments: %w", err)
	}

	for i := range segments {
		if segments[i].LabelKeysRaw != "" {
			_ = json.Unmarshal([]byte(segments[i].LabelKeysRaw), &segments[i].LabelKeys)
		}
	}

	return segments, nil
}
