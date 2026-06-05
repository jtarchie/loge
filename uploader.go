package loge

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"strings"
	"time"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg"
	"github.com/jtarchie/loge/managers"
	"github.com/klauspost/compress/zstd"
)

const (
	defaultRotateAge      = time.Hour
	defaultRotateInterval = time.Minute
	defaultRotateGrace    = time.Minute
	defaultUploaderVFS    = "zstd"
)

// ObjectStore is the remote storage the uploader rotates segments to. It is an
// interface so rotation can be tested without real S3.
type ObjectStore interface {
	// Put uploads the file at localPath under key and returns the public URL it
	// can be read back from over HTTP.
	Put(ctx context.Context, key, localPath string) (readURL string, err error)
	// Size returns the size of the stored object, for upload verification.
	Size(ctx context.Context, key string) (int64, error)
	// List returns the object keys stored under prefix.
	List(ctx context.Context, prefix string) ([]string, error)
	// ReadURL returns the public HTTP URL a key is read back from.
	ReadURL(key string) string
}

// Uploader rotates compacted segments older than a threshold to an ObjectStore
// (cold tier), flips the catalog to point at the remote copy, and deletes the
// local copy after a grace window. Recent segments stay local (hot tier).
type Uploader struct {
	dir      string
	catalog  *managers.Catalog
	store    ObjectStore
	prefix   string
	vfsName  string
	age      time.Duration
	grace    time.Duration
	interval time.Duration
}

// UploaderOption configures an Uploader.
type UploaderOption func(*Uploader)

// WithRotateAge sets how old (since sealing) a local segment must be before it
// rotates to remote storage.
func WithRotateAge(d time.Duration) UploaderOption {
	return func(u *Uploader) {
		if d >= 0 {
			u.age = d
		}
	}
}

// WithRotateGrace sets how long a rotated segment's local copy is kept (so
// in-flight queries that resolved it locally finish) before deletion.
func WithRotateGrace(d time.Duration) UploaderOption {
	return func(u *Uploader) {
		if d >= 0 {
			u.grace = d
		}
	}
}

// WithRotateInterval sets how often the rotation loop runs.
func WithRotateInterval(d time.Duration) UploaderOption {
	return func(u *Uploader) {
		if d > 0 {
			u.interval = d
		}
	}
}

// WithUploadPrefix sets the key prefix for uploaded objects.
func WithUploadPrefix(prefix string) UploaderOption {
	return func(u *Uploader) {
		u.prefix = prefix
	}
}

// WithUploaderVFS sets the sqlite VFS name used to open remote segments when
// ReconcileRemote must fall back to reading a legacy-named segment over HTTP.
func WithUploaderVFS(name string) UploaderOption {
	return func(u *Uploader) {
		if name != "" {
			u.vfsName = name
		}
	}
}

// NewUploader builds an Uploader for dir.
func NewUploader(dir string, catalog *managers.Catalog, store ObjectStore, opts ...UploaderOption) *Uploader {
	uploader := &Uploader{
		dir:      dir,
		catalog:  catalog,
		store:    store,
		vfsName:  defaultUploaderVFS,
		age:      defaultRotateAge,
		grace:    defaultRotateGrace,
		interval: defaultRotateInterval,
	}

	for _, opt := range opts {
		opt(uploader)
	}

	return uploader
}

// ReconcileRemote rebuilds catalog rows for remote (S3) segments from a bucket
// listing alone, so a fresh server with an empty catalog rediscovers cold-tier
// segments without opening any file. Keys whose bounds are encoded in the
// filename are cataloged from the name; legacy-named keys fall back to opening
// the segment over HTTP (the expensive path). It returns how many remote rows
// it added. Run it once at startup before the rotation loop starts.
func (u *Uploader) ReconcileRemote(ctx context.Context) (int, error) {
	keys, err := u.store.List(ctx, u.prefix)
	if err != nil {
		return 0, fmt.Errorf("could not list remote segments: %w", err)
	}

	existing, err := u.catalog.List()
	if err != nil {
		return 0, fmt.Errorf("could not read catalog: %w", err)
	}

	known := make(map[string]struct{}, len(existing))
	for _, segment := range existing {
		known[segment.ID] = struct{}{}
	}

	added := 0

	for _, key := range keys {
		id := path.Base(key)

		// Only compacted segment objects are catalog rows; ignore anything else
		// stored under the prefix.
		if !strings.HasPrefix(id, "segment-") || !strings.HasSuffix(id, ".sqlite.zst") {
			continue
		}

		if _, ok := known[id]; ok {
			continue // already cataloged (local or remote)
		}

		remoteURL := u.store.ReadURL(key)

		meta := managers.SegmentMeta{
			ID:        id,
			Location:  managers.LocationRemote,
			RemoteURL: remoteURL,
		}

		if bounds, ok := managers.ParseBounds(id); ok {
			meta.MinTimestamp = bounds.Min
			meta.MaxTimestamp = bounds.Max
		} else {
			// Legacy name without encoded bounds: open the segment over HTTP to
			// derive its metadata. This is the expensive path.
			slog.Warn("remote segment has a legacy name; opening it over HTTP to derive bounds",
				slog.String("key", key))

			derived, err := managers.DeriveRemoteSegmentMeta(remoteURL, u.vfsName)
			if err != nil {
				slog.Error("could not derive remote segment metadata; skipping",
					slog.String("key", key), slog.String("error", err.Error()))

				continue
			}

			meta = derived
			meta.ID = id
		}

		if err := u.catalog.Upsert(meta); err != nil {
			return added, fmt.Errorf("could not catalog remote segment %q: %w", id, err)
		}

		added++
	}

	return added, nil
}

// Run rotates and sweeps on a ticker until ctx is cancelled.
func (u *Uploader) Run(ctx context.Context) {
	timer := time.NewTimer(u.interval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if rotated, err := u.Rotate(ctx); err != nil {
				slog.Error("rotation failed", slog.String("error", err.Error()))
			} else if rotated > 0 {
				slog.Info("rotated segments to remote storage", slog.Int("segments", rotated))
			}

			if err := u.Sweep(); err != nil {
				slog.Warn("local cleanup failed", slog.String("error", err.Error()))
			}

			timer.Reset(u.interval)
		}
	}
}

// Rotate uploads every local segment older than the rotate age and flips the
// catalog to remote. A failed upload is logged and retried next cycle (the
// catalog is only flipped after the upload is verified), so an S3 outage never
// loses data — the segment just stays local.
func (u *Uploader) Rotate(ctx context.Context) (int, error) {
	cutoff := time.Now().Add(-u.age).UnixNano()

	candidates, err := u.catalog.LocalToRotate(cutoff)
	if err != nil {
		return 0, fmt.Errorf("could not list rotation candidates: %w", err)
	}

	rotated := 0

	for _, segment := range candidates {
		if err := u.rotateSegment(ctx, segment); err != nil {
			slog.Error("could not rotate segment to remote",
				slog.String("id", segment.ID), slog.String("error", err.Error()))

			continue
		}

		rotated++
	}

	return rotated, nil
}

// rotateSegment uploads one segment's cold-tier copy and flips the catalog to
// remote. The uploaded copy is FTS-stripped (see stripFTSForUpload): remote
// queries never use the trigram index — they scan with LIKE and the catalog
// line filter prunes whole segments — so it is dead weight (typically the
// majority of a segment's bytes) over the network and in storage. The local hot
// copy keeps its index until the grace sweep deletes it. Any failure leaves the
// segment local and is retried next cycle.
func (u *Uploader) rotateSegment(ctx context.Context, segment managers.SegmentMeta) error {
	if _, err := os.Stat(segment.LocalPath); err != nil {
		return fmt.Errorf("missing local segment: %w", err)
	}

	strippedPath, err := stripFTSForUpload(u.dir, segment.ID, segment.LocalPath)
	if err != nil {
		return fmt.Errorf("could not strip segment for upload: %w", err)
	}
	defer func() { _ = os.Remove(strippedPath) }()

	// Verify against the artifact we actually upload, not the local original.
	info, err := os.Stat(strippedPath)
	if err != nil {
		return fmt.Errorf("could not stat stripped segment: %w", err)
	}

	key := path.Join(u.prefix, segment.ID)

	readURL, err := u.store.Put(ctx, key, strippedPath)
	if err != nil {
		return fmt.Errorf("could not upload: %w", err)
	}

	size, err := u.store.Size(ctx, key)
	if err != nil {
		return fmt.Errorf("could not verify upload: %w", err)
	}

	if size != info.Size() {
		return fmt.Errorf("uploaded size mismatch: local %d remote %d", info.Size(), size)
	}

	if err := u.catalog.MarkRemote(segment.ID, readURL, time.Now().UnixNano()); err != nil {
		return fmt.Errorf("could not flip to remote: %w", err)
	}

	return nil
}

// stripFTSForUpload writes a cold-tier copy of the compressed segment at localZst
// with the FTS trigram index removed, and returns the path to the new compressed
// (.zst) file. It decompresses the segment, drops line_search (and a legacy
// label_id index if present), VACUUMs to reclaim the freed pages, and
// recompresses with the segment encoder. The labels, streams, metadata (min/max
// bounds and the line filter) and the timestamp index are all preserved, so
// remote LIKE/time-range queries and catalog reconciliation are unaffected. The
// caller owns the returned file and must remove it after uploading.
func stripFTSForUpload(dir, id, localZst string) (string, error) {
	// A temp basename that no query glob matches: the watcher tracks
	// `bucket-*.sqlite.zst` (with a catalog) or `*.sqlite.zst`, and ".strip" /
	// ".strip.zst" end in neither.
	plain := path.Join(dir, strings.TrimSuffix(id, ".zst")+".strip")

	if err := decompressSegment(localZst, plain); err != nil {
		_ = os.Remove(plain)

		return "", fmt.Errorf("could not decompress segment: %w", err)
	}

	if err := dropFTSIndex(plain); err != nil {
		_ = os.Remove(plain)

		return "", err
	}

	// compressSegment writes plain+".zst" and removes plain on success.
	if err := compressSegment(plain); err != nil {
		_ = os.Remove(plain)
		_ = os.Remove(plain + ".zst")

		return "", fmt.Errorf("could not compress stripped segment: %w", err)
	}

	return plain + ".zst", nil
}

// decompressSegment writes the decompressed contents of a seekable-zstd file to
// plainPath.
func decompressSegment(srcZst, plainPath string) error {
	in, err := os.Open(srcZst)
	if err != nil {
		return fmt.Errorf("could not open segment: %w", err)
	}
	defer func() { _ = in.Close() }()

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return fmt.Errorf("could not create decoder: %w", err)
	}
	defer decoder.Close()

	reader, err := seekable.NewReader(in, decoder)
	if err != nil {
		return fmt.Errorf("could not open seekable reader: %w", err)
	}
	defer func() { _ = reader.Close() }()

	out, err := os.Create(plainPath)
	if err != nil {
		return fmt.Errorf("could not create decompressed file: %w", err)
	}
	defer func() { _ = out.Close() }()

	if _, err := io.Copy(out, reader); err != nil {
		return fmt.Errorf("could not decompress: %w", err)
	}

	return out.Close()
}

// dropFTSIndex removes the FTS trigram index (and a legacy label_id index, if
// any) from the plain SQLite file and VACUUMs so the freed pages are reclaimed.
func dropFTSIndex(plainPath string) error {
	client, err := sql.Open("sqlite3", plainPath)
	if err != nil {
		return fmt.Errorf("could not open stripped segment: %w", err)
	}
	defer func() { _ = client.Close() }()

	if _, err := client.Exec(`
		PRAGMA temp_store = FILE;
		DROP TABLE IF EXISTS line_search;
		DROP INDEX IF EXISTS idx_streams_label_id;
		VACUUM;
	`); err != nil {
		return fmt.Errorf("could not drop fts index: %w", err)
	}

	return client.Close()
}

// Sweep deletes the local copies of remote segments whose grace window has
// elapsed, so any query that resolved them locally has finished.
func (u *Uploader) Sweep() error {
	cutoff := time.Now().Add(-u.grace).UnixNano()

	candidates, err := u.catalog.RemoteWithLocalCopy(cutoff)
	if err != nil {
		return fmt.Errorf("could not list local copies: %w", err)
	}

	for _, segment := range candidates {
		if err := os.Remove(segment.LocalPath); err != nil && !os.IsNotExist(err) {
			slog.Warn("could not remove local copy", slog.String("id", segment.ID), slog.String("error", err.Error()))

			continue
		}

		if err := u.catalog.ClearLocalPath(segment.ID); err != nil {
			slog.Warn("could not clear local path", slog.String("id", segment.ID), slog.String("error", err.Error()))
		}
	}

	return nil
}
