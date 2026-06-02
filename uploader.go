package loge

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path"
	"time"

	"github.com/jtarchie/loge/managers"
)

const (
	defaultRotateAge      = time.Hour
	defaultRotateInterval = time.Minute
	defaultRotateGrace    = time.Minute
)

// ObjectStore is the remote storage the uploader rotates segments to. It is an
// interface so rotation can be tested without real S3.
type ObjectStore interface {
	// Put uploads the file at localPath under key and returns the public URL it
	// can be read back from over HTTP.
	Put(ctx context.Context, key, localPath string) (readURL string, err error)
	// Size returns the size of the stored object, for upload verification.
	Size(ctx context.Context, key string) (int64, error)
}

// Uploader rotates compacted segments older than a threshold to an ObjectStore
// (cold tier), flips the catalog to point at the remote copy, and deletes the
// local copy after a grace window. Recent segments stay local (hot tier).
type Uploader struct {
	dir      string
	catalog  *managers.Catalog
	store    ObjectStore
	prefix   string
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

// NewUploader builds an Uploader for dir.
func NewUploader(dir string, catalog *managers.Catalog, store ObjectStore, opts ...UploaderOption) *Uploader {
	uploader := &Uploader{
		dir:      dir,
		catalog:  catalog,
		store:    store,
		age:      defaultRotateAge,
		grace:    defaultRotateGrace,
		interval: defaultRotateInterval,
	}

	for _, opt := range opts {
		opt(uploader)
	}

	return uploader
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
		info, err := os.Stat(segment.LocalPath)
		if err != nil {
			slog.Warn("skipping rotation of missing local segment",
				slog.String("id", segment.ID), slog.String("error", err.Error()))

			continue
		}

		key := path.Join(u.prefix, segment.ID)

		readURL, err := u.store.Put(ctx, key, segment.LocalPath)
		if err != nil {
			slog.Error("could not upload segment", slog.String("id", segment.ID), slog.String("error", err.Error()))

			continue
		}

		// Verify the upload before committing the catalog flip.
		size, err := u.store.Size(ctx, key)
		if err != nil {
			slog.Error("could not verify upload", slog.String("id", segment.ID), slog.String("error", err.Error()))

			continue
		}

		if size != info.Size() {
			slog.Error("uploaded size mismatch; not flipping",
				slog.String("id", segment.ID), slog.Int64("local", info.Size()), slog.Int64("remote", size))

			continue
		}

		if err := u.catalog.MarkRemote(segment.ID, readURL, time.Now().UnixNano()); err != nil {
			slog.Error("could not flip segment to remote", slog.String("id", segment.ID), slog.String("error", err.Error()))

			continue
		}

		rotated++
	}

	return rotated, nil
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
