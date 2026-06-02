package loge

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const defaultCheckpointInterval = 2 * time.Second

// durableReport is the per-file notification that a flushed, compressed segment
// (and the WAL sequences it contains) now exists.
type durableReport struct {
	filename string
	seqs     []uint64
}

// Checkpointer bounds the write-ahead log. It receives reports that segments
// have been written, fsyncs those segments (so the data is durable on its own),
// then advances the WAL's durable watermark and lets it delete the now-redundant
// log segments. Reports for files that were already compacted away are treated
// as durable, since the compactor fsyncs its merged segment before deleting the
// sources.
type Checkpointer struct {
	wal      *WAL
	interval time.Duration
	reports  chan durableReport
	closing  chan struct{}
	once     sync.Once
	wg       sync.WaitGroup
}

// NewCheckpointer creates a checkpointer for wal. A non-positive interval uses
// the default.
func NewCheckpointer(wal *WAL, interval time.Duration) *Checkpointer {
	if interval <= 0 {
		interval = defaultCheckpointInterval
	}

	checkpointer := &Checkpointer{
		wal:      wal,
		interval: interval,
		reports:  make(chan durableReport, 1024),
		closing:  make(chan struct{}),
	}

	checkpointer.wg.Add(1)
	go checkpointer.run()

	return checkpointer
}

// Report notes that filename is now a durable segment containing seqs. It is
// the callback passed to buckets via WithDurableReport and is safe to call
// after Stop (the report is simply dropped).
func (c *Checkpointer) Report(filename string, seqs []uint64) {
	if len(seqs) == 0 {
		return
	}

	select {
	case c.reports <- durableReport{filename: filename, seqs: seqs}:
	case <-c.closing:
	}
}

// Stop ends the checkpointer. It must be called only after the flush/compress
// pipeline has drained (so no further reports arrive).
func (c *Checkpointer) Stop() {
	c.once.Do(func() {
		close(c.closing)
	})
	c.wg.Wait()
}

func (c *Checkpointer) run() {
	defer c.wg.Done()

	timer := time.NewTimer(c.interval)
	defer timer.Stop()

	var pending []durableReport

	for {
		select {
		case <-c.closing:
			return
		case report := <-c.reports:
			pending = append(pending, report)
		case <-timer.C:
			c.checkpoint(pending)
			pending = pending[:0]
			timer.Reset(c.interval)
		}
	}
}

func (c *Checkpointer) checkpoint(pending []durableReport) {
	if len(pending) == 0 {
		return
	}

	seqs := make([]uint64, 0, len(pending))
	dirs := make(map[string]struct{})

	for _, report := range pending {
		err := fsyncFile(report.filename)

		switch {
		case err == nil:
			// Durable.
		case errors.Is(err, os.ErrNotExist):
			// Already compacted away; the compactor fsynced its merged segment
			// before deleting this file, so the data is still durable.
		default:
			// Could not confirm durability; leave these sequences in the WAL.
			slog.Warn("could not fsync segment for checkpoint",
				slog.String("filename", report.filename),
				slog.String("error", err.Error()),
			)

			continue
		}

		seqs = append(seqs, report.seqs...)
		dirs[filepath.Dir(report.filename)] = struct{}{}
	}

	for dir := range dirs {
		if err := fsyncDir(dir); err != nil {
			slog.Warn("could not fsync directory for checkpoint", slog.String("error", err.Error()))
		}
	}

	// Seal the active WAL segment so its already-durable sequences become
	// eligible for deletion, then advance the watermark and prune.
	c.wal.Rotate()

	if err := c.wal.MarkDurable(seqs); err != nil {
		slog.Warn("could not prune write-ahead log", slog.String("error", err.Error()))
	}
}

func fsyncFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
	}()

	return file.Sync()
}

func fsyncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("could not open directory: %w", err)
	}
	defer func() {
		_ = dir.Close()
	}()

	return dir.Sync()
}
