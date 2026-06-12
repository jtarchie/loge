package loge

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg"
	"github.com/jtarchie/loge/managers"
	"github.com/jtarchie/worker"
	"github.com/klauspost/compress/zstd"
	_ "github.com/mattn/go-sqlite3"
)

// seqPayload carries a payload through the receiver channel together with its
// write-ahead-log sequence number (0 when durability is disabled or for
// replayed payloads).
type seqPayload struct {
	seq     uint64
	payload Payload
}

// flushJob is a batch handed to a flusher worker, with the WAL sequence of each
// payload kept alongside so durability can be reported once the batch is
// flushed and compressed.
type flushJob struct {
	payloads []Payload
	seqs     []uint64
}

// compressJob is a flushed file plus the sequences it contains, handed to a
// compressor worker.
type compressJob struct {
	filename string
	seqs     []uint64
}

type Buckets struct {
	receiver           chan seqPayload
	ctx                context.Context
	cancel             context.CancelFunc
	wg                 sync.WaitGroup
	flushers           *worker.Worker[flushJob]
	compressors        *worker.Worker[compressJob]
	dropOnBackpressure bool
	flushInterval      time.Duration
	wal                *WAL
	onDurable          func(filename string, seqs []uint64)
}

const (
	defaultFlushInterval = 1 * time.Second // Flush frequently to reduce memory
	maxBatchInsert       = 500             // max rows per batch INSERT
	enqueueTimeout       = 100 * time.Millisecond
	enqueueRetries       = 3
)

// BucketOption configures optional Buckets behaviour.
type BucketOption func(*Buckets)

// WithFlushInterval overrides how often a bucket worker flushes a non-empty
// batch. A non-positive duration is ignored.
func WithFlushInterval(interval time.Duration) BucketOption {
	return func(b *Buckets) {
		if interval > 0 {
			b.flushInterval = interval
		}
	}
}

// WithWAL makes Append durably log each payload (and assign it a sequence
// number) before queueing it.
func WithWAL(wal *WAL) BucketOption {
	return func(b *Buckets) {
		b.wal = wal
	}
}

// WithDurableReport registers a callback invoked after a file has been flushed
// and compressed, reporting the WAL sequence numbers it contains so they can be
// checkpointed.
func WithDurableReport(fn func(filename string, seqs []uint64)) BucketOption {
	return func(b *Buckets) {
		b.onDurable = fn
	}
}

// Types for batch inserts
type labelEntry struct {
	payload string
}

type streamEntry struct {
	timestamp int64
	line      string
	labelID   int64
}

// betterEncoderPool holds reusable zstd encoders at SpeedBetterCompression,
// used for short-lived flush files on the latency-sensitive ingest path.
var betterEncoderPool = newEncoderPool(zstd.SpeedBetterCompression)

// bestEncoderPool holds reusable zstd encoders at SpeedBestCompression, used for
// compacted segments. Segments are built on the background compactor (which has
// CPU headroom) and are the long-lived, durable, S3-rotated files, so spending
// more CPU for a better ratio pays off in stored bytes.
var bestEncoderPool = newEncoderPool(zstd.SpeedBestCompression)

func newEncoderPool(level zstd.EncoderLevel) *sync.Pool {
	return &sync.Pool{
		New: func() any {
			enc, err := zstd.NewWriter(nil,
				zstd.WithEncoderLevel(level),
				zstd.WithEncoderConcurrency(1),
			)
			if err != nil {
				return nil
			}
			return enc
		},
	}
}

// copyBufferPool holds reusable buffers for io.CopyBuffer.
//
// Note: this buffer size does NOT control the seekable zstd frame size, and so
// does not affect the compression ratio. compress() copies an *os.File into the
// seekable writer, and *os.File implements io.WriterTo, so io.CopyBuffer takes
// the WriteTo fast-path and ignores this buffer; os.File.WriteTo frames at ~32
// KiB, which the size harness found is already at the knee (bigger frames buy
// ~1% for much worse remote read amplification). The buffer only bounds copy
// memory. See sizebench_internal_test.go (TestFrameSizeReal).
const copyBufferSize = 8 * 1024 // 8KB buffer - smaller to reduce memory

var copyBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, copyBufferSize)
		return &buf
	},
}

func NewBuckets(
	ctx context.Context,
	size int,
	payloadSize int,
	outputPath string,
	dropOnBackpressure bool,
	opts ...BucketOption,
) (*Buckets, error) {
	// Create a cancellable context for shutdown
	ctx, cancel := context.WithCancel(ctx)

	buckets := &Buckets{
		// Small buffer to reduce memory - trades throughput for lower memory usage
		receiver:           make(chan seqPayload, size),
		ctx:                ctx,
		cancel:             cancel,
		dropOnBackpressure: dropOnBackpressure,
		flushInterval:      defaultFlushInterval,
	}

	for _, opt := range opts {
		opt(buckets)
	}

	buckets.compressors = worker.NewWithContext(ctx, size, max(size/2, 2), func(_ int, job compressJob) {
		if err := compress(job.filename); err != nil {
			slog.Error("could not compress file", slog.String("filename", job.filename), slog.String("error", err.Error()))

			return
		}

		// The flushed file is now a durable, queryable segment; report its
		// sequences so the write-ahead log can be checkpointed.
		if buckets.onDurable != nil {
			buckets.onDurable(job.filename+".zst", job.seqs)
		}
	})

	buckets.flushers = worker.NewWithContext(ctx, size, max(size/2, 2), func(index int, job flushJob) {
		filename, err := flusher(job.payloads, outputPath, index)
		if err != nil {
			slog.Error("could not flush payloads", slog.Int("index", index), slog.String("error", err.Error()))
		} else {
			buckets.compressors.Enqueue(compressJob{filename: filename, seqs: job.seqs})
		}
	})

	for index := range size {
		buckets.wg.Add(1)
		go buckets.bucketWorker(index, payloadSize, buckets.flushers)
	}

	return buckets, nil
}

func (b *Buckets) bucketWorker(index int, payloadSize int, flushers *worker.Worker[flushJob]) {
	defer b.wg.Done()

	payloads := make([]Payload, 0, payloadSize)
	seqs := make([]uint64, 0, payloadSize)
	timer := time.NewTimer(b.flushInterval)
	defer timer.Stop()

	reset := func() {
		payloads = make([]Payload, 0, payloadSize)
		seqs = make([]uint64, 0, payloadSize)
	}

	job := func() flushJob {
		return flushJob{payloads: payloads, seqs: seqs}
	}

	flush := func(blocking bool) {
		if len(payloads) == 0 {
			return
		}

		// Shutdown path (blocking=false). The flusher pool is still draining at
		// this point (it is closed only after every bucket worker has finished,
		// see Buckets.Close), so a blocking enqueue makes progress and will be
		// accepted. Block to guarantee no data loss, unless the operator
		// explicitly opted into dropping on backpressure.
		if !blocking {
			if b.dropOnBackpressure {
				if !flushers.Enqueue(job(), worker.WithTimeout(time.Second)) {
					slog.Warn("shutdown flush timeout, dropping data",
						slog.Int("bucket", index),
						slog.Int("payloads", len(payloads)),
					)
				}
				reset()

				return
			}

			flushers.Enqueue(job()) // blocks until the flusher accepts it
			reset()

			return
		}

		// Normal operation: retry with exponential backoff
		for attempt := range enqueueRetries {
			timeout := enqueueTimeout * time.Duration(1<<attempt)
			if flushers.Enqueue(job(), worker.WithTimeout(timeout)) {
				reset()
				return
			}
			slog.Warn("flusher backpressure, retrying",
				slog.Int("bucket", index),
				slog.Int("attempt", attempt+1),
				slog.Int("payloads", len(payloads)),
			)
		}

		// Drop mode: log and discard data instead of blocking
		if b.dropOnBackpressure {
			slog.Warn("flusher backpressure, dropping data",
				slog.Int("bucket", index),
				slog.Int("payloads", len(payloads)),
			)
			reset()
			return
		}

		// Block mode: block until enqueue succeeds (no data loss)
		slog.Warn("flusher backpressure, blocking until flush completes",
			slog.Int("bucket", index),
			slog.Int("payloads", len(payloads)),
		)
		flushers.Enqueue(job()) // blocks until space available
		reset()
	}

	add := func(item seqPayload) {
		payloads = append(payloads, item.payload)
		seqs = append(seqs, item.seq)
	}

	for {
		select {
		case <-b.ctx.Done():
			// Graceful shutdown: drain receiver and flush
			// Drain any remaining items from receiver
			for {
				select {
				case item, ok := <-b.receiver:
					if !ok {
						// Receiver closed, do final flush and exit
						flush(false)
						return
					}
					add(item)
					if len(payloads) >= payloadSize {
						flush(false)
					}
				default:
					// Receiver empty, do final flush and exit
					flush(false)
					return
				}
			}

		case <-timer.C:
			flush(true)
			timer.Reset(b.flushInterval)

		case item, ok := <-b.receiver:
			if !ok {
				// Channel closed, flush and exit
				flush(false)
				return
			}

			add(item)

			// Flush as soon as the batch is full; otherwise let the timer fire
			// so a non-empty batch is never held longer than flushInterval.
			// (The timer is deliberately NOT reset per payload: under sustained
			// load that would keep pushing the deadline out so periodic flushes
			// never happen, unbounding memory and the write-ahead log.)
			if len(payloads) >= payloadSize {
				flush(true)
				timer.Reset(b.flushInterval)
			}
		}
	}
}

// Append queues a payload for flushing. When a write-ahead log is attached the
// payload is durably logged (and assigned a sequence number) before queueing,
// and any error is returned so the caller can reject the request; otherwise it
// always succeeds.
func (b *Buckets) Append(payload Payload) error {
	seq := uint64(0)

	if b.wal != nil {
		assigned, err := b.wal.Append(&payload)
		if err != nil {
			return fmt.Errorf("could not write to write-ahead log: %w", err)
		}

		seq = assigned
	}

	b.receiver <- seqPayload{seq: seq, payload: payload}

	return nil
}

// appendReplay queues a payload without logging it to the write-ahead log,
// used when replaying recovered WAL segments on startup.
func (b *Buckets) appendReplay(payload Payload) {
	b.receiver <- seqPayload{seq: 0, payload: payload}
}

// Close gracefully shuts down all bucket workers, flushers, and compressors.
// It ensures all in-flight data is flushed before returning.
func (b *Buckets) Close() error {
	// Signal all bucket workers to stop accepting new payloads via context cancellation
	b.cancel()

	// Close receiver to ensure no more payloads can be added
	// and bucket workers can drain remaining items
	close(b.receiver)

	// Wait for bucket workers to finish draining and flushing
	// They use short timeouts during shutdown so this should complete quickly
	b.wg.Wait()

	// Close flushers to process any remaining queued items
	b.flushers.Close()

	// Wait for compressors to complete all pending work
	b.compressors.Close()

	return nil
}

func flusher(payloads []Payload, outputDir string, index int) (string, error) {
	// The final name encodes the batch's min/max timestamps, but those are only
	// known after the rows are written. Use a provisional, bounds-less name for
	// the temp file (and error messages); the real published name is computed
	// below once the bounds are known.
	seq := time.Now().UnixNano()
	filename := filepath.Join(outputDir, fmt.Sprintf("bucket-%d-%d.sqlite", index, seq))
	// Write to a temporary file and atomically rename on success so the
	// compressor (and any reader) never observes a half-written database,
	// which matters because the DB is written with journal_mode=OFF.
	tmpFilename := filename + ".partial"

	err := os.MkdirAll(outputDir, 0o750)
	if err != nil {
		return "", fmt.Errorf("could not create directory: %w", err)
	}

	renamed := false
	defer func() {
		if !renamed {
			_ = os.Remove(tmpFilename)
		}
	}()

	client, err := sql.Open("sqlite3", tmpFilename)
	if err != nil {
		return "", fmt.Errorf("could not open sqlite3 %q: %w", tmpFilename, err)
	}
	defer func() {
		_ = client.Close()
	}()

	// Performance pragmas - trade speed for lower memory usage
	_, err = client.Exec(`
		PRAGMA journal_mode = OFF;
		PRAGMA synchronous = OFF;
		PRAGMA locking_mode = EXCLUSIVE;
		PRAGMA temp_store = FILE;
		PRAGMA cache_size = -8000;
		PRAGMA mmap_size = 33554432;
	`)
	if err != nil {
		return "", fmt.Errorf("could not set pragmas %q: %w", filename, err)
	}

	// Schema without AUTOINCREMENT for ~10-15% faster inserts
	_, err = client.Exec(`
		CREATE TABLE labels (
			id INTEGER PRIMARY KEY,
			payload BLOB
		) STRICT;

		CREATE TABLE streams (
			id INTEGER PRIMARY KEY,
			timestamp INTEGER,
			line TEXT,
			label_id INTEGER
		) STRICT;

		CREATE TABLE metadata (
			key TEXT PRIMARY KEY,
			value TEXT
		) STRICT, WITHOUT ROWID;
	`)
	if err != nil {
		return "", fmt.Errorf("could not create schema %q: %w", filename, err)
	}

	transaction, err := client.Begin()
	if err != nil {
		return "", fmt.Errorf("could not create transaction %q: %w", filename, err)
	}
	defer func() {
		_ = transaction.Rollback()
	}()

	var minTimestamp int64 = math.MaxInt64
	var maxTimestamp int64 = math.MinInt64

	// Pre-calculate sizes for slice allocation
	totalLabels := 0
	totalStreams := 0
	for _, payload := range payloads {
		totalLabels += len(payload.Streams)
		for _, stream := range payload.Streams {
			totalStreams += len(stream.Values)
		}
	}

	// Pre-allocate slices to avoid reallocation during append
	labels := make([]labelEntry, 0, totalLabels)
	streams := make([]streamEntry, 0, totalStreams)

	// Collect all labels and streams for batch insert
	skippedValues := 0
	labelID := int64(0)
	for _, payload := range payloads {
		for _, stream := range payload.Streams {
			labelID++
			labels = append(labels, labelEntry{
				payload: MarshalLabels(stream.Stream),
			})

			for _, value := range stream.Values {
				timestamp, err := value.Timestamp()
				if err != nil {
					// Skip values with malformed timestamps rather than
					// inserting timestamp=0 and corrupting min/max metadata.
					skippedValues++
					continue
				}
				streams = append(streams, streamEntry{
					timestamp: timestamp,
					line:      value[1],
					labelID:   labelID,
				})
				minTimestamp = min(minTimestamp, timestamp)
				maxTimestamp = max(maxTimestamp, timestamp)
			}
		}
	}

	if skippedValues > 0 {
		slog.Warn("skipped values with invalid timestamps",
			slog.String("filename", filename),
			slog.Int("skipped", skippedValues),
		)
	}

	// Guard against persisting sentinel bounds when no valid stream rows were
	// collected (e.g. every value had a malformed timestamp).
	if len(streams) == 0 {
		minTimestamp = 0
		maxTimestamp = 0
	}

	// Now that the batch's bounds are known, the published file is named to
	// encode them, so the query path can prune it (and a fresh catalog can be
	// rebuilt) from the filename alone.
	finalName := filepath.Join(outputDir, managers.FormatFlushName(index, minTimestamp, maxTimestamp, seq))

	// Batch insert labels
	if err := batchInsertLabels(transaction, labels); err != nil {
		return "", fmt.Errorf("could not insert labels %q: %w", filename, err)
	}

	// Batch insert streams
	if err := batchInsertStreams(transaction, streams); err != nil {
		return "", fmt.Errorf("could not insert streams %q: %w", filename, err)
	}

	// Insert metadata
	_, err = transaction.Exec(`INSERT INTO metadata (key, value) VALUES ('minTimestamp', ?), ('maxTimestamp', ?);`,
		minTimestamp, maxTimestamp)
	if err != nil {
		return "", fmt.Errorf("could not insert metadata %q: %w", filename, err)
	}

	// The expensive FTS5 (line) index and a timestamp index are built once per
	// compacted segment (see managers/compactor.go), not per flush: building
	// them on every tiny file dominated write cost and they were never read on
	// the ingest hot path. Fresh, uncompacted files are queried with a scan.

	err = transaction.Commit()
	if err != nil {
		return "", fmt.Errorf("could not commit transaction %q: %w", filename, err)
	}

	// No PRAGMA optimize / VACUUM here: a flush segment carries no secondary
	// indexes (the timestamp index and trigram line filter are built later, in
	// compaction) and is compressed and discarded immediately, so analyzing the
	// query planner buys nothing and the optimize cost recurs on every flush —
	// measurable on the ingest hot path under sustained load.
	err = client.Close()
	if err != nil {
		return "", fmt.Errorf("could not close sqlite: %w", err)
	}

	if err := os.Rename(tmpFilename, finalName); err != nil {
		return "", fmt.Errorf("could not finalize %q: %w", finalName, err)
	}
	renamed = true

	return finalName, nil
}

func batchInsertLabels(tx *sql.Tx, labels []labelEntry) error {
	if len(labels) == 0 {
		return nil
	}

	for i := 0; i < len(labels); i += maxBatchInsert {
		end := min(i+maxBatchInsert, len(labels))
		batch := labels[i:end]

		var sb strings.Builder
		sb.WriteString("INSERT INTO labels (payload) VALUES ")

		args := make([]any, 0, len(batch))
		for j, l := range batch {
			if j > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString("(jsonb(?))")
			args = append(args, l.payload)
		}

		_, err := tx.Exec(sb.String(), args...)
		if err != nil {
			return err
		}
	}

	return nil
}

func batchInsertStreams(tx *sql.Tx, streams []streamEntry) error {
	if len(streams) == 0 {
		return nil
	}

	for i := 0; i < len(streams); i += maxBatchInsert {
		end := min(i+maxBatchInsert, len(streams))
		batch := streams[i:end]

		var sb strings.Builder
		sb.WriteString("INSERT INTO streams (timestamp, line, label_id) VALUES ")

		args := make([]any, 0, len(batch)*3)
		for j, s := range batch {
			if j > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString("(?,?,?)")
			args = append(args, s.timestamp, s.line, s.labelID)
		}

		_, err := tx.Exec(sb.String(), args...)
		if err != nil {
			return err
		}
	}

	return nil
}

// compress writes filename as a seekable-zstd archive (filename+".zst") using
// the flush-tier encoder (SpeedBetterCompression) and removes the source.
func compress(filename string) error {
	return compressWithPool(filename, betterEncoderPool)
}

// compressSegment is compress for compacted segments: it uses the
// SpeedBestCompression encoder, trading background CPU for a smaller durable
// (and S3-rotated) file.
func compressSegment(filename string) error {
	return compressWithPool(filename, bestEncoderPool)
}

func compressWithPool(filename string, pool *sync.Pool) error {
	partial := filename + ".zst.partial"
	final := filename + ".zst"

	output, err := os.Create(partial)
	if err != nil {
		return fmt.Errorf("could not create file: %w", err)
	}

	// Only publish the compressed file if everything below succeeds; on any
	// failure remove the partial and leave the source file in place so it can
	// be retried instead of publishing a truncated, corrupt archive.
	finalized := false
	defer func() {
		_ = output.Close()
		if !finalized {
			_ = os.Remove(partial)
		}
	}()

	input, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("could not open file: %w", err)
	}
	defer func() {
		_ = input.Close()
	}()

	// Get encoder from pool
	encoderIface := pool.Get()
	if encoderIface == nil {
		return fmt.Errorf("could not get encoder from pool")
	}
	encoder := encoderIface.(*zstd.Encoder)
	defer pool.Put(encoder)

	// Reset encoder for reuse
	encoder.Reset(nil)

	writer, err := seekable.NewWriter(output, encoder)
	if err != nil {
		return fmt.Errorf("could not load writer: %w", err)
	}

	// Use pooled buffer for copying.
	bufPtr := copyBufferPool.Get().(*[]byte)
	defer copyBufferPool.Put(bufPtr)

	_, err = io.CopyBuffer(writer, input, *bufPtr)
	if err != nil {
		_ = writer.Close()
		return fmt.Errorf("could not compress file: %w", err)
	}

	// Close the seekable writer to flush all zstd frames and the seek table
	// before syncing and publishing.
	if err := writer.Close(); err != nil {
		return fmt.Errorf("could not close writer: %w", err)
	}

	if err := output.Close(); err != nil {
		return fmt.Errorf("could not close compressed file: %w", err)
	}

	if err := os.Rename(partial, final); err != nil {
		return fmt.Errorf("could not finalize compressed file: %w", err)
	}
	finalized = true

	err = os.Remove(filename)
	if err != nil {
		return fmt.Errorf("could not remove original file: %w", err)
	}

	return nil
}
