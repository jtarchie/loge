package loge

import (
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
	"github.com/jtarchie/worker"
	"github.com/klauspost/compress/zstd"
	_ "github.com/mattn/go-sqlite3"
)

type Buckets struct {
	receiver          chan Payload
	done              chan struct{}
	wg                sync.WaitGroup
	flushers          *worker.Worker[[]Payload]
	compressors       *worker.Worker[string]
	dropOnBackpressure bool
}

const (
	flushInterval  = 5 * time.Second
	maxBatchInsert = 500 // max rows per batch INSERT
	enqueueTimeout = 100 * time.Millisecond
	enqueueRetries = 3
)

// Types for batch inserts
type labelEntry struct {
	payload string
}

type streamEntry struct {
	timestamp int64
	line      string
	labelID   int64
}

// encoderPool holds reusable zstd encoders to avoid allocation overhead
var encoderPool = sync.Pool{
	New: func() any {
		enc, err := zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstd.SpeedBetterCompression),
			zstd.WithEncoderConcurrency(1),
		)
		if err != nil {
			return nil
		}
		return enc
	},
}

// copyBufferPool holds reusable buffers for io.CopyBuffer
const copyBufferSize = 32 * 1024 // 32KB buffer

var copyBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, copyBufferSize)
		return &buf
	},
}

// payloadPool reduces GC pressure by reusing Payload objects
var payloadPool = sync.Pool{
	New: func() any {
		return &Payload{
			Streams: make(Streams, 0, 8),
		}
	},
}

// GetPayload gets a Payload from the pool
func GetPayload() *Payload {
	return payloadPool.Get().(*Payload)
}

// PutPayload returns a Payload to the pool after resetting it
func PutPayload(p *Payload) {
	// Reset slices but keep capacity
	for i := range p.Streams {
		p.Streams[i].Stream = nil
		p.Streams[i].Values = nil
	}
	p.Streams = p.Streams[:0]
	payloadPool.Put(p)
}

func NewBuckets(
	size int,
	payloadSize int,
	outputPath string,
	dropOnBackpressure bool,
) (*Buckets, error) {
	// Larger buffer: size * payloadSize for better burst handling
	receiver := make(chan Payload, size*payloadSize)
	done := make(chan struct{})

	compressors := worker.New(size, max(size/2, 2), func(_ int, filename string) {
		err := compress(filename)
		if err != nil {
			slog.Error("could not compress file", slog.String("filename", filename), slog.String("error", err.Error()))
		}
	})

	flushers := worker.New(size, max(size/2, 2), func(index int, payloads []Payload) {
		filename, err := flusher(payloads, outputPath, fmt.Sprintf("bucket-%d", index))
		if err != nil {
			slog.Error("could not flush payloads", slog.Int("index", index), slog.String("error", err.Error()))
		} else {
			compressors.Enqueue(filename)
		}
	})

	buckets := &Buckets{
		receiver:          receiver,
		done:              done,
		flushers:          flushers,
		compressors:       compressors,
		dropOnBackpressure: dropOnBackpressure,
	}

	for index := range size {
		buckets.wg.Add(1)
		go buckets.bucketWorker(index, payloadSize, flushers)
	}

	return buckets, nil
}

func (b *Buckets) bucketWorker(index int, payloadSize int, flushers *worker.Worker[[]Payload]) {
	defer b.wg.Done()

	payloads := make([]Payload, 0, payloadSize)
	timer := time.NewTimer(flushInterval)
	defer timer.Stop()

	flush := func(blocking bool) {
		if len(payloads) == 0 {
			return
		}

		// During shutdown (blocking=false), use shorter timeouts
		if !blocking {
			// Try once with short timeout, don't retry forever during shutdown
			if flushers.Enqueue(payloads, worker.WithTimeout(time.Second)) {
				payloads = make([]Payload, 0, payloadSize)
			} else {
				slog.Warn("shutdown flush timeout, data will be processed by flusher close",
					slog.Int("bucket", index),
					slog.Int("payloads", len(payloads)),
				)
			}
			return
		}

		// Normal operation: retry with exponential backoff
		for attempt := range enqueueRetries {
			timeout := enqueueTimeout * time.Duration(1<<attempt)
			if flushers.Enqueue(payloads, worker.WithTimeout(timeout)) {
				payloads = make([]Payload, 0, payloadSize)
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
			payloads = make([]Payload, 0, payloadSize)
			return
		}

		// Block mode: block until enqueue succeeds (no data loss)
		slog.Warn("flusher backpressure, blocking until flush completes",
			slog.Int("bucket", index),
			slog.Int("payloads", len(payloads)),
		)
		flushers.Enqueue(payloads) // blocks until space available
		payloads = make([]Payload, 0, payloadSize)
	}

	for {
		select {
		case <-b.done:
			// Graceful shutdown: drain receiver and flush
			// Drain any remaining items from receiver
			for {
				select {
				case payload, ok := <-b.receiver:
					if !ok {
						// Receiver closed, do final flush and exit
						flush(false)
						return
					}
					payloads = append(payloads, payload)
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
			timer.Reset(flushInterval)

		case payload, ok := <-b.receiver:
			if !ok {
				// Channel closed, flush and exit
				flush(false)
				return
			}

			payloads = append(payloads, payload)
			timer.Reset(flushInterval)

			if len(payloads) >= payloadSize {
				flush(true)
			}
		}
	}
}

func (b *Buckets) Append(payload Payload) {
	b.receiver <- payload
}

// Close gracefully shuts down all bucket workers, flushers, and compressors.
// It ensures all in-flight data is flushed before returning.
func (b *Buckets) Close() error {
	// Signal all bucket workers to stop accepting new payloads
	close(b.done)

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

func flusher(payloads []Payload, outputDir string, prefix string) (string, error) {
	filename := filepath.Join(outputDir, fmt.Sprintf("%s-%d.sqlite", prefix, time.Now().UnixNano()))

	err := os.MkdirAll(outputDir, os.ModePerm)
	if err != nil {
		return "", fmt.Errorf("could not create directory: %w", err)
	}

	client, err := sql.Open("sqlite3", filename)
	if err != nil {
		return "", fmt.Errorf("could not open sqlite3 %q: %w", filename, err)
	}
	defer func() {
		_ = client.Close()
	}()

	// Performance pragmas - since we compress atomically, we don't need durability here
	_, err = client.Exec(`
		PRAGMA journal_mode = OFF;
		PRAGMA synchronous = OFF;
		PRAGMA locking_mode = EXCLUSIVE;
		PRAGMA temp_store = MEMORY;
		PRAGMA cache_size = -64000;
		PRAGMA mmap_size = 268435456;
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

		CREATE VIRTUAL TABLE stream_tree USING rtree(
			id,
			minTimestamp, maxTimestamp
		);
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
	labelID := int64(0)
	for _, payload := range payloads {
		for _, stream := range payload.Streams {
			labelID++
			labels = append(labels, labelEntry{
				payload: MarshalLabels(stream.Stream),
			})

			for _, value := range stream.Values {
				timestamp := value.Timestamp()
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

	_, err = transaction.Exec(`
		CREATE VIRTUAL TABLE
			search
		USING
			fts5(payload, content = '', columnsize=0, tokenize="trigram");

		WITH payload AS (
			SELECT
				labels.id AS id,
				json_each.key || ' ' || json_each.value AS kv
			FROM
				labels,
				json_each(labels.payload)
		)
		INSERT INTO
			search(rowid, payload)
		SELECT
			id,
			GROUP_CONCAT(kv, ' ')
		FROM
			payload
		GROUP BY id;

		INSERT INTO stream_tree (id, minTimestamp, maxTimestamp) SELECT id, timestamp, timestamp FROM streams;
	`)
	if err != nil {
		return "", fmt.Errorf("could not create search index %q: %w", filename, err)
	}

	err = transaction.Commit()
	if err != nil {
		return "", fmt.Errorf("could not commit transaction %q: %w", filename, err)
	}

	// Skip VACUUM and FTS5 optimize - they're expensive and we compress immediately anyway
	// PRAGMA optimize is cheap and helps query planner
	_, err = client.Exec(`PRAGMA optimize;`)
	if err != nil {
		return "", fmt.Errorf("could not optimize %q: %w", filename, err)
	}

	err = client.Close()
	if err != nil {
		return "", fmt.Errorf("could not close sqlite: %w", err)
	}

	return filename, nil
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

func compress(filename string) error {
	output, err := os.Create(filename + ".zst.partial")
	if err != nil {
		return fmt.Errorf("could not create file: %w", err)
	}

	defer func() {
		_ = output.Close()
		_ = os.Rename(filename+".zst.partial", filename+".zst")
	}()

	input, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("could not open file: %w", err)
	}
	defer func() {
		_ = input.Close()
	}()

	// Get encoder from pool
	encoderIface := encoderPool.Get()
	if encoderIface == nil {
		return fmt.Errorf("could not get encoder from pool")
	}
	encoder := encoderIface.(*zstd.Encoder)
	defer encoderPool.Put(encoder)

	// Reset encoder for reuse
	encoder.Reset(nil)

	writer, err := seekable.NewWriter(output, encoder)
	if err != nil {
		return fmt.Errorf("could not load writer: %w", err)
	}
	defer func() {
		_ = writer.Close()
	}()

	// Use pooled buffer for copying
	bufPtr := copyBufferPool.Get().(*[]byte)
	defer copyBufferPool.Put(bufPtr)

	_, err = io.CopyBuffer(writer, input, *bufPtr)
	if err != nil {
		return fmt.Errorf("could not compress file: %w", err)
	}

	err = os.Remove(filename)
	if err != nil {
		return fmt.Errorf("could not remove original file: %w", err)
	}

	return nil
}
