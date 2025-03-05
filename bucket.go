package loge

import (
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg"
	"github.com/jtarchie/worker"
	"github.com/klauspost/compress/zstd"
	_ "github.com/mattn/go-sqlite3"
)

type Bucket struct {
	outputDir   string
	payloads    []Payload
	payloadSize atomic.Int64
	prefix      string
}

type Buckets struct {
	workers *worker.Worker[Payload]
}

func NewBuckets(
	size int,
	payloadSize int,
	outputPath string,
) (*Buckets, error) {
	buckets := make([]*Bucket, 0, size)

	for index := range size {
		bucket, err := NewBucket(outputPath, fmt.Sprintf("bucket-%d", index))
		if err != nil {
			return nil, fmt.Errorf("could not create bucket: %w", err)
		}
		buckets = append(buckets, bucket)
	}

	compressors := worker.New(size*10, 1, func(_ int, filename string) {
		err := compress(filename)
		if err != nil {
			slog.Error("could not compress file", slog.String("filename", filename), slog.String("error", err.Error()))
		}
	})

	flushers := worker.New(size, max(size/2, 1), func(index int, payloads []Payload) {
		filename, err := flusher(payloads, outputPath, fmt.Sprintf("bucket-%d", index))
		if err != nil {
			slog.Error("could not flush payloads", slog.Int("index", index), slog.String("error", err.Error()))
		}

		compressors.Enqueue(filename)
	})

	workers := worker.New(payloadSize, size, func(index int, payload Payload) {
		bucket := buckets[index-1]
		err := bucket.Append(payload)
		if err != nil {
			slog.Error("could not append to bucket", slog.Int("index", index), slog.String("error", err.Error()))
		}

		if bucket.Length() >= int64(payloadSize) {
			if flushers.Enqueue(bucket.payloads, worker.WithTimeout(time.Millisecond)) {
				bucket.payloads = []Payload{}
				bucket.payloadSize.Store(0)
			}
		}
	})

	return &Buckets{
		workers: workers,
	}, nil
}

func (b *Buckets) Append(payload Payload) {
	_ = b.workers.Enqueue(payload)
}

func NewBucket(
	outputDir string,
	prefix string,
) (*Bucket, error) {
	bucket := &Bucket{
		outputDir: outputDir,
		payloads:  []Payload{},
		prefix:    prefix,
	}

	return bucket, nil
}

func (b *Bucket) Append(payload Payload) error {
	b.payloads = append(b.payloads, payload)
	b.payloadSize.Add(1)

	return nil
}

func (b *Bucket) Length() int64 {
	return b.payloadSize.Load()
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

	_, err = client.Exec(`
		CREATE TABLE labels (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			-- payload is a key-value store of string-string pairs
			payload BLOB
		) STRICT;

		CREATE TABLE streams (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			-- timestamp is the unix timestamp in nanoseconds
			timestamp INTEGER,
			-- line is the log line
			line TEXT,
			-- label_id is the foreign key to the labels table
			-- no constraint is enforced
			label_id INTEGER
		) STRICT;

		CREATE TABLE metadata (
			key TEXT PRIMARY KEY,
			value TEXT
		) STRICT;

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

	insertStream, err := transaction.Prepare(`
		INSERT INTO streams
			(timestamp, line, label_id)
				VALUES
			(?, ?, ?);
	`)
	if err != nil {
		return "", fmt.Errorf("could not prepare insert %q: %w", filename, err)
	}

	insertLabels, err := transaction.Prepare(`
		INSERT INTO labels
			(payload)
				VALUES
			(jsonb(?));
	`)
	if err != nil {
		return "", fmt.Errorf("could not prepare insert %q: %w", filename, err)
	}

	var minTimestamp int64 = math.MaxInt64
	var maxTimestamp int64 = math.MinInt64

	for _, payload := range payloads {
		for _, stream := range payload.Streams {
			resultLabel, err := insertLabels.Exec(MarshalLabels(stream.Stream))
			if err != nil {
				return "", fmt.Errorf("could not insert %q: %w", filename, err)
			}

			labelID, _ := resultLabel.LastInsertId()

			for _, value := range stream.Values {
				timestamp := value.Timestamp()

				_, err := insertStream.Exec(timestamp, value[1], labelID)
				if err != nil {
					return "", fmt.Errorf("could not insert %q: %w", filename, err)
				}

				minTimestamp = min(minTimestamp, timestamp)
				maxTimestamp = max(maxTimestamp, timestamp)
			}
		}
	}

	_, err = transaction.Exec(`INSERT INTO metadata (key, value) VALUES ('minTimestamp', ?);`, minTimestamp)
	if err != nil {
		return "", fmt.Errorf("could not insert minTimestamp %q: %w", filename, err)
	}

	_, err = transaction.Exec(`INSERT INTO metadata (key, value) VALUES ('maxTimestamp', ?);`, maxTimestamp)
	if err != nil {
		return "", fmt.Errorf("could not insert maxTimestamp %q: %w", filename, err)
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
			payload;

		INSERT INTO stream_tree (id, minTimestamp, maxTimestamp) SELECT id, timestamp, timestamp FROM streams;
	`)
	if err != nil {
		return "", fmt.Errorf("could not optimize %q: %w", filename, err)
	}

	err = transaction.Commit()
	if err != nil {
		return "", fmt.Errorf("could not commit transaction %q: %w", filename, err)
	}

	_, err = client.Exec(`
			INSERT INTO
				search(search)
			VALUES
				('optimize');

			vacuum;
			pragma optimize;
	`)
	if err != nil {
		return "", fmt.Errorf("could not optimize %q: %w", filename, err)
	}

	err = client.Close()
	if err != nil {
		return "", fmt.Errorf("could not close sqlite: %w", err)
	}

	return filename, nil
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
		return fmt.Errorf("could not create file: %w", err)
	}
	defer input.Close()

	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
	if err != nil {
		return fmt.Errorf("could not load zstd: %w", err)
	}
	defer encoder.Close()

	writer, err := seekable.NewWriter(output, encoder)
	if err != nil {
		return fmt.Errorf("could not load writer: %w", err)
	}
	defer writer.Close()

	_, err = io.Copy(writer, input)
	if err != nil {
		return fmt.Errorf("could not compress file: %w", err)
	}

	err = os.Remove(filename)
	if err != nil {
		return fmt.Errorf("could not remove original file: %w", err)
	}

	return nil
}
