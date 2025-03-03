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
	client       *sql.DB
	transaction  *sql.Tx
	insertStream *sql.Stmt
	insertLabels *sql.Stmt
	filename     string

	minTimestamp int64
	maxTimestamp int64

	maxPayloadSize int
	payloadSize    atomic.Int64
	outputDir      string
	prefix         string
}

type Buckets struct {
	buckets []*Bucket
	workers *worker.Worker[*Payload]
}

func NewBuckets(
	size int,
	payloadSize int,
	outputPath string,
) *Buckets {
	buckets := make([]*Bucket, 0, size)

	for index := range size {
		buckets = append(buckets, NewBucket(payloadSize, outputPath, fmt.Sprintf("bucket-%d", index)))
	}

	workers := worker.New(payloadSize, size, func(index int, payload *Payload) {
		err := buckets[index-1].Append(payload)
		if err != nil {
			slog.Error("could not append to bucket", slog.Int("index", index), slog.String("error", err.Error()))
		}
	})

	return &Buckets{
		buckets: buckets,
		workers: workers,
	}
}

func (b *Buckets) Append(payload *Payload) {
	_ = b.workers.Enqueue(payload)
}

func NewBucket(
	payloadSize int,
	outputDir string,
	prefix string,
) *Bucket {
	return &Bucket{
		outputDir:      outputDir,
		maxPayloadSize: payloadSize,
		prefix:         prefix,
	}
}

func (b *Bucket) Append(payload *Payload) error {
	if b.client == nil {
		err := b.prepare()
		if err != nil {
			return fmt.Errorf("could not start database: %w", err)
		}
	}

	for _, stream := range payload.Streams {
		resultLabel, err := b.insertLabels.Exec(MarshalLabels(stream.Stream))
		if err != nil {
			return fmt.Errorf("could not insert %q: %w", b.filename, err)
		}

		labelID, _ := resultLabel.LastInsertId()

		for _, value := range stream.Values {
			timestamp := value.Timestamp()

			_, err := b.insertStream.Exec(timestamp, value[1], labelID)
			if err != nil {
				return fmt.Errorf("could not insert %q: %w", b.filename, err)
			}

			b.minTimestamp = min(b.minTimestamp, timestamp)
			b.maxTimestamp = max(b.maxTimestamp, timestamp)
		}
	}

	b.payloadSize.Add(1)

	if b.payloadSize.Load() >= int64(b.maxPayloadSize) {
		err := b.flush()
		if err != nil {
			return fmt.Errorf("could not flush %q: %w", b.filename, err)
		}

		b.payloadSize.Store(0)
	}

	return nil
}

func (b *Bucket) prepare() error {
	filename := filepath.Join(b.outputDir, fmt.Sprintf("%s-%d.sqlite", b.prefix, time.Now().UnixNano()))

	err := os.MkdirAll(b.outputDir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("could not create directory: %w", err)
	}

	client, err := sql.Open("sqlite3", filename)
	if err != nil {
		return fmt.Errorf("could not open sqlite3 %q: %w", filename, err)
	}

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
		return fmt.Errorf("could not create schema %q: %w", filename, err)
	}

	transaction, err := client.Begin()
	if err != nil {
		return fmt.Errorf("could not create transaction %q: %w", filename, err)
	}

	insertStream, err := transaction.Prepare(`
		INSERT INTO streams
			(timestamp, line, label_id)
				VALUES
			(?, ?, ?);
	`)
	if err != nil {
		return fmt.Errorf("could not prepare insert %q: %w", filename, err)
	}

	insertLabels, err := transaction.Prepare(`
		INSERT INTO labels
			(payload)
				VALUES
			(jsonb(?));
	`)
	if err != nil {
		return fmt.Errorf("could not prepare insert %q: %w", filename, err)
	}

	b.client = client
	b.transaction = transaction
	b.insertStream = insertStream
	b.insertLabels = insertLabels
	b.filename = filename

	b.minTimestamp = math.MaxInt64
	b.maxTimestamp = math.MinInt64

	return nil
}

func (b *Bucket) flush() error {
	defer func() {
		b.insertStream.Close()
		b.insertLabels.Close()
		_ = b.transaction.Rollback()
		b.client.Close()

		b.client = nil
		b.transaction = nil
		b.insertStream = nil
		b.insertLabels = nil

		b.minTimestamp = math.MaxInt64
		b.maxTimestamp = math.MinInt64
	}()

	err := b.transaction.Commit()
	if err != nil {
		return fmt.Errorf("could not commit transaction %q: %w", b.filename, err)
	}

	_, err = b.client.Exec(`INSERT INTO metadata (key, value) VALUES ('minTimestamp', ?);`, b.minTimestamp)
	if err != nil {
		return fmt.Errorf("could not insert minTimestamp %q: %w", b.filename, err)
	}

	_, err = b.client.Exec(`INSERT INTO metadata (key, value) VALUES ('maxTimestamp', ?);`, b.maxTimestamp)
	if err != nil {
		return fmt.Errorf("could not insert maxTimestamp %q: %w", b.filename, err)
	}

	_, err = b.client.Exec(`
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
		
		INSERT INTO
			search(search)
		VALUES
			('optimize');

		vacuum;
		pragma optimize;
	`)
	if err != nil {
		return fmt.Errorf("could not optimize %q: %w", b.filename, err)
	}

	err = b.client.Close()
	if err != nil {
		return fmt.Errorf("could not close sqlite: %w", err)
	}

	output, err := os.Create(b.filename + ".zst.partial")
	if err != nil {
		return fmt.Errorf("could not create file: %w", err)
	}

	defer func() {
		_ = output.Close()
		_ = os.Rename(b.filename+".zst.partial", b.filename+".zst")
	}()

	input, err := os.Open(b.filename)
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

	err = os.Remove(b.filename)
	if err != nil {
		return fmt.Errorf("could not remove original file: %w", err)
	}

	return nil
}
