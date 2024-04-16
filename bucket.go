package loge

import (
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go"
	"github.com/jtarchie/worker"
	"github.com/klauspost/compress/zstd"
	_ "github.com/mattn/go-sqlite3"
)

type Bucket struct {
	payload     []*Payload
	payloadSize int
	outputDir   string
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

	for range size {
		buckets = append(buckets, NewBucket(payloadSize, outputPath))
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
) *Bucket {
	return &Bucket{
		outputDir:   outputDir,
		payload:     make([]*Payload, 0, payloadSize),
		payloadSize: payloadSize,
	}
}

func (b *Bucket) Append(payload *Payload) error {
	b.payload = append(b.payload, payload)

	if b.payloadSize <= len(b.payload) {
		err := b.flush()
		if err != nil {
			return fmt.Errorf("could not flush: %w", err)
		}
	}

	return nil
}

func (b *Bucket) flush() error {
	filename := filepath.Join(b.outputDir, fmt.Sprintf("%d.sqlite", time.Now().UnixNano()))

	client, err := sql.Open("sqlite3", filename)
	if err != nil {
		return fmt.Errorf("could not open sqlite3 %q: %w", filename, err)
	}

	defer func() {
		client.Close()

		b.payload = b.payload[:0]
	}()

	_, err = client.Exec(`
		CREATE TABLE labels (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			payload JSONB
		);

		CREATE TABLE streams (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			timestamp INTEGER,
			line TEXT,
			label_id INTEGER
		);
	`)
	if err != nil {
		return fmt.Errorf("could not create schema %q: %w", filename, err)
	}

	transaction, err := client.Begin()
	if err != nil {
		return fmt.Errorf("could not create transaction %q: %w", filename, err)
	}

	defer transaction.Rollback()

	insertStream, err := transaction.Prepare(`
		INSERT INTO streams
			(timestamp, line, label_id)
				VALUES
			(?, ?, ?);
	`)
	if err != nil {
		return fmt.Errorf("could not prepare insert %q: %w", filename, err)
	}

	defer insertStream.Close()

	insertLabels, err := transaction.Prepare(`
		INSERT INTO labels
			(payload)
				VALUES
			(?);
	`)
	if err != nil {
		return fmt.Errorf("could not prepare insert %q: %w", filename, err)
	}

	defer insertLabels.Close()

	for _, payload := range b.payload {
		for _, stream := range payload.Streams {
			resultLabel, err := insertLabels.Exec(MarshalLabels(stream.Stream))
			if err != nil {
				return fmt.Errorf("could not insert %q: %w", filename, err)
			}

			labelID, _ := resultLabel.LastInsertId()

			for _, value := range stream.Values {
				_, err := insertStream.Exec(value[0], value[1], labelID)
				if err != nil {
					return fmt.Errorf("could not insert %q: %w", filename, err)
				}
			}
		}
	}

	err = transaction.Commit()
	if err != nil {
		return fmt.Errorf("could not commit transaction %q: %w", filename, err)
	}

	_, err = client.Exec(`
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
		
		INSERT INTO
			search(search)
		VALUES
			('optimize');

		vacuum;
		pragma optimize;
	`)
	if err != nil {
		return fmt.Errorf("could not optimize %q: %w", filename, err)
	}

	err = client.Close()
	if err != nil {
		return fmt.Errorf("could not close sqlite: %w", err)
	}

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
