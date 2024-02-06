package loge

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type Bucket struct {
	payload     []*Payload
	payloadSize int
	outputDir   string
}

type Buckets []*Bucket

func NewBuckets(
	size int,
	payloadSize int,
	outputPath string,
) Buckets {
	var buckets Buckets

	for i := 0; i < size; i++ {
		buckets = append(buckets, NewBucket(payloadSize, outputPath))
	}

	return buckets
}

func (b Buckets) Append(index int, payload *Payload) error {
	err := b[index].Append(payload)
	if err != nil {
		return fmt.Errorf("could not append to bucket %d: %w", index, err)
	}

	return nil
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
	defer client.Close()

	client.Exec(`
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

	insertLabels, err := transaction.Prepare(`
		INSERT INTO labels
			(payload)
				VALUES
			(?);
	`)
	if err != nil {
		return fmt.Errorf("could not prepare insert %q: %w", filename, err)
	}

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
		vacuum;
		pragma optimize;
	`)
	if err != nil {
		return fmt.Errorf("could not optimize %q: %w", filename, err)
	}

	b.payload = b.payload[:0]

	return nil
}
