package loge

import (
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/jtarchie/worker"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	_ "github.com/mattn/go-sqlite3"
	slogecho "github.com/samber/slog-echo"
)

type CLI struct {
	Port       int `help:"start HTTP server on port" default:"3000" required:""`
	Buckets    int `help:"number of buckets to fill into" required:"" default:"2"`
	BucketSize int `help:"size of the bucket" required:"" default:"10000"`
}

func (c *CLI) Run() error {

	buckets := make([][]*Payload, c.Buckets)
	for i := 0; i < c.Buckets; i++ {
		buckets[i] = make([]*Payload, 0, c.BucketSize)
	}

	bucketWorkers := worker.New(c.BucketSize, c.Buckets, func(index int, payload *Payload) {
		buckets[index-1] = append(buckets[index-1], payload)

		// flush to sqlite
		if c.BucketSize <= len(buckets[index-1]) {
			defer func() {
				buckets[index-1] = buckets[index-1][:0]
			}()

			filename := fmt.Sprintf("tmp/%d.sqlite", time.Now().UnixNano())

			client, err := sql.Open("sqlite3", filename)
			if err != nil {
				slog.Error(
					"could not open sqlite",
					slog.String("error", err.Error()),
					slog.String("filename", filename),
				)

				return
			}
			defer client.Close()

			client.Exec(`
					CREATE TABLE streams (
						id INTEGER PRIMARY KEY AUTOINCREMENT,
						timestamp INTEGER,
						line TEXT,
						labels JSON
					);
				`)

			transaction, err := client.Begin()
			if err != nil {
				slog.Error(
					"could not create transaction",
					slog.String("error", err.Error()),
					slog.String("filename", filename),
				)

				os.Exit(1)

				return
			}
			defer transaction.Rollback()

			insert, err := transaction.Prepare(`
				INSERT INTO streams
					(timestamp, line, labels)
						VALUES
					(?, ?, ?);
				`)
			if err != nil {
				slog.Error(
					"could not prepare insert",
					slog.String("error", err.Error()),
					slog.String("filename", filename),
				)

				os.Exit(1)

				return
			}

			for _, payload := range buckets[index-1] {
				for _, stream := range payload.Streams {
					labels := MarshalLabels(stream.Stream)
					for _, value := range stream.Values {
						_, err := insert.Exec(value[0], value[1], labels)
						if err != nil {
							slog.Error(
								"could not insert",
								slog.String("error", err.Error()),
								slog.String("filename", filename),
							)

							os.Exit(1)
						}
					}
				}
			}

			err = transaction.Commit()
			if err != nil {
				slog.Error(
					"could not commit",
					slog.String("error", err.Error()),
					slog.String("filename", filename),
				)

				os.Exit(1)
			}

			client.Exec(`
				vacuum;
				pragma optimize;
			`)
		}
	})

	router := echo.New()
	router.Use(slogecho.New(slog.Default()))
	router.Use(middleware.Recover())
	router.HideBanner = true
	router.JSONSerializer = DefaultJSONSerializer{}

	router.PUT("/api/streams", func(c echo.Context) error {
		payload := &Payload{}

		err := c.Bind(payload)
		if err != nil {
			return fmt.Errorf("could not read streams: %w", err)
		}

		bucketWorkers.Enqueue(payload)

		return c.String(http.StatusOK, "")
	})

	return router.Start(fmt.Sprintf(":%d", c.Port))
}
