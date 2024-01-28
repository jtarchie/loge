package loge

import (
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"github.com/jtarchie/worker"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	_ "github.com/mattn/go-sqlite3"
	slogecho "github.com/samber/slog-echo"
	"github.com/tinylib/msgp/msgp"
)

type CLI struct {
	Port       int `help:"start HTTP server on port" default:"3000" required:""`
	Buckets    int `help:"number of buckets to fill into" required:"" default:"8"`
	BucketSize int `help:"size of the bucket" required:"" default:"100000"`
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
				slog.Error(
					"could not create transaction",
					slog.String("error", err.Error()),
					slog.String("filename", filename),
				)

				os.Exit(1)

				return
			}
			defer transaction.Rollback()

			insertStream, err := transaction.Prepare(`
				INSERT INTO streams
					(timestamp, line, label_id)
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

			insertLabels, err := transaction.Prepare(`
			INSERT INTO labels
				(payload)
					VALUES
				(?);
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
					resultLabel, err := insertLabels.Exec(MarshalLabels(stream.Stream))
					if err != nil {
						slog.Error(
							"could not insert",
							slog.String("error", err.Error()),
							slog.String("filename", filename),
						)

						os.Exit(1)
					}

					labelID, _ := resultLabel.LastInsertId()

					for _, value := range stream.Values {
						_, err := insertStream.Exec(value[0], value[1], labelID)
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
				pragma journal_mode = delete; -- to be able to actually set page size
				pragma page_size = 1024; -- trade off of number of requests that need to be made vs overhead. 

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

		contentType := c.Request().Header.Get(echo.HeaderContentType)
		switch {
		case strings.HasPrefix(contentType, "application/msgpack"):
			err := msgp.Decode(c.Request().Body, payload)
			if err != nil {
				return fmt.Errorf("could not unmarshal msgpack: %w", err)
			}
		case strings.HasPrefix(contentType, "application/json"):
			err := json.NewDecoder(c.Request().Body).Decode(payload)
			if err != nil {
				return fmt.Errorf("could not unmarshal json: %w", err)
			}
		default:
			return fmt.Errorf("could not read streams")
		}

		bucketWorkers.Enqueue(payload)

		return c.String(http.StatusOK, "")
	})

	return router.Start(fmt.Sprintf(":%d", c.Port))
}
