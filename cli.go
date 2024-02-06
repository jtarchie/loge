package loge

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/goccy/go-json"
	"github.com/jtarchie/worker"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	slogecho "github.com/samber/slog-echo"
	"github.com/tinylib/msgp/msgp"
)

type CLI struct {
	Port        int    `help:"start HTTP server on port" default:"3000" required:""`
	Buckets     int    `help:"number of buckets to fill into" required:"" default:"8"`
	PayloadSize int    `help:"size of the bucket payload" required:"" default:"10000"`
	OutputPath  string `help:"output path for all the sqlite files" required:"" default:"tmp/"`
}

func (c *CLI) Run() error {
	err := os.MkdirAll(c.OutputPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("could not create directory: %w", err)
	}

	buckets := NewBuckets(c.Buckets, c.PayloadSize, c.OutputPath)

	bucketWorkers := worker.New(c.PayloadSize, c.Buckets, func(index int, payload *Payload) {
		err := buckets.Append(index-1, payload)
		if err != nil {
			slog.Error("could not append to bucket", slog.Int("index", index), slog.String("error", err.Error()))
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
		case strings.Contains(contentType, "application/msgpack"):
			err := msgp.Decode(c.Request().Body, payload)
			if err != nil {
				return fmt.Errorf("could not unmarshal msgpack: %w", err)
			}
		case strings.Contains(contentType, "application/json"):
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
