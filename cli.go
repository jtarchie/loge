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
	Port        int    `default:"3000"  help:"start HTTP server on port"            required:""`
	Buckets     int    `default:"8"     help:"number of buckets to fill into"       required:""`
	PayloadSize int    `default:"10000" help:"size of the bucket payload"           required:""`
	OutputPath  string `default:"tmp/"  help:"output path for all the sqlite files" required:""`
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

	router.PUT("/api/streams", func(echoContext echo.Context) error {
		payload := &Payload{}

		contentType := echoContext.Request().Header.Get(echo.HeaderContentType)
		switch {
		case strings.Contains(contentType, "application/msgpack"):
			err := msgp.Decode(echoContext.Request().Body, payload)
			if err != nil {
				return fmt.Errorf("could not unmarshal msgpack: %w", err)
			}
		case strings.Contains(contentType, "application/json"):
			err := json.NewDecoder(echoContext.Request().Body).Decode(payload)
			if err != nil {
				return fmt.Errorf("could not unmarshal json: %w", err)
			}
		default:
			return fmt.Errorf("could not read streams")
		}

		bucketWorkers.Enqueue(payload)

		return echoContext.String(http.StatusOK, "")
	})

	return router.Start(fmt.Sprintf(":%d", c.Port))
}
