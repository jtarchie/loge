package loge

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/goccy/go-json"
	"github.com/jtarchie/sqlitezstd"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/samber/lo"
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
	err := sqlitezstd.Init()
	if err != nil {
		return fmt.Errorf("could not initialize zstd support: %w", err)
	}

	err = os.MkdirAll(c.OutputPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("could not create directory: %w", err)
	}

	buckets := NewBuckets(c.Buckets, c.PayloadSize, c.OutputPath)

	router := echo.New()
	router.Use(slogecho.New(slog.Default()))
	router.Use(middleware.Recover())
	router.HideBanner = true
	router.JSONSerializer = DefaultJSONSerializer{}

	router.POST("/api/v1/push", func(echoContext echo.Context) error {
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
			return errors.New("could not read streams")
		}

		buckets.Append(payload)

		return echoContext.String(http.StatusOK, "")
	})

	router.GET("/api/v1/labels", func(echoContext echo.Context) error {
		files, err := filepath.Glob(filepath.Join(c.OutputPath, "*.sqlite.zst"))
		if err != nil {
			return fmt.Errorf("could not load files: %w", err)
		}

		var foundLabels []string

		for _, filename := range files {
			client, err := sql.Open("sqlite3", filename+"?vfs=zstd")
			if err != nil {
				return fmt.Errorf("could not open sqlite3 %q: %w", filename, err)
			}
			defer client.Close()

			labels, err := findLabels(echoContext.Request().Context(), client)
			if err != nil {
				return fmt.Errorf("could not find labels: %w", err)
			}

			foundLabels = append(foundLabels, labels...)

			err = client.Close()
			if err != nil {
				return fmt.Errorf("could not close connection: %w", err)
			}
		}

		return echoContext.JSON(http.StatusOK, LabelResponse{
			Status: "success",
			Data:   lo.Uniq(foundLabels),
		})
	})

	return router.Start(fmt.Sprintf(":%d", c.Port))
}
