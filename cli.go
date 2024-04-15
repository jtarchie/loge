package loge

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/jtarchie/loge/managers"
	"github.com/jtarchie/sqlitezstd"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	slogecho "github.com/samber/slog-echo"
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
	manager := managers.NewLocal(c.OutputPath)

	router := echo.New()
	router.Use(slogecho.New(slog.Default()))
	router.Use(middleware.Recover())
	router.HideBanner = true
	router.JSONSerializer = DefaultJSONSerializer{}

	router.POST("/api/v1/push", func(context echo.Context) error {
		payload := &Payload{}

		err := bind(context, payload)
		if err != nil {
			return fmt.Errorf("could not bind payload: %w", err)
		}

		buckets.Append(payload)

		return context.String(http.StatusOK, "")
	})

	router.GET("/api/v1/labels", func(context echo.Context) error {
		labels, err := manager.Labels()
		if err != nil {
			return fmt.Errorf("could not load labels: %w", err)
		}

		return response(context, http.StatusOK, &LabelResponse{
			Status: "success",
			Data:   labels,
		})
	})

	return router.Start(fmt.Sprintf(":%d", c.Port))
}
