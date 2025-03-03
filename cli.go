package loge

import (
	"fmt"
	"net/http"
	"os"

	"github.com/jtarchie/loge/managers"
	_ "github.com/jtarchie/sqlitezstd"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
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

	manager, err := managers.NewLocal(c.OutputPath)
	if err != nil {
		return fmt.Errorf("could not start manager: %w", err)
	}
	defer manager.Close()

	buckets, err := NewBuckets(c.Buckets, c.PayloadSize, c.OutputPath)
	if err != nil {
		return fmt.Errorf("could not create buckets: %w", err)
	}

	router := echo.New()
	router.Use(middleware.Recover())
	router.HideBanner = true
	router.JSONSerializer = DefaultJSONSerializer{}

	router.POST("/api/v1/push", func(context echo.Context) error {
		payload := &Payload{}

		err := bind(context, payload)
		if err != nil {
			return fmt.Errorf("could not bind payload: %w", err)
		}
		defer context.Request().Body.Close()

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
