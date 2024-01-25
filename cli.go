package loge

import (
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"
)

type CLI struct {
	Port int `help:"start HTTP server on port" default:"3000" required:""`
}

func (c *CLI) Run() error {
	router := echo.New()
	router.HideBanner = true
	router.JSONSerializer = DefaultJSONSerializer{}

	router.PUT("/api/streams", func(c echo.Context) error {
		payload := &Payload{}
		
		err := c.Bind(payload)
		if err != nil {
			return fmt.Errorf("could not read streams: %w", err)
		}

		return c.String(http.StatusOK, "")
	})

	return router.Start(fmt.Sprintf(":%d", c.Port))
}