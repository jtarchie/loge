package loge

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jtarchie/loge/managers"
	_ "github.com/jtarchie/sqlitezstd"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

type CLI struct {
	Port               int    `default:"3000"  help:"start HTTP server on port"            required:""`
	Buckets            int    `default:"4"     help:"number of buckets to fill into"       required:""`
	PayloadSize        int    `default:"1000"  help:"size of the bucket payload"           required:""`
	OutputPath         string `default:"tmp/"  help:"output path for all the sqlite files" required:""`
	DropOnBackpressure bool   `default:"false" help:"drop data instead of blocking when backpressure occurs"`
}

func (c *CLI) Run() error {
	err := os.MkdirAll(c.OutputPath, 0o750)
	if err != nil {
		return fmt.Errorf("could not create directory: %w", err)
	}

	manager, err := managers.NewLocal(c.OutputPath)
	if err != nil {
		return fmt.Errorf("could not start manager: %w", err)
	}
	defer func() {
		_ = manager.Close()
	}()

	// Create a context that will be cancelled on shutdown signal
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	buckets, err := NewBuckets(ctx, c.Buckets, c.PayloadSize, c.OutputPath, c.DropOnBackpressure)
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
		defer func() {
			_ = context.Request().Body.Close()
		}()

		// Reject malformed payloads (empty streams, missing labels/values)
		// instead of silently persisting empty or inconsistent files.
		if msg, ok := payload.Valid(); !ok {
			return echo.NewHTTPError(http.StatusBadRequest, msg)
		}

		buckets.Append(*payload)

		// 202 Accepted: the payload is queued for asynchronous flushing and is
		// not yet durably on disk. This becomes 200 once a write-ahead log
		// makes ingestion durable before acknowledgement.
		return context.NoContent(http.StatusAccepted)
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

	// Graceful shutdown handling
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		slog.Info("shutting down server...")

		// Cancel the bucket context to signal shutdown
		cancel()

		// Create a deadline for graceful HTTP shutdown
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()

		// Stop accepting new requests
		if err := router.Shutdown(shutdownCtx); err != nil {
			slog.Error("server shutdown error", slog.String("error", err.Error()))
		}
	}()

	// Start server
	serverErr := router.Start(fmt.Sprintf(":%d", c.Port))
	if serverErr != nil && serverErr != http.ErrServerClosed {
		return serverErr
	}

	// After server stops, gracefully close buckets to flush all remaining data
	slog.Info("flushing remaining data...")
	if err := buckets.Close(); err != nil {
		return fmt.Errorf("could not close buckets: %w", err)
	}
	slog.Info("shutdown complete")

	return nil
}
