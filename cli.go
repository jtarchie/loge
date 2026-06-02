package loge

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/goccy/go-json"
	"github.com/jtarchie/loge/managers"
	_ "github.com/jtarchie/sqlitezstd"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

// QueryResponse is the JSON shape returned by the query endpoint.
type QueryResponse struct {
	Status string                `json:"status"`
	Data   []managers.QueryEntry `json:"data"`
}

type CLI struct {
	Port               int           `default:"3000"  help:"start HTTP server on port"            required:""`
	Buckets            int           `default:"4"     help:"number of buckets to fill into"       required:""`
	PayloadSize        int           `default:"1000"  help:"size of the bucket payload"           required:""`
	OutputPath         string        `default:"tmp/"  help:"output path for all the sqlite files" required:""`
	DropOnBackpressure bool          `default:"false" help:"drop data instead of blocking when backpressure occurs"`
	FlushInterval      time.Duration `default:"1s"    help:"how often a bucket flushes a non-empty batch"`
	CompactInterval    time.Duration `default:"30s"   help:"how often to compact small files into segments (0 disables)"`
	CompactMinFiles    int           `default:"8"     help:"minimum number of flush files before a compaction pass runs"`
	Durable            bool          `default:"true"  help:"write-ahead log each payload (fsync) before acknowledging; disable for faster, lossy-on-crash ingest"`
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

	buckets, err := NewBuckets(ctx, c.Buckets, c.PayloadSize, c.OutputPath, c.DropOnBackpressure,
		WithFlushInterval(c.FlushInterval))
	if err != nil {
		return fmt.Errorf("could not create buckets: %w", err)
	}

	// Background compaction merges the many small flush files into fewer,
	// larger indexed segments. Disabled when the interval is non-positive.
	if c.CompactInterval > 0 {
		compactor := NewCompactor(c.OutputPath, c.CompactMinFiles, 0, c.CompactInterval)
		go compactor.Run(ctx)
	}

	// Optional write-ahead log: durably record (and fsync) each payload before
	// acknowledging it, and replay any segments left by a previous crash.
	var wal *WAL

	walDir := filepath.Join(c.OutputPath, "wal")

	if c.Durable {
		recovered, err := ReplayWAL(walDir, func(payload *Payload) {
			buckets.Append(*payload)
		})
		if err != nil {
			return fmt.Errorf("could not replay write-ahead log: %w", err)
		}

		if recovered > 0 {
			slog.Info("replayed write-ahead log", slog.Int("payloads", recovered))
		}

		wal, err = OpenWAL(walDir)
		if err != nil {
			return fmt.Errorf("could not open write-ahead log: %w", err)
		}
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

		if wal != nil {
			// Durably record the payload before acknowledging so a crash cannot
			// lose acknowledged data (at-least-once).
			if _, err := wal.Append(payload); err != nil {
				return fmt.Errorf("could not write to write-ahead log: %w", err)
			}

			buckets.Append(*payload)

			return context.NoContent(http.StatusOK)
		}

		buckets.Append(*payload)

		// 202 Accepted: the payload is queued for asynchronous flushing and is
		// not yet durably on disk (write-ahead log disabled).
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

	router.POST("/api/v1/query", func(context echo.Context) error {
		defer func() {
			_ = context.Request().Body.Close()
		}()

		var request managers.QueryRequest
		if err := json.NewDecoder(context.Request().Body).Decode(&request); err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "invalid query request")
		}

		results, err := manager.Query(context.Request().Context(), request)
		if err != nil {
			return fmt.Errorf("could not query: %w", err)
		}

		if results == nil {
			results = []managers.QueryEntry{}
		}

		return context.JSON(http.StatusOK, &QueryResponse{
			Status: "success",
			Data:   results,
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

	// Buckets.Close flushed everything durably, so the write-ahead log is no
	// longer needed: stop it and remove its segments. (On a crash the segments
	// survive and are replayed on the next start.)
	if wal != nil {
		if err := wal.Close(); err != nil {
			return fmt.Errorf("could not close write-ahead log: %w", err)
		}

		if err := RemoveWAL(walDir); err != nil {
			return fmt.Errorf("could not clear write-ahead log: %w", err)
		}
	}

	slog.Info("shutdown complete")

	return nil
}
