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

	"sync"

	"github.com/goccy/go-json"
	"github.com/jtarchie/loge/managers"
	"github.com/jtarchie/sqlitezstd"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

// cacheVFSName is a sqlite VFS registered once with an HTTP read cache, used to
// open (local and remote) segments. The cache is a no-op for local files.
const cacheVFSName = "zstdcache"

var (
	cacheVFSOnce sync.Once
	cacheVFSErr  error
)

func ensureCacheVFS(cacheBytes int64, pageBytes int) (string, error) {
	cacheVFSOnce.Do(func() {
		cacheVFSErr = sqlitezstd.Register(cacheVFSName,
			sqlitezstd.WithHTTPCacheSize(cacheBytes),
			sqlitezstd.WithHTTPPageSize(pageBytes),
			sqlitezstd.WithLogger(slog.Default()),
		)
	})

	return cacheVFSName, cacheVFSErr
}

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
	CheckpointInterval time.Duration `default:"2s"    help:"how often to fsync new segments and prune the write-ahead log"`
	QueryConcurrency   int           `default:"8"     help:"max segments a query opens in parallel"`

	// S3 tiered storage (rotate old segments to S3, read them back over HTTP).
	// Leave --s3-bucket empty to keep everything local.
	S3Bucket         string        `name:"s3-bucket"            default:""      help:"S3 bucket to rotate old segments into (empty disables S3 tiering)"`
	S3Prefix         string        `name:"s3-prefix"            default:"loge/" help:"key prefix for uploaded segments"`
	S3Endpoint       string        `name:"s3-endpoint"          default:""      help:"custom S3 endpoint (e.g. MinIO); empty uses AWS"`
	S3Region         string        `name:"s3-region"            default:""      help:"S3 region (else from the AWS environment)"`
	S3ForcePathStyle bool          `name:"s3-force-path-style"  default:"false" help:"use path-style S3 addressing (needed for MinIO)"`
	S3ReadURLBase    string        `name:"s3-read-url-base"     default:""      help:"public/CDN base URL reads are served from; empty derives from bucket/region/endpoint"`
	S3ACL            string        `name:"s3-acl"               default:""      help:"canned ACL for uploaded objects (e.g. public-read); empty relies on bucket policy"`
	S3RotateAge      time.Duration `name:"s3-rotate-age"        default:"1h"    help:"rotate local segments older than this to S3"`
	S3RotateInterval time.Duration `name:"s3-rotate-interval"   default:"1m"    help:"how often the rotation loop runs"`
	S3RotateGrace    time.Duration `name:"s3-rotate-grace"      default:"1m"    help:"keep a rotated segment's local copy this long before deleting it"`
	S3HTTPCacheBytes int64         `name:"s3-http-cache-bytes"  default:"33554432" help:"per-file in-memory HTTP read cache for remote segments (bytes; 0 disables)"`
	S3HTTPPageBytes  int           `name:"s3-http-page-bytes"   default:"0"     help:"coalescing page size for the HTTP read cache (0 uses the default)"`
}

func (c *CLI) Run() error {
	err := os.MkdirAll(c.OutputPath, 0o750)
	if err != nil {
		return fmt.Errorf("could not create directory: %w", err)
	}

	// Register the HTTP-caching VFS used to open segments (local and remote).
	vfsName, err := ensureCacheVFS(c.S3HTTPCacheBytes, c.S3HTTPPageBytes)
	if err != nil {
		return fmt.Errorf("could not register cache vfs: %w", err)
	}

	// The catalog indexes every compacted segment (local or remote) so queries
	// can prune by time without opening files. Reconcile rebuilds it from the
	// segments on disk in case it lagged a crash.
	catalog, err := managers.OpenCatalog(c.OutputPath)
	if err != nil {
		return fmt.Errorf("could not open catalog: %w", err)
	}
	defer func() {
		_ = catalog.Close()
	}()

	if err := catalog.Reconcile(c.OutputPath); err != nil {
		return fmt.Errorf("could not reconcile catalog: %w", err)
	}

	manager, err := managers.NewLocal(c.OutputPath,
		managers.WithCatalog(catalog),
		managers.WithVFS(vfsName),
		managers.WithQueryConcurrency(c.QueryConcurrency),
	)
	if err != nil {
		return fmt.Errorf("could not start manager: %w", err)
	}
	defer func() {
		_ = manager.Close()
	}()

	// Create a context that will be cancelled on shutdown signal
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Optional write-ahead log + checkpointer: durably record (and fsync) each
	// payload before acknowledging it, replay any segments left by a previous
	// crash, and keep the log bounded by pruning segments once their data is
	// durably in a queryable segment.
	var (
		wal          *WAL
		checkpointer *Checkpointer
		walDir       = filepath.Join(c.OutputPath, "wal")
	)

	bucketOpts := []BucketOption{WithFlushInterval(c.FlushInterval)}

	// Durable acknowledgement requires the payload to reach a segment, so it is
	// incompatible with dropping on backpressure; durability wins.
	dropOnBackpressure := c.DropOnBackpressure

	if c.Durable {
		if dropOnBackpressure {
			slog.Warn("ignoring --drop-on-backpressure because --durable is set")

			dropOnBackpressure = false
		}

		wal, err = OpenWAL(walDir)
		if err != nil {
			return fmt.Errorf("could not open write-ahead log: %w", err)
		}

		checkpointer = NewCheckpointer(wal, c.CheckpointInterval)
		bucketOpts = append(bucketOpts, WithWAL(wal), WithDurableReport(checkpointer.Report))
	}

	buckets, err := NewBuckets(ctx, c.Buckets, c.PayloadSize, c.OutputPath, dropOnBackpressure, bucketOpts...)
	if err != nil {
		return fmt.Errorf("could not create buckets: %w", err)
	}

	// Replay segments left by a previous crash through the pipeline without
	// re-logging them.
	if c.Durable {
		recovered, err := ReplaySegments(wal.Recovered(), func(payload *Payload) {
			buckets.appendReplay(*payload)
		})
		if err != nil {
			return fmt.Errorf("could not replay write-ahead log: %w", err)
		}

		if recovered > 0 {
			slog.Info("replayed write-ahead log", slog.Int("payloads", recovered))
		}
	}

	// Background compaction merges the many small flush files into fewer,
	// larger indexed segments. Disabled when the interval is non-positive.
	if c.CompactInterval > 0 {
		compactor := NewCompactor(c.OutputPath, c.CompactMinFiles, 0, c.CompactInterval,
			WithCompactorCatalog(catalog))
		go compactor.Run(ctx)
	}

	// Optional S3 tiering: rotate segments older than --s3-rotate-age to S3 and
	// serve their reads back over HTTP. Ingest durability is unaffected — S3
	// only receives already-durable, compacted, queryable segments, so an upload
	// failure or S3 outage just keeps a segment local.
	if c.S3Bucket != "" {
		store, err := NewS3Store(ctx, S3Config{
			Bucket:         c.S3Bucket,
			Prefix:         c.S3Prefix,
			Endpoint:       c.S3Endpoint,
			Region:         c.S3Region,
			ForcePathStyle: c.S3ForcePathStyle,
			ReadURLBase:    c.S3ReadURLBase,
			ACL:            c.S3ACL,
		})
		if err != nil {
			return fmt.Errorf("could not create s3 store: %w", err)
		}

		uploader := NewUploader(c.OutputPath, catalog, store,
			WithUploadPrefix(c.S3Prefix),
			WithRotateAge(c.S3RotateAge),
			WithRotateGrace(c.S3RotateGrace),
			WithRotateInterval(c.S3RotateInterval),
		)
		go uploader.Run(ctx)
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

		// Append durably logs the payload first when the WAL is enabled,
		// returning an error if it could not be persisted.
		if err := buckets.Append(*payload); err != nil {
			return fmt.Errorf("could not append payload: %w", err)
		}

		if c.Durable {
			// 200 OK: the payload is durably logged (at-least-once).
			return context.NoContent(http.StatusOK)
		}

		// 202 Accepted: queued for asynchronous flushing, not yet durable.
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

	// After server stops, gracefully close buckets to flush all remaining data.
	// This drains the flush/compress pipeline (which feeds the checkpointer),
	// so it must happen before the checkpointer is stopped.
	slog.Info("flushing remaining data...")
	if err := buckets.Close(); err != nil {
		return fmt.Errorf("could not close buckets: %w", err)
	}

	// Buckets.Close flushed everything, so the write-ahead log is no longer
	// needed: stop the checkpointer, close the log, and remove its segments.
	// (On a crash the segments survive and are replayed on the next start.)
	if checkpointer != nil {
		checkpointer.Stop()
	}

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
