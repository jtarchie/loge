package loge

import (
	"context"
	"crypto/subtle"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"sync"

	"github.com/goccy/go-json"
	"github.com/jtarchie/loge/managers"
	"github.com/jtarchie/sqlitezstd"
	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
)

// cacheVFSName is a sqlite VFS registered once with a decoded-frame cache, used
// to open (local and remote) segments. The cache holds decoded zstd frames, so
// it avoids re-decoding (and, for remote segments, re-fetching) on repeated reads.
const cacheVFSName = "zstdcache"

var (
	cacheVFSOnce sync.Once
	cacheVFSErr  error
)

func ensureCacheVFS(frames int) (string, error) {
	cacheVFSOnce.Do(func() {
		cacheVFSErr = sqlitezstd.Register(cacheVFSName,
			sqlitezstd.WithFrameCacheSize(frames),
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

// queryRequest is the HTTP query payload: the engine's QueryRequest plus an
// optional raw LogQL selector. When Query is non-empty it is parsed with
// ParseSelector to fill Matchers+Line, so the web UI (and any caller) can post a
// single selector string like `{app="web"} |= "error"`; structured callers (the
// CLI, existing clients) omit Query and keep posting matchers/line directly.
type queryRequest struct {
	managers.QueryRequest
	Query string `json:"query"`
}

// StatsResponse summarizes the catalog (segment tiering, row count, time span)
// for benchmarking and observability.
type StatsResponse struct {
	Status         string `json:"status"`
	SegmentsTotal  int    `json:"segments_total"`
	SegmentsLocal  int    `json:"segments_local"`
	SegmentsRemote int    `json:"segments_remote"`
	RowsTotal      int64  `json:"rows_total"`
	MinTimestamp   int64  `json:"min_timestamp"`
	MaxTimestamp   int64  `json:"max_timestamp"`
}

// PlanSegment is one cold-tier (S3) segment in a client-side search plan: its
// catalog ID and a short-lived presigned URL the client reads it from directly.
type PlanSegment struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

// PlanResponse answers a client-side search plan request. The server scans the
// hot tier itself (Hot) and hands the client the pruned, presigned cold segments
// (Segments) to scan on its own machine; ExpiresAt is when the presigned URLs
// stop working.
type PlanResponse struct {
	Status    string                `json:"status"`
	Hot       []managers.QueryEntry `json:"hot"`
	Segments  []PlanSegment         `json:"segments"`
	ExpiresAt int64                 `json:"expires_at"`
}

// bearerAuth gates a route with a shared API key presented as a bearer token.
// When apiKey is empty the route is left open, matching the other endpoints'
// trusted-network posture.
func bearerAuth(apiKey string) echo.MiddlewareFunc {
	const prefix = "Bearer "

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			if apiKey == "" {
				return next(c)
			}

			header := c.Request().Header.Get("Authorization")

			token := strings.TrimPrefix(header, prefix)
			if !strings.HasPrefix(header, prefix) ||
				subtle.ConstantTimeCompare([]byte(token), []byte(apiKey)) != 1 {
				return echo.NewHTTPError(http.StatusUnauthorized, "invalid or missing api key")
			}

			return next(c)
		}
	}
}

// CLI is the top-level command. The server is the default command (so bare
// flags like `loge --port 6500` still start it), and `loge search` queries a
// running server. CLI itself has no Run() method on purpose: kong invokes the
// Run() of every selected node's ancestors, so a CLI.Run() would also fire for
// the search subcommand.
type CLI struct {
	Serve  ServeCmd  `cmd:"" default:"withargs" help:"run the loge ingest+query HTTP server (default)"`
	Search SearchCmd `cmd:""                    help:"query a running loge server with a LogQL-style selector"`
}

// ServeCmd runs the ingest+query HTTP server. Its fields are every server flag.
type ServeCmd struct {
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
	S3Bucket         string        `name:"s3-bucket"            env:"BUCKET_NAME"         default:""      help:"S3 bucket to rotate old segments into (empty disables S3 tiering)"`
	S3Prefix         string        `name:"s3-prefix"            default:"loge/" help:"key prefix for uploaded segments"`
	S3Endpoint       string        `name:"s3-endpoint"          env:"AWS_ENDPOINT_URL_S3" default:""      help:"custom S3 endpoint (e.g. MinIO/Tigris); empty uses AWS"`
	S3Region         string        `name:"s3-region"            env:"AWS_REGION"          default:""      help:"S3 region (else from the AWS environment)"`
	S3ForcePathStyle bool          `name:"s3-force-path-style"  default:"false" help:"use path-style S3 addressing (needed for MinIO)"`
	S3ReadURLBase    string        `name:"s3-read-url-base"     default:""      help:"public/CDN base URL reads are served from; empty derives from bucket/region/endpoint"`
	S3ACL            string        `name:"s3-acl"               default:""      help:"canned ACL for uploaded objects (e.g. public-read); empty relies on bucket policy"`
	S3RotateAge      time.Duration `name:"s3-rotate-age"        default:"1h"    help:"rotate local segments older than this to S3"`
	S3RotateInterval time.Duration `name:"s3-rotate-interval"   default:"1m"    help:"how often the rotation loop runs"`
	S3RotateGrace    time.Duration `name:"s3-rotate-grace"      default:"1m"    help:"keep a rotated segment's local copy this long before deleting it"`
	S3FrameCacheSize int           `name:"s3-frame-cache-size"  default:"512"   help:"per-file decoded zstd frames to cache for segment reads (each frame is ~64 KiB)"`
	S3PresignExpiry  time.Duration `name:"s3-presign-expiry"    default:"1h"    help:"validity of presigned segment URLs handed to client-side (--local) searches"`

	// APIKey gates the read + search endpoints (/query, /labels, /stats,
	// /search/plan) and the web UI's data calls. When set, callers must present
	// it as a bearer token; when empty everything is unauthenticated (matching
	// the trusted-network posture). Ingest (/push) and the static UI assets stay
	// open regardless, so the login page can load.
	APIKey string `name:"api-key" env:"LOGE_API_KEY" default:"" help:"shared secret required (as a bearer token) to read/query and to load the web UI's data; empty disables auth"`
}

func (c *ServeCmd) Run() error {
	err := os.MkdirAll(c.OutputPath, 0o750)
	if err != nil {
		return fmt.Errorf("could not create directory: %w", err)
	}

	// Register the frame-caching VFS used to open segments (local and remote).
	vfsName, err := ensureCacheVFS(c.S3FrameCacheSize)
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
	// store is retained beyond S3 setup so the client-side search plan endpoint
	// can presign segment URLs. It stays nil when S3 tiering is disabled.
	var store *S3Store

	if c.S3Bucket != "" {
		store, err = NewS3Store(ctx, S3Config{
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
			WithUploaderVFS(vfsName),
		)

		// Rebuild catalog rows for already-rotated (cold) segments from the
		// bucket listing, so a fresh server with no local catalog can still see
		// and query them. Degrade rather than fail: an S3 hiccup at boot still
		// lets the server serve its local data.
		if rediscovered, err := uploader.ReconcileRemote(ctx); err != nil {
			slog.Warn("could not reconcile remote segments from S3 listing",
				slog.String("error", err.Error()))
		} else if rediscovered > 0 {
			slog.Info("rediscovered remote segments from S3 listing",
				slog.Int("segments", rediscovered))
		}

		go uploader.Run(ctx)
	}

	router := echo.New()
	// Echo v5 logs through log/slog; route its logs to the app's default handler
	// (stderr) instead of v5's own stdout JSON handler.
	router.Logger = slog.Default()
	router.Use(middleware.Recover())
	router.JSONSerializer = DefaultJSONSerializer{}

	router.POST("/api/v1/push", func(context *echo.Context) error {
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

	router.GET("/api/v1/labels", func(context *echo.Context) error {
		labels, err := manager.Labels()
		if err != nil {
			return fmt.Errorf("could not load labels: %w", err)
		}

		return response(context, http.StatusOK, &LabelResponse{
			Status: "success",
			Data:   labels,
		})
	}, bearerAuth(c.APIKey))

	// auth probe: 200 when the presented token is accepted (or no key is set),
	// 401 otherwise. The web UI calls this on load to decide whether to show a
	// login prompt; `required` tells it whether a token is needed at all.
	router.GET("/api/v1/auth", func(context *echo.Context) error {
		return context.JSON(http.StatusOK, map[string]bool{"required": c.APIKey != ""})
	}, bearerAuth(c.APIKey))

	// Stats summarizes the catalog so benchmarks can observe tiering (local vs
	// remote segment counts), total rows, and the indexed time span.
	router.GET("/api/v1/stats", func(context *echo.Context) error {
		segments, err := catalog.List()
		if err != nil {
			return fmt.Errorf("could not list segments: %w", err)
		}

		stats := StatsResponse{Status: "success"}

		for index, segment := range segments {
			stats.SegmentsTotal++

			if segment.Location == managers.LocationRemote {
				stats.SegmentsRemote++
			} else {
				stats.SegmentsLocal++
			}

			stats.RowsTotal += segment.RowCount

			if index == 0 || segment.MinTimestamp < stats.MinTimestamp {
				stats.MinTimestamp = segment.MinTimestamp
			}

			if segment.MaxTimestamp > stats.MaxTimestamp {
				stats.MaxTimestamp = segment.MaxTimestamp
			}
		}

		return context.JSON(http.StatusOK, &stats)
	}, bearerAuth(c.APIKey))

	router.POST("/api/v1/query", func(context *echo.Context) error {
		defer func() {
			_ = context.Request().Body.Close()
		}()

		var request queryRequest
		if err := json.NewDecoder(context.Request().Body).Decode(&request); err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "invalid query request")
		}

		// A raw LogQL selector (used by the web UI) takes precedence: parse it
		// into the matchers+line filter the engine expects, reusing the same
		// ParseSelector the CLI uses. Structured callers omit it and keep their
		// matchers/line untouched.
		if strings.TrimSpace(request.Query) != "" {
			matchers, line, err := ParseSelector(request.Query)
			if err != nil {
				return echo.NewHTTPError(http.StatusBadRequest, "invalid query: "+err.Error())
			}

			request.Matchers = matchers
			request.Line = line
		}

		results, err := manager.Query(context.Request().Context(), request.QueryRequest)
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
	}, bearerAuth(c.APIKey))

	if store != nil && c.APIKey == "" {
		slog.Warn("client-side search plan endpoint is unauthenticated; set --api-key to require a bearer token")
	}

	// Client-side search: the server scans only the hot tier itself and hands the
	// caller a plan of presigned cold (S3) segments to scan on its own machine,
	// moving the heavy historical scan off the server. Requires S3 tiering.
	router.POST("/api/v1/search/plan", func(context *echo.Context) error {
		defer func() {
			_ = context.Request().Body.Close()
		}()

		if store == nil {
			return echo.NewHTTPError(http.StatusBadRequest, "client-side search requires S3 tiering (set --s3-bucket)")
		}

		var request managers.QueryRequest
		if err := json.NewDecoder(context.Request().Body).Decode(&request); err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "invalid query request")
		}

		reqCtx := context.Request().Context()

		// Hot tier: scan only the server's local/recent sources.
		request.LocalOnly = true

		hot, err := manager.Query(reqCtx, request)
		if err != nil {
			return fmt.Errorf("could not query hot tier: %w", err)
		}

		if hot == nil {
			hot = []managers.QueryEntry{}
		}

		// Cold tier: the remote segments overlapping the window that the trigram
		// filter cannot rule out — presigned for the client to scan directly.
		segments, err := catalog.Overlapping(request.Start, request.End)
		if err != nil {
			return fmt.Errorf("could not prune segments: %w", err)
		}

		plan := make([]PlanSegment, 0, len(segments))

		for _, segment := range segments {
			if segment.Location != managers.LocationRemote {
				continue
			}

			if request.Line != "" && !managers.LineFilterMayContain(segment.LineFilter, request.Line) {
				continue
			}

			url, err := store.Presign(reqCtx, path.Join(c.S3Prefix, segment.ID), c.S3PresignExpiry)
			if err != nil {
				return fmt.Errorf("could not presign segment: %w", err)
			}

			plan = append(plan, PlanSegment{ID: segment.ID, URL: url})
		}

		return context.JSON(http.StatusOK, &PlanResponse{
			Status:    "success",
			Hot:       hot,
			Segments:  plan,
			ExpiresAt: time.Now().Add(c.S3PresignExpiry).UnixNano(),
		})
	}, bearerAuth(c.APIKey))

	// Serve the embedded web UI (built into webdist/). The static assets are left
	// unauthenticated so the login page can load; the UI's data calls carry the
	// bearer token to the gated /api/v1/* endpoints above. FileFS maps GET / to
	// index.html (Echo's static dir handler would 301/404 it instead).
	uiFS := echo.MustSubFS(webDist, "webdist")
	router.FileFS("/", "index.html", uiFS)
	router.StaticFS("/assets", echo.MustSubFS(uiFS, "assets"))

	// Graceful shutdown handling: a SIGINT/SIGTERM cancels serverCtx, which drives
	// StartConfig.Start to gracefully drain in-flight requests (up to GracefulTimeout)
	// before returning. Start swallows http.ErrServerClosed and returns nil on a
	// clean shutdown, so any non-nil error here is a genuine startup failure.
	serverCtx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()

	serverErr := echo.StartConfig{
		Address:         fmt.Sprintf(":%d", c.Port),
		HideBanner:      true,
		GracefulTimeout: 30 * time.Second,
	}.Start(serverCtx, router)
	if serverErr != nil {
		return serverErr
	}

	// The server has drained, so stop background work (compaction, S3 rotation,
	// bucket flush timers) before the final flush below.
	slog.Info("shutting down server...")
	cancel()

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
