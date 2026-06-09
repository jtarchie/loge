package slogloge

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Defaults applied when an option is left unset.
const (
	defaultEndpoint      = "http://localhost:3000"
	defaultPath          = "/api/v1/push"
	defaultLevelLabel    = "level"
	defaultBatchSize     = 100
	defaultFlushInterval = time.Second
	defaultQueueCapacity = 4096
	defaultMaxRetries    = 2
)

// Options configures a Handler. Prefer New with the With* option funcs; the
// fields are exported for documentation and advanced inspection. A zero or
// invalid field falls back to its documented default, except LevelLabel, where
// an empty string intentionally disables the level label.
type Options struct {
	// Endpoint is the base URL of the loge server. Default
	// "http://localhost:3000".
	Endpoint string
	// Path is the push route appended to Endpoint. Default "/api/v1/push".
	Path string
	// StaticLabels are attached to every stream (e.g. app, env, host). Keep them
	// low-cardinality; high-cardinality data belongs in the log line.
	StaticLabels map[string]string
	// LevelLabel is the label key the record's level is written to. Empty
	// disables the level label. Default "level".
	LevelLabel string
	// MinLevel is the minimum level to emit. Default slog.LevelInfo.
	MinLevel slog.Leveler
	// BatchSize flushes the buffer once this many records accumulate. Default 100.
	BatchSize int
	// FlushInterval flushes a non-empty buffer at least this often. Default 1s.
	FlushInterval time.Duration
	// QueueCapacity bounds the in-flight queue between logging and delivery.
	// Default 4096.
	QueueCapacity int
	// HTTPClient sends the push requests. Default &http.Client{Timeout: 30s}.
	HTTPClient *http.Client
	// BearerToken, if set, is sent as Authorization: Bearer <token>. The push
	// path is unauthenticated today; this is for forward-compatibility.
	BearerToken string
	// MaxRetries is the number of extra delivery attempts per batch. Default 2.
	MaxRetries int
	// OnError handles delivery failures. Default writes to os.Stderr. It MUST NOT
	// log back through this handler.
	OnError func(error)
	// BlockOnFull makes Handle block until the queue has room instead of dropping
	// records. Default false (drop-and-count).
	BlockOnFull bool
}

// Option mutates Options. Returned by the With* funcs and passed to New.
type Option func(*Options)

// WithEndpoint sets the loge server base URL.
func WithEndpoint(endpoint string) Option {
	return func(o *Options) { o.Endpoint = endpoint }
}

// WithPath overrides the push route appended to the endpoint.
func WithPath(path string) Option {
	return func(o *Options) { o.Path = path }
}

// WithStaticLabels sets the labels attached to every stream.
func WithStaticLabels(labels map[string]string) Option {
	return func(o *Options) { o.StaticLabels = labels }
}

// WithLevelLabel sets the label key the record's level is written to. Pass ""
// to disable the level label.
func WithLevelLabel(key string) Option {
	return func(o *Options) { o.LevelLabel = key }
}

// WithMinLevel sets the minimum level to emit.
func WithMinLevel(level slog.Leveler) Option {
	return func(o *Options) { o.MinLevel = level }
}

// WithBatchSize sets how many records accumulate before a flush.
func WithBatchSize(size int) Option {
	return func(o *Options) { o.BatchSize = size }
}

// WithFlushInterval sets how often a non-empty buffer is flushed.
func WithFlushInterval(interval time.Duration) Option {
	return func(o *Options) { o.FlushInterval = interval }
}

// WithQueueCapacity bounds the in-flight queue between logging and delivery.
func WithQueueCapacity(capacity int) Option {
	return func(o *Options) { o.QueueCapacity = capacity }
}

// WithHTTPClient supplies the http.Client used for delivery.
func WithHTTPClient(client *http.Client) Option {
	return func(o *Options) { o.HTTPClient = client }
}

// WithBearerToken sets an Authorization: Bearer token on push requests.
func WithBearerToken(token string) Option {
	return func(o *Options) { o.BearerToken = token }
}

// WithMaxRetries sets the number of extra delivery attempts per batch.
func WithMaxRetries(retries int) Option {
	return func(o *Options) { o.MaxRetries = retries }
}

// WithOnError sets the delivery-failure callback. It must not log back through
// this handler.
func WithOnError(fn func(error)) Option {
	return func(o *Options) { o.OnError = fn }
}

// WithBlockOnFull makes Handle block until the queue drains instead of dropping
// records when the queue is full.
func WithBlockOnFull(block bool) Option {
	return func(o *Options) { o.BlockOnFull = block }
}

// Handler is a slog.Handler that renders each record as a JSON line and ships it
// to a loge server's /api/v1/push endpoint. Delivery is asynchronous and
// batched; call Close on shutdown to flush. The zero value is not usable — build
// one with New.
type Handler struct {
	inner      slog.Handler  // stdlib JSONHandler rendering the line into buf
	buf        *bytes.Buffer // shared write target for inner, guarded by mu
	mu         *sync.Mutex   // serializes a single render into buf
	sender     *sender       // shared async batcher (shared across clones)
	labels     map[string]string
	levelLabel string
}

var _ slog.Handler = (*Handler)(nil)

// New builds a Handler from the given options and starts its background sender.
// Pair it with Close to flush on shutdown.
func New(opts ...Option) *Handler {
	options := resolveOptions(opts)

	buf := &bytes.Buffer{}
	inner := slog.NewJSONHandler(buf, &slog.HandlerOptions{
		Level:       options.MinLevel,
		ReplaceAttr: dropTopLevelTime,
	})

	return &Handler{
		inner:      inner,
		buf:        buf,
		mu:         &sync.Mutex{},
		sender:     newSender(&options),
		labels:     cloneLabels(options.StaticLabels),
		levelLabel: options.LevelLabel,
	}
}

// Enabled reports whether the level passes the configured minimum.
func (h *Handler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

// Handle renders the record into a JSON line under the shared buffer lock, then
// enqueues it with its labels for asynchronous delivery. The record is not
// retained beyond this call.
func (h *Handler) Handle(ctx context.Context, record slog.Record) error {
	timestamp := record.Time
	if timestamp.IsZero() {
		timestamp = time.Now()
	}

	h.mu.Lock()
	h.buf.Reset()
	err := h.inner.Handle(ctx, record)
	line := strings.TrimRight(h.buf.String(), "\n")
	h.mu.Unlock()

	if err != nil {
		return err
	}

	labels := make(map[string]string, len(h.labels)+1)
	for key, value := range h.labels {
		labels[key] = value
	}

	if h.levelLabel != "" {
		labels[h.levelLabel] = record.Level.String()
	}

	h.sender.enqueue(item{
		nanos:  strconv.FormatInt(timestamp.UnixNano(), 10),
		line:   line,
		labels: labels,
		key:    labelKey(labels),
	})

	return nil
}

// WithAttrs returns a Handler that adds attrs to every record's line. The clone
// shares the underlying buffer and sender.
func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}

	clone := h.clone()
	clone.inner = h.inner.WithAttrs(attrs)

	return clone
}

// WithGroup returns a Handler that nests subsequent attrs under name. The clone
// shares the underlying buffer and sender.
func (h *Handler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}

	clone := h.clone()
	clone.inner = h.inner.WithGroup(name)

	return clone
}

func (h *Handler) clone() *Handler {
	copied := *h
	return &copied
}

// Close flushes buffered records and stops the background sender, blocking until
// the flush completes or ctx is cancelled. It is safe to call more than once.
// Close returns a non-nil error if ctx expired first, or if any records were
// dropped over the Handler's lifetime because the queue was full.
func (h *Handler) Close(ctx context.Context) error {
	return h.sender.close(ctx)
}

// Flush blocks until every record enqueued before the call has been delivered
// (or routed to OnError). Useful in tests and short-lived programs; long-running
// services can rely on the periodic flush.
func (h *Handler) Flush() {
	h.sender.flush()
}

// Dropped reports how many records have been dropped so far because the queue
// was full.
func (h *Handler) Dropped() int64 {
	return h.sender.droppedCount()
}

// resolveOptions applies defaults, runs the user options, then guards fields
// whose zero value is invalid. LevelLabel is deliberately not re-defaulted after
// the options run, so WithLevelLabel("") can disable the level label.
func resolveOptions(opts []Option) Options {
	options := Options{
		Endpoint:      defaultEndpoint,
		Path:          defaultPath,
		LevelLabel:    defaultLevelLabel,
		MinLevel:      slog.LevelInfo,
		BatchSize:     defaultBatchSize,
		FlushInterval: defaultFlushInterval,
		QueueCapacity: defaultQueueCapacity,
		MaxRetries:    defaultMaxRetries,
	}

	for _, opt := range opts {
		opt(&options)
	}

	if options.Endpoint == "" {
		options.Endpoint = defaultEndpoint
	}

	if options.Path == "" {
		options.Path = defaultPath
	}

	if options.MinLevel == nil {
		options.MinLevel = slog.LevelInfo
	}

	if options.BatchSize < 1 {
		options.BatchSize = defaultBatchSize
	}

	if options.FlushInterval <= 0 {
		options.FlushInterval = defaultFlushInterval
	}

	if options.QueueCapacity < 1 {
		options.QueueCapacity = defaultQueueCapacity
	}

	if options.MaxRetries < 0 {
		options.MaxRetries = 0
	}

	if options.HTTPClient == nil {
		options.HTTPClient = &http.Client{Timeout: requestTimeout}
	}

	if options.OnError == nil {
		options.OnError = func(err error) {
			fmt.Fprintf(os.Stderr, "slogloge: %v\n", err)
		}
	}

	return options
}

// dropTopLevelTime removes the top-level time attribute from the rendered line;
// the timestamp travels out-of-band in the push value's first element.
func dropTopLevelTime(groups []string, a slog.Attr) slog.Attr {
	if len(groups) == 0 && a.Key == slog.TimeKey {
		return slog.Attr{}
	}

	return a
}

func cloneLabels(labels map[string]string) map[string]string {
	out := make(map[string]string, len(labels))
	for key, value := range labels {
		out[key] = value
	}

	return out
}
