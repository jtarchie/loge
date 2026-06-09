package slogloge

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// requestTimeout bounds a single push attempt even when the caller supplies an
// http.Client without its own timeout, so a hung server can never wedge the
// sender goroutine forever.
const requestTimeout = 30 * time.Second

// Wire types matching the /api/v1/push JSON shape
// ({"streams":[{"stream":{...},"values":[["<unix_ns>","line"]]}]}). They are
// defined locally — and this package depends only on the standard library — so
// importing the handler never drags in the root loge package's cgo sqlite,
// echo, and aws-sdk dependencies.
type (
	wireStream map[string]string
	wireValue  [2]string
	wireEntry  struct {
		Stream wireStream  `json:"stream"`
		Values []wireValue `json:"values"`
	}
	wirePayload struct {
		Streams []wireEntry `json:"streams"`
	}
)

// item is one rendered log line awaiting delivery. Handle flattens the
// slog.Record into plain strings before enqueuing, so the sender goroutine
// never touches slog state off the logging goroutine.
type item struct {
	nanos  string
	line   string
	labels map[string]string
	key    string // labelKey(labels), precomputed on the hot path
}

// sender batches items and POSTs them to the loge push endpoint from a single
// background goroutine.
type sender struct {
	url     string
	client  *http.Client
	bearer  string
	batchN  int
	flushIv time.Duration
	retries int
	onError func(error)
	block   bool

	queue   chan item
	flushQ  chan chan struct{}
	dropped atomic.Int64
	quit    chan struct{}
	done    chan struct{}
	once    sync.Once
}

func newSender(options *Options) *sender {
	s := &sender{
		url:     strings.TrimRight(options.Endpoint, "/") + options.Path,
		client:  options.HTTPClient,
		bearer:  options.BearerToken,
		batchN:  options.BatchSize,
		flushIv: options.FlushInterval,
		retries: options.MaxRetries,
		onError: options.OnError,
		block:   options.BlockOnFull,
		queue:   make(chan item, options.QueueCapacity),
		flushQ:  make(chan chan struct{}),
		quit:    make(chan struct{}),
		done:    make(chan struct{}),
	}

	go s.loop()

	return s
}

// enqueue hands an item to the sender. By default it never blocks the logging
// goroutine: a full queue drops the item and bumps the dropped counter. With
// BlockOnFull it waits for room (or for shutdown).
func (s *sender) enqueue(it item) {
	if s.block {
		select {
		case s.queue <- it:
		case <-s.quit:
			s.dropped.Add(1)
		}

		return
	}

	select {
	case s.queue <- it:
	default:
		s.dropped.Add(1)
	}
}

// loop owns the batch buffer and is the only goroutine that POSTs. It flushes
// when the batch reaches BatchSize, on the flush interval, on an explicit
// flush, and once more while draining on shutdown.
func (s *sender) loop() {
	defer close(s.done)

	ticker := time.NewTicker(s.flushIv)
	defer ticker.Stop()

	batch := make([]item, 0, s.batchN)

	flush := func() {
		if len(batch) == 0 {
			return
		}

		s.post(batch)
		batch = batch[:0]
	}

	for {
		select {
		case it := <-s.queue:
			batch = append(batch, it)
			if len(batch) >= s.batchN {
				flush()
			}
		case <-ticker.C:
			flush()
		case reply := <-s.flushQ:
			s.drainInto(&batch)
			flush()
			close(reply)
		case <-s.quit:
			s.drainInto(&batch)
			flush()

			return
		}
	}
}

// drainInto moves every currently-queued item into batch without blocking, so
// a flush or shutdown delivers everything already accepted.
func (s *sender) drainInto(batch *[]item) {
	for {
		select {
		case it := <-s.queue:
			*batch = append(*batch, it)
		default:
			return
		}
	}
}

// post groups items that share a label set into Loki-style streams, marshals
// the payload, and sends it. Delivery errors are routed to OnError.
func (s *sender) post(batch []item) {
	grouped := make(map[string]*wireEntry, len(batch))
	order := make([]string, 0, len(batch))

	for _, it := range batch {
		entry, ok := grouped[it.key]
		if !ok {
			entry = &wireEntry{Stream: wireStream(it.labels)}
			grouped[it.key] = entry
			order = append(order, it.key)
		}

		entry.Values = append(entry.Values, wireValue{it.nanos, it.line})
	}

	out := wirePayload{Streams: make([]wireEntry, 0, len(order))}
	for _, key := range order {
		out.Streams = append(out.Streams, *grouped[key])
	}

	body, err := json.Marshal(out)
	if err != nil {
		s.fail(fmt.Errorf("could not marshal push payload: %w", err))

		return
	}

	if err := s.send(body); err != nil {
		s.fail(err)
	}
}

// send POSTs the body, retrying up to MaxRetries times. Any 2xx is success.
func (s *sender) send(body []byte) error {
	var lastErr error

	for attempt := 0; attempt <= s.retries; attempt++ {
		if err := s.attempt(body); err != nil {
			lastErr = err

			continue
		}

		return nil
	}

	return lastErr
}

func (s *sender) attempt(body []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("could not build push request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	if s.bearer != "" {
		req.Header.Set("Authorization", "Bearer "+s.bearer)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("could not POST to loge: %w", err)
	}

	defer func() { _ = resp.Body.Close() }()

	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("loge push returned %s", resp.Status)
	}

	return nil
}

// fail routes a delivery error to the configured OnError. OnError must never log
// back through this handler, or it would recurse / feed its own queue.
func (s *sender) fail(err error) {
	if err != nil {
		s.onError(err)
	}
}

// flush blocks until every item enqueued before the call has been delivered (or
// routed to OnError). It returns immediately once the sender is shutting down.
func (s *sender) flush() {
	reply := make(chan struct{})

	select {
	case s.flushQ <- reply:
		<-reply
	case <-s.quit:
	}
}

// close stops the sender after a final draining flush. It is idempotent and
// reports any lifetime drops once the goroutine has exited.
func (s *sender) close(ctx context.Context) error {
	s.once.Do(func() { close(s.quit) })

	select {
	case <-s.done:
	case <-ctx.Done():
		return ctx.Err()
	}

	if dropped := s.dropped.Load(); dropped > 0 {
		return fmt.Errorf("slogloge: dropped %d records because the queue was full", dropped)
	}

	return nil
}

// dropped reports how many records have been dropped so far because the queue
// was full.
func (s *sender) droppedCount() int64 {
	return s.dropped.Load()
}

// labelKey builds a stable, collision-resistant fingerprint of a label set so
// values sharing labels group into one stream. Mirrors cmd/loge-loadgen.
func labelKey(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	var sb strings.Builder

	for _, key := range keys {
		sb.WriteString(key)
		sb.WriteByte('=')
		sb.WriteString(labels[key])
		sb.WriteByte('\x00')
	}

	return sb.String()
}
