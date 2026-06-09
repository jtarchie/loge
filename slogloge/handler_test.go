package slogloge

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"
)

// capture records the payloads a test server receives.
type capture struct {
	mu       sync.Mutex
	payloads []wirePayload
	headers  []http.Header
}

func (c *capture) record(r *http.Request) {
	var p wirePayload

	_ = json.NewDecoder(r.Body).Decode(&p)

	c.mu.Lock()
	defer c.mu.Unlock()

	c.payloads = append(c.payloads, p)
	c.headers = append(c.headers, r.Header.Clone())
}

func (c *capture) requests() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.payloads)
}

func (c *capture) entries() []wireEntry {
	c.mu.Lock()
	defer c.mu.Unlock()

	var out []wireEntry
	for _, p := range c.payloads {
		out = append(out, p.Streams...)
	}

	return out
}

func (c *capture) valueCount() int {
	count := 0
	for _, e := range c.entries() {
		count += len(e.Values)
	}

	return count
}

// okServer captures requests and always responds 200.
func okServer(t *testing.T) (*capture, *httptest.Server) {
	t.Helper()

	cap := &capture{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cap.record(r)
		w.WriteHeader(http.StatusOK)
	}))

	t.Cleanup(srv.Close)

	return cap, srv
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}

		time.Sleep(2 * time.Millisecond)
	}

	t.Fatalf("condition not met within %s", timeout)
}

func mustClose(t *testing.T, h *Handler) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := h.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestWireFormat(t *testing.T) {
	cap, srv := okServer(t)

	h := New(
		WithEndpoint(srv.URL),
		WithStaticLabels(map[string]string{"app": "web", "env": "test"}),
	)

	log := slog.New(h)

	before := time.Now().UnixNano()
	log.Info("hello", "user", "alice")
	after := time.Now().UnixNano()

	mustClose(t, h)

	if got := cap.headers[0].Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}

	entries := cap.entries()
	if len(entries) != 1 {
		t.Fatalf("got %d streams, want 1", len(entries))
	}

	labels := entries[0].Stream
	for key, want := range map[string]string{"app": "web", "env": "test", "level": "INFO"} {
		if labels[key] != want {
			t.Errorf("label %q = %q, want %q", key, labels[key], want)
		}
	}

	if len(entries[0].Values) != 1 {
		t.Fatalf("got %d values, want 1", len(entries[0].Values))
	}

	value := entries[0].Values[0]

	ts, err := strconv.ParseInt(value[0], 10, 64)
	if err != nil {
		t.Fatalf("timestamp %q not an int64: %v", value[0], err)
	}

	if ts < before || ts > after {
		t.Errorf("timestamp %d outside [%d, %d]", ts, before, after)
	}

	var line map[string]any
	if err := json.Unmarshal([]byte(value[1]), &line); err != nil {
		t.Fatalf("line %q not JSON: %v", value[1], err)
	}

	if line["msg"] != "hello" {
		t.Errorf("line msg = %v, want hello", line["msg"])
	}

	if line["user"] != "alice" {
		t.Errorf("line user = %v, want alice", line["user"])
	}

	if _, ok := line["time"]; ok {
		t.Errorf("line should not carry top-level time; got %v", line["time"])
	}
}

func TestLevelLabelToggle(t *testing.T) {
	cap, srv := okServer(t)

	h := New(WithEndpoint(srv.URL), WithStaticLabels(map[string]string{"app": "web"}), WithLevelLabel(""))
	slog.New(h).Info("no level label")
	mustClose(t, h)

	if _, ok := cap.entries()[0].Stream["level"]; ok {
		t.Error("level label present despite WithLevelLabel(\"\")")
	}

	cap2, srv2 := okServer(t)

	h2 := New(WithEndpoint(srv2.URL), WithStaticLabels(map[string]string{"app": "web"}))
	slog.New(h2).Warn("default level label")
	mustClose(t, h2)

	if got := cap2.entries()[0].Stream["level"]; got != "WARN" {
		t.Errorf("level label = %q, want WARN", got)
	}
}

func TestGroupingByLabelSet(t *testing.T) {
	cap, srv := okServer(t)

	h := New(WithEndpoint(srv.URL), WithStaticLabels(map[string]string{"app": "web"}))

	log := slog.New(h)
	log.Info("first")
	log.Error("second")
	log.Info("third")

	mustClose(t, h)

	byLevel := map[string]int{}
	for _, e := range cap.entries() {
		byLevel[e.Stream["level"]] += len(e.Values)
	}

	if byLevel["INFO"] != 2 || byLevel["ERROR"] != 1 {
		t.Fatalf("grouping = %v, want INFO:2 ERROR:1", byLevel)
	}
}

func TestBatchBySize(t *testing.T) {
	cap, srv := okServer(t)

	// A one-hour interval guarantees any flush came from the batch threshold,
	// not the ticker.
	h := New(
		WithEndpoint(srv.URL),
		WithStaticLabels(map[string]string{"app": "web"}),
		WithBatchSize(3),
		WithFlushInterval(time.Hour),
	)
	defer mustClose(t, h)

	log := slog.New(h)
	for i := 0; i < 3; i++ {
		log.Info("line")
	}

	waitFor(t, 2*time.Second, func() bool { return cap.requests() == 1 })

	if got := cap.valueCount(); got != 3 {
		t.Fatalf("delivered %d values, want 3", got)
	}
}

func TestFlushByInterval(t *testing.T) {
	cap, srv := okServer(t)

	h := New(
		WithEndpoint(srv.URL),
		WithStaticLabels(map[string]string{"app": "web"}),
		WithBatchSize(1000),
		WithFlushInterval(20*time.Millisecond),
	)
	defer mustClose(t, h)

	slog.New(h).Info("tick me out")

	waitFor(t, 2*time.Second, func() bool { return cap.valueCount() == 1 })
}

func TestCloseFlushes(t *testing.T) {
	cap, srv := okServer(t)

	h := New(
		WithEndpoint(srv.URL),
		WithStaticLabels(map[string]string{"app": "web"}),
		WithBatchSize(1000),
		WithFlushInterval(time.Hour),
	)

	log := slog.New(h)
	log.Info("a")
	log.Info("b")

	mustClose(t, h)

	if got := cap.valueCount(); got != 2 {
		t.Fatalf("delivered %d values after Close, want 2", got)
	}
}

func TestDropOnFull(t *testing.T) {
	release := make(chan struct{})

	cap := &capture{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release // hold the sender goroutine in its POST so the queue backs up
		cap.record(r)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	h := New(
		WithEndpoint(srv.URL),
		WithStaticLabels(map[string]string{"app": "web"}),
		WithBatchSize(1),
		WithQueueCapacity(1),
		WithFlushInterval(time.Hour),
	)

	log := slog.New(h)

	start := time.Now()
	for i := 0; i < 200; i++ {
		log.Info("flood")
	}
	elapsed := time.Since(start)

	if elapsed > time.Second {
		t.Fatalf("Handle blocked: 200 emits took %s", elapsed)
	}

	close(release) // let the held POST and the drain flush complete

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := h.Close(ctx)
	if err == nil {
		t.Fatal("expected a drop error from Close, got nil")
	}

	if h.Dropped() == 0 {
		t.Error("expected dropped > 0")
	}
}

func TestRetryThenSuccess(t *testing.T) {
	var mu sync.Mutex
	attempts := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		attempts++
		n := attempts
		mu.Unlock()

		if n < 3 {
			w.WriteHeader(http.StatusInternalServerError)

			return
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	var onErr int

	h := New(
		WithEndpoint(srv.URL),
		WithStaticLabels(map[string]string{"app": "web"}),
		WithMaxRetries(2),
		WithOnError(func(error) { onErr++ }),
	)

	slog.New(h).Info("eventually delivered")
	mustClose(t, h)

	if onErr != 0 {
		t.Errorf("OnError called %d times, want 0 (3rd attempt succeeds)", onErr)
	}

	mu.Lock()
	defer mu.Unlock()

	if attempts != 3 {
		t.Errorf("attempts = %d, want 3", attempts)
	}
}

func TestOnErrorAfterRetriesExhausted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	var (
		mu      sync.Mutex
		errs    []error
		onError = func(err error) {
			mu.Lock()
			defer mu.Unlock()
			errs = append(errs, err) // no logging here -> no recursion
		}
	)

	h := New(
		WithEndpoint(srv.URL),
		WithStaticLabels(map[string]string{"app": "web"}),
		WithMaxRetries(1),
		WithOnError(onError),
	)

	slog.New(h).Info("never delivered")
	mustClose(t, h)

	mu.Lock()
	defer mu.Unlock()

	if len(errs) != 1 || errs[0] == nil {
		t.Fatalf("OnError errs = %v, want exactly one non-nil error", errs)
	}
}

func TestConcurrentEmit(t *testing.T) {
	cap, srv := okServer(t)

	h := New(
		WithEndpoint(srv.URL),
		WithStaticLabels(map[string]string{"app": "web"}),
		WithQueueCapacity(8192),
		WithBatchSize(50),
		WithFlushInterval(10*time.Millisecond),
	)

	const (
		goroutines = 10
		perG       = 50
	)

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			log := slog.New(h).
				WithGroup("req").
				With("worker", id)

			for i := 0; i < perG; i++ {
				log.Info("work", "i", i)
			}
		}(g)
	}

	wg.Wait()
	mustClose(t, h)

	if got := cap.valueCount(); got != goroutines*perG {
		t.Fatalf("delivered %d values, want %d", got, goroutines*perG)
	}
}

func ExampleNew() {
	handler := New(
		WithEndpoint("http://localhost:3000"),
		WithStaticLabels(map[string]string{"app": "checkout", "env": "prod"}),
	)
	defer func() { _ = handler.Close(context.Background()) }()

	logger := slog.New(handler)
	logger.Info("user logged in", "user_id", "abc123")
}
