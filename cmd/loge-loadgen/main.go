// Command loge-loadgen generates realistic web-access and structured JSON app
// logs and pushes them to a loge server's /api/v1/push endpoint. It is a
// self-contained, CGO-free binary (no sqlite) so it can be shipped as its own
// Fly machine alongside the loge app under benchmark.
//
// Unlike the older k6 generators, it uses BOUNDED label cardinality (a small set
// of app/env/level/host values) and REAL words in the log lines, so label
// matching, time-window pruning, and FTS keyword search all behave like a real
// workload. High-cardinality identifiers (trace_id/user_id) live in the log
// LINE, not the labels — the correct modeling for loge — making them
// needle-in-haystack keyword-search targets.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/alecthomas/kong"
)

// Wire types matching the /api/v1/push JSON shape
// ({"streams":[{"stream":{...},"values":[["<unix_ns>","line"]]}]}).
type (
	stream map[string]string
	value  [2]string
	entry  struct {
		Stream stream  `json:"stream"`
		Values []value `json:"values"`
	}
	payload struct {
		Streams []entry `json:"streams"`
	}
)

type CLI struct {
	Target          string        `default:"http://localhost:6500" help:"base URL of the loge server"`
	Rate            int           `default:"2000"   help:"approximate target log lines per second across all workers"`
	Duration        time.Duration `default:"60s"    help:"how long to generate (0 = until interrupted)"`
	Workers         int           `default:"8"      help:"concurrent POST workers"`
	Batch           int           `default:"500"    help:"log lines per push payload"`
	Mix             string        `default:"web,json" help:"corpora to generate, comma-separated: web, json"`
	HostCardinality int           `default:"20"     help:"number of distinct host label values"`
	Seed            int64         `default:"0"      help:"RNG seed (0 = time-based)"`
}

func main() {
	cli := &CLI{}
	kong.Parse(cli,
		kong.Name("loge-loadgen"),
		kong.Description("Generate realistic web-access and JSON app logs and push them to a loge server."),
	)

	if err := cli.Run(); err != nil {
		fmt.Println("error:", err)
	}
}

func (c *CLI) Run() error {
	corpora := parseMix(c.Mix)
	if len(corpora) == 0 {
		return fmt.Errorf("no valid corpora in --mix %q (want web and/or json)", c.Mix)
	}

	workers := c.Workers
	if workers < 1 {
		workers = 1
	}

	seed := c.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}

	hosts := make([]string, max(c.HostCardinality, 1))
	for i := range hosts {
		hosts[i] = fmt.Sprintf("host-%02d", i)
	}

	pushURL := strings.TrimRight(c.Target, "/") + "/api/v1/push"

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if c.Duration > 0 {
		var cancel context.CancelFunc

		ctx, cancel = context.WithTimeout(ctx, c.Duration)
		defer cancel()
	}

	// Each worker emits its share of the target rate; sleep between batches to
	// hold the pace.
	var batchInterval time.Duration
	if perWorker := float64(c.Rate) / float64(workers); perWorker > 0 {
		batchInterval = time.Duration(float64(c.Batch) / perWorker * float64(time.Second))
	}

	client := &http.Client{Timeout: 30 * time.Second}

	var lines, bytesOut, payloads, errors atomic.Int64

	fmt.Printf("loge-loadgen → %s | mix=%v workers=%d batch=%d rate=%d/s duration=%s\n",
		pushURL, corpora, workers, c.Batch, c.Rate, c.Duration)

	start := time.Now()

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)

		go func(workerSeed int64) {
			defer wg.Done()

			gen := &generator{rand: rand.New(rand.NewSource(workerSeed)), corpora: corpora, hosts: hosts} //nolint:gosec

			for ctx.Err() == nil {
				iterStart := time.Now()

				body, err := json.Marshal(gen.payload(c.Batch))
				if err != nil {
					errors.Add(1)

					continue
				}

				if post(ctx, client, pushURL, body) {
					lines.Add(int64(c.Batch))
					bytesOut.Add(int64(len(body)))
					payloads.Add(1)
				} else if ctx.Err() == nil {
					errors.Add(1)
				}

				if batchInterval > 0 {
					if sleep := batchInterval - time.Since(iterStart); sleep > 0 {
						select {
						case <-ctx.Done():
						case <-time.After(sleep):
						}
					}
				}
			}
		}(seed + int64(w))
	}

	wg.Wait()

	printSummary(time.Since(start), lines.Load(), bytesOut.Load(), payloads.Load(), errors.Load())

	return nil
}

func post(ctx context.Context, client *http.Client, url string, body []byte) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return false
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return false
	}

	defer func() { _ = resp.Body.Close() }()

	_, _ = io.Copy(io.Discard, resp.Body)

	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

func parseMix(mix string) []string {
	var out []string

	for _, part := range strings.Split(mix, ",") {
		switch strings.TrimSpace(part) {
		case "web":
			out = append(out, "web")
		case "json":
			out = append(out, "json")
		}
	}

	return out
}

func printSummary(elapsed time.Duration, lines, bytesOut, payloads, errs int64) {
	secs := elapsed.Seconds()
	if secs <= 0 {
		secs = 1
	}

	fmt.Printf("\n=== loge-loadgen summary ===\n")
	fmt.Printf("duration:   %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("payloads:   %d (%d errors)\n", payloads, errs)
	fmt.Printf("lines:      %d  (%.0f lines/s)\n", lines, float64(lines)/secs)
	fmt.Printf("bytes sent: %.1f MB  (%.2f MB/s)\n", float64(bytesOut)/1e6, float64(bytesOut)/1e6/secs)
}

// generator builds realistic payloads. It is single-goroutine (one per worker)
// so its *rand.Rand needs no locking.
type generator struct {
	rand    *rand.Rand
	corpora []string
	hosts   []string
}

var (
	apps     = []string{"checkout", "auth", "payments", "catalog", "search", "gateway"}
	envs     = []string{"prod", "staging"}
	levels   = []string{"info", "info", "info", "info", "warn", "error", "debug"}
	methods  = []string{"GET", "GET", "GET", "POST", "PUT", "DELETE"}
	statuses = []string{"200", "200", "200", "200", "301", "302", "404", "404", "500", "503"}
	paths    = []string{"/", "/login", "/api/v1/users", "/api/v1/orders", "/checkout", "/search", "/static/app.js", "/health", "/cart", "/products/42"}
	agents   = []string{
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) Firefox/121.0",
		"curl/8.4.0",
		"kube-probe/1.28",
	}
	messages = []string{
		"request completed", "connection refused", "cache miss", "cache hit",
		"user login succeeded", "payment authorized", "timeout waiting for upstream",
		"database query slow", "retrying request", "rate limit exceeded",
	}
)

// payload builds one push payload of `batch` lines, grouping values that share a
// label set into Loki-style streams.
func (g *generator) payload(batch int) *payload {
	grouped := map[string]*entry{}
	order := make([]string, 0, batch)
	now := time.Now()

	for i := 0; i < batch; i++ {
		var (
			labels stream
			line   string
		)

		if g.corpora[g.rand.Intn(len(g.corpora))] == "web" {
			labels, line = g.webLine()
		} else {
			labels, line = g.appLine()
		}

		key := labelKey(labels)

		group, ok := grouped[key]
		if !ok {
			group = &entry{Stream: labels}
			grouped[key] = group
			order = append(order, key)
		}

		// Spread timestamps across the last second so windows have sub-second
		// resolution and the data spans the run.
		ts := now.Add(-time.Duration(g.rand.Intn(1000)) * time.Millisecond).UnixNano()
		group.Values = append(group.Values, value{strconv.FormatInt(ts, 10), line})
	}

	out := &payload{Streams: make([]entry, 0, len(order))}
	for _, key := range order {
		out.Streams = append(out.Streams, *grouped[key])
	}

	return out
}

func (g *generator) webLine() (stream, string) {
	labels := stream{
		"service": "nginx",
		"env":     pick(g.rand, envs),
		"host":    pick(g.rand, g.hosts),
	}

	line := fmt.Sprintf(`%s - - [%s] "%s %s HTTP/1.1" %s %d "-" "%s"`,
		g.ipv4(),
		time.Now().Format("02/Jan/2006:15:04:05 -0700"),
		pick(g.rand, methods),
		pick(g.rand, paths),
		pick(g.rand, statuses),
		g.rand.Intn(20000),
		pick(g.rand, agents),
	)

	return labels, line
}

func (g *generator) appLine() (stream, string) {
	level := pick(g.rand, levels)

	labels := stream{
		"app":   pick(g.rand, apps),
		"env":   pick(g.rand, envs),
		"level": level,
		"host":  pick(g.rand, g.hosts),
	}

	// trace_id / user_id are high-cardinality and live in the line, not labels.
	line := fmt.Sprintf(`{"level":%q,"msg":%q,"trace_id":%q,"user_id":%q,"latency_ms":%d}`,
		level,
		pick(g.rand, messages),
		g.hex(16),
		g.hex(8),
		g.rand.Intn(2000),
	)

	return labels, line
}

func pick(r *rand.Rand, s []string) string { return s[r.Intn(len(s))] }

func (g *generator) ipv4() string {
	return fmt.Sprintf("%d.%d.%d.%d", g.rand.Intn(223)+1, g.rand.Intn(256), g.rand.Intn(256), g.rand.Intn(256))
}

func (g *generator) hex(n int) string {
	const digits = "0123456789abcdef"

	b := make([]byte, n)
	for i := range b {
		b[i] = digits[g.rand.Intn(16)]
	}

	return string(b)
}

func labelKey(labels stream) string {
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
