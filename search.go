package loge

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"github.com/jtarchie/loge/managers"
)

// SearchCmd queries a running loge server over its HTTP API using a LogQL-style
// selector. It is a thin client: it builds the same managers.QueryRequest the
// server's POST /api/v1/query handler consumes, so CLI results match the server.
type SearchCmd struct {
	Selector string `arg:"" help:"LogQL-style selector, e.g. '{app=\"web\"} |= \"timeout\"'"`

	Addr   string        `default:"http://localhost:3000" help:"base URL of the loge server"`
	Since  time.Duration `help:"relative window start, e.g. 1h (from now); ignored if --start is set"`
	Until  time.Duration `help:"relative window end, e.g. 5m ago (from now); ignored if --end is set"`
	Start  string        `help:"absolute window start: RFC3339 or unix nanoseconds; overrides --since"`
	End    string        `help:"absolute window end: RFC3339 or unix nanoseconds; overrides --until"`
	Limit  int           `default:"100"  help:"max results (server caps at 5000)"`
	Output string        `default:"text" enum:"text,json" help:"output format"`

	// Out is where results are written; nil defaults to os.Stdout. Excluded from
	// kong flag parsing so tests can capture output.
	Out io.Writer `kong:"-"`
}

func (c *SearchCmd) Run() error {
	matchers, line, err := ParseSelector(c.Selector)
	if err != nil {
		return fmt.Errorf("invalid selector: %w", err)
	}

	start, end, err := c.resolveWindow(time.Now())
	if err != nil {
		return err
	}

	response, err := c.query(context.Background(), managers.QueryRequest{
		Start:    start,
		End:      end,
		Matchers: matchers,
		Line:     line,
		Limit:    c.Limit,
	})
	if err != nil {
		return err
	}

	out := c.Out
	if out == nil {
		out = os.Stdout
	}

	return c.render(out, response)
}

// resolveWindow turns the relative (--since/--until) and absolute
// (--start/--end) flags into an inclusive [start, end] window in nanoseconds (0
// meaning unbounded). Absolute flags override their relative counterparts.
func (c *SearchCmd) resolveWindow(now time.Time) (int64, int64, error) {
	var start, end int64

	if c.Since > 0 {
		start = now.Add(-c.Since).UnixNano()
	}

	if c.Until > 0 {
		end = now.Add(-c.Until).UnixNano()
	}

	if c.Start != "" {
		value, err := parseTimeArg(c.Start)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid --start: %w", err)
		}

		start = value
	}

	if c.End != "" {
		value, err := parseTimeArg(c.End)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid --end: %w", err)
		}

		end = value
	}

	if start != 0 && end != 0 && start > end {
		return 0, 0, fmt.Errorf("start (%d) is after end (%d)", start, end)
	}

	return start, end, nil
}

// parseTimeArg parses an absolute time given as unix nanoseconds or an RFC3339
// timestamp, returning unix nanoseconds.
func parseTimeArg(value string) (int64, error) {
	if nanos, err := strconv.ParseInt(value, 10, 64); err == nil {
		return nanos, nil
	}

	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, value); err == nil {
			return t.UnixNano(), nil
		}
	}

	return 0, fmt.Errorf("%q is not unix nanoseconds or an RFC3339 time", value)
}

// query POSTs the request to the server's query endpoint and decodes the result.
func (c *SearchCmd) query(ctx context.Context, request managers.QueryRequest) (QueryResponse, error) {
	var response QueryResponse

	body, err := json.Marshal(request)
	if err != nil {
		return response, fmt.Errorf("could not encode query: %w", err)
	}

	url := strings.TrimRight(c.Addr, "/") + "/api/v1/query"

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return response, fmt.Errorf("could not build request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return response, fmt.Errorf("could not reach loge server at %s: %w", c.Addr, err)
	}

	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))

		return response, fmt.Errorf("server returned %s: %s", resp.Status, strings.TrimSpace(string(snippet)))
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return response, fmt.Errorf("could not decode query response: %w", err)
	}

	return response, nil
}

// render writes the results in the configured output format. Entries are
// returned newest-first by the server.
func (c *SearchCmd) render(w io.Writer, response QueryResponse) error {
	if c.Output == "json" {
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")

		if err := encoder.Encode(response.Data); err != nil {
			return fmt.Errorf("could not encode results: %w", err)
		}

		return nil
	}

	for _, entry := range response.Data {
		timestamp := time.Unix(0, entry.Timestamp).UTC().Format(time.RFC3339Nano)

		if _, err := fmt.Fprintf(w, "%s %s %s\n", timestamp, formatLabels(entry.Labels), entry.Line); err != nil {
			return fmt.Errorf("could not write result: %w", err)
		}
	}

	return nil
}

// formatLabels renders labels as a stable {k="v", ...} string with sorted keys.
func formatLabels(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	pairs := make([]string, 0, len(keys))
	for _, key := range keys {
		pairs = append(pairs, fmt.Sprintf("%s=%q", key, labels[key]))
	}

	return "{" + strings.Join(pairs, ", ") + "}"
}
