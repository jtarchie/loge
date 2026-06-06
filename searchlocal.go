package loge

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"

	"github.com/goccy/go-json"
	"github.com/jtarchie/loge/managers"
	"github.com/jtarchie/sqlitezstd"
)

// clientVFSName is the sqlite VFS the CLI opens cold segments through in --local
// mode: a zstd VFS whose HTTP RoundTripper re-attaches presigned S3 signatures.
const clientVFSName = "zstds3client"

// presignTransport re-attaches presigned S3 query strings to the path-based URLs
// the zstd VFS requests. go-sqlite3 strips the DSN query string before the VFS
// sees it, so the signature cannot travel in the DSN; instead the DSN stays
// path-based and this RoundTripper restores the query per request, keyed by the
// canonical (query-less) URL. Range is not a signed header, so one presigned GET
// URL serves every range read of a segment.
type presignTransport struct {
	base    http.RoundTripper
	queries sync.Map // canonical url (scheme://host/path) -> raw query
}

// put records the presigned query string for a canonical segment URL.
func (t *presignTransport) put(canonical, rawQuery string) {
	t.queries.Store(canonical, rawQuery)
}

func (t *presignTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	raw, ok := t.queries.Load(canonicalURL(req.URL))
	if !ok {
		return t.base.RoundTrip(req)
	}

	// Clone so the cached, path-based request the VFS reuses is never mutated.
	clone := req.Clone(req.Context())
	clone.URL.RawQuery = raw.(string)

	return t.base.RoundTrip(clone)
}

// canonicalURL is the query-less key both sides agree on: scheme://host + path.
func canonicalURL(u *url.URL) string {
	return u.Scheme + "://" + u.Host + u.EscapedPath()
}

// clientVFS registers the client VFS exactly once and returns its transport so
// callers can register presigned segment URLs on it. The frame-cache size is
// fixed at first registration (one search per CLI process).
//
// nolint: gochecknoglobals
var (
	clientTransport *presignTransport
	clientVFSOnce   sync.Once
	clientVFSErr    error
)

func clientVFS(frames int) (*presignTransport, error) {
	clientVFSOnce.Do(func() {
		clientTransport = &presignTransport{base: http.DefaultTransport}
		clientVFSErr = sqlitezstd.Register(clientVFSName,
			sqlitezstd.WithRoundTripper(clientTransport),
			sqlitezstd.WithFrameCacheSize(frames),
		)
	})

	return clientTransport, clientVFSErr
}

// runLocal performs the search on this machine: it asks the server for a plan
// (the hot-tier results it scanned itself plus presigned cold segments), scans
// those cold segments locally, and merges both into one newest-first result set.
func (c *SearchCmd) runLocal(ctx context.Context, request managers.QueryRequest) (QueryResponse, error) {
	var response QueryResponse

	plan, err := c.requestPlan(ctx, request)
	if err != nil {
		return response, err
	}

	transport, err := clientVFS(c.FrameCacheSize)
	if err != nil {
		return response, fmt.Errorf("could not register client vfs: %w", err)
	}

	dsns := make([]string, 0, len(plan.Segments))

	for _, segment := range plan.Segments {
		parsed, err := url.Parse(segment.URL)
		if err != nil {
			return response, fmt.Errorf("invalid presigned url for segment %q: %w", segment.ID, err)
		}

		canonical := canonicalURL(parsed)
		transport.put(canonical, parsed.RawQuery)
		dsns = append(dsns, canonical)
	}

	manager, err := managers.NewLocal("",
		managers.WithoutWatcher(),
		managers.WithVFS(clientVFSName),
		managers.WithQueryConcurrency(c.Concurrency),
	)
	if err != nil {
		return response, fmt.Errorf("could not start local query engine: %w", err)
	}

	defer func() {
		_ = manager.Close()
	}()

	cold, err := manager.QuerySources(ctx, request, dsns)
	if err != nil {
		return response, fmt.Errorf("could not scan cold segments: %w", err)
	}

	// Merge hot (server) + cold (local): newest first, then cap to the limit.
	merged := make([]managers.QueryEntry, 0, len(plan.Hot)+len(cold))
	merged = append(merged, plan.Hot...)
	merged = append(merged, cold...)

	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Timestamp > merged[j].Timestamp
	})

	if limit := effectiveLimit(request.Limit); len(merged) > limit {
		merged = merged[:limit]
	}

	response.Status = "success"
	response.Data = merged

	return response, nil
}

// effectiveLimit mirrors the server's clamp (managers.defaultQueryLimit /
// maxQueryLimit) so the merged cap matches each side's per-source cap.
func effectiveLimit(limit int) int {
	const (
		defaultLimit = 100
		maxLimit     = 5000
	)

	if limit <= 0 || limit > maxLimit {
		return defaultLimit
	}

	return limit
}

// requestPlan POSTs the query to the server's plan endpoint (with the bearer API
// key when set) and decodes the plan.
func (c *SearchCmd) requestPlan(ctx context.Context, request managers.QueryRequest) (PlanResponse, error) {
	var plan PlanResponse

	body, err := json.Marshal(request)
	if err != nil {
		return plan, fmt.Errorf("could not encode query: %w", err)
	}

	endpoint := strings.TrimRight(c.Addr, "/") + "/api/v1/search/plan"

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return plan, fmt.Errorf("could not build request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")

	if c.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.APIKey)
	}

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return plan, fmt.Errorf("could not reach loge server at %s: %w", c.Addr, err)
	}

	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))

		return plan, fmt.Errorf("server returned %s: %s", resp.Status, strings.TrimSpace(string(snippet)))
	}

	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		return plan, fmt.Errorf("could not decode plan response: %w", err)
	}

	return plan, nil
}
