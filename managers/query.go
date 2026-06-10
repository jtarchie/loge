package managers

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/georgysavva/scany/v2/sqlscan"
	"github.com/goccy/go-json"
)

const (
	defaultQueryLimit = 100
	maxQueryLimit     = 5000
)

// Matcher selects streams by a single label. Type is one of "=", "!=", "=~",
// "!~"; an empty Type is treated as "=".
type Matcher struct {
	Name  string `json:"name"`
	Value string `json:"value"`
	Type  string `json:"type"`
}

// QueryRequest describes a log query: an optional inclusive time window (in
// nanoseconds, 0 meaning unbounded), label matchers, an optional substring
// filter on the log line, and a result limit.
type QueryRequest struct {
	Start    int64     `json:"start"`
	End      int64     `json:"end"`
	Matchers []Matcher `json:"matchers"`
	Line     string    `json:"line"`
	Limit    int       `json:"limit"`
	// LocalOnly restricts the scan to hot sources — the watcher's flush files and
	// catalog segments still living locally — skipping remote (S3) segments. It
	// is used by the client-side search path, where the server scans only the hot
	// tier and the CLI scans the cold tier itself.
	LocalOnly bool `json:"local_only"`
}

// QueryEntry is a single matching log line with its stream's labels.
type QueryEntry struct {
	Timestamp int64             `json:"timestamp"`
	Line      string            `json:"line"`
	Labels    map[string]string `json:"labels"`
}

// rawEntry carries a scanned row until the post-merge label decode. labels
// holds the raw JSON payload; decoded is only populated when regex matchers
// forced an early decode (the map is then reused in the final result).
type rawEntry struct {
	timestamp int64
	line      string
	labels    string
	decoded   map[string]string
}

// labelMapHint pre-sizes decoded label maps; streams typically carry a
// handful of labels (the realistic generator emits five).
const labelMapHint = 8

// Query searches all known files for log lines matching req, pruning files
// whose stored time bounds do not overlap the requested window, and returns
// the most recent matches up to the requested limit.
func (m *Local) Query(ctx context.Context, req QueryRequest) ([]QueryEntry, error) {
	limit := req.Limit
	if limit <= 0 || limit > maxQueryLimit {
		limit = defaultQueryLimit
	}

	// Regex matchers cannot be expressed in SQL here, so they are applied in
	// Go after the SQL narrows results by time, exact matchers, and line.
	regexMatchers, err := compileRegexMatchers(req.Matchers)
	if err != nil {
		return nil, err
	}

	sources, err := m.sources(req.Start, req.End, req.Line, req.Matchers, req.LocalOnly)
	if err != nil {
		return nil, err
	}

	return m.queryOver(ctx, req, sources, limit, regexMatchers), nil
}

// QuerySources runs req against an explicit set of already-pruned segments given
// by their DSNs (local paths or HTTP(S) URLs), reusing the same SQL/regex
// pipeline as Query. Unlike Query it consults neither the catalog nor the
// watcher: the caller supplies the exact sources to scan (e.g. a CLI handed a
// server-built plan of presigned S3 segment URLs). Each DSN is treated as
// pre-pruned — already known to overlap the window and possibly contain the
// keyword — so no per-file time-bounds or trigram check is repeated.
func (m *Local) QuerySources(ctx context.Context, req QueryRequest, dsns []string) ([]QueryEntry, error) {
	limit := req.Limit
	if limit <= 0 || limit > maxQueryLimit {
		limit = defaultQueryLimit
	}

	regexMatchers, err := compileRegexMatchers(req.Matchers)
	if err != nil {
		return nil, err
	}

	sources := make([]querySource, 0, len(dsns))
	for _, dsn := range dsns {
		sources = append(sources, querySource{id: dsn, dsn: dsn, prePruned: true})
	}

	return m.queryOver(ctx, req, sources, limit, regexMatchers), nil
}

// queryOver scans every source for rows matching req (each source capped to
// limit), applies the Go-side regex matchers, then merges the rows newest-first
// and caps the merged set to limit. A failure on one source degrades the result
// to the sources that succeeded rather than failing the whole query.
//
// Label JSON is decoded only for rows that survive the merge cap (each source
// contributes up to limit rows but at most limit survive overall), except when
// regex matchers need the labels up front to filter.
func (m *Local) queryOver(
	ctx context.Context,
	req QueryRequest,
	sources []querySource,
	limit int,
	regexMatchers []regexMatcher,
) []QueryEntry {
	var (
		mu      sync.Mutex
		results []rawEntry
	)

	joinErr := m.forEach(sources, func(src querySource, client *sql.DB) error {
		// Catalog segments are already time-pruned; watcher flush files are not,
		// so prune them by their own stored bounds.
		if !src.prePruned && (req.Start != 0 || req.End != 0) {
			overlaps, err := fileOverlaps(ctx, client, req.Start, req.End)
			if err != nil {
				return fmt.Errorf("could not read file bounds: %w", err)
			}

			if !overlaps {
				return nil
			}
		}

		// Keyword queries scan with LIKE (no FTS index exists). The timestamp
		// index orders the scan newest-first and it stops at LIMIT, and the
		// catalog line filter has already skipped segments that cannot contain
		// the keyword — so a segment is only scanned when it may match.
		sqlText, args := buildQuery(req, limit)

		rows, err := client.QueryContext(ctx, sqlText, args...)
		if err != nil {
			return fmt.Errorf("could not query streams: %w", err)
		}
		defer func() {
			_ = rows.Close()
		}()

		var local []rawEntry

		for rows.Next() {
			var entry rawEntry
			if err := rows.Scan(&entry.timestamp, &entry.line, &entry.labels); err != nil {
				return fmt.Errorf("could not scan stream row: %w", err)
			}

			if len(regexMatchers) > 0 {
				labels := make(map[string]string, labelMapHint)
				if err := json.Unmarshal([]byte(entry.labels), &labels); err != nil {
					return fmt.Errorf("could not decode labels: %w", err)
				}

				if !matchesRegex(labels, regexMatchers) {
					continue
				}

				entry.decoded = labels
			}

			local = append(local, entry)
		}

		if err := rows.Err(); err != nil {
			return fmt.Errorf("could not read streams: %w", err)
		}

		mu.Lock()
		results = append(results, local...)
		mu.Unlock()

		return nil
	})
	if joinErr != nil {
		// Degrade rather than fail: return whatever sources succeeded (e.g. local
		// data when S3 is unreachable) and surface the failures as a warning.
		slog.Warn("some sources failed during query", slog.String("error", joinErr.Error()))
	}

	// Merge across files: newest first, then cap to the requested limit.
	sort.Slice(results, func(i, j int) bool {
		return results[i].timestamp > results[j].timestamp
	})

	if len(results) > limit {
		results = results[:limit]
	}

	// Decode labels for the surviving rows only (regex-filtered rows already
	// carry their map). A row with corrupt labels is dropped, not the query.
	entries := make([]QueryEntry, 0, len(results))

	for _, row := range results {
		labels := row.decoded
		if labels == nil {
			labels = make(map[string]string, labelMapHint)
			if err := json.Unmarshal([]byte(row.labels), &labels); err != nil {
				slog.Warn("could not decode labels", slog.String("error", err.Error()))

				continue
			}
		}

		entries = append(entries, QueryEntry{
			Timestamp: row.timestamp,
			Line:      row.line,
			Labels:    labels,
		})
	}

	return entries
}

// fileOverlaps reports whether a file's stored [minTimestamp, maxTimestamp]
// bounds overlap the requested [start, end] window (0 meaning unbounded). If
// the bounds cannot be read it conservatively reports an overlap so the file
// is not skipped.
func fileOverlaps(ctx context.Context, client *sql.DB, start, end int64) (bool, error) {
	var bounds struct {
		Min int64 `db:"min"`
		Max int64 `db:"max"`
	}

	err := sqlscan.Get(ctx, client, &bounds, `
		SELECT
			(SELECT value FROM metadata WHERE key = 'minTimestamp') AS min,
			(SELECT value FROM metadata WHERE key = 'maxTimestamp') AS max;
	`)
	if err != nil {
		// No usable bounds (older file / missing metadata): do not prune.
		return true, nil //nolint:nilerr
	}

	if start != 0 && bounds.Max < start {
		return false, nil
	}

	if end != 0 && bounds.Min > end {
		return false, nil
	}

	return true, nil
}

// buildQuery assembles the per-file SQL and its arguments. Each file returns at
// most limit rows (newest first); the caller merges across files. Keyword
// filtering is a literal LIKE: there is no FTS index (it was retired for size,
// and a timestamp-ordered LIKE scan that short-circuits at LIMIT beats it on the
// common/recent queries), and the catalog line filter has already pruned
// segments that cannot contain the keyword.
func buildQuery(req QueryRequest, limit int) (string, []any) {
	var sb strings.Builder

	sb.WriteString(`SELECT s.timestamp AS timestamp, s.line AS line, json(l.payload) AS labels
		FROM streams s JOIN labels l ON l.id = s.label_id`)

	args := make([]any, 0, len(req.Matchers)+4)

	sb.WriteString(" WHERE 1 = 1")

	if req.Start != 0 {
		sb.WriteString(" AND s.timestamp >= ?")
		args = append(args, req.Start)
	}

	if req.End != 0 {
		sb.WriteString(" AND s.timestamp <= ?")
		args = append(args, req.End)
	}

	for _, matcher := range req.Matchers {
		switch matcher.Type {
		case "", "=":
			sb.WriteString(" AND json_extract(l.payload, ?) = ?")
			args = append(args, jsonPath(matcher.Name), matcher.Value)
		case "!=":
			sb.WriteString(" AND json_extract(l.payload, ?) IS NOT ?")
			args = append(args, jsonPath(matcher.Name), matcher.Value)
		}
		// "=~" / "!~" are applied in Go (see matchesRegex).
	}

	if req.Line != "" {
		sb.WriteString(` AND s.line LIKE ? ESCAPE '\'`)
		args = append(args, "%"+escapeLike(req.Line)+"%")
	}

	sb.WriteString(" ORDER BY s.timestamp DESC, s.id DESC LIMIT ?")
	args = append(args, limit)

	return sb.String(), args
}

type regexMatcher struct {
	name    string
	re      *regexp.Regexp
	negated bool
}

func compileRegexMatchers(matchers []Matcher) ([]regexMatcher, error) {
	var compiled []regexMatcher

	for _, matcher := range matchers {
		var negated bool

		switch matcher.Type {
		case "=~":
			negated = false
		case "!~":
			negated = true
		default:
			continue
		}

		re, err := regexp.Compile(matcher.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid regex matcher for %q: %w", matcher.Name, err)
		}

		compiled = append(compiled, regexMatcher{name: matcher.Name, re: re, negated: negated})
	}

	return compiled, nil
}

func matchesRegex(labels map[string]string, matchers []regexMatcher) bool {
	for _, matcher := range matchers {
		matched := matcher.re.MatchString(labels[matcher.name])
		if matched == matcher.negated {
			return false
		}
	}

	return true
}

// jsonPath builds a SQLite JSON path for a label name, quoting it so names with
// dots or other special characters are handled correctly.
func jsonPath(name string) string {
	return `$."` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// escapeLike escapes the LIKE wildcards in a literal substring filter.
func escapeLike(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)

	return replacer.Replace(value)
}
