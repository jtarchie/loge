package managers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/georgysavva/scany/v2/sqlscan"
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
}

// QueryEntry is a single matching log line with its stream's labels.
type QueryEntry struct {
	Timestamp int64             `json:"timestamp"`
	Line      string            `json:"line"`
	Labels    map[string]string `json:"labels"`
}

// queryRow is the per-row scan target for the SQL query.
type queryRow struct {
	Timestamp int64  `db:"timestamp"`
	Line      string `db:"line"`
	Labels    string `db:"labels"`
}

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

	sources, err := m.sources(req.Start, req.End, req.Line)
	if err != nil {
		return nil, err
	}

	var (
		mu      sync.Mutex
		results []QueryEntry
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

		// Use the trigram line index to accelerate the line filter on segments
		// that have it; LIKE still runs as the exact filter so correctness does
		// not depend on the index.
		useLineIndex := req.Line != "" && len(req.Line) >= 3 && m.hasLineSearch(ctx, src.dsn, client)

		sqlText, args := buildQuery(req, limit, useLineIndex, isHTTP(src.dsn))

		var rows []queryRow
		if err := sqlscan.Select(ctx, client, &rows, sqlText, args...); err != nil {
			return fmt.Errorf("could not query streams: %w", err)
		}

		local := make([]QueryEntry, 0, len(rows))

		for _, row := range rows {
			labels := map[string]string{}
			if err := json.Unmarshal([]byte(row.Labels), &labels); err != nil {
				return fmt.Errorf("could not decode labels: %w", err)
			}

			if !matchesRegex(labels, regexMatchers) {
				continue
			}

			local = append(local, QueryEntry{
				Timestamp: row.Timestamp,
				Line:      row.Line,
				Labels:    labels,
			})
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
		return results[i].Timestamp > results[j].Timestamp
	})

	if len(results) > limit {
		results = results[:limit]
	}

	return results, nil
}

// hasLineSearch reports whether a file has the trigram line_search index,
// caching the (immutable) answer per file.
func (m *Local) hasLineSearch(ctx context.Context, filename string, client *sql.DB) bool {
	if cached, ok := m.lineSearch.Load(filename); ok {
		return cached.(bool)
	}

	var count int

	err := client.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'line_search'`,
	).Scan(&count)

	has := err == nil && count > 0
	m.lineSearch.Store(filename, has)

	return has
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
// most limit rows (newest first); the caller merges across files. When
// useLineIndex is set the trigram line_search index is joined in to accelerate
// the line filter (the literal LIKE still runs as the exact filter). For remote
// (S3) sources we MATCH only the keyword's first word — a single, cheaper
// trigram probe that means fewer random reads over HTTP — and let the LIKE
// refine; local sources keep the full-phrase MATCH (random reads are cheap on
// SSD, where a more selective MATCH wins).
func buildQuery(req QueryRequest, limit int, useLineIndex, remote bool) (string, []any) {
	var sb strings.Builder

	sb.WriteString(`SELECT s.timestamp AS timestamp, s.line AS line, json(l.payload) AS labels
		FROM streams s JOIN labels l ON l.id = s.label_id`)

	args := make([]any, 0, len(req.Matchers)+4)

	// The MATCH term must be a necessary condition for the full-keyword LIKE: the
	// whole keyword always is, and so is its first word (a substring of it) at a
	// fraction of the trigram probes — preferred for remote sources.
	matchTerm := req.Line
	useMatch := useLineIndex

	if useLineIndex && remote {
		if word, ok := firstIndexableWord(req.Line); ok {
			matchTerm = word
		} else {
			useMatch = false // no token long enough to trigram-match; LIKE only
		}
	}

	if useMatch {
		sb.WriteString(" JOIN line_search ON line_search.rowid = s.id")
	}

	sb.WriteString(" WHERE 1 = 1")

	if useMatch {
		sb.WriteString(" AND line_search MATCH ?")
		args = append(args, `"`+strings.ReplaceAll(matchTerm, `"`, `""`)+`"`)
	}

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

// firstIndexableWord returns the first whitespace-delimited token of line that
// is at least three runes long (the trigram minimum), usable as a
// necessary-condition MATCH term. Any token is a substring of the keyword, so a
// line matching the full keyword always matches the token's trigrams too. ok is
// false when no token qualifies, in which case the caller skips the MATCH and
// relies on the LIKE scan.
func firstIndexableWord(line string) (string, bool) {
	for _, field := range strings.Fields(line) {
		if utf8.RuneCountInString(field) >= 3 {
			return field, true
		}
	}

	return "", false
}

// escapeLike escapes the LIKE wildcards in a literal substring filter.
func escapeLike(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)

	return replacer.Replace(value)
}
