package managers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

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

	sqlText, args := buildQuery(req, limit)

	var results []QueryEntry

	err = m.execute(func(client *sql.DB) error {
		// Skip files whose stored time bounds do not overlap the query window.
		if req.Start != 0 || req.End != 0 {
			overlaps, err := fileOverlaps(ctx, client, req.Start, req.End)
			if err != nil {
				return fmt.Errorf("could not read file bounds: %w", err)
			}

			if !overlaps {
				return nil
			}
		}

		var rows []queryRow
		if err := sqlscan.Select(ctx, client, &rows, sqlText, args...); err != nil {
			return fmt.Errorf("could not query streams: %w", err)
		}

		for _, row := range rows {
			labels := map[string]string{}
			if err := json.Unmarshal([]byte(row.Labels), &labels); err != nil {
				return fmt.Errorf("could not decode labels: %w", err)
			}

			if !matchesRegex(labels, regexMatchers) {
				continue
			}

			results = append(results, QueryEntry{
				Timestamp: row.Timestamp,
				Line:      row.Line,
				Labels:    labels,
			})
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("could not run query: %w", err)
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
// most limit rows (newest first); the caller merges across files.
func buildQuery(req QueryRequest, limit int) (string, []any) {
	var sb strings.Builder

	sb.WriteString(`SELECT s.timestamp AS timestamp, s.line AS line, json(l.payload) AS labels
		FROM streams s JOIN labels l ON l.id = s.label_id WHERE 1 = 1`)

	args := make([]any, 0, len(req.Matchers)+3)

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
