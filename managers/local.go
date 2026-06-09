package managers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/georgysavva/scany/v2/sqlscan"
	lru "github.com/hashicorp/golang-lru/v2"
	filewatcher "github.com/jtarchie/loge/file_watcher"
	"github.com/samber/lo"
)

const (
	defaultCachedDBs        = 64
	defaultQueryConcurrency = 8
	defaultVFS              = "zstd"
)

type Local struct {
	cache       *lru.Cache[string, *sql.DB]
	outputDir   string
	watcher     *filewatcher.FileWatcher
	catalog     *Catalog
	vfsName     string
	concurrency int
	noWatcher   bool
}

// Option configures a Local manager.
type Option func(*Local)

// WithCatalog makes the manager prune compacted segments via the catalog (and
// resolve their local/remote location) instead of tracking them with the file
// watcher; the watcher then only tracks ephemeral local flush files.
func WithCatalog(catalog *Catalog) Option {
	return func(l *Local) {
		l.catalog = catalog
	}
}

// WithVFS sets the sqlite VFS name used to open sources (e.g. a cache-enabled
// "zstdcache" VFS for HTTP segments). Defaults to "zstd".
func WithVFS(name string) Option {
	return func(l *Local) {
		if name != "" {
			l.vfsName = name
		}
	}
}

// WithQueryConcurrency bounds how many sources a query opens in parallel.
func WithQueryConcurrency(n int) Option {
	return func(l *Local) {
		if n > 0 {
			l.concurrency = n
		}
	}
}

// WithoutWatcher builds a manager that tracks no local files: it neither starts
// a filesystem watcher nor consults a catalog. Such a manager only serves
// QuerySources (an explicit list of sources), which is what the client-side
// search path needs — it scans the cold segments the server's plan names and
// has no local segment directory of its own.
func WithoutWatcher() Option {
	return func(l *Local) {
		l.noWatcher = true
	}
}

func NewLocal(outputPath string, opts ...Option) (*Local, error) {
	local := &Local{
		outputDir:   outputPath,
		vfsName:     defaultVFS,
		concurrency: defaultQueryConcurrency,
	}

	for _, opt := range opts {
		opt(local)
	}

	// A client-side manager (WithoutWatcher) tracks no local files and only
	// serves QuerySources, so it skips the watcher entirely.
	if !local.noWatcher {
		// With a catalog, segments are tracked there and the watcher only needs the
		// ephemeral local flush files; without one, the watcher tracks everything.
		pattern := `\.sqlite\.zst$`
		if local.catalog != nil {
			pattern = `bucket-.*\.sqlite\.zst$`
		}

		watcher, err := filewatcher.New(outputPath, regexp.MustCompile(pattern))
		if err != nil {
			return nil, fmt.Errorf("could not start watcher: %w", err)
		}

		local.watcher = watcher
	}

	cache, err := lru.NewWithEvict[string, *sql.DB](defaultCachedDBs, func(_ string, client *sql.DB) {
		_ = client.Close()
	})
	if err != nil {
		return nil, fmt.Errorf("could not start cache: %w", err)
	}

	local.cache = cache

	return local, nil
}

func (m *Local) Close() error {
	defer m.cache.Purge()

	if m.watcher == nil {
		return nil
	}

	err := m.watcher.Close()
	if err != nil {
		return fmt.Errorf("could close manager: %w", err)
	}

	return nil
}

func isHTTP(dsn string) bool {
	return strings.HasPrefix(dsn, "http://") || strings.HasPrefix(dsn, "https://")
}

// querySource is one file or segment to query: a local path or a remote URL.
// prePruned is true when the catalog already confirmed it overlaps the window.
type querySource struct {
	id        string
	dsn       string
	prePruned bool
}

// watcherSources returns the local files the watcher is tracking.
func (m *Local) watcherSources() []querySource {
	if m.watcher == nil {
		return nil
	}

	var sources []querySource

	_ = m.watcher.Iterate(func(filename string) error {
		sources = append(sources, querySource{id: filename, dsn: filename})

		return nil
	})

	return sources
}

// sources returns every source to query for the [start,end] window (0 meaning
// unbounded): local flush files from the watcher plus catalog segments pruned
// to the window (resolved to a local copy when present, else their remote URL).
// When line is set, catalog segments whose trigram filter proves they cannot
// contain it are pruned too — with no file/S3 read. Likewise, catalog segments
// whose label filter proves they hold no stream matching an equality matcher are
// pruned. When localOnly is set, remote (S3) catalog segments are skipped so only
// the hot tier is scanned.
func (m *Local) sources(start, end int64, line string, matchers []Matcher, localOnly bool) ([]querySource, error) {
	var sources []querySource

	// Local flush files are tracked by the watcher. Prune them by their
	// filename-encoded bounds when possible so non-overlapping files are never
	// opened; legacy names (no parseable bounds) fall through to the open-based
	// fileOverlaps check in Query.
	if m.watcher != nil {
		_ = m.watcher.Iterate(func(filename string) error {
			if start != 0 || end != 0 {
				if bounds, ok := ParseBounds(filepath.Base(filename)); ok {
					if (start != 0 && bounds.Max < start) || (end != 0 && bounds.Min > end) {
						return nil
					}
				}
			}

			sources = append(sources, querySource{id: filename, dsn: filename})

			return nil
		})
	}

	if m.catalog != nil {
		segments, err := m.catalog.Overlapping(start, end)
		if err != nil {
			return nil, fmt.Errorf("could not prune segments: %w", err)
		}

		for _, segment := range segments {
			// Hot-only scan: leave remote segments to the client-side cold path.
			if localOnly && segment.Location == LocationRemote {
				continue
			}

			// Skip segments whose trigram filter proves they cannot contain the
			// keyword — zero file/S3 reads. Segments without a filter (older, or
			// rebuilt from an S3 listing) fall through and are scanned.
			if line != "" && !LineFilterMayContain(segment.LineFilter, line) {
				continue
			}

			// Likewise skip segments whose label filter proves they hold no stream
			// matching an equality matcher — zero file/S3 reads.
			if !LabelFilterAllows(segment.LabelFilter, matchers) {
				continue
			}

			dsn := segment.RemoteURL

			if segment.LocalPath != "" {
				if _, statErr := os.Stat(segment.LocalPath); statErr == nil {
					dsn = segment.LocalPath // prefer a local copy when present
				}
			}

			if dsn == "" {
				continue
			}

			sources = append(sources, querySource{id: segment.ID, dsn: dsn, prePruned: true})
		}
	}

	return sources, nil
}

// LabelFilterAllows reports whether a segment with the given label filter could
// satisfy every equality matcher. Only "="/"" matchers prune (the filter holds
// exact key=value pairs); "!=" and regex matchers cannot and are ignored here.
func LabelFilterAllows(blob []byte, matchers []Matcher) bool {
	for _, matcher := range matchers {
		switch matcher.Type {
		case "", "=":
			if !LabelFilterMayContain(blob, matcher.Name, matcher.Value) {
				return false
			}
		}
	}

	return true
}

// forEach runs fn over sources with bounded concurrency. A failure on one
// source is collected (so the result degrades to the sources that did work,
// e.g. local when S3 is unreachable) rather than failing the whole query.
func (m *Local) forEach(sources []querySource, fn func(querySource, *sql.DB) error) error {
	concurrency := m.concurrency
	if concurrency < 1 {
		concurrency = 1
	}

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)

	sem := make(chan struct{}, concurrency)

	for _, src := range sources {
		wg.Add(1)
		sem <- struct{}{}

		go func(src querySource) {
			defer wg.Done()
			defer func() { <-sem }()

			if err := m.withClient(src, fn); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("source %q: %w", src.id, err))
				mu.Unlock()
			}
		}(src)
	}

	wg.Wait()

	return errors.Join(errs...)
}

func (m *Local) withClient(src querySource, fn func(querySource, *sql.DB) error) error {
	// A local file may have been compacted/rotated away between resolution and
	// now; skip it rather than failing.
	if !isHTTP(src.dsn) {
		if _, statErr := os.Stat(src.dsn); errors.Is(statErr, os.ErrNotExist) {
			return nil
		}
	}

	client, ok := m.cache.Get(src.dsn)
	if !ok {
		var err error

		client, err = sql.Open("sqlite3", src.dsn+"?vfs="+m.vfsName)
		if err != nil {
			return fmt.Errorf("could not open (%q): %w", src.dsn, err)
		}

		// sqlitezstd is read-only and expects a single connection per handle.
		client.SetMaxOpenConns(1)

		m.cache.Add(src.dsn, client)
	}

	return fn(src, client)
}

func (m *Local) Labels() ([]string, error) {
	seen := map[string]struct{}{}

	// Segment label keys come from the catalog with no file/S3 access.
	if m.catalog != nil {
		keys, err := m.catalog.LabelKeys()
		if err != nil {
			return nil, fmt.Errorf("could not read catalog labels: %w", err)
		}

		for _, key := range keys {
			seen[key] = struct{}{}
		}
	}

	// Plus the (local) files the watcher tracks.
	var (
		mu    sync.Mutex
		found []string
	)

	if err := m.forEach(m.watcherSources(), func(_ querySource, client *sql.DB) error {
		var labels []string

		err := sqlscan.Select(context.TODO(), client, &labels, `
			SELECT DISTINCT json_each.key FROM labels, json_each(labels.payload);
		`)
		if err != nil {
			return fmt.Errorf("could not scan labels: %w", err)
		}

		mu.Lock()
		found = append(found, labels...)
		mu.Unlock()

		return nil
	}); err != nil {
		slog.Warn("some sources failed while reading labels", slog.String("error", err.Error()))
	}

	for _, key := range found {
		seen[key] = struct{}{}
	}

	unique := lo.Keys(seen)
	sort.Strings(unique)

	return unique, nil
}
