package managers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"sync"

	"github.com/georgysavva/scany/v2/sqlscan"
	lru "github.com/hashicorp/golang-lru/v2"
	filewatcher "github.com/jtarchie/loge/file_watcher"
	"github.com/samber/lo"
)

type Local struct {
	cache      *lru.Cache[string, *sql.DB]
	outputDir  string
	watcher    *filewatcher.FileWatcher
	lineSearch sync.Map // filename -> bool: whether the file has the line_search index
}

func NewLocal(
	outputPath string,
) (*Local, error) {
	watcher, err := filewatcher.New(outputPath, regexp.MustCompile(`\.sqlite\.zst$`))
	if err != nil {
		return nil, fmt.Errorf("could not start watcher: %w", err)
	}

	const numCachedDBs = 64

	local := &Local{
		outputDir: outputPath,
		watcher:   watcher,
	}

	cache, err := lru.NewWithEvict[string, *sql.DB](numCachedDBs, func(filename string, client *sql.DB) {
		_ = client.Close()
		// Drop the cached line_search answer so the map stays bounded by the
		// handle cache.
		local.lineSearch.Delete(filename)
	})
	if err != nil {
		return nil, fmt.Errorf("could not start cache: %w", err)
	}

	local.cache = cache

	return local, nil
}

func (m *Local) Close() error {
	defer m.cache.Purge()

	err := m.watcher.Close()
	if err != nil {
		return fmt.Errorf("could close manager: %w", err)
	}

	return nil
}

func (m *Local) execute(fun func(filename string, client *sql.DB) error) error {
	err := m.watcher.Iterate(func(filename string) error {
		// The file may have been compacted or cleaned up between the watcher
		// learning about it and this query running; skip it rather than
		// failing the whole query.
		if _, statErr := os.Stat(filename); errors.Is(statErr, os.ErrNotExist) {
			return nil
		}

		client, ok := m.cache.Get(filename)
		if !ok {
			var err error

			client, err = sql.Open("sqlite3", filename+"?vfs=zstd")
			if err != nil {
				return fmt.Errorf("could not open sqlite3 (%q): %w", filename, err)
			}

			// sqlitezstd is a read-only VFS and expects a single connection per
			// database handle.
			client.SetMaxOpenConns(1)

			m.cache.Add(filename, client)
		}

		err := fun(filename, client)
		if err != nil {
			return fmt.Errorf("could not execute (%q): %w", filename, err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("could not execute manager: %w", err)
	}

	return nil
}

func (m *Local) Labels() ([]string, error) {
	var foundLabels []string

	err := m.execute(func(_ string, client *sql.DB) error {
		var labels []string

		err := sqlscan.Select(context.TODO(), client, &labels, `
			SELECT
				DISTINCT json_each.key
			FROM labels,
				json_each(labels.payload);
		`)
		if err != nil {
			return fmt.Errorf("could not scan labels: %w", err)
		}

		foundLabels = append(foundLabels, labels...)

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("could not read all labels: %w", err)
	}

	// Files are iterated in a nondeterministic order (sync.Map.Range), so sort
	// for a stable result across calls.
	unique := lo.Uniq(foundLabels)
	sort.Strings(unique)

	return unique, nil
}
