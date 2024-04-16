package managers

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"

	"github.com/georgysavva/scany/v2/sqlscan"
	"github.com/hashicorp/golang-lru/v2/simplelru"
	filewatcher "github.com/jtarchie/loge/file_watcher"
	"github.com/samber/lo"
)

type Local struct {
	cache     *simplelru.LRU[string, *sql.DB]
	outputDir string
	watcher   *filewatcher.FileWatcher
}

func NewLocal(
	outputPath string,
) (*Local, error) {
	watcher, err := filewatcher.New(outputPath, regexp.MustCompile(`\.sqlite\.zst$`))
	if err != nil {
		return nil, fmt.Errorf("could not start watcher: %w", err)
	}

	const numCachedDBs = 10

	cache, err := simplelru.NewLRU[string, *sql.DB](numCachedDBs, func(_ string, client *sql.DB) {
		_ = client.Close()
	})
	if err != nil {
		return nil, fmt.Errorf("could not start cache: %w", err)
	}

	return &Local{
		cache:     cache,
		outputDir: outputPath,
		watcher:   watcher,
	}, nil
}

func (m *Local) Close() error {
	defer m.cache.Purge()

	err := m.watcher.Close()
	if err != nil {
		return fmt.Errorf("could close manager: %w", err)
	}

	return nil
}

func (m *Local) execute(fun func(*sql.DB) error) error {
	err := m.watcher.Iterate(func(filename string) error {
		client, ok := m.cache.Get(filename)
		if !ok {
			var err error

			client, err = sql.Open("sqlite3", filename+"?vfs=zstd")
			if err != nil {
				return fmt.Errorf("could not open sqlite3 (%q): %w", filename, err)
			}

			m.cache.Add(filename, client)
		}

		err := fun(client)
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

	err := m.execute(func(client *sql.DB) error {
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

	return lo.Uniq(foundLabels), nil
}
