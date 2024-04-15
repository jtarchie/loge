package managers

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"

	"github.com/georgysavva/scany/v2/sqlscan"
	filewatcher "github.com/jtarchie/loge/file_watcher"
	"github.com/samber/lo"
)

type Local struct {
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

	return &Local{
		outputDir: outputPath,
		watcher:   watcher,
	}, nil
}

func (m *Local) Close() error {
	err := m.watcher.Close()
	if err != nil {
		return fmt.Errorf("could close manager: %w", err)
	}

	return nil
}

func (m *Local) execute(fun func(*sql.DB) error) error {
	err := m.watcher.Iterate(func(filename string) error {
		client, err := sql.Open("sqlite3", filename+"?vfs=zstd")
		if err != nil {
			return fmt.Errorf("could not open sqlite3 (%q): %w", filename, err)
		}
		defer client.Close()

		err = fun(client)
		if err != nil {
			return fmt.Errorf("could not execute (%q): %w", filename, err)
		}

		err = client.Close()
		if err != nil {
			return fmt.Errorf("could not close connection (%q): %w", filename, err)
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
