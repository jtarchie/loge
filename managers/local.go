package managers

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"

	"github.com/georgysavva/scany/v2/sqlscan"
	"github.com/samber/lo"
)

type Local struct {
	outputDir string
}

func NewLocal(
	outputPath string,
) *Local {
	return &Local{
		outputDir: outputPath,
	}
}

func (m *Local) execute(fun func(*sql.DB) error) error {
	files, err := filepath.Glob(filepath.Join(m.outputDir, "*.sqlite.zst"))
	if err != nil {
		return fmt.Errorf("could not load files: %w", err)
	}

	for _, filename := range files {
		client, err := sql.Open("sqlite3", filename+"?vfs=zstd")
		if err != nil {
			return fmt.Errorf("could not open sqlite3 %q: %w", filename, err)
		}
		defer client.Close()

		err = fun(client)
		if err != nil {
			return fmt.Errorf("could not execute: %w", err)
		}

		err = client.Close()
		if err != nil {
			return fmt.Errorf("could not close connection: %w", err)
		}
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
