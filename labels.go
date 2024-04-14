package loge

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/georgysavva/scany/v2/sqlscan"
)

type LabelResponse struct {
	Status string   `json:"status"`
	Data   []string `json:"data"`
}

func findLabels(ctx context.Context, db *sql.DB) ([]string, error) {
	var labels []string

	err := sqlscan.Select(ctx, db, &labels, `
		SELECT
			DISTINCT json_each.key
		FROM labels,
			json_each(labels.payload);
	`)
	if err != nil {
		return nil, fmt.Errorf("could not scan labels: %w", err)
	}

	return labels, nil
}
