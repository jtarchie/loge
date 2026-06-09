package managers

import (
	"strings"
	"testing"
)

func TestBuildQueryKeywordIsLikeOnly(t *testing.T) {
	t.Parallel()

	req := QueryRequest{Line: "connection refused"}

	// There is no FTS index anywhere: keyword filtering is always a literal LIKE
	// (the timestamp-ordered scan short-circuits at LIMIT, and the catalog line
	// filter prunes whole segments that cannot contain the keyword).
	query, args := buildQuery(req, 100)
	if strings.Contains(query, "line_search MATCH") {
		t.Errorf("query must not MATCH (FTS retired): %s", query)
	}

	if !strings.Contains(query, "s.line LIKE") {
		t.Errorf("keyword query must LIKE: %s", query)
	}

	if !containsArg(args, "%connection refused%") {
		t.Errorf("LIKE arg should wrap the keyword, args=%v", args)
	}
}

func containsArg(args []any, want string) bool {
	for _, a := range args {
		if s, ok := a.(string); ok && s == want {
			return true
		}
	}

	return false
}
