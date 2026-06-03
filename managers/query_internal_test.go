package managers

import (
	"strings"
	"testing"
)

func TestBuildQueryLineFilterPerTier(t *testing.T) {
	t.Parallel()

	req := QueryRequest{Line: "connection refused"}

	// Local segments (useLineIndex=true): trigram MATCH on the full keyword plus
	// the LIKE exact filter.
	indexed, indexedArgs := buildQuery(req, 100, true)
	if !strings.Contains(indexed, "line_search MATCH") {
		t.Fatalf("indexed query should MATCH: %s", indexed)
	}

	if !containsArg(indexedArgs, `"connection refused"`) {
		t.Errorf("MATCH term should be the full keyword, args=%v", indexedArgs)
	}

	if !strings.Contains(indexed, "s.line LIKE") {
		t.Errorf("indexed query must also LIKE: %s", indexed)
	}

	// Remote segments (useLineIndex=false): a sequential LIKE only — no FTS.
	likeOnly, _ := buildQuery(req, 100, false)
	if strings.Contains(likeOnly, "line_search MATCH") {
		t.Errorf("LIKE-only query must not MATCH: %s", likeOnly)
	}

	if !strings.Contains(likeOnly, "s.line LIKE") {
		t.Errorf("LIKE-only query must still LIKE: %s", likeOnly)
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
