package managers

import (
	"strings"
	"testing"
)

func TestFirstIndexableWord(t *testing.T) {
	t.Parallel()

	cases := []struct {
		line string
		want string
		ok   bool
	}{
		{"connection refused", "connection", true}, // multi-word → first word
		{"/checkout", "/checkout", true},           // single token unchanged
		{"  the  quick", "the", true},              // leading/extra spaces
		{"a bc def", "def", true},                  // skips tokens shorter than 3 runes
		{"ab cd", "", false},                       // no token long enough
		{"", "", false},                            // empty
	}

	for _, tc := range cases {
		got, ok := firstIndexableWord(tc.line)
		if got != tc.want || ok != tc.ok {
			t.Errorf("firstIndexableWord(%q) = (%q, %v), want (%q, %v)", tc.line, got, ok, tc.want, tc.ok)
		}
	}
}

func TestBuildQueryMatchTermPerTier(t *testing.T) {
	t.Parallel()

	req := QueryRequest{Line: "connection refused"}

	localSQL, _ := buildQuery(req, 100, true, false)
	if !strings.Contains(localSQL, "line_search MATCH") {
		t.Fatalf("local query should keep the trigram MATCH: %s", localSQL)
	}

	remoteSQL, remoteArgs := buildQuery(req, 100, true, true)
	if !strings.Contains(remoteSQL, "line_search MATCH") {
		t.Fatalf("remote query should still MATCH (minimally): %s", remoteSQL)
	}

	// The remote MATCH term is the first word; the LIKE is the full keyword.
	if !containsArg(remoteArgs, `"connection"`) {
		t.Errorf("remote MATCH term should be the first word, args=%v", remoteArgs)
	}

	if !containsArg(remoteArgs, "%connection refused%") {
		t.Errorf("LIKE must still use the full keyword, args=%v", remoteArgs)
	}

	// A keyword with no token >= 3 runes drops the MATCH entirely on remote.
	noMatchSQL, _ := buildQuery(QueryRequest{Line: "ab cd"}, 100, true, true)
	if strings.Contains(noMatchSQL, "line_search MATCH") {
		t.Errorf("remote query with no indexable word should skip MATCH: %s", noMatchSQL)
	}

	if !strings.Contains(noMatchSQL, "s.line LIKE") {
		t.Errorf("remote query must still apply the LIKE filter: %s", noMatchSQL)
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
