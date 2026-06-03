package managers_test

import (
	"testing"

	"github.com/jtarchie/loge/managers"
)

func TestLineFilter(t *testing.T) {
	t.Parallel()

	set := map[uint64]struct{}{}
	managers.AddTrigramHashes(`{"level":"error","msg":"connection refused","trace_id":"abc123def456"}`, set)
	managers.AddTrigramHashes(`{"level":"info","msg":"request completed","trace_id":"0011223344556677"}`, set)

	blob, err := managers.BuildLineFilter(set)
	if err != nil {
		t.Fatalf("BuildLineFilter: %v", err)
	}

	if len(blob) == 0 {
		t.Fatal("expected a non-empty filter")
	}

	// No false negatives: every term actually present must pass (including a
	// different-case variant, since LIKE is ASCII-case-insensitive).
	for _, present := range []string{"connection refused", "request", "abc123def456", "error", "CONNECTION", "Request"} {
		if !managers.LineFilterMayContain(blob, present) {
			t.Errorf("present term %q was wrongly pruned (false negative)", present)
		}
	}

	// A needle with several distinct absent trigrams is pruned (the chance all
	// of them are false positives is ~0.004^n, negligible).
	if managers.LineFilterMayContain(blob, "qwxzjvkbmp") {
		t.Error("absent needle should be pruned")
	}

	// Cannot prune: term too short to have a trigram, or no filter.
	if !managers.LineFilterMayContain(blob, "ab") {
		t.Error("sub-trigram term must pass")
	}

	if !managers.LineFilterMayContain(nil, "connection refused") {
		t.Error("nil filter must pass (no pruning possible)")
	}
}
