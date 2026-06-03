package managers

import (
	"bytes"
	"fmt"

	"github.com/FastFilter/xorfilter"
)

// Each compacted segment carries a binary-fuse filter of the byte-trigrams of
// every log line, stored in the segment metadata and mirrored into the local
// catalog. The query path tests a keyword's trigrams against it to skip whole
// segments that cannot contain the keyword — with zero file/S3 reads. A binary
// fuse filter (vs a bloom) is ideal here: the trigram set is built once and
// never mutated, and fuse filters are smaller and faster for static sets, which
// matters twice — less to store and less to load on a cold start.
//
// Correctness: the filter has no false negatives, so an "absent" verdict is
// always true → a segment is never wrongly skipped. Trigrams are byte-level over
// the ASCII-lowercased line, which is sound against SQLite's ASCII-case-
// insensitive LIKE (the keyword is a byte-substring of any matching line, so its
// trigrams are a subset of the line's). The LIKE remains the binding filter.

const fnvOffset64 = 14695981039346656037
const fnvPrime64 = 1099511628211

// asciiLower lowercases ASCII A–Z in place, matching LIKE's case-insensitivity.
func asciiLower(b []byte) {
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + ('a' - 'A')
		}
	}
}

// fnv64a3 is FNV-1a over exactly b[0:3] — a stable hash (unlike hash/maphash) so
// a filter built at compaction is queryable in any later process.
func fnv64a3(b []byte) uint64 {
	h := uint64(fnvOffset64)
	for i := 0; i < 3; i++ {
		h = (h ^ uint64(b[i])) * fnvPrime64
	}

	return h
}

// AddTrigramHashes adds the byte-trigram hashes of line to set.
func AddTrigramHashes(line string, set map[uint64]struct{}) {
	if len(line) < 3 {
		return
	}

	b := []byte(line)
	asciiLower(b)

	for i := 0; i+3 <= len(b); i++ {
		set[fnv64a3(b[i:i+3])] = struct{}{}
	}
}

// BuildLineFilter builds and serializes a binary-fuse filter from a trigram-hash
// set. Returns nil bytes for an empty set (nothing to prune by).
func BuildLineFilter(set map[uint64]struct{}) ([]byte, error) {
	if len(set) == 0 {
		return nil, nil
	}

	keys := make([]uint64, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}

	filter, err := xorfilter.PopulateBinaryFuse8(keys)
	if err != nil {
		return nil, fmt.Errorf("could not build line filter: %w", err)
	}

	var buf bytes.Buffer
	if err := filter.Save(&buf); err != nil {
		return nil, fmt.Errorf("could not serialize line filter: %w", err)
	}

	return buf.Bytes(), nil
}

// LineFilterMayContain reports whether a segment whose trigram filter is blob
// could contain term as a substring. It returns true (must scan) when there is
// no filter, the term is too short to have a trigram, or the blob is unreadable;
// it returns false only when a trigram of term is provably absent.
func LineFilterMayContain(blob []byte, term string) bool {
	if len(blob) == 0 || len(term) < 3 {
		return true
	}

	filter, err := xorfilter.LoadBinaryFuse8(bytes.NewReader(blob))
	if err != nil {
		return true
	}

	b := []byte(term)
	asciiLower(b)

	for i := 0; i+3 <= len(b); i++ {
		if !filter.Contains(fnv64a3(b[i : i+3])) {
			return false
		}
	}

	return true
}
