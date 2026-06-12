package managers

import (
	"bytes"
	"encoding/binary"
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
	return buildFuse(set)
}

// SerializeHashSet packs a trigram-hash set as little-endian uint64s (8 bytes
// each, unordered). A flush stores this so compaction can union the per-file sets
// instead of re-scanning every line out of SQLite to rebuild the trigram set.
func SerializeHashSet(set map[uint64]struct{}) []byte {
	buf := make([]byte, 0, len(set)*8)

	var tmp [8]byte
	for key := range set {
		binary.LittleEndian.PutUint64(tmp[:], key)
		buf = append(buf, tmp[:]...)
	}

	return buf
}

// UnionHashSet adds the uint64 hashes packed by SerializeHashSet into set. A
// trailing partial element (len not a multiple of 8) is ignored. Since the
// trigram hashes are stable, unioning the per-flush sets yields exactly the set a
// full scan of the merged lines would produce.
func UnionHashSet(packed []byte, set map[uint64]struct{}) {
	for i := 0; i+8 <= len(packed); i += 8 {
		set[binary.LittleEndian.Uint64(packed[i:i+8])] = struct{}{}
	}
}

// buildFuse serializes a binary-fuse filter over a hash set. Returns nil bytes
// for an empty set (nothing to prune by). Shared by the line and label filters.
func buildFuse(set map[uint64]struct{}) ([]byte, error) {
	if len(set) == 0 {
		return nil, nil
	}

	keys := make([]uint64, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}

	filter, err := xorfilter.PopulateBinaryFuse8(keys)
	if err != nil {
		return nil, fmt.Errorf("could not build fuse filter: %w", err)
	}

	var buf bytes.Buffer
	if err := filter.Save(&buf); err != nil {
		return nil, fmt.Errorf("could not serialize fuse filter: %w", err)
	}

	return buf.Bytes(), nil
}

// Each compacted segment also carries a binary-fuse filter of its exact label
// key=value pairs, stored in the segment metadata and mirrored into the catalog.
// An equality matcher (app="api") tests the pair against it to skip whole
// segments that hold no such stream — with zero file/S3 reads. Benchmarks showed
// an in-segment inverted label index costs +8–51% of the segment size and only
// helps selective high-cardinality labels; this filter gets the cross-segment
// pruning for near-zero size and leaves the within-segment scan to json_extract.
//
// Correctness: no false negatives, so an "absent" verdict is always true and a
// segment is never wrongly skipped. Only equality matchers prune; !=/regex do
// not. The json_extract filter remains the binding filter.

// fnv64aBytes is FNV-1a over all of b — a stable hash (stored at compaction,
// queried in any later process).
func fnv64aBytes(b []byte) uint64 {
	h := uint64(fnvOffset64)
	for _, c := range b {
		h = (h ^ uint64(c)) * fnvPrime64
	}

	return h
}

// labelPairHash hashes an exact key=value pair. The NUL separator keeps e.g.
// {"ab","c"} and {"a","bc"} from colliding.
func labelPairHash(key, value string) uint64 {
	b := make([]byte, 0, len(key)+1+len(value))
	b = append(b, key...)
	b = append(b, 0)
	b = append(b, value...)

	return fnv64aBytes(b)
}

// AddLabelPairHash adds the hash of an exact key=value pair to set.
func AddLabelPairHash(key, value string, set map[uint64]struct{}) {
	set[labelPairHash(key, value)] = struct{}{}
}

// BuildLabelFilter builds and serializes a binary-fuse filter from a set of
// label-pair hashes. Returns nil bytes for an empty set.
func BuildLabelFilter(set map[uint64]struct{}) ([]byte, error) {
	return buildFuse(set)
}

// LabelFilterMayContain reports whether a segment whose label filter is blob
// could hold a stream with the exact label key=value. It returns true (must
// scan) when there is no filter or the blob is unreadable; false only when the
// pair is provably absent.
func LabelFilterMayContain(blob []byte, key, value string) bool {
	if len(blob) == 0 {
		return true
	}

	filter, err := xorfilter.LoadBinaryFuse8(bytes.NewReader(blob))
	if err != nil {
		return true
	}

	return filter.Contains(labelPairHash(key, value))
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
