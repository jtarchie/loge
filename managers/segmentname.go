package managers

import (
	"fmt"
	"strconv"
	"strings"
)

// Segment and flush files encode their data time-bounds in the filename so the
// bounds can be read from a directory or S3 listing without opening the file:
//
//	segment-<min>-<max>-<seal>.sqlite[.zst]
//	bucket-<index>-<min>-<max>-<seq>.sqlite[.zst]
//
// The numeric timestamp fields are nanosecond int64s (always positive, so '-' is
// an unambiguous separator) zero-padded to tsWidth digits. Padding keeps
// lexical order equal to numeric order, which the compactor relies on when it
// sorts flush files to merge time-local batches (see compactor.Compact).
const tsWidth = 19 // digits in math.MaxInt64

// Bounds is the [Min, Max] nanosecond time range encoded in a filename.
type Bounds struct {
	Min int64
	Max int64
}

// FormatSegmentName builds a segment base name (without the .zst suffix).
func FormatSegmentName(minTS, maxTS, sealedAt int64) string {
	return fmt.Sprintf("segment-%0*d-%0*d-%0*d.sqlite", tsWidth, minTS, tsWidth, maxTS, tsWidth, sealedAt)
}

// FormatFlushName builds a flush-file base name (without the .zst suffix).
func FormatFlushName(index int, minTS, maxTS, seq int64) string {
	return fmt.Sprintf("bucket-%d-%0*d-%0*d-%0*d.sqlite", index, tsWidth, minTS, tsWidth, maxTS, tsWidth, seq)
}

// ParseBounds extracts the time bounds encoded in a segment or flush filename.
// The name may carry any of the .partial/.zst/.sqlite suffixes. It returns
// ok=false for legacy names (segment-<n>, bucket-<i>-<n>) or anything it cannot
// parse, signalling the caller to fall back to opening the file.
func ParseBounds(name string) (Bounds, bool) {
	base := name
	for _, suffix := range []string{".partial", ".zst", ".sqlite"} {
		base = strings.TrimSuffix(base, suffix)
	}

	parts := strings.Split(base, "-")

	var minRaw, maxRaw string

	switch {
	case len(parts) == 4 && parts[0] == "segment":
		minRaw, maxRaw = parts[1], parts[2]
	case len(parts) == 5 && parts[0] == "bucket":
		minRaw, maxRaw = parts[2], parts[3]
	default:
		return Bounds{}, false
	}

	minTS, err := strconv.ParseInt(minRaw, 10, 64)
	if err != nil {
		return Bounds{}, false
	}

	maxTS, err := strconv.ParseInt(maxRaw, 10, 64)
	if err != nil {
		return Bounds{}, false
	}

	return Bounds{Min: minTS, Max: maxTS}, true
}
