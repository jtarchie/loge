package loge

import (
	"fmt"
	"strconv"
)

type Stream map[string]string

type (
	Value  [2]string
	Values []Value
)

// Timestamp parses the nanosecond timestamp string. It returns an error
// (rather than silently returning 0) so callers can reject or skip values
// with malformed timestamps instead of corrupting time-range metadata.
func (v *Value) Timestamp() (int64, error) {
	value, err := strconv.ParseInt(v[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("could not parse timestamp %q: %w", v[0], err)
	}

	return value, nil
}

type Entry struct {
	Stream Stream `json:"stream" msg:"stream"`
	Values Values `json:"values" msg:"values"`
}

type Streams []Entry

//go:generate go run github.com/tinylib/msgp -tests=false
type Payload struct {
	Streams Streams `json:"streams" msg:"streams"`
}

func (p *Payload) Valid() (string, bool) {
	if len(p.Streams) == 0 {
		return "At least one stream is required", false
	}

	for _, stream := range p.Streams {
		if len(stream.Stream) == 0 {
			return "Each stream requires labels", false
		}

		if len(stream.Values) == 0 {
			return "Each stream requires a value", false
		}
	}

	return "", true
}
