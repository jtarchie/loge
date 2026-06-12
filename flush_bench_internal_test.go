package loge

// Internal benchmark for flusher() — the flush WRITE path (loge.flusher, ~34% of
// ingest CPU in the Fly profile): open a fresh SQLite, batch-insert labels+streams,
// commit, close. Isolated from the channel/backpressure machinery so the effect of
// the INSERT batch size (maxBatchInsert) is measurable.
// Run: go test -run '^$' -bench '^BenchmarkFlush$' -benchmem .

import (
	"fmt"
	"os"
	"strconv"
	"testing"
)

func benchPayloads(streams, valuesPerStream int) []Payload {
	apps := []string{"checkout", "auth", "payments", "catalog", "search", "gateway"}
	base := int64(1_700_000_000_000_000_000)
	out := make([]Payload, 0, streams)
	n := 0

	for s := 0; s < streams; s++ {
		entry := Entry{
			Stream: Stream{"app": apps[s%len(apps)], "env": "prod", "host": fmt.Sprintf("host-%02d", s%20)},
			Values: make(Values, 0, valuesPerStream),
		}
		for v := 0; v < valuesPerStream; v++ {
			line := fmt.Sprintf(`{"level":"info","msg":"connection refused","trace_id":"%016x","latency_ms":%d}`, n, n%2000)
			entry.Values = append(entry.Values, Value{strconv.FormatInt(base+int64(n), 10), line})
			n++
		}
		out = append(out, Payload{Streams: Streams{entry}})
	}

	return out
}

func BenchmarkFlush(b *testing.B) {
	payloads := benchPayloads(40, 1250) // 40 streams x 1250 = 50k rows
	dir := b.TempDir()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		name, err := flusher(payloads, dir, 0)
		if err != nil {
			b.Fatal(err)
		}

		b.StopTimer()
		_ = os.Remove(name)
		b.StartTimer()
	}
}
