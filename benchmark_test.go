package loge_test

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jtarchie/loge"
	"github.com/jtarchie/loge/managers"
	_ "github.com/jtarchie/sqlitezstd"
)

func BenchmarkLocalManager(b *testing.B) {
	outputPath, err := os.MkdirTemp("", "")
	if err != nil {
		b.Fatalf("could not create directory: %s", err)
	}

	manager, err := managers.NewLocal(outputPath)
	if err != nil {
		b.Fatalf("could not start manager: %s", err)
	}
	defer func() {
		if err := manager.Close(); err != nil {
			b.Fatalf("could not close manager: %s", err)
		}
	}()

	buckets, err := loge.NewBuckets(context.Background(), 10, 100, outputPath, false)
	if err != nil {
		b.Fatalf("could not create buckets: %s", err)
	}

	for tagIndex := range 1_000 {
		payload := createPayload(1, 1)

		_, valid := payload.Valid()
		if !valid {
			b.Fatalf("payload not valid %d", tagIndex)
		}

		buckets.Append(payload)
	}

	// warmup manager
	for range 1_000 {
		labels, _ := manager.Labels()
		if len(labels) == 1000 {
			break
		}

		time.Sleep(time.Millisecond)
	}

	b.ResetTimer() // Start timing now.

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			labels, _ := manager.Labels()
			if len(labels) > 1000 {
				b.Fatalf("something happened")
			}
		}
	})
}

// BenchmarkLocalManagerQuery benchmarks the query path end to end across flush
// files: SQL scan, label decode, regex filtering, and the newest-first merge.
func BenchmarkLocalManagerQuery(b *testing.B) {
	outputPath, err := os.MkdirTemp("", "")
	if err != nil {
		b.Fatalf("could not create directory: %s", err)
	}
	defer func() {
		_ = os.RemoveAll(outputPath)
	}()

	manager, err := managers.NewLocal(outputPath)
	if err != nil {
		b.Fatalf("could not start manager: %s", err)
	}
	defer func() {
		if err := manager.Close(); err != nil {
			b.Fatalf("could not close manager: %s", err)
		}
	}()

	buckets, err := loge.NewBuckets(context.Background(), 10, 100, outputPath, false)
	if err != nil {
		b.Fatalf("could not create buckets: %s", err)
	}
	defer func() {
		_ = buckets.Close()
	}()

	// Bounded label cardinality with high-cardinality IDs in the line, the
	// labeling model the realistic generator (cmd/loge-loadgen) uses.
	for index := range 1_000 {
		timestamp := strconv.FormatInt(time.Now().UnixNano(), 10)
		payload := loge.Payload{
			Streams: loge.Streams{
				{
					Stream: loge.Stream{
						"app":     "app_" + strconv.Itoa(index%6),
						"env":     "production",
						"host":    "host_" + strconv.Itoa(index%20),
						"level":   "info",
						"service": "api",
					},
					Values: loge.Values{
						{timestamp, "GET /api/v1/orders 200 trace_id=" + timestamp},
					},
				},
			},
		}

		_, valid := payload.Valid()
		if !valid {
			b.Fatalf("payload not valid %d", index)
		}

		if err := buckets.Append(payload); err != nil {
			b.Fatalf("could not append payload: %s", err)
		}
	}

	// warmup: wait until every line has flushed and is queryable
	for range 10_000 {
		entries, err := manager.Query(context.Background(), managers.QueryRequest{Limit: 5000})
		if err == nil && len(entries) == 1_000 {
			break
		}

		time.Sleep(time.Millisecond)
	}

	b.ResetTimer() // Start timing now.

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			entries, err := manager.Query(context.Background(), managers.QueryRequest{Limit: 100})
			if err != nil {
				b.Fatalf("could not query: %s", err)
			}

			if len(entries) != 100 {
				b.Fatalf("expected 100 entries, got %d", len(entries))
			}
		}
	})
}

// BenchmarkBucketsWithBackpressure benchmarks bucket ingestion with blocking on backpressure
func BenchmarkBucketsWithBackpressure(b *testing.B) {
	outputPath, err := os.MkdirTemp("", "")
	if err != nil {
		b.Fatalf("could not create directory: %s", err)
	}
	defer os.RemoveAll(outputPath)

	buckets, err := loge.NewBuckets(context.Background(), 2, 1000, outputPath, false)
	if err != nil {
		b.Fatalf("could not create buckets: %s", err)
	}
	defer buckets.Close()

	payload := createPayload(1, 1)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buckets.Append(payload)
	}

	b.StopTimer()
}

// BenchmarkBucketsDropOnBackpressure benchmarks bucket ingestion with drop on backpressure
func BenchmarkBucketsDropOnBackpressure(b *testing.B) {
	outputPath, err := os.MkdirTemp("", "")
	if err != nil {
		b.Fatalf("could not create directory: %s", err)
	}
	defer os.RemoveAll(outputPath)

	buckets, err := loge.NewBuckets(context.Background(), 2, 1000, outputPath, true)
	if err != nil {
		b.Fatalf("could not create buckets: %s", err)
	}
	defer buckets.Close()

	payload := createPayload(1, 1)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buckets.Append(payload)
	}

	b.StopTimer()
}

// BenchmarkBucketsParallelWithBackpressure benchmarks parallel ingestion with blocking
func BenchmarkBucketsParallelWithBackpressure(b *testing.B) {
	outputPath, err := os.MkdirTemp("", "")
	if err != nil {
		b.Fatalf("could not create directory: %s", err)
	}
	defer os.RemoveAll(outputPath)

	buckets, err := loge.NewBuckets(context.Background(), 4, 1000, outputPath, false)
	if err != nil {
		b.Fatalf("could not create buckets: %s", err)
	}
	defer buckets.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		payload := createPayload(1, 1)
		for pb.Next() {
			buckets.Append(payload)
		}
	})

	b.StopTimer()
}

// BenchmarkBucketsParallelDropOnBackpressure benchmarks parallel ingestion with drop mode
func BenchmarkBucketsParallelDropOnBackpressure(b *testing.B) {
	outputPath, err := os.MkdirTemp("", "")
	if err != nil {
		b.Fatalf("could not create directory: %s", err)
	}
	defer os.RemoveAll(outputPath)

	buckets, err := loge.NewBuckets(context.Background(), 4, 1000, outputPath, true)
	if err != nil {
		b.Fatalf("could not create buckets: %s", err)
	}
	defer buckets.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		payload := createPayload(1, 1)
		for pb.Next() {
			buckets.Append(payload)
		}
	})

	b.StopTimer()
}
