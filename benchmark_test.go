package loge_test

import (
	"os"
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

	buckets, err := loge.NewBuckets(10, 100, outputPath)
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
