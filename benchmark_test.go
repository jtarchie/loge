package loge_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/jtarchie/loge"
	"github.com/jtarchie/loge/managers"
	"github.com/jtarchie/sqlitezstd"
)

func BenchmarkLocalManager(b *testing.B) {
	err := sqlitezstd.Init()
	if err != nil {
		b.Fatalf("could not init: %s", err)
	}

	outputPath, err := os.MkdirTemp("", "")
	if err != nil {
		b.Fatalf("could not create directory: %s", err)
	}

	manager, err := managers.NewLocal(outputPath)
	if err != nil {
		b.Fatalf("could not start manager: %s", err)
	}
	defer manager.Close()

	buckets := loge.NewBuckets(10, 100, outputPath)

	for tagIndex := range 1_000 {
		payload := &loge.Payload{
			Streams: loge.Streams{
				{
					Stream: loge.Stream{fmt.Sprintf("tag_%d", tagIndex): "value"},
					Values: loge.Values{
						loge.Value{"", ""},
					},
				},
			},
		}

		_, valid := payload.Valid()
		if !valid {
			b.Fatalf("payload not valid %d", tagIndex)
		}

		buckets.Append(payload)
	}

	b.ResetTimer() // Start timing now.

	// b.RunParallel(func(pb *testing.PB) {
	// 	for pb.Next() {
	// 		labels, _ := manager.Labels()
	// 		if len(labels) > 1000 {
	// 			b.Fatalf("something happened")
	// 		}
	// 	}
	// })
	for i := 0; i < b.N; i++ {
		labels, _ := manager.Labels()
		if len(labels) > 1000 {
			b.Fatalf("something happened")
		}
	}
}
