package loge_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/jtarchie/loge"
	"github.com/jtarchie/loge/managers"
	"github.com/jtarchie/sqlitezstd"
)

func BenchmarkParseRegex(b *testing.B) {
	err := sqlitezstd.Init()
	if err != nil {
		b.Errorf("could not init: %s", err)
	}

	outputPath, err := os.MkdirTemp("", "")
	if err != nil {
		b.Errorf("could not create directory: %s", err)
	}

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
			b.Errorf("payload not valid %d", tagIndex)
		}

		buckets.Append(payload)
	}

	manager := managers.NewLocal(outputPath)

	b.ResetTimer() // Start timing now.

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = manager.Labels()
		}
	})
}
