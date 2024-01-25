package loge_test

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/imroc/req/v3"
	"github.com/pioz/faker"
	"github.com/jtarchie/loge"
)

var response *req.Response

func BenchmarkPayloadUnmarshal(b *testing.B) {
	payload := generatePayload()

	contents, err := json.Marshal(payload)
	if err != nil {
		b.Fatalf("could not marshal payload")
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		json.Unmarshal(contents, payload)
	}
}

func generatePayload() *loge.Payload {
	payload := &loge.Payload{}

	for i := 0; i < rand.Intn(10)+1; i++ {
	payload := &loge.Payload{}
	entry := loge.Entry{
			Stream: loge.Stream{},
		}

		for i := 0; i < rand.Intn(10)+1; i++ {
			entry.Stream[faker.Username()] = faker.Letters()
		}

		for i := 0; i < rand.Intn(10)+1; i++ {
			entry.Values = append(entry.Values, loge.Value{
				fmt.Sprint(time.Now().UnixNano()),
				faker.Sentence(),
			})
		}

		payload.Streams = append(payload.Streams, entry)
	}

	return payload
}