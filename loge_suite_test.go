package loge_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/jtarchie/loge"
	_ "github.com/mattn/go-sqlite3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestLoge(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Loge Suite")
}

func createPayload(streams int, values int) *loge.Payload {
	payload := &loge.Payload{
		Streams: make(loge.Streams, streams),
	}

	for i := 0; i < streams; i++ {
		payload.Streams[i] = loge.Entry{
			Stream: loge.Stream{"tag": "value"},
			Values: make(loge.Values, values),
		}

		for j := 0; j < values; j++ {
			// timestamps in nanoseconds as a string
			timestamp := strconv.FormatInt(time.Now().UnixNano(), 10)
			payload.Streams[i].Values[j] = loge.Value{
				timestamp,
				"",
			}
		}
	}

	return payload
}
