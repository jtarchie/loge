package loge_test

import (
	"testing"

	"github.com/jtarchie/loge"
	_ "github.com/mattn/go-sqlite3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestLoge(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Loge Suite")
}

// nolint: gochecknoglobals
var oneValue = &loge.Payload{
	Streams: loge.Streams{
		{
			Stream: loge.Stream{"tag": "value"},
			Values: loge.Values{
				loge.Value{"", ""},
			},
		},
	},
}
