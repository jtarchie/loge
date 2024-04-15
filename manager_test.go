package loge_test

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/jtarchie/loge"
	"github.com/jtarchie/sqlitezstd"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Manager", func() {
	var outputPath string

	BeforeEach(func() {
		err := sqlitezstd.Init()
		Expect(err).NotTo(HaveOccurred())

		outputPath, err = os.MkdirTemp("", "")
		Expect(err).NotTo(HaveOccurred())
	})

	It("returns all the labels", func() {
		buckets := loge.NewBuckets(1, 5, outputPath)

		for i := range 5 {
			payload := &loge.Payload{
				Streams: loge.Streams{
					{
						Stream: loge.Stream{fmt.Sprintf("tag_%d", i): "value"},
						Values: loge.Values{
							loge.Value{"", ""},
						},
					},
				},
			}

			_, valid := payload.Valid()
			Expect(valid).To(BeTrue())

			buckets.Append(payload)
		}

		Eventually(func() int {
			matches, _ := filepath.Glob(filepath.Join(outputPath, "*.sqlite.zst"))

			return len(matches)
		}).Should(BeNumerically("==", 1))

		manager := loge.NewManager(outputPath)

		labels, err := manager.Labels()
		Expect(err).NotTo(HaveOccurred())

		Expect(labels).To(Equal([]string{"tag_0", "tag_1", "tag_2", "tag_3", "tag_4"}))
	})
})
