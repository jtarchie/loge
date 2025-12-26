package managers_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/jtarchie/loge"
	"github.com/jtarchie/loge/managers"
	_ "github.com/jtarchie/sqlitezstd"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Local", func() {
	var outputPath string

	BeforeEach(func() {
		var err error

		outputPath, err = os.MkdirTemp("", "")
		Expect(err).NotTo(HaveOccurred())
	})

	It("returns all the labels", func() {
		buckets, err := loge.NewBuckets(context.Background(), 1, 5, outputPath, false)
		Expect(err).NotTo(HaveOccurred())

		for i := range 5 {
			payload := loge.Payload{
				Streams: loge.Streams{
					loge.Entry{
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
		}).Should(BeNumerically("==", 1), "5s")

		manager, err := managers.NewLocal(outputPath)
		Expect(err).NotTo(HaveOccurred())
		defer func() {
			if err := manager.Close(); err != nil {
				Fail(err.Error())
			}
		}()

		labels, err := manager.Labels()
		Expect(err).NotTo(HaveOccurred())

		Expect(labels).To(Equal([]string{"tag_0", "tag_1", "tag_2", "tag_3", "tag_4"}))
	})
})
