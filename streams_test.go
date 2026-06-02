package loge_test

import (
	"github.com/jtarchie/loge"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Value.Timestamp", func() {
	It("parses a valid nanosecond timestamp", func() {
		value := loge.Value{"1700000000000000000", "line"}

		ts, err := value.Timestamp()
		Expect(err).NotTo(HaveOccurred())
		Expect(ts).To(Equal(int64(1700000000000000000)))
	})

	It("returns an error for a malformed timestamp instead of 0", func() {
		value := loge.Value{"not-a-number", "line"}

		ts, err := value.Timestamp()
		Expect(err).To(HaveOccurred())
		Expect(ts).To(Equal(int64(0)))
	})

	It("returns an error for an empty timestamp", func() {
		value := loge.Value{"", "line"}

		_, err := value.Timestamp()
		Expect(err).To(HaveOccurred())
	})
})
