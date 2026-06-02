package loge

import (
	"bytes"
	"time"

	"github.com/jtarchie/loge/managers"
	ginkgo "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// ginkgo is imported qualified (not dot-imported) because this internal test
// lives in package loge, whose Entry type would collide with ginkgo's Entry.

var _ = ginkgo.Describe("SearchCmd internals", func() {
	ginkgo.Describe("parseTimeArg", func() {
		ginkgo.It("parses unix nanoseconds", func() {
			value, err := parseTimeArg("1700000000000000000")
			Expect(err).NotTo(HaveOccurred())
			Expect(value).To(Equal(int64(1_700_000_000_000_000_000)))
		})

		ginkgo.It("parses an RFC3339 timestamp", func() {
			value, err := parseTimeArg("2023-11-14T22:13:20Z")
			Expect(err).NotTo(HaveOccurred())
			Expect(value).To(Equal(time.Date(2023, 11, 14, 22, 13, 20, 0, time.UTC).UnixNano()))
		})

		ginkgo.It("errors on an unparseable value", func() {
			_, err := parseTimeArg("not-a-time")
			Expect(err).To(HaveOccurred())
		})
	})

	ginkgo.Describe("resolveWindow", func() {
		now := time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC)

		ginkgo.It("defaults to an unbounded window", func() {
			start, end, err := (&SearchCmd{}).resolveWindow(now)
			Expect(err).NotTo(HaveOccurred())
			Expect(start).To(BeZero())
			Expect(end).To(BeZero())
		})

		ginkgo.It("resolves --since relative to now", func() {
			start, end, err := (&SearchCmd{Since: time.Hour}).resolveWindow(now)
			Expect(err).NotTo(HaveOccurred())
			Expect(start).To(Equal(now.Add(-time.Hour).UnixNano()))
			Expect(end).To(BeZero())
		})

		ginkgo.It("lets an absolute --start override --since", func() {
			start, _, err := (&SearchCmd{Since: time.Hour, Start: "1700000000000000000"}).resolveWindow(now)
			Expect(err).NotTo(HaveOccurred())
			Expect(start).To(Equal(int64(1_700_000_000_000_000_000)))
		})

		ginkgo.It("errors when start is after end", func() {
			_, _, err := (&SearchCmd{Start: "2000", End: "1000"}).resolveWindow(now)
			Expect(err).To(HaveOccurred())
		})
	})

	ginkgo.Describe("render", func() {
		response := QueryResponse{
			Status: "success",
			Data: []managers.QueryEntry{
				{
					Timestamp: 1_700_000_000_000_000_000,
					Line:      "GET /index 200",
					Labels:    map[string]string{"app": "web", "level": "info"},
				},
			},
		}

		ginkgo.It("renders text with timestamp, sorted labels and line", func() {
			buf := &bytes.Buffer{}
			Expect((&SearchCmd{Output: "text"}).render(buf, response)).To(Succeed())
			Expect(buf.String()).To(ContainSubstring("2023-11-14T22:13:20Z"))
			Expect(buf.String()).To(ContainSubstring(`{app="web", level="info"}`))
			Expect(buf.String()).To(ContainSubstring("GET /index 200"))
		})

		ginkgo.It("renders the data array as JSON", func() {
			buf := &bytes.Buffer{}
			Expect((&SearchCmd{Output: "json"}).render(buf, response)).To(Succeed())
			Expect(buf.String()).To(ContainSubstring(`"line": "GET /index 200"`))
			Expect(buf.String()).To(ContainSubstring(`"app": "web"`))
		})
	})
})
