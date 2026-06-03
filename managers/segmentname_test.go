package managers_test

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/jtarchie/loge"
	"github.com/jtarchie/loge/managers"
	_ "github.com/jtarchie/sqlitezstd"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("segment filename bounds", func() {
	It("round-trips a segment name", func() {
		bounds, ok := managers.ParseBounds(managers.FormatSegmentName(100, 200, 300) + ".zst")
		Expect(ok).To(BeTrue())
		Expect(bounds).To(Equal(managers.Bounds{Min: 100, Max: 200}))
	})

	It("round-trips a flush name", func() {
		bounds, ok := managers.ParseBounds(managers.FormatFlushName(3, 100, 200, 400) + ".zst")
		Expect(ok).To(BeTrue())
		Expect(bounds).To(Equal(managers.Bounds{Min: 100, Max: 200}))
	})

	It("round-trips an empty (0,0) segment", func() {
		bounds, ok := managers.ParseBounds(managers.FormatSegmentName(0, 0, 123))
		Expect(ok).To(BeTrue())
		Expect(bounds).To(Equal(managers.Bounds{Min: 0, Max: 0}))
	})

	It("parses through the .partial and .sqlite suffixes", func() {
		bounds, ok := managers.ParseBounds(managers.FormatSegmentName(5, 9, 1) + ".partial")
		Expect(ok).To(BeTrue())
		Expect(bounds).To(Equal(managers.Bounds{Min: 5, Max: 9}))
	})

	DescribeTable("returns ok=false for names without encoded bounds",
		func(name string) {
			_, ok := managers.ParseBounds(name)
			Expect(ok).To(BeFalse())
		},
		Entry("legacy segment", "segment-1700000000000000000.sqlite.zst"),
		Entry("legacy flush", "bucket-0-1700000000000000000.sqlite.zst"),
		Entry("non-numeric bounds", "segment-aaa-bbb-ccc.sqlite.zst"),
		Entry("unrelated file", "catalog.sqlite"),
		Entry("empty", ""),
	)
})

var _ = Describe("Query filename pruning", func() {
	const base = int64(1_700_000_000_000_000_000)

	It("prunes a flush file whose filename bounds fall outside the window", func() {
		dir := GinkgoT().TempDir()

		buckets, err := loge.NewBuckets(context.Background(), 1, 1, dir, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(buckets.Append(loge.Payload{Streams: loge.Streams{loge.Entry{
			Stream: loge.Stream{"app": "svc"},
			Values: loge.Values{loge.Value{strconv.FormatInt(base, 10), "needle line"}},
		}}})).To(Succeed())

		Eventually(func() int {
			matches, _ := filepath.Glob(filepath.Join(dir, "bucket-*.sqlite.zst"))

			return len(matches)
		}, "5s").Should(Equal(1))
		Expect(buckets.Close()).To(Succeed())

		matches, _ := filepath.Glob(filepath.Join(dir, "bucket-*.sqlite.zst"))
		Expect(matches).To(HaveLen(1))

		// Rename so the NAME claims a far-future window while the CONTENT stays at
		// `base`. Only filename-based pruning can exclude it from a base-window query
		// (the metadata-table path would open the file and find the row in range).
		far := base + int64(time.Hour)
		lying := filepath.Join(dir, managers.FormatFlushName(0, far, far+10, far)+".zst")
		Expect(os.Rename(matches[0], lying)).To(Succeed())

		manager, err := managers.NewLocal(dir)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = manager.Close() })

		// Unbounded query: confirm the file is tracked and its content is queryable.
		Eventually(func() int {
			results, _ := manager.Query(context.Background(), managers.QueryRequest{})

			return len(results)
		}, "5s").Should(Equal(1))

		// Bounded around `base`: the lying far-future name prunes the file without
		// opening it, so its in-window content never appears.
		bounded, err := manager.Query(context.Background(), managers.QueryRequest{
			Start: base - 5,
			End:   base + 5,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(bounded).To(BeEmpty())
	})
})
