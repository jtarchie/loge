package loge_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"github.com/jtarchie/loge"
	_ "github.com/jtarchie/sqlitezstd"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Catalog", func() {
	var dir string

	BeforeEach(func() {
		dir = GinkgoT().TempDir()
	})

	It("prunes segments by overlapping time window", func() {
		catalog, err := loge.OpenCatalog(dir)
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = catalog.Close() }()

		Expect(catalog.Upsert(loge.SegmentMeta{
			ID: "segment-1.sqlite.zst", Location: loge.LocationLocal, LocalPath: "/tmp/segment-1.sqlite.zst",
			MinTimestamp: 100, MaxTimestamp: 200, RowCount: 10, LabelKeys: []string{"app"}, SealedAt: 1,
		})).To(Succeed())
		Expect(catalog.Upsert(loge.SegmentMeta{
			ID: "segment-2.sqlite.zst", Location: loge.LocationLocal, LocalPath: "/tmp/segment-2.sqlite.zst",
			MinTimestamp: 1000, MaxTimestamp: 2000, RowCount: 20, LabelKeys: []string{"env"}, SealedAt: 2,
		})).To(Succeed())

		// Window overlaps only the first segment.
		overlapping, err := catalog.Overlapping(50, 300)
		Expect(err).NotTo(HaveOccurred())
		Expect(overlapping).To(HaveLen(1))
		Expect(overlapping[0].ID).To(Equal("segment-1.sqlite.zst"))

		// Unbounded returns both, newest (max_timestamp) first.
		all, err := catalog.Overlapping(0, 0)
		Expect(err).NotTo(HaveOccurred())
		Expect(all).To(HaveLen(2))
		Expect(all[0].ID).To(Equal("segment-2.sqlite.zst"))

		keys, err := catalog.LabelKeys()
		Expect(err).NotTo(HaveOccurred())
		Expect(keys).To(Equal([]string{"app", "env"}))
	})

	It("flips a segment to remote and lists rotation candidates", func() {
		catalog, err := loge.OpenCatalog(dir)
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = catalog.Close() }()

		Expect(catalog.Upsert(loge.SegmentMeta{
			ID: "segment-1.sqlite.zst", Location: loge.LocationLocal, LocalPath: "/tmp/segment-1.sqlite.zst",
			MinTimestamp: 100, MaxTimestamp: 200, RowCount: 10, SealedAt: 1,
		})).To(Succeed())

		candidates, err := catalog.LocalToRotate(2)
		Expect(err).NotTo(HaveOccurred())
		Expect(candidates).To(HaveLen(1))

		Expect(catalog.MarkRemote("segment-1.sqlite.zst", "https://host/segment-1.sqlite.zst", 5)).To(Succeed())

		overlapping, err := catalog.Overlapping(0, 0)
		Expect(err).NotTo(HaveOccurred())
		Expect(overlapping[0].Location).To(Equal(loge.LocationRemote))
		Expect(overlapping[0].RemoteURL).To(Equal("https://host/segment-1.sqlite.zst"))

		// No longer a local rotation candidate.
		candidates, err = catalog.LocalToRotate(10)
		Expect(err).NotTo(HaveOccurred())
		Expect(candidates).To(BeEmpty())
	})

	Describe("populated by compaction and Reconcile", func() {
		makePayload := func(app, line string, ts int64) loge.Payload {
			return loge.Payload{
				Streams: loge.Streams{
					loge.Entry{
						Stream: loge.Stream{"app": app},
						Values: loge.Values{loge.Value{strconv.FormatInt(ts, 10), line}},
					},
				},
			}
		}

		produceSegment := func(catalog *loge.Catalog) {
			buckets, err := loge.NewBuckets(context.Background(), 1, 1, dir, false)
			Expect(err).NotTo(HaveOccurred())

			base := int64(1_700_000_000_000_000_000)
			for i := range 5 {
				buckets.Append(makePayload("svc", fmt.Sprintf("line-%d", i), base+int64(i)))
			}

			Eventually(func() int {
				matches, _ := filepath.Glob(filepath.Join(dir, "bucket-*.sqlite.zst"))

				return len(matches)
			}, "5s").Should(Equal(5))

			Expect(buckets.Close()).To(Succeed())

			opts := []loge.CompactorOption{}
			if catalog != nil {
				opts = append(opts, loge.WithCompactorCatalog(catalog))
			}

			merged, err := loge.NewCompactor(dir, 2, 128, time.Hour, opts...).Compact()
			Expect(err).NotTo(HaveOccurred())
			Expect(merged).To(Equal(5))
		}

		It("records each compacted segment in the catalog", func() {
			catalog, err := loge.OpenCatalog(dir)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = catalog.Close() }()

			produceSegment(catalog)

			segments, err := catalog.List()
			Expect(err).NotTo(HaveOccurred())
			Expect(segments).To(HaveLen(1))
			Expect(segments[0].Location).To(Equal(loge.LocationLocal))
			Expect(segments[0].RowCount).To(Equal(int64(5)))
			Expect(segments[0].MinTimestamp).To(Equal(int64(1_700_000_000_000_000_000)))
			Expect(segments[0].LabelKeys).To(ContainElement("app"))
			Expect(segments[0].LocalPath).To(BeAnExistingFile())
		})

		It("rebuilds missing rows and drops vanished ones via Reconcile", func() {
			// Produce a segment WITHOUT a catalog, then reconcile a fresh one.
			produceSegment(nil)

			catalog, err := loge.OpenCatalog(dir)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = catalog.Close() }()

			// Stale row for a file that does not exist should be dropped.
			Expect(catalog.Upsert(loge.SegmentMeta{
				ID: "segment-gone.sqlite.zst", Location: loge.LocationLocal, LocalPath: "/tmp/gone.sqlite.zst",
				MinTimestamp: 1, MaxTimestamp: 2, RowCount: 1, SealedAt: 1,
			})).To(Succeed())

			Expect(catalog.Reconcile(dir)).To(Succeed())

			segments, err := catalog.List()
			Expect(err).NotTo(HaveOccurred())
			Expect(segments).To(HaveLen(1))
			Expect(segments[0].ID).To(HavePrefix("segment-"))
			Expect(segments[0].RowCount).To(Equal(int64(5)))
		})
	})
})
