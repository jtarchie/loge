package managers_test

import (
	"context"
	"path/filepath"
	"strconv"

	"github.com/jtarchie/loge"
	"github.com/jtarchie/loge/managers"
	_ "github.com/jtarchie/sqlitezstd"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Query", func() {
	const base = int64(1_700_000_000_000_000_000)

	var (
		outputPath string
		manager    *managers.Local
	)

	entry := func(ts int64, app, line string) loge.Payload {
		return loge.Payload{
			Streams: loge.Streams{
				loge.Entry{
					Stream: loge.Stream{"app": app},
					Values: loge.Values{
						loge.Value{strconv.FormatInt(ts, 10), line},
					},
				},
			},
		}
	}

	BeforeEach(func() {
		var err error

		outputPath = GinkgoT().TempDir()

		// payload-size 1 flushes every payload to its own file, exercising the
		// cross-file merge and metadata pruning paths.
		buckets, err := loge.NewBuckets(context.Background(), 1, 1, outputPath, false)
		Expect(err).NotTo(HaveOccurred())

		buckets.Append(entry(base+0, "web", "GET /index 200"))
		buckets.Append(entry(base+1000, "db", "SELECT slow query"))
		buckets.Append(entry(base+2000, "web", "POST /login 500 error"))

		Eventually(func() int {
			matches, _ := filepath.Glob(filepath.Join(outputPath, "*.sqlite.zst"))

			return len(matches)
		}, "5s").Should(BeNumerically("==", 3))

		Expect(buckets.Close()).To(Succeed())

		manager, err = managers.NewLocal(outputPath)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			Expect(manager.Close()).To(Succeed())
		})
	})

	It("returns all entries newest-first with no filters", func() {
		results, err := manager.Query(context.Background(), managers.QueryRequest{})
		Expect(err).NotTo(HaveOccurred())
		Expect(results).To(HaveLen(3))
		Expect(results[0].Timestamp).To(Equal(base + 2000))
		Expect(results[2].Timestamp).To(Equal(base + 0))
	})

	It("filters by an exact label matcher", func() {
		results, err := manager.Query(context.Background(), managers.QueryRequest{
			Matchers: []managers.Matcher{{Name: "app", Value: "web", Type: "="}},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(results).To(HaveLen(2))
		for _, r := range results {
			Expect(r.Labels).To(HaveKeyWithValue("app", "web"))
		}
	})

	It("filters by a time window, pruning out-of-range files", func() {
		results, err := manager.Query(context.Background(), managers.QueryRequest{
			Start: base + 500,
			End:   base + 1500,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(results).To(HaveLen(1))
		Expect(results[0].Line).To(Equal("SELECT slow query"))
	})

	It("filters by a line substring", func() {
		results, err := manager.Query(context.Background(), managers.QueryRequest{
			Line: "error",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(results).To(HaveLen(1))
		Expect(results[0].Line).To(Equal("POST /login 500 error"))
	})

	It("filters by a regex label matcher", func() {
		results, err := manager.Query(context.Background(), managers.QueryRequest{
			Matchers: []managers.Matcher{{Name: "app", Value: "w.b", Type: "=~"}},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(results).To(HaveLen(2))
	})

	It("respects the limit", func() {
		results, err := manager.Query(context.Background(), managers.QueryRequest{
			Limit: 1,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(results).To(HaveLen(1))
		Expect(results[0].Timestamp).To(Equal(base + 2000))
	})

	It("filters by line over a compacted segment (LIKE scan)", func() {
		// Compact the three flush files into one segment, then query by line so the
		// LIKE scan path over a compacted segment is exercised (no FTS index).
		compactor := loge.NewCompactor(outputPath, 2, 128, 0)
		merged, err := compactor.Compact()
		Expect(err).NotTo(HaveOccurred())
		Expect(merged).To(Equal(3))

		// A fresh manager picks up the new segment and forgets the removed files.
		segmentManager, err := managers.NewLocal(outputPath)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			Expect(segmentManager.Close()).To(Succeed())
		})

		Eventually(func() ([]managers.QueryEntry, error) {
			return segmentManager.Query(context.Background(), managers.QueryRequest{Line: "slow"})
		}, "5s").Should(HaveLen(1))

		results, err := segmentManager.Query(context.Background(), managers.QueryRequest{Line: "slow"})
		Expect(err).NotTo(HaveOccurred())
		Expect(results[0].Line).To(Equal("SELECT slow query"))
	})
})
