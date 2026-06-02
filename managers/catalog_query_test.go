package managers_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"github.com/jtarchie/loge"
	"github.com/jtarchie/loge/managers"
	_ "github.com/jtarchie/sqlitezstd"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Query via catalog", func() {
	var (
		dir     string
		catalog *managers.Catalog
	)

	BeforeEach(func() {
		dir = GinkgoT().TempDir()

		var err error
		catalog, err = managers.OpenCatalog(dir)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = catalog.Close() })
	})

	// produceSegment ingests `count` lines starting at baseTs, then compacts
	// them (recording the segment in the catalog) and returns once the segment
	// exists and the flush files are gone.
	produceSegment := func(baseTs int64, count int, app string) {
		buckets, err := loge.NewBuckets(context.Background(), 1, 1, dir, false)
		Expect(err).NotTo(HaveOccurred())

		for i := range count {
			buckets.Append(loge.Payload{
				Streams: loge.Streams{
					loge.Entry{
						Stream: loge.Stream{"app": app},
						Values: loge.Values{loge.Value{strconv.FormatInt(baseTs+int64(i), 10), fmt.Sprintf("%s line %d", app, i)}},
					},
				},
			})
		}

		Eventually(func() int {
			matches, _ := filepath.Glob(filepath.Join(dir, "bucket-*.sqlite.zst"))

			return len(matches)
		}, "5s").Should(Equal(count))

		Expect(buckets.Close()).To(Succeed())

		merged, err := loge.NewCompactor(dir, 2, 128, time.Hour, loge.WithCompactorCatalog(catalog)).Compact()
		Expect(err).NotTo(HaveOccurred())
		Expect(merged).To(Equal(count))
	}

	It("prunes and merges segments by time window via the catalog", func() {
		early := int64(1_700_000_000_000_000_000)
		late := int64(1_800_000_000_000_000_000)

		produceSegment(early, 3, "web")
		produceSegment(late, 3, "db")

		// Two segments cataloged, no flush files left.
		segments, err := catalog.List()
		Expect(err).NotTo(HaveOccurred())
		Expect(segments).To(HaveLen(2))

		manager, err := managers.NewLocal(dir, managers.WithCatalog(catalog))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = manager.Close() })

		// A window over only the early segment returns just its rows.
		early0, err := manager.Query(context.Background(), managers.QueryRequest{Start: early, End: early + 2})
		Expect(err).NotTo(HaveOccurred())
		Expect(early0).To(HaveLen(3))
		for _, entry := range early0 {
			Expect(entry.Labels).To(HaveKeyWithValue("app", "web"))
		}

		// Unbounded returns both segments merged newest-first.
		all, err := manager.Query(context.Background(), managers.QueryRequest{})
		Expect(err).NotTo(HaveOccurred())
		Expect(all).To(HaveLen(6))
		Expect(all[0].Timestamp).To(Equal(late + 2))

		// Line filter through the segment trigram index.
		hits, err := manager.Query(context.Background(), managers.QueryRequest{Line: "db line 1"})
		Expect(err).NotTo(HaveOccurred())
		Expect(hits).To(HaveLen(1))
		Expect(hits[0].Line).To(Equal("db line 1"))

		// Labels come from the catalog (no file/S3 access).
		labels, err := manager.Labels()
		Expect(err).NotTo(HaveOccurred())
		Expect(labels).To(Equal([]string{"app"}))
	})
})
