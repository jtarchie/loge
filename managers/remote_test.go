package managers_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/jtarchie/loge"
	"github.com/jtarchie/loge/managers"
	_ "github.com/jtarchie/sqlitezstd"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Query over remote (HTTP) segments", func() {
	var (
		dir      string
		catalog  *managers.Catalog
		requests atomic.Int64
		server   *httptest.Server
	)

	BeforeEach(func() {
		dir = GinkgoT().TempDir()

		var err error
		catalog, err = managers.OpenCatalog(dir)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = catalog.Close() })

		requests.Store(0)
		// A counting file server stands in for a public S3 bucket.
		fileServer := http.FileServer(http.Dir(dir))
		server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)
			fileServer.ServeHTTP(w, r)
		}))
		DeferCleanup(server.Close)
	})

	// produceRemoteSegment ingests `count` lines at baseTs, compacts them into a
	// segment, then flips the catalog row to remote (served by the httptest
	// server) and clears the local path so the query path must read over HTTP.
	produceRemoteSegment := func(baseTs int64, count int, app string) string {
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

		_, err = loge.NewCompactor(dir, 2, 128, time.Hour, loge.WithCompactorCatalog(catalog)).Compact()
		Expect(err).NotTo(HaveOccurred())

		segments, err := catalog.List()
		Expect(err).NotTo(HaveOccurred())
		newest := segments[0]

		// The file physically stays in dir (the "bucket"); clearing the local
		// path forces the manager to read it over HTTP.
		url := server.URL + "/" + newest.ID
		Expect(catalog.MarkRemote(newest.ID, url, time.Now().UnixNano())).To(Succeed())
		Expect(catalog.ClearLocalPath(newest.ID)).To(Succeed())

		return newest.ID
	}

	It("reads a remote segment over HTTP and merges with a local one", func() {
		early := int64(1_700_000_000_000_000_000)
		late := int64(1_800_000_000_000_000_000)

		produceRemoteSegment(early, 3, "web") // remote
		// A second, still-local segment.
		produceSegmentLocal(dir, catalog, late, 3, "db")

		manager, err := managers.NewLocal(dir, managers.WithCatalog(catalog))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = manager.Close() })

		all, err := manager.Query(context.Background(), managers.QueryRequest{})
		Expect(err).NotTo(HaveOccurred())
		Expect(all).To(HaveLen(6))
		Expect(requests.Load()).To(BeNumerically(">", 0), "the remote segment must be fetched over HTTP")

		// A line filter still works over the remote trigram index.
		hits, err := manager.Query(context.Background(), managers.QueryRequest{Line: "web line 1"})
		Expect(err).NotTo(HaveOccurred())
		Expect(hits).To(HaveLen(1))
	})

	It("issues zero HTTP requests for a window that prunes the remote segment", func() {
		early := int64(1_700_000_000_000_000_000)
		produceRemoteSegment(early, 3, "web")

		manager, err := managers.NewLocal(dir, managers.WithCatalog(catalog))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = manager.Close() })

		requests.Store(0)

		// A far-future window does not overlap the segment's bounds.
		results, err := manager.Query(context.Background(), managers.QueryRequest{
			Start: int64(1_900_000_000_000_000_000),
			End:   int64(1_900_000_000_000_000_100),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(results).To(BeEmpty())
		Expect(requests.Load()).To(Equal(int64(0)), "pruned segment must not be fetched at all")
	})

	It("degrades to local data when the remote endpoint is unreachable", func() {
		early := int64(1_700_000_000_000_000_000)
		late := int64(1_800_000_000_000_000_000)

		id := produceRemoteSegment(early, 3, "web")
		// Point the remote at a dead URL.
		Expect(catalog.MarkRemote(id, "http://127.0.0.1:1/"+id, time.Now().UnixNano())).To(Succeed())

		produceSegmentLocal(dir, catalog, late, 3, "db")

		manager, err := managers.NewLocal(dir, managers.WithCatalog(catalog))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = manager.Close() })

		// Query still succeeds, returning the local segment's rows.
		results, err := manager.Query(context.Background(), managers.QueryRequest{})
		Expect(err).NotTo(HaveOccurred())
		Expect(results).To(HaveLen(3))
		for _, entry := range results {
			Expect(entry.Labels).To(HaveKeyWithValue("app", "db"))
		}
	})

	It("matches a multi-word keyword over a remote segment with a sequential LIKE", func() {
		early := int64(1_700_000_000_000_000_000)
		produceRemoteSegment(early, 5, "web") // lines "web line 0".."web line 4"

		manager, err := managers.NewLocal(dir, managers.WithCatalog(catalog))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = manager.Close() })

		// Remote segments use a sequential LIKE (no FTS) — the index is a
		// pessimization over HTTP. The keyword's trigrams are present, so the
		// catalog filter does not prune, and the LIKE finds the one exact line.
		hits, err := manager.Query(context.Background(), managers.QueryRequest{Line: "web line 3"})
		Expect(err).NotTo(HaveOccurred())
		Expect(hits).To(HaveLen(1))
		Expect(hits[0].Line).To(Equal("web line 3"))
		Expect(requests.Load()).To(BeNumerically(">", 0))

		// A phrase that does not occur returns nothing.
		none, err := manager.Query(context.Background(), managers.QueryRequest{Line: "web line 33"})
		Expect(err).NotTo(HaveOccurred())
		Expect(none).To(BeEmpty())
	})

	It("skips a remote segment whose trigram filter rejects the keyword (zero HTTP reads)", func() {
		early := int64(1_700_000_000_000_000_000)
		produceRemoteSegment(early, 5, "web") // lines "web line 0".."web line 4"

		manager, err := managers.NewLocal(dir, managers.WithCatalog(catalog))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = manager.Close() })

		// A keyword whose trigrams are absent from the segment is pruned by the
		// catalog filter — the segment is never fetched over HTTP.
		requests.Store(0)
		results, err := manager.Query(context.Background(), managers.QueryRequest{Line: "qwxzjvkbmp"})
		Expect(err).NotTo(HaveOccurred())
		Expect(results).To(BeEmpty())
		Expect(requests.Load()).To(Equal(int64(0)), "filter must skip the segment with zero HTTP reads")

		// Sanity: a present keyword still fetches the segment and returns its row.
		requests.Store(0)
		hits, err := manager.Query(context.Background(), managers.QueryRequest{Line: "web line 3"})
		Expect(err).NotTo(HaveOccurred())
		Expect(hits).To(HaveLen(1))
		Expect(requests.Load()).To(BeNumerically(">", 0))
	})

	It("skips a remote segment whose label filter rejects an equality matcher (zero HTTP reads)", func() {
		early := int64(1_700_000_000_000_000_000)
		produceRemoteSegment(early, 5, "web") // every stream has app="web"

		manager, err := managers.NewLocal(dir, managers.WithCatalog(catalog))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = manager.Close() })

		// An equality matcher for a label value the segment never holds is pruned
		// by the catalog label filter — the segment is never fetched over HTTP.
		requests.Store(0)
		results, err := manager.Query(context.Background(), managers.QueryRequest{
			Matchers: []managers.Matcher{{Name: "app", Value: "db", Type: "="}},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(results).To(BeEmpty())
		Expect(requests.Load()).To(Equal(int64(0)), "label filter must skip the segment with zero HTTP reads")

		// Sanity: a matcher for the present value still fetches and returns rows.
		requests.Store(0)
		hits, err := manager.Query(context.Background(), managers.QueryRequest{
			Matchers: []managers.Matcher{{Name: "app", Value: "web", Type: "="}},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(hits).To(HaveLen(5))
		Expect(requests.Load()).To(BeNumerically(">", 0))
	})
})

// produceSegmentLocal ingests and compacts a segment that stays local.
func produceSegmentLocal(dir string, catalog *managers.Catalog, baseTs int64, count int, app string) {
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

	_, err = loge.NewCompactor(dir, 2, 128, time.Hour, loge.WithCompactorCatalog(catalog)).Compact()
	Expect(err).NotTo(HaveOccurred())
}
