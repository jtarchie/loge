package managers_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/jtarchie/loge/managers"
	_ "github.com/jtarchie/sqlitezstd"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("QuerySources (client-side cold scan)", func() {
	var (
		dir     string
		catalog *managers.Catalog
		server  *httptest.Server
	)

	BeforeEach(func() {
		dir = GinkgoT().TempDir()

		var err error
		catalog, err = managers.OpenCatalog(dir)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = catalog.Close() })

		// A plain file server stands in for a bucket of presigned segment URLs.
		server = httptest.NewServer(http.FileServer(http.Dir(dir)))
		DeferCleanup(server.Close)
	})

	// segmentURLs returns the HTTP URL each catalog segment is served from.
	segmentURLs := func() []string {
		segments, err := catalog.List()
		Expect(err).NotTo(HaveOccurred())

		urls := make([]string, 0, len(segments))
		for _, segment := range segments {
			urls = append(urls, server.URL+"/"+segment.ID)
		}

		return urls
	}

	It("scans an explicit set of segment URLs and merges newest-first", func() {
		produceSegmentLocal(dir, catalog, 1_700_000_000_000_000_000, 3, "web")
		produceSegmentLocal(dir, catalog, 1_800_000_000_000_000_000, 2, "db")

		manager, err := managers.NewLocal("", managers.WithoutWatcher())
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = manager.Close() })

		all, err := manager.QuerySources(context.Background(), managers.QueryRequest{}, segmentURLs())
		Expect(err).NotTo(HaveOccurred())
		Expect(all).To(HaveLen(5))

		for i := 1; i < len(all); i++ {
			Expect(all[i-1].Timestamp).To(BeNumerically(">=", all[i].Timestamp))
		}
	})

	It("applies a line filter (sequential LIKE) over the segments", func() {
		produceSegmentLocal(dir, catalog, 1_700_000_000_000_000_000, 5, "web")

		manager, err := managers.NewLocal("", managers.WithoutWatcher())
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = manager.Close() })

		hits, err := manager.QuerySources(context.Background(),
			managers.QueryRequest{Line: "web line 3"}, segmentURLs())
		Expect(err).NotTo(HaveOccurred())
		Expect(hits).To(HaveLen(1))
		Expect(hits[0].Line).To(Equal("web line 3"))
	})

	It("applies label matchers and respects the limit", func() {
		produceSegmentLocal(dir, catalog, 1_700_000_000_000_000_000, 4, "web")

		manager, err := managers.NewLocal("", managers.WithoutWatcher())
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = manager.Close() })

		hits, err := manager.QuerySources(context.Background(), managers.QueryRequest{
			Matchers: []managers.Matcher{{Name: "app", Value: "web", Type: "="}},
			Limit:    2,
		}, segmentURLs())
		Expect(err).NotTo(HaveOccurred())
		Expect(hits).To(HaveLen(2))
	})

	It("returns nothing for an empty source list", func() {
		manager, err := managers.NewLocal("", managers.WithoutWatcher())
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = manager.Close() })

		results, err := manager.QuerySources(context.Background(), managers.QueryRequest{}, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(results).To(BeEmpty())
	})
})

var _ = Describe("LocalOnly query (hot/cold split)", func() {
	It("scans only local segments when LocalOnly is set", func() {
		dir := GinkgoT().TempDir()

		catalog, err := managers.OpenCatalog(dir)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = catalog.Close() })

		server := httptest.NewServer(http.FileServer(http.Dir(dir)))
		DeferCleanup(server.Close)

		// One segment stays local ("web"); the later one ("db") is flipped to remote.
		produceSegmentLocal(dir, catalog, 1_700_000_000_000_000_000, 3, "web")
		produceSegmentLocal(dir, catalog, 1_800_000_000_000_000_000, 2, "db")

		segments, err := catalog.List()
		Expect(err).NotTo(HaveOccurred())

		remote := segments[0] // newest-first: the "db" segment
		Expect(catalog.MarkRemote(remote.ID, server.URL+"/"+remote.ID, time.Now().UnixNano())).To(Succeed())
		Expect(catalog.ClearLocalPath(remote.ID)).To(Succeed())

		manager, err := managers.NewLocal(dir, managers.WithCatalog(catalog))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = manager.Close() })

		// A full scan sees both tiers.
		all, err := manager.Query(context.Background(), managers.QueryRequest{})
		Expect(err).NotTo(HaveOccurred())
		Expect(all).To(HaveLen(5))

		// A hot-only scan skips the remote segment.
		hot, err := manager.Query(context.Background(), managers.QueryRequest{LocalOnly: true})
		Expect(err).NotTo(HaveOccurred())
		Expect(hot).To(HaveLen(3))

		for _, entry := range hot {
			Expect(entry.Labels).To(HaveKeyWithValue("app", "web"))
		}
	})
})
